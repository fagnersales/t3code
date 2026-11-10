// @effect-diagnostics nodeBuiltinImport:off - Row snapshots hash every column of every table.
import * as NodeCrypto from "node:crypto";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  ContextTransferId,
  EventId,
  ProviderDriverKind,
  ProviderThreadId,
  ServerSettingsError,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { materializeReplayTranscriptWorkspace } from "@t3tools/provider-testing/replayTranscript";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Random from "effect/Random";
import * as SqlClient from "effect/sql/SqlClient";
import { TestClock } from "effect/testing";

import * as ServerConfig from "../config.ts";
import { ClaudeOrchestratorReplayHarness } from "../orchestration-v2/Adapters/ClaudeAdapterV2.testkit.ts";
import { CodexOrchestratorReplayHarness } from "../orchestration-v2/Adapters/CodexAdapterV2.testkit.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { materializeFixtureInput } from "../orchestration-v2/testkit/fixtures/shared.ts";
import {
  runOrchestratorV2ProviderReplayScenario,
  type OrchestratorV2ProviderReplayHarness,
} from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import {
  findReplayFixture,
  readReplayFixtureTranscript,
} from "../orchestration-v2/testkit/ProviderReplayFixtures.ts";
import { materializeReplayTranscriptRuntimeInstructions } from "../orchestration-v2/testkit/ReplayRuntimeInstructions.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import { threadKey } from "./bundle.ts";
import * as ThreadOffloadHttp from "./http.ts";
import * as ThreadOffload from "./ThreadOffload.ts";

/** About the size of a screenshot a review subagent reads, as base64 text. */
const SCREENSHOT = NodeCrypto.createHash("sha256")
  .update("screenshot")
  .digest("base64")
  .repeat(48_000);

/** Tool results become as large as the screenshots review subagents read inline. */
const inflateToolResults = (transcript: ProviderReplayTranscript): ProviderReplayTranscript => {
  const visit = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(visit);
    if (typeof value !== "object" || value === null) return value;
    const entries = Object.entries(value).map(([key, inner]) => [key, visit(inner)] as const);
    const object = Object.fromEntries(entries);
    if (object.type === "tool_result" && typeof object.content === "string") {
      object.content = `${object.content}\n${SCREENSHOT}`;
    }
    return object;
  };
  return visit(transcript) as ProviderReplayTranscript;
};

const seedFixture = Effect.fn("seedFixture")(function* <
  T extends ProviderReplayTranscript,
  E,
>(input: {
  readonly dbPath: string;
  readonly fixtureName: string;
  readonly driver: string;
  readonly harness: OrchestratorV2ProviderReplayHarness<T, E>;
  readonly randomSeed: number;
  readonly inflate?: boolean;
}) {
  const fixture = findReplayFixture(input.fixtureName)!;
  const provider = fixture.providers.find((candidate) => candidate.driver === input.driver)!;
  const recorded = yield* readReplayFixtureTranscript(provider.transcriptFile);
  const replay = materializeReplayTranscriptRuntimeInstructions(
    input.inflate === true ? inflateToolResults(recorded) : recorded,
    { driver: provider.driver, model: provider.modelSelection.model },
  );
  const fixtureInput = fixture.buildInput();
  const workspace = yield* checkpointWorkspace(input.fixtureName, fixtureInput.workspaceFiles);
  const transcript = yield* input.harness.decodeTranscript(
    materializeReplayTranscriptWorkspace(replay, workspace),
  );
  const deterministic = <A, E2, R>(effect: Effect.Effect<A, E2, R>) =>
    effect.pipe(Effect.provide(TestClock.layer()), Random.withSeed(input.randomSeed));
  const materialized = yield* materializeFixtureInput({
    scenario: input.fixtureName,
    fixtureInput,
    driver: provider.driver,
    modelSelection: provider.modelSelection,
  }).pipe(Effect.provide(IdAllocator.layer), deterministic);
  const result = yield* runOrchestratorV2ProviderReplayScenario(
    {
      name: `${input.fixtureName}/${input.driver}`,
      transcript,
      commands: materialized.commands,
      steps: materialized.steps,
      projectionThreadIds: materialized.projectionThreadIds,
      runtimePolicyOverride: { ...provider.runtimePolicyOverride, cwd: workspace },
    },
    input.harness,
    {
      databaseLayer: SqlitePersistence.layerFromPath(input.dbPath).pipe(
        Layer.provide(NodeServices.layer),
      ),
    },
  ).pipe(deterministic);
  return [...result.projections.values()].map((projection) => projection.thread);
}, Effect.scoped);

/** Every row of every table (and the search index), with each value's SQLite type. */
const snapshot = Effect.fn("snapshot")(function* (options: { readonly withRowids: boolean }) {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql<{ readonly name: string; readonly rowless: number }>`
    SELECT name, sql LIKE '%WITHOUT ROWID%' AS rowless FROM sqlite_master
    WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
      AND name NOT LIKE 'orchestration_v2_message_search_%'
    ORDER BY name
  `;
  const result: Record<string, Array<string>> = {};
  for (const { name, rowless } of tables) {
    const columns =
      name === "orchestration_v2_message_search"
        ? ["text"]
        : (yield* sql<{ readonly name: string }>`SELECT name FROM pragma_table_info(${name})`).map(
            (column) => column.name,
          );
    const select = columns.map((column) => `typeof("${column}"), quote("${column}")`).join(", ");
    const rows = yield* sql.unsafe<Record<string, unknown>>(
      `SELECT ${options.withRowids && rowless === 0 ? "rowid, " : ""}${select} FROM "${name}"`,
    );
    result[name] = rows
      .map((row) =>
        NodeCrypto.createHash("sha256")
          .update(Object.values(row).map(String).join("\u0000"))
          .digest("hex"),
      )
      .toSorted();
  }
  return result;
});

/** Tables whose rows differ, so a failure names them instead of printing hashes. */
const differingTables = (
  left: Record<string, Array<string>>,
  right: Record<string, Array<string>>,
) =>
  [...new Set([...Object.keys(left), ...Object.keys(right)])].filter(
    (name) => (left[name] ?? []).join() !== (right[name] ?? []).join(),
  );

const without = (tables: Record<string, Array<string>>, names: ReadonlyArray<string>) =>
  Object.fromEntries(Object.entries(tables).filter(([name]) => !names.includes(name)));

/**
 * Rows one thread's events write for another: the parent records the child's result transfer and,
 * as some providers do, the child's provider thread; the child records its spawn transfer.
 */
const writeCrossThreadEvents = Effect.fn("writeCrossThreadEvents")(function* (
  parent: OrchestrationV2AppThread,
  child: OrchestrationV2AppThread,
) {
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const providerInstanceId = parent.providerInstanceId;
  const transfer = (
    name: string,
    type: "subagent_spawn" | "subagent_result",
    source: OrchestrationV2AppThread,
    target: OrchestrationV2AppThread,
  ): OrchestrationV2DomainEvent => ({
    id: EventId.make(`event:offload:${name}`),
    type: "context-transfer.created",
    threadId: target.id,
    providerInstanceId,
    occurredAt: now,
    payload: {
      id: ContextTransferId.make(`transfer:offload:${name}`),
      type,
      sourceThreadId: source.id,
      targetThreadId: target.id,
      sourcePoint: { threadId: source.id },
      basePoint: null,
      sourceProviderInstanceId: providerInstanceId,
      targetProviderInstanceId: providerInstanceId,
      targetRunId: null,
      status: "consumed",
      resolution: null,
      createdBy: "system",
      error: null,
      createdAt: now,
      updatedAt: now,
      consumedAt: now,
    },
  });
  yield* sink.write({
    events: [
      transfer("spawn", "subagent_spawn", parent, child),
      transfer("result", "subagent_result", child, parent),
      {
        id: EventId.make("event:offload:child-provider-thread"),
        type: "provider-thread.updated",
        threadId: parent.id,
        driver: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId,
        occurredAt: now,
        payload: {
          id: ProviderThreadId.make("provider-thread:offload:child"),
          driver: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId,
          providerSessionId: null,
          appThreadId: child.id,
          ownerNodeId: null,
          nativeThreadRef: null,
          nativeConversationHeadRef: null,
          status: "idle",
          firstRunOrdinal: 1,
          lastRunOrdinal: 1,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        },
      },
    ],
  });
});

/** The projections equal what T3 rebuilds from the events left, on a copy of the file. */
const assertProjectionsMatchRebuild = Effect.fn("assertProjectionsMatchRebuild")(function* (
  copyPath: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const offloaded = yield* snapshot({ withRowids: false });
  yield* sql.unsafe(`VACUUM INTO '${copyPath}'`);
  // Its own memo map, or the shared store layers would rebuild the original file.
  const rebuilt = yield* Effect.gen(function* () {
    const context = yield* Layer.buildWithMemoMap(
      ProjectionMaintenance.layer.pipe(
        Layer.provide(Layer.merge(EventStore.layer, ProjectionStore.layer)),
        Layer.provideMerge(NodeSqliteClient.layer({ filename: copyPath })),
      ),
      yield* Layer.makeMemoMap,
      yield* Effect.scope,
    );
    return yield* Effect.gen(function* () {
      const copy = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      assert.isTrue((yield* copy.rebuild).valid);
      return yield* snapshot({ withRowids: false });
    }).pipe(Effect.provide(context));
  }).pipe(Effect.scoped);
  assert.deepEqual(
    differingTables(
      without(rebuilt, ["orchestration_v2_projection_metadata"]),
      without(offloaded, ["orchestration_v2_projection_metadata"]),
    ),
    [],
  );
});

const layerFor = (dbPath: string) =>
  ThreadOffload.layer.pipe(
    Layer.provideMerge(ThreadCommandExecutor.layer),
    Layer.provideMerge(
      ProjectionMaintenance.layer.pipe(
        Layer.provide(Layer.merge(EventStore.layer, ProjectionStore.layer)),
        Layer.provideMerge(ProjectionStore.layer),
      ),
    ),
    Layer.provideMerge(NodeSqliteClient.layer({ filename: dbPath })),
  );

const EVERYTHING_IDLE = "9999-01-01T00:00:00.000Z";

describe("ThreadOffload", () => {
  it.effect("selects units whose every thread is idle, keeping readers with what they read", () =>
    Effect.sync(() => {
      const thread = (id: string, parent: string | null, reasons: Array<string> = []) => ({
        id: ThreadId.make(id),
        title: id,
        parentThreadId: parent === null ? null : ThreadId.make(parent),
        lastActivityAt: "2026-01-01T00:00:00.000Z",
        lastSequence: 1,
        reasons,
      });
      const threads = [
        thread("parent", null),
        thread("child", "parent"),
        thread("grandchild", "child"),
        thread("busy-parent", null, ["running"]),
        thread("old-child", "busy-parent"),
        thread("source", null),
        thread("fork", "source", ["pinned"]),
      ];
      const candidates = {
        threads: new Map(threads.map((value) => [value.id, value])),
        dependents: new Map([
          ["parent", [ThreadId.make("child")]],
          ["child", [ThreadId.make("grandchild")]],
          ["busy-parent", [ThreadId.make("old-child")]],
          ["source", [ThreadId.make("fork")]],
        ]),
        forkSources: new Map([["fork", ThreadId.make("source")]]),
      };
      const units = ThreadOffload.selectUnits(candidates, "2026-06-01T00:00:00.000Z");
      assert.deepEqual(
        units.map((unit) => [unit.rootThreadId, unit.threadIds, unit.remainingParentThreadId]),
        [
          ["parent", ["parent", "child", "grandchild"], null],
          ["old-child", ["old-child"], "busy-parent"],
        ],
      );
    }),
  );

  it.effect(
    "offloads a subagent child alone and whole units, verifies, and restores every row exactly",
    () =>
      Effect.gen(function* () {
        const config = yield* ServerConfig.ServerConfig;
        const claudeThreads = yield* seedFixture({
          dbPath: config.dbPath,
          fixtureName: "subagent",
          driver: "claudeAgent",
          harness: ClaudeOrchestratorReplayHarness,
          randomSeed: 1,
          inflate: true,
        });
        const parent = claudeThreads.find((thread) => thread.lineage.parentThreadId === null)!;
        const child = claudeThreads.find((thread) => thread.lineage.parentThreadId === parent.id)!;
        yield* writeCrossThreadEvents(parent, child).pipe(
          Effect.provide(
            EventSink.layer.pipe(
              Layer.provide(Layer.merge(EventStore.layer, ProjectionStore.layer)),
              Layer.provide(NodeSqliteClient.layer({ filename: config.dbPath })),
            ),
          ),
        );
        const codexThreads = yield* seedFixture({
          dbPath: config.dbPath,
          fixtureName: "subagent_v2_nested",
          driver: "codex",
          harness: CodexOrchestratorReplayHarness,
          randomSeed: 2,
        });
        // Seeded last, so it holds the latest event the projection records.
        const [latest] = yield* seedFixture({
          dbPath: config.dbPath,
          fixtureName: "simple",
          driver: "claudeAgent",
          harness: ClaudeOrchestratorReplayHarness,
          randomSeed: 3,
        });
        const codexRoot = codexThreads.find((thread) => thread.lineage.parentThreadId === null)!;
        assert.isAbove(codexThreads.length, 2);

        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const offload = yield* ThreadOffload.ThreadOffload;
          const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const fs = yield* FileSystem.FileSystem;
          const keepThreadIds = new Set<string>();
          const original = yield* snapshot({ withRowids: true });
          const dynamicToolBytes = yield* sql<{ readonly bytes: number }>`
            SELECT SUM(length(payload_json)) AS bytes FROM orchestration_v2_projection_turn_items
            WHERE type = 'dynamic_tool' AND thread_id = ${child.id}
          `;
          assert.isAbove(dynamicToolBytes[0]!.bytes, 1_000_000);

          const plan = yield* offload.plan({
            before: EVERYTHING_IDLE,
            withSizes: true,
            keepThreadIds,
          });
          assert.deepEqual(
            plan.units.map((unit) => unit.rootThreadId).toSorted(),
            [parent.id, codexRoot.id].toSorted(),
          );
          assert.isTrue(plan.kept.some((kept) => kept.reason === "latest event"));
          const busy = yield* offload
            .offload({ threadId: latest!.id, before: EVERYTHING_IDLE, keepThreadIds })
            .pipe(Effect.flip);
          assert.strictEqual(busy.reason, "busy");

          // A child alone: its parent keeps exactly what its own events project.
          const alone = yield* offload.offload({
            threadId: child.id,
            before: EVERYTHING_IDLE,
            keepThreadIds,
          });
          assert.deepEqual(alone.threadIds, [child.id]);
          assert.isBelow(alone.bundleBytes, alone.rowBytes / 4);
          assert.isAbove(alone.storage.reclaimableBytes, 0);
          assert.isTrue((yield* maintenance.verify).valid);
          assert.isNull(yield* projections.getThreadShell(child.id));
          const parentProjection = yield* projections.getThreadProjection(parent.id);
          assert.isTrue(parentProjection.subagents.some((task) => task.childThreadId === child.id));
          yield* projections.getShellSnapshot();
          const kept = yield* sql<{ readonly id: string }>`
            SELECT context_transfer_id AS id FROM orchestration_v2_projection_context_transfers
            UNION ALL SELECT provider_thread_id FROM orchestration_v2_projection_provider_threads
              WHERE thread_id = ${child.id}
          `;
          assert.deepEqual(kept.map((row) => row.id).toSorted(), [
            "provider-thread:offload:child",
            "transfer:offload:result",
          ]);
          yield* assertProjectionsMatchRebuild(`${config.dbPath}.rebuild-1`);

          const restored = yield* offload.restore({ threadId: child.id });
          assert.deepEqual(restored.threadIds, [child.id]);
          assert.strictEqual(restored.rowCount, alone.rowCount);
          assert.deepEqual(differingTables(yield* snapshot({ withRowids: true }), original), []);
          // The bundle is kept, out of the way.
          assert.isFalse(yield* fs.exists(alone.bundlePath));
          assert.isTrue(yield* fs.exists(restored.bundlePath));
          assert.strictEqual(
            restored.bundlePath,
            `${offload.offloadDirectory}/restored/${alone.bundlePath.split("/").at(-1)}`,
          );

          // An app-owned child with a grandchild: the unit takes the grandchild too.
          const codexChild = codexThreads.find(
            (thread) => thread.lineage.parentThreadId === codexRoot.id,
          )!;
          const branch = yield* offload.offload({
            threadId: codexChild.id,
            before: EVERYTHING_IDLE,
            keepThreadIds,
          });
          assert.isAbove(branch.threadIds.length, 1);
          assert.isTrue((yield* maintenance.verify).valid);
          yield* projections.getThreadProjection(codexRoot.id);
          yield* assertProjectionsMatchRebuild(`${config.dbPath}.rebuild-2`);
          yield* offload.restore({ threadId: codexChild.id });
          assert.deepEqual(differingTables(yield* snapshot({ withRowids: true }), original), []);

          // Whole units, then back in the other order.
          const units = [];
          for (const unit of plan.units) {
            units.push(
              yield* offload.offload({
                threadId: unit.rootThreadId,
                before: EVERYTHING_IDLE,
                keepThreadIds,
              }),
            );
          }
          // The plan's one pass per table counts each unit as its export does.
          assert.deepEqual(
            units.map((unit) => unit.rowBytes),
            plan.units.map((unit) => unit.bytes),
          );
          assert.isTrue((yield* maintenance.verify).valid);
          const remaining = yield* sql<{ readonly thread_id: string }>`
            SELECT thread_id FROM orchestration_v2_projection_threads
          `;
          assert.deepEqual(
            remaining.map((row) => row.thread_id),
            [latest!.id],
          );
          const missing = yield* offload
            .offload({ threadId: child.id, before: EVERYTHING_IDLE, keepThreadIds })
            .pipe(Effect.flip);
          assert.strictEqual(missing.detail, `${child.id} is already offloaded.`);
          // Any thread of a unit names its bundle.
          yield* offload.restore({ threadId: child.id });
          yield* offload.restore({ threadId: codexThreads.at(-1)!.id });
          assert.deepEqual(differingTables(yield* snapshot({ withRowids: true }), original), []);
          assert.isTrue((yield* maintenance.verify).valid);
          assert.isAbove(units.length, 1);
        }).pipe(Effect.provide(layerFor(config.dbPath)));
      }).pipe(
        Effect.provide(
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), { prefix: "thread-offload-" }),
            NodeServices.layer,
          ),
        ),
      ),
  );

  it.effect("refuses whatever could lose a thread or misplace its rows, changing nothing", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const threads = yield* seedFixture({
        dbPath: config.dbPath,
        fixtureName: "subagent",
        driver: "claudeAgent",
        harness: ClaudeOrchestratorReplayHarness,
        randomSeed: 1,
      });
      const parent = threads.find((thread) => thread.lineage.parentThreadId === null)!;
      const child = threads.find((thread) => thread.lineage.parentThreadId === parent.id)!;
      yield* writeCrossThreadEvents(parent, child).pipe(
        Effect.provide(
          EventSink.layer.pipe(
            Layer.provide(Layer.merge(EventStore.layer, ProjectionStore.layer)),
            Layer.provide(NodeSqliteClient.layer({ filename: config.dbPath })),
          ),
        ),
      );
      yield* seedFixture({
        dbPath: config.dbPath,
        fixtureName: "simple",
        driver: "claudeAgent",
        harness: ClaudeOrchestratorReplayHarness,
        randomSeed: 3,
      });

      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const offload = yield* ThreadOffload.ThreadOffload;
        const fs = yield* FileSystem.FileSystem;
        const keepThreadIds = new Set<string>();
        const offloadChild = offload.offload({
          threadId: child.id,
          before: EVERYTHING_IDLE,
          keepThreadIds,
        });
        const original = yield* snapshot({ withRowids: true });
        const assertUnchanged = Effect.gen(function* () {
          assert.deepEqual(differingTables(yield* snapshot({ withRowids: true }), original), []);
        });
        const bundles = fs
          .readDirectory(`${offload.offloadDirectory}/bundles`)
          .pipe(Effect.orElseSucceed(() => []));

        // Unreadable settings: thread groups are unknown, so nothing may go.
        const settingsFailure = yield* ThreadOffloadHttp.groupedThreadIds.pipe(
          Effect.provide(
            Layer.mock(ServerSettings.ServerSettingsService)({
              getSettings: Effect.fail(
                new ServerSettingsError({ settingsPath: "settings.json", operation: "read-file" }),
              ),
            }),
          ),
          Effect.flip,
        );
        assert.strictEqual(settingsFailure.reason, "failed");

        // A pending transfer keeps both threads: the child as its target (spawn), then as its
        // source (result, which the parent's next message would need).
        for (const transferId of ["transfer:offload:spawn", "transfer:offload:result"]) {
          yield* sql`
            UPDATE orchestration_v2_projection_context_transfers
            SET status = 'pending' WHERE context_transfer_id = ${transferId}
          `;
          const pending = yield* offloadChild.pipe(Effect.flip);
          assert.strictEqual(pending.reason, "busy");
          assert.include(pending.detail, "context transfer pending");
          yield* sql`
            UPDATE orchestration_v2_projection_context_transfers
            SET status = 'consumed' WHERE context_transfer_id = ${transferId}
          `;
        }
        yield* assertUnchanged;

        // An imported session's runtime row is how the importer knows it is imported.
        yield* sql`
          INSERT INTO provider_session_runtime
            (thread_id, provider_name, adapter_key, status, last_seen_at, runtime_payload_json)
          VALUES (${child.id}, 'claudeAgent', 'claudeAgent', 'stopped', '2026-01-01T00:00:00.000Z',
            '{"importedTranscripts":[]}')
        `;
        const imported = yield* offloadChild.pipe(Effect.flip);
        assert.include(imported.detail, "imported session");
        yield* sql`DELETE FROM provider_session_runtime WHERE thread_id = ${child.id}`;

        // A table with thread rows that offloading does not know.
        yield* sql`CREATE TABLE offload_probe (owner_thread_id TEXT)`;
        const unknownTable = yield* offloadChild.pipe(Effect.flip);
        assert.strictEqual(unknownTable.reason, "failed");
        assert.include(unknownTable.detail, "offload_probe");
        yield* sql`DROP TABLE offload_probe`;
        yield* assertUnchanged;

        // A write between the export and the delete that keeps every row count.
        const before = yield* sql<{ readonly updated_at: string }>`
          SELECT updated_at FROM orchestration_v2_projection_threads WHERE thread_id = ${child.id}
        `;
        const changed = yield* offloadChild.pipe(
          Effect.provideService(ThreadOffload.BeforeDelete, () =>
            sql`
              UPDATE orchestration_v2_projection_threads
              SET updated_at = '2000-01-01T00:00:00.000Z' WHERE thread_id = ${child.id}
            `.pipe(Effect.asVoid, Effect.orDie),
          ),
          Effect.flip,
        );
        assert.strictEqual(changed.reason, "conflict");
        // The thread stays, with no record saying otherwise and no leftover bundle.
        assert.isTrue(
          Option.isNone(yield* ThreadOffload.readRecord(offload.offloadDirectory, child.id)),
        );
        assert.deepEqual(yield* bundles, []);
        yield* sql`
          UPDATE orchestration_v2_projection_threads
          SET updated_at = ${before[0]!.updated_at} WHERE thread_id = ${child.id}
        `;
        yield* assertUnchanged;

        const alone = yield* offloadChild;
        // Any event will do as the shape of the recreated thread's first one.
        const template = (yield* sql.unsafe<Record<string, unknown>>(
          `SELECT * FROM orchestration_events WHERE sequence = (
            SELECT MAX(sequence) FROM orchestration_events)`,
        ))[0]!;
        const restoreChild = offload.restore({ threadId: child.id });
        const assertKept = Effect.gen(function* () {
          assert.isTrue(yield* fs.exists(alone.bundlePath));
          assert.isTrue(
            Option.isSome(yield* ThreadOffload.readRecord(offload.offloadDirectory, child.id)),
          );
        });

        // The thread was recreated under its id (as an importer would): its history differs.
        const columns = Object.keys(template).filter((column) => column !== "sequence");
        yield* sql.unsafe(
          `INSERT INTO orchestration_events (${columns.join(", ")})
           VALUES (${columns.map(() => "?").join(", ")})`,
          columns.map((column) =>
            column === "event_id"
              ? "event:offload:recreated"
              : column === "stream_id"
                ? child.id
                : template[column],
          ),
        );
        const recreated = yield* restoreChild.pipe(Effect.flip);
        assert.strictEqual(recreated.reason, "conflict");
        assert.include(recreated.detail, "exists again with 1 event(s)");
        yield* assertKept;
        yield* sql`DELETE FROM orchestration_events WHERE event_id = 'event:offload:recreated'`;

        // Rows of the thread without its events.
        yield* sql`
          INSERT INTO provider_session_runtime
            (thread_id, provider_name, adapter_key, status, last_seen_at)
          VALUES (${child.id}, 'claudeAgent', 'claudeAgent', 'stopped', '2026-01-01T00:00:00.000Z')
        `;
        const leftovers = yield* restoreChild.pipe(Effect.flip);
        assert.strictEqual(leftovers.reason, "conflict");
        assert.include(leftovers.detail, "provider_session_runtime");
        yield* assertKept;
        yield* sql`DELETE FROM provider_session_runtime WHERE thread_id = ${child.id}`;

        // A schema other than the one the bundle was written from.
        yield* sql`
          INSERT INTO effect_sql_migrations (migration_id, name, created_at)
          VALUES (100000, 'OffloadProbe', '2026-01-01 00:00:00')
        `;
        const newer = yield* restoreChild.pipe(Effect.flip);
        assert.strictEqual(newer.reason, "conflict");
        assert.include(newer.detail, "this one is at migration 100000");
        yield* assertKept;
        yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id = 100000`;

        // Its record is lost: the bundle alone brings it back.
        yield* fs.remove(`${offload.offloadDirectory}/records/${threadKey(child.id)}.json`);
        assert.strictEqual((yield* restoreChild.pipe(Effect.flip)).reason, "not-found");
        const restored = yield* offload.restore({ bundlePath: alone.bundlePath });
        assert.deepEqual(restored.threadIds, [child.id]);
        yield* assertUnchanged;

        // Restoring the kept bundle again finds the thread as it is and changes nothing.
        const again = yield* offload.restore({ bundlePath: restored.bundlePath });
        assert.deepEqual(again.threadIds, []);
        assert.strictEqual(again.bundlePath, restored.bundlePath);
        yield* assertUnchanged;
      }).pipe(Effect.provide(layerFor(config.dbPath)));
    }).pipe(
      Effect.provide(
        Layer.provideMerge(
          ServerConfig.layerTest(process.cwd(), { prefix: "thread-offload-safety-" }),
          NodeServices.layer,
        ),
      ),
    ),
  );
});
