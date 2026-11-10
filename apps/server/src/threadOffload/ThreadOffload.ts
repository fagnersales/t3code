import {
  ThreadId,
  ThreadOffloadRecord,
  type ThreadOffloadPlan,
  type ThreadOffloadResult,
  type ThreadOffloadStorage,
  type ThreadOffloadUnit,
  type ThreadRestoreResult,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import { ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION } from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import {
  BUNDLE_HEADER,
  BUNDLE_TRAILER,
  type BundleReader,
  fileSize,
  moveFileDurably,
  openBundleReader,
  openBundleWriter,
  threadKey,
  writeFileAtomically,
} from "./bundle.ts";

export class ThreadOffloadError extends Schema.TaggedError<ThreadOffloadError>()(
  "ThreadOffloadError",
  {
    reason: Schema.Literals(["not-found", "busy", "conflict", "failed"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.detail;
  }
}

const isThreadOffloadError = Schema.is(ThreadOffloadError);

const fail = (reason: ThreadOffloadError["reason"], detail: string, cause?: unknown) =>
  new ThreadOffloadError({ reason, detail, ...(cause === undefined ? {} : { cause }) });

/**
 * Every table that holds a thread's rows, in the order a restore inserts them: the thread row
 * comes first because the message search trigger reads it. `where` selects one thread's rows
 * with a single `?`. Each must exist with its column, or nothing is offloaded.
 */
const THREAD_TABLES: ReadonlyArray<{
  readonly table: string;
  readonly where: string;
  readonly column: string;
}> = [
  ...[
    "orchestration_v2_projection_threads",
    "orchestration_v2_projection_runs",
    "orchestration_v2_projection_run_attempts",
    "orchestration_v2_projection_nodes",
    "orchestration_v2_projection_subagents",
    "orchestration_v2_projection_provider_sessions",
    "orchestration_v2_projection_provider_session_bindings",
    "orchestration_v2_projection_provider_turns",
    "orchestration_v2_projection_runtime_requests",
    "orchestration_v2_projection_messages",
    "orchestration_v2_projection_plans",
    "orchestration_v2_projection_turn_items",
    "orchestration_v2_projection_checkpoint_scopes",
    "orchestration_v2_projection_checkpoints",
    "orchestration_v2_projection_context_handoffs",
  ].map((table) => ({ table, where: "thread_id = ?", column: "thread_id" })),
  // A transfer names two threads and a provider thread names its app thread, which another
  // thread's events may write; each row goes with the thread whose events project it.
  {
    table: "orchestration_v2_projection_provider_threads",
    where: `provider_thread_id IN (
      SELECT json_extract(payload_json, '$.id') FROM orchestration_events
      WHERE aggregate_kind = 'thread' AND stream_id = ?
        AND event_type = 'provider-thread.updated')`,
    column: "provider_thread_id",
  },
  {
    table: "orchestration_v2_projection_context_transfers",
    where: `context_transfer_id IN (
      SELECT json_extract(payload_json, '$.id') FROM orchestration_events
      WHERE aggregate_kind = 'thread' AND stream_id = ?
        AND event_type IN ('context-transfer.created', 'context-transfer.updated'))`,
    column: "context_transfer_id",
  },
  { table: "orchestration_v2_turn_item_positions", where: "thread_id = ?", column: "thread_id" },
  {
    table: "orchestration_events",
    where: "aggregate_kind = 'thread' AND stream_id = ?",
    column: "stream_id",
  },
  {
    table: "orchestration_command_receipts",
    where: "aggregate_kind = 'thread' AND aggregate_id = ?",
    column: "aggregate_id",
  },
  // The legacy importer recreates a thread whose v1 rows outlive its v2 events.
  ...[
    "orchestration_v2_events",
    "orchestration_v2_command_receipts",
    "orchestration_v2_effect_outbox",
    "orchestration_v2_legacy_imports",
    "orchestration_v2_thread_launch_workflows",
    "mcp_app_model_context",
    "provider_session_runtime",
    "projection_threads",
    "projection_thread_messages",
    "projection_thread_sessions",
    "projection_thread_pull_requests",
  ].map((table) => ({ table, where: "thread_id = ?", column: "thread_id" })),
];

/**
 * Tables with a thread column that offloading leaves alone on purpose. Any other table with one
 * stops offloading until it is added here or to `THREAD_TABLES`.
 */
const IGNORED_THREAD_TABLES: Readonly<Record<string, string>> = {
  scheduled_tasks: "a thread a scheduled task targets is kept, so no offloaded thread has a row",
};

/** Columns that name a thread, by convention: a table with one may hold a thread's rows. */
const THREAD_COLUMN_FILTER = `p.name IN ('thread_id', 'stream_id', 'aggregate_id')
  OR p.name GLOB '*_thread_id'`;

interface TableShape {
  readonly table: string;
  readonly where: string;
  readonly columns: ReadonlyArray<string>;
  /** Set when the primary key is the rowid itself, so the row carries it. */
  readonly rowidAlias: boolean;
  readonly sizeExpression: string;
}

interface Fingerprint {
  readonly events: number;
  readonly sequence: number | null;
  readonly updatedAt: string | null;
}

const quote = (identifier: string) => `"${identifier.replaceAll('"', '""')}"`;

const TERMINAL_RUN_STATUSES = "('completed','failed','cancelled','interrupted','rolled_back')";

interface ThreadRow {
  readonly thread_id: string;
  readonly title: string;
  readonly parent_thread_id: string | null;
  readonly fork_type: string | null;
  readonly fork_thread_id: string | null;
  readonly pinned_at: string | null;
  readonly snoozed_until: string | null;
}

interface ThreadState {
  readonly id: ThreadId;
  readonly title: string;
  readonly parentThreadId: ThreadId | null;
  readonly lastActivityAt: string | null;
  readonly lastSequence: number;
  readonly reasons: ReadonlyArray<string>;
}

interface Candidates {
  readonly threads: ReadonlyMap<string, ThreadState>;
  /** Threads that read each thread: subagent children and forks. */
  readonly dependents: ReadonlyMap<string, ReadonlyArray<ThreadId>>;
  readonly forkSources: ReadonlyMap<string, ThreadId>;
}

/** The thread and every thread that reads it, root first. */
const unitOf = (candidates: Candidates, rootThreadId: ThreadId): Array<ThreadId> => {
  const unit: Array<ThreadId> = [rootThreadId];
  const seen = new Set<string>(unit);
  for (let index = 0; index < unit.length; index += 1) {
    for (const child of candidates.dependents.get(unit[index]!) ?? []) {
      if (seen.has(child)) continue;
      seen.add(child);
      unit.push(child);
    }
  }
  return unit;
};

const unitBlocker = (candidates: Candidates, unit: ReadonlyArray<ThreadId>, before: string) => {
  for (const threadId of unit) {
    const thread = candidates.threads.get(threadId);
    if (thread === undefined) return `${threadId} is gone`;
    if (thread.reasons.length > 0) return `${threadId}: ${thread.reasons.join(", ")}`;
    if (thread.lastActivityAt === null) return `${threadId} has no events`;
    if (thread.lastActivityAt >= before) return `${threadId} was active ${thread.lastActivityAt}`;
  }
  return null;
};

/** Units to offload: a qualifying thread's unit, unless its parent's or fork source's unit holds it. */
export const selectUnits = (
  candidates: Candidates,
  before: string,
  rootThreadIds?: ReadonlyArray<ThreadId>,
) => {
  const qualifies = new Map<string, boolean>();
  const unitQualifies = (threadId: ThreadId) => {
    const known = qualifies.get(threadId);
    if (known !== undefined) return known;
    const result = unitBlocker(candidates, unitOf(candidates, threadId), before) === null;
    qualifies.set(threadId, result);
    return result;
  };
  const holder = (thread: ThreadState) =>
    [thread.parentThreadId, candidates.forkSources.get(thread.id) ?? null].find(
      (owner): owner is ThreadId =>
        owner !== null && candidates.threads.has(owner) && unitQualifies(owner),
    );
  const roots =
    rootThreadIds ??
    [...candidates.threads.values()]
      .filter((thread) => unitQualifies(thread.id) && holder(thread) === undefined)
      .map((thread) => thread.id);
  return roots
    .filter((threadId) => candidates.threads.has(threadId) && unitQualifies(threadId))
    .map((rootThreadId) => {
      const root = candidates.threads.get(rootThreadId)!;
      const threadIds = unitOf(candidates, rootThreadId);
      return {
        rootThreadId,
        title: root.title,
        threadIds,
        remainingParentThreadId:
          root.parentThreadId !== null && candidates.threads.has(root.parentThreadId)
            ? root.parentThreadId
            : null,
        lastActivityAt: threadIds
          .map((threadId) => candidates.threads.get(threadId)!.lastActivityAt!)
          .reduce((latest, at) => (at > latest ? at : latest)),
      };
    })
    .toSorted((left, right) => left.lastActivityAt.localeCompare(right.lastActivityAt));
};

/** The schema a bundle's rows fit; a restore into any other schema is refused. */
const BundleSchema = Schema.Struct({
  /** The database's latest applied migration. */
  migrationId: Schema.Int,
  projectionSchemaVersion: Schema.Int,
});
type BundleSchema = typeof BundleSchema.Type;

const BundleHeader = Schema.Struct({
  format: Schema.Literal("t3code-thread-offload"),
  version: Schema.Literal(2),
  schema: BundleSchema,
  rootThreadId: ThreadId,
  threadIds: Schema.Array(ThreadId),
  titles: Schema.Record(Schema.String, Schema.String),
  parentThreadIds: Schema.Record(Schema.String, Schema.NullOr(ThreadId)),
  createdAt: Schema.String,
  tables: Schema.Record(Schema.String, Schema.Array(Schema.String)),
});
type BundleHeader = typeof BundleHeader.Type;

const EVENTS_TABLE = "orchestration_events";
/** How a row line of the event log starts: lines are JSON without spaces. */
const EVENT_LINE_PREFIX = `["${EVENTS_TABLE}",`;

const BundleTrailer = Schema.Struct({ sha256: Schema.String, lineCount: Schema.Int });

const BundleLine = Schema.Union([
  Schema.Tuple([Schema.Literal(BUNDLE_HEADER), BundleHeader]),
  Schema.Tuple([Schema.Literal(BUNDLE_TRAILER), BundleTrailer]),
  Schema.Tuple([
    Schema.String,
    ThreadId,
    Schema.NullOr(Schema.Number),
    Schema.Array(Schema.Unknown),
  ]),
]);
const encodeLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeLine = Schema.decodeUnknownSync(Schema.fromJsonString(BundleLine));
const encodeRecord = Schema.encodeSync(Schema.fromJsonString(ThreadOffloadRecord));
const decodeRecord = Schema.decodeUnknownOption(Schema.fromJsonString(ThreadOffloadRecord));

const BlobValue = Schema.Struct({ $blob: Schema.String });
const isBlobValue = Schema.is(BlobValue);
const toLineValue = (value: unknown) =>
  value instanceof Uint8Array ? { $blob: Buffer.from(value).toString("base64") } : value;
const fromLineValue = (value: unknown) =>
  isBlobValue(value) ? new Uint8Array(Buffer.from(value.$blob, "base64")) : value;

const ROW_BATCH_BYTES = 16 * 1024 * 1024;
const ROW_BATCH_COUNT = 400;
const ID_CHUNK = 400;

const chunk = <A>(items: ReadonlyArray<A>, size: number) => {
  const chunks: Array<ReadonlyArray<A>> = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
};

export interface ThreadOffloadShape {
  readonly plan: (input: {
    readonly before: string;
    readonly limit?: number | undefined;
    readonly threadIds?: ReadonlyArray<ThreadId> | undefined;
    readonly withSizes: boolean;
    readonly keepThreadIds: ReadonlySet<string>;
  }) => Effect.Effect<ThreadOffloadPlan, ThreadOffloadError>;
  readonly offload: (input: {
    readonly threadId: ThreadId;
    readonly before: string;
    readonly keepThreadIds: ReadonlySet<string>;
  }) => Effect.Effect<ThreadOffloadResult, ThreadOffloadError>;
  /** By an offloaded thread's record, or by a bundle's absolute path when its records are lost. */
  readonly restore: (
    input: { readonly threadId: ThreadId } | { readonly bundlePath: string },
  ) => Effect.Effect<ThreadRestoreResult, ThreadOffloadError>;
  readonly offloadDirectory: string;
  /**
   * Offloading adds no event, so a client resuming the shell from a cursor at or below this
   * sequence still lists threads that are gone (or misses restored ones): it needs a snapshot.
   */
  readonly shellResyncSequence: Effect.Effect<number>;
  /** Emits after each offload or restore, for shell subscribers to send a fresh snapshot. */
  readonly shellResyncs: Stream.Stream<void>;
}

export class ThreadOffload extends Context.Service<ThreadOffload, ThreadOffloadShape>()(
  "t3/threadOffload/ThreadOffload",
) {}

/** Runs after a unit's bundle and records are written, before its rows are deleted; for tests. */
export class BeforeDelete extends Context.Reference<
  (threadIds: ReadonlyArray<ThreadId>) => Effect.Effect<void>
>("t3/threadOffload/BeforeDelete", { defaultValue: () => () => Effect.void }) {}

/** Where the offloaded threads of a T3 home live; the CLI reads records from here directly. */
export const offloadDirectoryFor = (baseDir: string, path: Path.Path) =>
  path.join(baseDir, "offload");

/** Bundles whose threads were restored. Restore never deletes a bundle; `t3 thread prune-restored` does. */
export const restoredDirectoryFor = (offloadDirectory: string, path: Path.Path) =>
  path.join(offloadDirectory, "restored");

const recordPathFor = (offloadDirectory: string, threadId: string, path: Path.Path) =>
  path.join(offloadDirectory, "records", `${threadKey(threadId)}.json`);

export const readRecord = Effect.fn("ThreadOffload.readRecord")(function* (
  offloadDirectory: string,
  threadId: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const text = yield* fs
    .readFileString(recordPathFor(offloadDirectory, threadId, path))
    .pipe(Effect.option);
  return Option.flatMap(text, decodeRecord);
});

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const locks = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const offloadDirectory = offloadDirectoryFor(config.baseDir, path);
  const bundleDirectory = path.join(offloadDirectory, "bundles");
  const recordDirectory = path.join(offloadDirectory, "records");
  const restoredDirectory = restoredDirectoryFor(offloadDirectory, path);
  const resyncPath = path.join(offloadDirectory, "shell-resync-sequence");
  const resyncSequence = yield* Ref.make(
    Number((yield* fs.readFileString(resyncPath).pipe(Effect.orElseSucceed(() => "0"))).trim()) ||
      0,
  );
  const resyncs = yield* PubSub.unbounded<void>();

  const recordOf = (threadId: string) =>
    readRecord(offloadDirectory, threadId).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
    );

  const read = <A extends object>(text: string, params: ReadonlyArray<unknown> = []) =>
    sql.unsafe<A>(text, params).pipe(NodeSqliteClient.readOnly);

  const promise = <A>(operation: string, run: () => Promise<A>) =>
    Effect.tryPromise({
      try: run,
      catch: (cause) => fail("failed", `${operation}: ${String(cause)}`, cause),
    });

  const describeTables = Effect.gen(function* () {
    const keyed = yield* read<{ readonly name: string }>(`
      SELECT DISTINCT m.name FROM sqlite_master AS m, pragma_table_info(m.name) AS p
      WHERE m.type = 'table' AND (${THREAD_COLUMN_FILTER})
      ORDER BY m.name
    `);
    const handled = new Set(THREAD_TABLES.map((spec) => spec.table));
    const unknown = keyed
      .map((row) => row.name)
      .filter((name) => !handled.has(name) && IGNORED_THREAD_TABLES[name] === undefined);
    if (unknown.length > 0) {
      return yield* fail(
        "failed",
        `The database has thread rows in ${unknown.join(", ")}, which offloading does not know; nothing was offloaded. Add each to THREAD_TABLES or IGNORED_THREAD_TABLES in threadOffload/ThreadOffload.ts.`,
      );
    }
    const shapes: Array<TableShape> = [];
    for (const spec of THREAD_TABLES) {
      const info = yield* read<{
        readonly name: string;
        readonly type: string;
        readonly pk: number;
      }>(`SELECT name, type, pk FROM pragma_table_info(?)`, [spec.table]);
      if (!info.some((column) => column.name === spec.column)) {
        return yield* fail(
          "failed",
          `${spec.table} has no ${spec.column} column in this database; nothing was offloaded. Update THREAD_TABLES in threadOffload/ThreadOffload.ts.`,
        );
      }
      const definition = yield* read<{ readonly rowless: number }>(
        `SELECT sql LIKE '%WITHOUT ROWID%' AS rowless FROM sqlite_master WHERE name = ?`,
        [spec.table],
      );
      if (definition[0]?.rowless === 1) {
        return yield* fail("failed", `${spec.table} has no rowid, which offloading needs.`);
      }
      const keys = info.filter((column) => column.pk > 0);
      shapes.push({
        table: spec.table,
        where: spec.where,
        columns: info.map((column) => column.name),
        rowidAlias: keys.length === 1 && keys[0]!.type.toUpperCase() === "INTEGER",
        sizeExpression: info
          .map((column) => `COALESCE(octet_length(${quote(column.name)}), 0)`)
          .join(" + "),
      });
    }
    return shapes;
  });

  const storage = Effect.gen(function* () {
    const rows = yield* read<{
      readonly pages: number;
      readonly free: number;
      readonly size: number;
    }>(
      `SELECT (SELECT page_count FROM pragma_page_count) AS pages,
        (SELECT freelist_count FROM pragma_freelist_count) AS free,
        (SELECT page_size FROM pragma_page_size) AS size`,
    );
    const row = rows[0];
    return {
      databaseBytes: (row?.pages ?? 0) * (row?.size ?? 0),
      reclaimableBytes: (row?.free ?? 0) * (row?.size ?? 0),
    } satisfies ThreadOffloadStorage;
  });

  const currentSchema = Effect.gen(function* () {
    const rows = yield* read<{ readonly id: number | null }>(
      `SELECT MAX(migration_id) AS id FROM effect_sql_migrations`,
    );
    return {
      migrationId: rows[0]?.id ?? 0,
      projectionSchemaVersion: ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
    } satisfies BundleSchema;
  });

  /**
   * What says a thread changed: some writers (provider sessions, command output, checkpoints,
   * runs, the session importers) do not take the command lock, so the delete checks this again.
   */
  const fingerprintQuery = `
    SELECT
      (SELECT COUNT(*) FROM orchestration_events
        WHERE aggregate_kind = 'thread' AND stream_id = ?1) AS events,
      (SELECT MAX(sequence) FROM orchestration_events
        WHERE aggregate_kind = 'thread' AND stream_id = ?1) AS sequence,
      (SELECT updated_at FROM orchestration_v2_projection_threads
        WHERE thread_id = ?1) AS updatedAt`;
  const sameFingerprint = (left: Fingerprint | undefined, right: Fingerprint | undefined) =>
    left !== undefined &&
    right !== undefined &&
    left.events === right.events &&
    left.sequence === right.sequence &&
    left.updatedAt === right.updatedAt;

  const announceChange = Effect.gen(function* () {
    const rows = yield* sql<{ readonly sequence: number | null }>`
      SELECT MAX(sequence) AS sequence
      FROM orchestration_events INDEXED BY idx_orchestration_events_application_high_water
      WHERE aggregate_kind = 'project'
        OR (application_event_version = 2 AND aggregate_kind = 'thread')
    `;
    const sequence = rows[0]?.sequence ?? 0;
    yield* Ref.set(resyncSequence, sequence);
    yield* promise("Could not save the shell resync sequence", () =>
      writeFileAtomically(resyncPath, `${sequence}\n`),
    );
    yield* PubSub.publish(resyncs, undefined);
  });

  const loadCandidates = Effect.fn("ThreadOffload.loadCandidates")(function* (
    keepThreadIds: ReadonlySet<string>,
  ) {
    const now = DateTime.formatIso(yield* DateTime.now);
    const threadRows = yield* read<ThreadRow>(`
      SELECT thread_id, title,
        json_extract(payload_json, '$.lineage.parentThreadId') AS parent_thread_id,
        json_extract(payload_json, '$.forkedFrom.type') AS fork_type,
        json_extract(payload_json, '$.forkedFrom.threadId') AS fork_thread_id,
        json_extract(payload_json, '$.pinnedAt') AS pinned_at,
        json_extract(payload_json, '$.snoozedUntil') AS snoozed_until
      FROM orchestration_v2_projection_threads
    `);
    const lastEvents = yield* read<{
      readonly stream_id: string;
      readonly occurred_at: string;
      readonly sequence: number;
    }>(`
      SELECT event.stream_id, event.occurred_at, event.sequence
      FROM (
        SELECT stream_id, MAX(sequence) AS sequence
        FROM orchestration_events INDEXED BY idx_orchestration_events_agent_stream_sequence
        WHERE application_event_version = 2 AND aggregate_kind = 'thread'
        GROUP BY stream_id
      ) AS latest
      JOIN orchestration_events AS event ON event.sequence = latest.sequence
    `);
    // Mirrors the threads the server recovers at startup: anything still running or open.
    const busyRows = yield* read<{ readonly thread_id: string; readonly reason: string }>(`
      SELECT thread_id, 'running' AS reason FROM orchestration_v2_projection_runs
        WHERE status NOT IN ${TERMINAL_RUN_STATUSES}
      UNION SELECT thread_id, 'waiting for an answer' FROM orchestration_v2_projection_runtime_requests
        WHERE status = 'pending'
      UNION SELECT thread_id, 'subagent running' FROM orchestration_v2_projection_subagents
        WHERE status IN ('pending', 'running', 'waiting')
      UNION SELECT item.thread_id, 'tool running' FROM orchestration_v2_projection_turn_items AS item
        WHERE type IN ('command_execution', 'dynamic_tool', 'subagent')
          AND status IN ('pending', 'running', 'waiting')
          AND NOT EXISTS (
            SELECT 1 FROM orchestration_v2_projection_runs AS run
            WHERE run.run_id = item.run_id AND run.status = 'rolled_back')
      UNION SELECT thread_id, 'provider session open' FROM orchestration_v2_projection_provider_sessions
        WHERE thread_id IS NOT NULL AND status NOT IN ('stopped', 'error')
      UNION SELECT bindings.thread_id, 'provider session open'
        FROM orchestration_v2_projection_provider_session_bindings AS bindings
        JOIN orchestration_v2_projection_provider_sessions AS sessions
          ON sessions.provider_session_id = bindings.provider_session_id
        WHERE sessions.status NOT IN ('stopped', 'error')
      UNION SELECT thread_id, 'provider thread active' FROM orchestration_v2_projection_provider_threads
        WHERE status = 'active'
      UNION SELECT thread_id, 'effects pending' FROM orchestration_v2_effect_outbox
        WHERE status IN ('pending', 'running')
      UNION SELECT thread_id, 'scheduled task' FROM scheduled_tasks WHERE thread_id IS NOT NULL
      UNION SELECT source_thread_id, 'context transfer pending'
        FROM orchestration_v2_projection_context_transfers WHERE status = 'pending'
      UNION SELECT target_thread_id, 'context transfer pending'
        FROM orchestration_v2_projection_context_transfers WHERE status = 'pending'
      UNION SELECT thread_id, 'imported session' FROM orchestration_v2_projection_threads
        WHERE thread_id LIKE 'import:%'
      UNION SELECT thread_id, 'imported session' FROM provider_session_runtime
        WHERE json_valid(runtime_payload_json)
          AND json_type(runtime_payload_json, '$.importedTranscripts') IS NOT NULL
    `);
    const latestSequence = Math.max(0, ...lastEvents.map((event) => event.sequence));
    const lastByThread = new Map(lastEvents.map((event) => [event.stream_id, event]));
    const reasonsByThread = new Map<string, Array<string>>();
    const addReason = (threadId: string, reason: string) => {
      const reasons = reasonsByThread.get(threadId) ?? [];
      if (!reasons.includes(reason)) reasons.push(reason);
      reasonsByThread.set(threadId, reasons);
    };
    for (const row of busyRows) addReason(row.thread_id, row.reason);
    const threads = new Map<string, ThreadState>();
    const dependents = new Map<string, Array<ThreadId>>();
    const forkSources = new Map<string, ThreadId>();
    const ids = new Set(threadRows.map((row) => row.thread_id));
    for (const row of threadRows) {
      const id = ThreadId.make(row.thread_id);
      if (row.pinned_at !== null) addReason(id, "pinned");
      if (row.snoozed_until !== null && row.snoozed_until > now) addReason(id, "snoozed");
      if (keepThreadIds.has(id)) addReason(id, "in a thread group");
      const last = lastByThread.get(id);
      // The projection records how far it has applied events; that event must stay.
      if (last !== undefined && last.sequence === latestSequence) addReason(id, "latest event");
      const owners = [
        row.parent_thread_id,
        row.fork_type === "run" ? row.fork_thread_id : null,
      ].filter((owner): owner is string => owner !== null && owner !== id && ids.has(owner));
      for (const owner of new Set(owners)) {
        dependents.set(owner, [...(dependents.get(owner) ?? []), id]);
      }
      if (row.fork_type === "run" && row.fork_thread_id !== null && ids.has(row.fork_thread_id)) {
        forkSources.set(id, ThreadId.make(row.fork_thread_id));
      }
      threads.set(id, {
        id,
        title: row.title,
        parentThreadId: row.parent_thread_id === null ? null : ThreadId.make(row.parent_thread_id),
        lastActivityAt: last?.occurred_at ?? null,
        lastSequence: last?.sequence ?? 0,
        reasons: reasonsByThread.get(id) ?? [],
      });
    }
    return { threads, dependents, forkSources } satisfies Candidates;
  });

  const threadBytes = Effect.fn("ThreadOffload.threadBytes")(function* (
    shapes: ReadonlyArray<TableShape>,
    threadId: ThreadId,
  ) {
    let bytes = 0;
    for (const shape of shapes) {
      const rows = yield* read<{ readonly bytes: number | null }>(
        `SELECT SUM(${shape.sizeExpression}) AS bytes FROM ${quote(shape.table)} WHERE ${shape.where}`,
        [threadId],
      );
      bytes += rows[0]?.bytes ?? 0;
    }
    return bytes;
  });

  const plan: ThreadOffloadShape["plan"] = Effect.fn("ThreadOffload.plan")(
    function* (input) {
      const candidates = yield* loadCandidates(input.keepThreadIds);
      const selected = selectUnits(candidates, input.before, input.threadIds);
      const limit = input.limit ?? selected.length;
      const shapes = input.withSizes ? yield* describeTables : [];
      const units: Array<ThreadOffloadUnit> = [];
      for (const unit of selected.slice(0, limit)) {
        let bytes: number | null = null;
        if (input.withSizes) {
          bytes = 0;
          for (const threadId of unit.threadIds) bytes += yield* threadBytes(shapes, threadId);
        }
        units.push({ ...unit, bytes });
      }
      const kept = new Map<string, number>();
      for (const thread of candidates.threads.values()) {
        const reasons =
          thread.reasons.length > 0
            ? thread.reasons
            : thread.lastActivityAt !== null && thread.lastActivityAt >= input.before
              ? ["active since the cutoff"]
              : [];
        for (const reason of reasons) kept.set(reason, (kept.get(reason) ?? 0) + 1);
      }
      return {
        units,
        moreUnitCount: Math.max(0, selected.length - limit),
        kept: [...kept].map(([reason, threadCount]) => ({ reason, threadCount })),
        storage: yield* storage,
      } satisfies ThreadOffloadPlan;
    },
    Effect.mapError((cause) =>
      isThreadOffloadError(cause) ? cause : fail("failed", "Could not plan.", cause),
    ),
  );

  /** Takes each thread's command lock, parents before children like the orchestrator. */
  const withThreadLocks = <A, E, R>(
    threadIds: ReadonlyArray<ThreadId>,
    effect: Effect.Effect<A, E, R>,
  ) => threadIds.reduceRight((inner, threadId) => locks.withLock(threadId, inner), effect);

  const writeBundle = Effect.fn("ThreadOffload.writeBundle")(function* (input: {
    readonly bundlePath: string;
    readonly header: BundleHeader;
    readonly shapes: ReadonlyArray<TableShape>;
  }) {
    const writer = yield* promise("Could not create the bundle", () =>
      openBundleWriter(input.bundlePath),
    );
    /** Exported rowids per thread and table, which the delete must match exactly. */
    const exported = new Map<string, Map<string, Array<number>>>();
    /** Each thread as its rows started being exported, which the delete must still find. */
    const fingerprints = new Map<string, Fingerprint>();
    let rowCount = 0;
    let rowBytes = 0;
    const exportRows = Effect.gen(function* () {
      yield* promise("Could not write the bundle", () =>
        writer.writeLine(encodeLine([BUNDLE_HEADER, input.header])),
      );
      for (const threadId of input.header.threadIds) {
        const byTable = new Map<string, Array<number>>();
        exported.set(threadId, byTable);
        const fingerprint = yield* read<Fingerprint>(fingerprintQuery, [threadId]);
        fingerprints.set(threadId, fingerprint[0]!);
        for (const shape of input.shapes) {
          const ids = yield* read<{ readonly r: number; readonly b: number }>(
            `SELECT rowid AS r, ${shape.sizeExpression} AS b FROM ${quote(shape.table)}
             WHERE ${shape.where} ORDER BY rowid`,
            [threadId],
          );
          byTable.set(
            shape.table,
            ids.map((row) => row.r),
          );
          const batches: Array<Array<number>> = [];
          let batchBytes = 0;
          for (const row of ids) {
            const last = batches.at(-1);
            if (
              last === undefined ||
              last.length >= ROW_BATCH_COUNT ||
              batchBytes + row.b > ROW_BATCH_BYTES
            ) {
              batches.push([row.r]);
              batchBytes = row.b;
            } else {
              last.push(row.r);
              batchBytes += row.b;
            }
            rowBytes += row.b;
          }
          const columns = shape.columns.map(quote).join(", ");
          for (const batch of batches) {
            const rows = yield* read<Record<string, unknown>>(
              `SELECT rowid AS "__offload_rowid", ${columns} FROM ${quote(shape.table)}
               WHERE rowid IN (${batch.map(() => "?").join(",")}) ORDER BY rowid`,
              batch,
            );
            if (rows.length !== batch.length) {
              return yield* fail("conflict", `Rows of ${threadId} changed while being offloaded.`);
            }
            for (const row of rows) {
              const line = encodeLine([
                shape.table,
                threadId,
                shape.rowidAlias ? null : row.__offload_rowid,
                shape.columns.map((column) => toLineValue(row[column])),
              ]);
              yield* promise("Could not write the bundle", () => writer.writeLine(line));
              rowCount += 1;
            }
            yield* Effect.yieldNow;
          }
        }
      }
      return yield* promise("Could not finish the bundle", () =>
        writer.finish((summary) => encodeLine([BUNDLE_TRAILER, summary])),
      );
    }).pipe(
      Effect.onError(() => Effect.promise(() => writer.abort()).pipe(Effect.ignore({ log: true }))),
    );
    const summary = yield* exportRows;
    return { summary, exported, fingerprints, rowCount, rowBytes };
  });

  /**
   * Reads the bundle back and checks every line against the trailer and the expected hash. With
   * `collectEvents`, also returns the `event_id` of each thread's events in the bundle.
   */
  const verifyBundle = Effect.fn("ThreadOffload.verifyBundle")(function* (
    bundlePath: string,
    options: {
      readonly expected?: { readonly sha256: string; readonly lineCount: number };
      readonly collectEvents?: boolean;
    } = {},
  ) {
    const expected = options.expected;
    const reader = openBundleReader(bundlePath);
    return yield* Effect.gen(function* () {
      let header: BundleHeader | undefined;
      let trailer: typeof BundleTrailer.Type | undefined;
      let eventIdIndex = -1;
      const events = new Map<string, Array<string>>();
      while (true) {
        const line = yield* promise("Could not read the bundle", () => reader.next());
        if (line === undefined) break;
        if (trailer !== undefined)
          return yield* fail("failed", "The bundle has lines after its end.");
        if (header === undefined) {
          const first = yield* Effect.try({
            try: () => decodeLine(line),
            catch: (cause) => fail("failed", "The bundle's header is unreadable.", cause),
          });
          if (first.length !== 2 || first[0] !== BUNDLE_HEADER) {
            return yield* fail("failed", "The bundle has no header.");
          }
          header = first[1];
          eventIdIndex = header.tables[EVENTS_TABLE]?.indexOf("event_id") ?? -1;
          if (options.collectEvents === true && eventIdIndex === -1) {
            return yield* fail("failed", "The bundle has no event ids.");
          }
          continue;
        }
        if (options.collectEvents === true && line.startsWith(EVENT_LINE_PREFIX)) {
          const row = yield* Effect.try({
            try: () => decodeLine(line),
            catch: (cause) => fail("failed", "A bundle line is unreadable.", cause),
          });
          if (row.length === 4) {
            const ids = events.get(row[1]) ?? [];
            ids.push(String(row[3][eventIdIndex]));
            events.set(row[1], ids);
          }
          continue;
        }
        if (line.startsWith(`["${BUNDLE_TRAILER}"`)) {
          const last = yield* Effect.try({
            try: () => decodeLine(line),
            catch: (cause) => fail("failed", "The bundle's end is unreadable.", cause),
          });
          if (last.length === 2 && last[0] === BUNDLE_TRAILER) trailer = last[1];
        }
      }
      const read = reader.summary();
      if (header === undefined || trailer === undefined) {
        return yield* fail("failed", `${bundlePath} is incomplete.`);
      }
      if (trailer.sha256 !== read.sha256 || trailer.lineCount !== read.lineCount) {
        return yield* fail("failed", `${bundlePath} does not match its checksum.`);
      }
      if (
        expected !== undefined &&
        (expected.sha256 !== read.sha256 || expected.lineCount !== read.lineCount)
      ) {
        return yield* fail("failed", `${bundlePath} differs from what was written.`);
      }
      return { header, events };
    }).pipe(Effect.ensuring(Effect.sync(() => reader.close())));
  });

  const deleteThreadRows = Effect.fn("ThreadOffload.deleteThreadRows")(function* (input: {
    readonly threadId: ThreadId;
    readonly shapes: ReadonlyArray<TableShape>;
    readonly rowids: ReadonlyMap<string, ReadonlyArray<number>>;
    readonly fingerprint: Fingerprint | undefined;
  }) {
    yield* Effect.gen(function* () {
      const now = yield* sql.unsafe<Fingerprint>(fingerprintQuery, [input.threadId]);
      if (!sameFingerprint(input.fingerprint, now[0])) {
        return yield* fail(
          "conflict",
          `${input.threadId} changed while being offloaded; nothing of it was removed.`,
        );
      }
      const latest = yield* sql<{ readonly sequence: number | null }>`
        SELECT MAX(sequence) AS sequence FROM orchestration_events
        WHERE application_event_version = 2 AND aggregate_kind = 'thread'
      `;
      const own = yield* sql<{ readonly sequence: number | null }>`
        SELECT MAX(sequence) AS sequence
        FROM orchestration_events INDEXED BY idx_orchestration_events_agent_stream_sequence
        WHERE application_event_version = 2 AND aggregate_kind = 'thread'
          AND stream_id = ${input.threadId}
      `;
      if (own[0]?.sequence != null && own[0].sequence === latest[0]?.sequence) {
        return yield* fail("busy", `${input.threadId} holds the latest event; try again later.`);
      }
      for (const shape of input.shapes) {
        const rowids = input.rowids.get(shape.table) ?? [];
        let deleted = 0;
        for (const ids of chunk(rowids, ID_CHUNK)) {
          yield* sql.unsafe(
            `DELETE FROM ${quote(shape.table)} WHERE rowid IN (${ids.map(() => "?").join(",")})`,
            ids,
          );
          const changes = yield* sql<{ readonly n: number }>`SELECT changes() AS n`;
          deleted += changes[0]?.n ?? 0;
        }
        const left = yield* sql.unsafe<{ readonly n: number }>(
          `SELECT COUNT(*) AS n FROM ${quote(shape.table)} WHERE ${shape.where}`,
          [input.threadId],
        );
        if (deleted !== rowids.length || (left[0]?.n ?? 0) !== 0) {
          return yield* fail(
            "conflict",
            `${shape.table} rows of ${input.threadId} changed while being offloaded; nothing of it was removed.`,
          );
        }
      }
    }).pipe(sql.withTransaction);
  });

  /**
   * After a failed delete: threads still in the database lose their records, so nothing says they
   * are offloaded. The bundle goes too when no thread was removed, as the database holds it all.
   */
  const forgetLiveThreads = Effect.fn("ThreadOffload.forgetLiveThreads")(function* (input: {
    readonly threadIds: ReadonlyArray<ThreadId>;
    readonly removed: ReadonlyArray<ThreadId>;
    readonly bundlePath: string;
  }) {
    for (const threadId of input.threadIds) {
      if (input.removed.includes(threadId)) continue;
      yield* fs.remove(recordPathFor(offloadDirectory, threadId, path), { force: true });
    }
    if (input.removed.length === 0) yield* fs.remove(input.bundlePath, { force: true });
  });

  const offload: ThreadOffloadShape["offload"] = Effect.fn("ThreadOffload.offload")(
    function* (input) {
      const initial = yield* loadCandidates(input.keepThreadIds);
      if (!initial.threads.has(input.threadId)) {
        const record = yield* recordOf(input.threadId);
        return yield* fail(
          "not-found",
          Option.isSome(record)
            ? `${input.threadId} is already offloaded.`
            : `No thread ${input.threadId}.`,
        );
      }
      const lockedIds = unitOf(initial, input.threadId);
      return yield* withThreadLocks(
        lockedIds,
        Effect.gen(function* () {
          const candidates = yield* loadCandidates(input.keepThreadIds);
          const threadIds = unitOf(candidates, input.threadId);
          if (threadIds.some((threadId) => !lockedIds.includes(threadId))) {
            return yield* fail("conflict", `${input.threadId} gained threads; try again.`);
          }
          const blocker = unitBlocker(candidates, threadIds, input.before);
          if (blocker !== null) return yield* fail("busy", `Kept: ${blocker}.`);
          const shapes = yield* describeTables;
          const createdAt = DateTime.formatIso(yield* DateTime.now);
          yield* fs
            .makeDirectory(bundleDirectory, { recursive: true })
            .pipe(
              Effect.mapError((cause) => fail("failed", "Could not create the folder.", cause)),
            );
          yield* fs
            .makeDirectory(recordDirectory, { recursive: true })
            .pipe(
              Effect.mapError((cause) => fail("failed", "Could not create the folder.", cause)),
            );
          const bundleName = `${createdAt.replaceAll(/[:.]/g, "-")}-${threadKey(input.threadId)}.jsonl.zst`;
          const bundlePath = path.join(bundleDirectory, bundleName);
          const thread = (threadId: ThreadId) => candidates.threads.get(threadId)!;
          const header: BundleHeader = {
            format: "t3code-thread-offload",
            version: 2,
            schema: yield* currentSchema,
            rootThreadId: input.threadId,
            threadIds,
            titles: Object.fromEntries(threadIds.map((id) => [id, thread(id).title])),
            parentThreadIds: Object.fromEntries(
              threadIds.map((id) => [id, thread(id).parentThreadId]),
            ),
            createdAt,
            tables: Object.fromEntries(shapes.map((shape) => [shape.table, shape.columns])),
          };
          const written = yield* writeBundle({ bundlePath, header, shapes });
          yield* verifyBundle(bundlePath, { expected: written.summary }).pipe(
            Effect.tapError(() => fs.remove(bundlePath, { force: true }).pipe(Effect.ignore)),
          );
          // Records first: a thread removed before a crash still says where it went.
          for (const threadId of threadIds) {
            yield* promise("Could not write the record", () =>
              writeFileAtomically(
                recordPathFor(offloadDirectory, threadId, path),
                encodeRecord({
                  threadId,
                  rootThreadId: input.threadId,
                  title: thread(threadId).title,
                  parentThreadId: thread(threadId).parentThreadId,
                  bundle: path.join("bundles", bundleName),
                  offloadedAt: createdAt,
                }),
              ),
            );
          }
          yield* (yield* BeforeDelete)(threadIds);
          // Children before parents, so no remaining thread reads one already removed.
          const removed: Array<ThreadId> = [];
          for (const threadId of threadIds.toReversed()) {
            yield* deleteThreadRows({
              threadId,
              shapes,
              rowids: written.exported.get(threadId) ?? new Map(),
              fingerprint: written.fingerprints.get(threadId),
            }).pipe(
              Effect.onError(() =>
                forgetLiveThreads({ threadIds, removed, bundlePath }).pipe(
                  Effect.andThen(announceChange),
                  Effect.ignore({ log: true }),
                ),
              ),
            );
            removed.push(threadId);
          }
          yield* announceChange;
          return {
            rootThreadId: input.threadId,
            threadIds,
            bundlePath,
            bundleBytes: yield* promise("Could not read the bundle size", () =>
              fileSize(bundlePath),
            ),
            rowCount: written.rowCount,
            rowBytes: written.rowBytes,
            storage: yield* storage,
          } satisfies ThreadOffloadResult;
        }),
      );
    },
    Effect.mapError((cause) =>
      isThreadOffloadError(cause) ? cause : fail("failed", "Could not offload.", cause),
    ),
  );

  /** Tables of this database that hold rows of the thread, by the bundle's own table list. */
  const tablesHolding = Effect.fn("ThreadOffload.tablesHolding")(function* (
    header: BundleHeader,
    threadId: ThreadId,
  ) {
    const holding: Array<string> = [];
    for (const spec of THREAD_TABLES) {
      if (header.tables[spec.table] === undefined) continue;
      const rows = yield* sql.unsafe<{ readonly n: number }>(
        `SELECT EXISTS (SELECT 1 FROM ${quote(spec.table)} WHERE ${spec.where}) AS n`,
        [threadId],
      );
      if (rows[0]?.n === 1) holding.push(spec.table);
    }
    return holding;
  });

  /**
   * Whether a thread of the bundle goes back in. One the database has with exactly the bundle's
   * events was never removed (an offload that stopped partway) and is left alone. One it has in
   * any other shape was recreated or changed since: restoring would lose one of the two, so
   * nothing is restored.
   */
  const restoreState = Effect.fn("ThreadOffload.restoreState")(function* (input: {
    readonly header: BundleHeader;
    readonly threadId: ThreadId;
    readonly bundleEventIds: ReadonlyArray<string>;
    readonly bundlePath: string;
  }) {
    const live = yield* sql<{ readonly id: string }>`
      SELECT event_id AS id FROM orchestration_events
      WHERE aggregate_kind = 'thread' AND stream_id = ${input.threadId}
    `;
    if (live.length === 0) {
      const holding = yield* tablesHolding(input.header, input.threadId);
      if (holding.length === 0) return "missing" as const;
      return yield* fail(
        "conflict",
        `${input.threadId} has no events but still has rows in ${holding.join(", ")}. Nothing was restored; the bundle stays at ${input.bundlePath}.`,
      );
    }
    const liveIds = new Set(live.map((row) => row.id));
    const common = input.bundleEventIds.filter((id) => liveIds.has(id)).length;
    if (
      live.length === input.bundleEventIds.length &&
      common === input.bundleEventIds.length &&
      liveIds.size === live.length
    ) {
      return "present" as const;
    }
    return yield* fail(
      "conflict",
      `${input.threadId} exists again with ${live.length} event(s); the bundle holds ${input.bundleEventIds.length}, ${common} of them the same. It was recreated or changed after it was offloaded, so nothing was restored and the bundle stays at ${input.bundlePath}.`,
    );
  });

  const insertRows = Effect.fn("ThreadOffload.insertRows")(function* (input: {
    readonly reader: BundleReader;
    readonly header: BundleHeader;
    readonly threadId: ThreadId;
    readonly insert: boolean;
    readonly first: string | undefined;
  }) {
    let line = input.first;
    let inserted = 0;
    while (line !== undefined) {
      const decoded = yield* Effect.try({
        try: () => decodeLine(line!),
        catch: (cause) => fail("failed", "A bundle line is unreadable.", cause),
      });
      if (decoded.length !== 4) return { next: line, inserted };
      const [table, threadId, rowid, values] = decoded;
      if (threadId !== input.threadId) return { next: line, inserted };
      if (input.insert) {
        const columns = input.header.tables[table];
        if (columns === undefined || columns.length !== values.length) {
          return yield* fail("failed", `The bundle's ${table} rows do not match its header.`);
        }
        const names = [...(rowid === null ? [] : ["rowid"]), ...columns].map(quote).join(", ");
        // Rows go back under their old rowids. One another row took since becomes NULL, and SQLite
        // gives the row one past the table's largest, so it then sorts after rows written after
        // the offload, its columns unchanged. A table whose key is the rowid (the event log's
        // `sequence`) carries it among the columns: a taken one fails the insert instead.
        const rowidValue =
          rowid === null
            ? []
            : [
                `CASE WHEN EXISTS (SELECT 1 FROM ${quote(table)} WHERE rowid = ?) THEN NULL ELSE ? END`,
              ];
        yield* sql.unsafe(
          `INSERT INTO ${quote(table)} (${names}) VALUES (${[
            ...rowidValue,
            ...columns.map(() => "?"),
          ].join(", ")})`,
          [...(rowid === null ? [] : [rowid, rowid]), ...values.map(fromLineValue)],
        );
        inserted += 1;
      }
      line = yield* promise("Could not read the bundle", () => input.reader.next());
    }
    return { next: undefined, inserted };
  });

  const restore: ThreadOffloadShape["restore"] = Effect.fn("ThreadOffload.restore")(
    function* (input) {
      let bundlePath: string;
      if ("threadId" in input) {
        const record = yield* recordOf(input.threadId);
        if (Option.isNone(record)) {
          return yield* fail("not-found", `${input.threadId} is not offloaded here.`);
        }
        bundlePath = path.join(offloadDirectory, record.value.bundle);
      } else {
        if (!path.isAbsolute(input.bundlePath)) {
          return yield* fail("not-found", `${input.bundlePath} is not an absolute path.`);
        }
        bundlePath = input.bundlePath;
      }
      if (!(yield* fs.exists(bundlePath).pipe(Effect.orElseSucceed(() => false)))) {
        return yield* fail("not-found", `The bundle ${bundlePath} is missing.`);
      }
      const { header, events } = yield* verifyBundle(bundlePath, { collectEvents: true });
      if ("threadId" in input && !header.threadIds.includes(input.threadId)) {
        return yield* fail("not-found", `${bundlePath} does not hold ${input.threadId}.`);
      }
      const schema = yield* currentSchema;
      if (
        header.schema.migrationId !== schema.migrationId ||
        header.schema.projectionSchemaVersion !== schema.projectionSchemaVersion
      ) {
        return yield* fail(
          "conflict",
          `${bundlePath} was written by a database at migration ${header.schema.migrationId} with projection schema ${header.schema.projectionSchemaVersion}; this one is at migration ${schema.migrationId} with projection schema ${schema.projectionSchemaVersion}. Its rows may not fit, so nothing was restored. Restore it with the T3 Code version that wrote it.`,
        );
      }
      return yield* withThreadLocks(
        header.threadIds,
        Effect.gen(function* () {
          // Every thread is checked before any goes back, so a conflict changes nothing.
          const missing = new Set<ThreadId>();
          for (const threadId of header.threadIds) {
            const state = yield* restoreState({
              header,
              threadId,
              bundleEventIds: events.get(threadId) ?? [],
              bundlePath,
            });
            if (state === "missing") missing.add(threadId);
          }
          const reader = openBundleReader(bundlePath);
          const restored: Array<ThreadId> = [];
          let rowCount = 0;
          yield* Effect.gen(function* () {
            yield* promise("Could not read the bundle", () => reader.next());
            let line = yield* promise("Could not read the bundle", () => reader.next());
            for (const threadId of header.threadIds) {
              const insert = missing.has(threadId);
              const result = yield* Effect.gen(function* () {
                // Writers that skip the command lock may have recreated it since the check.
                if (insert && (yield* tablesHolding(header, threadId)).length > 0) {
                  return yield* fail(
                    "conflict",
                    `${threadId} was recreated while being restored; the bundle stays at ${bundlePath}.`,
                  );
                }
                return yield* insertRows({ reader, header, threadId, insert, first: line });
              }).pipe(insert ? sql.withTransaction : (effect) => effect);
              line = result.next;
              rowCount += result.inserted;
              if (insert) restored.push(threadId);
            }
          }).pipe(
            Effect.onError(() => announceChange.pipe(Effect.ignore)),
            Effect.ensuring(Effect.sync(() => reader.close())),
          );
          // The bundle is kept: a bundle offloading wrote moves to restored/, others stay put.
          // A failed move leaves it where it was, which a later restore also accepts.
          const movedAtSeconds = DateTime.toEpochMillis(yield* DateTime.now) / 1000;
          const keptPath =
            path.dirname(bundlePath) === bundleDirectory
              ? yield* promise("Could not move the restored bundle", () =>
                  moveFileDurably(
                    bundlePath,
                    path.join(restoredDirectory, path.basename(bundlePath)),
                    movedAtSeconds,
                  ),
                ).pipe(
                  Effect.as(path.join(restoredDirectory, path.basename(bundlePath))),
                  Effect.tapError((error) => Effect.logWarning(error.detail)),
                  Effect.orElseSucceed(() => bundlePath),
                )
              : bundlePath;
          const relativeBundle = path.relative(offloadDirectory, bundlePath);
          for (const threadId of header.threadIds) {
            const record = yield* recordOf(threadId);
            if (Option.isNone(record) || record.value.bundle !== relativeBundle) continue;
            yield* fs
              .remove(recordPathFor(offloadDirectory, threadId, path), { force: true })
              .pipe(Effect.ignore({ log: true }));
          }
          yield* announceChange;
          return {
            rootThreadId: header.rootThreadId,
            threadIds: restored,
            rowCount,
            bundlePath: keptPath,
          } satisfies ThreadRestoreResult;
        }),
      );
    },
    Effect.mapError((cause) =>
      isThreadOffloadError(cause) ? cause : fail("failed", "Could not restore.", cause),
    ),
  );

  return ThreadOffload.of({
    plan,
    offload,
    restore,
    offloadDirectory,
    shellResyncSequence: Ref.get(resyncSequence),
    shellResyncs: Stream.fromPubSub(resyncs).pipe(Stream.map(() => undefined)),
  });
});

/**
 * Needs the orchestrator's own `ThreadCommandExecutor`, not one of its own: a second lock would
 * not keep commands out while a thread is offloaded or restored.
 */
export const layer = Layer.effect(ThreadOffload, make);
