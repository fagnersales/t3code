import {
  AuthEnvironmentMaintainScope,
  AuthOrchestrationReadScope,
  THREAD_OFFLOAD_PLAN_ROUTE,
  THREAD_OFFLOAD_ROUTE,
  THREAD_RESTORE_ROUTE,
  ThreadId,
  ThreadOffloadPlan,
  ThreadOffloadRecord,
  ThreadOffloadResponseLine,
  ThreadOffloadResult,
  ThreadRestoreResult,
} from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as References from "effect/References";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { Argument, Command, Flag, GlobalFlag } from "effect/cli";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import { readPersistedServerRuntimeState } from "../serverRuntimeState.ts";
import {
  offloadDirectoryFor,
  readRecord,
  restoredDirectoryFor,
} from "../threadOffload/ThreadOffload.ts";
import { projectLocationFlags, resolveCliAuthConfig } from "./config.ts";

export class ThreadOffloadCliError extends Schema.TaggedError<ThreadOffloadCliError>()(
  "ThreadOffloadCliError",
  { detail: Schema.String },
) {
  override readonly [Runtime.errorReported] = false;
  override get message(): string {
    return this.detail;
  }
}

const isCliError = Schema.is(ThreadOffloadCliError);
const decodeLine = Schema.decodeUnknownEffect(Schema.fromJsonString(ThreadOffloadResponseLine));
const decodeThreadId = Schema.decodeEffect(ThreadId);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeRecord = Schema.decodeUnknownOption(Schema.fromJsonString(ThreadOffloadRecord));

const formatBytes = (bytes: number) =>
  bytes >= 1024 ** 3
    ? `${(bytes / 1024 ** 3).toFixed(2)} GiB`
    : bytes >= 1024 ** 2
      ? `${(bytes / 1024 ** 2).toFixed(1)} MiB`
      : `${Math.ceil(bytes / 1024)} KiB`;

type CliFlags = Parameters<typeof resolveCliAuthConfig>[0];

/**
 * Offloading runs inside the server: it takes each thread's command lock, so it never races a
 * write to those threads, and reads through the server's reader connection.
 */
const withServer = Effect.fn("cli.thread.offload.withServer")(
  function* <A, E, R>(
    flags: CliFlags,
    label: string,
    run: (
      call: <S extends Schema.Top>(
        route: string,
        body: unknown,
        schema: S,
      ) => Effect.Effect<S["Type"], ThreadOffloadCliError, S["DecodingServices"]>,
    ) => Effect.Effect<A, E, R>,
  ) {
    const logLevel = yield* GlobalFlag.LogLevel;
    const config = yield* resolveCliAuthConfig(flags, logLevel);
    return yield* Effect.gen(function* () {
      const runtimeState = yield* readPersistedServerRuntimeState(config.serverRuntimeStatePath);
      if (Option.isNone(runtimeState)) {
        return yield* new ThreadOffloadCliError({
          detail: `No running T3 Code server uses ${config.baseDir}. Start T3 Code, then retry: offloading runs inside it.`,
        });
      }
      const origin = runtimeState.value.origin;
      const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const issued = yield* Effect.acquireRelease(
        environmentAuth.issueSession({
          label,
          scopes: [AuthOrchestrationReadScope, AuthEnvironmentMaintainScope],
        }),
        (session) =>
          environmentAuth.revokeSession(session.sessionId).pipe(Effect.ignore({ log: true })),
      );
      const client = yield* HttpClient.HttpClient;
      const call = Effect.fn("cli.thread.offload.call")(
        function* <S extends Schema.Top>(route: string, body: unknown, schema: S) {
          const response = yield* client.execute(
            HttpClientRequest.post(new URL(route, origin)).pipe(
              HttpClientRequest.bodyJsonUnsafe(body),
              HttpClientRequest.bearerToken(issued.token),
            ),
          );
          if (response.status !== 200) {
            const json = yield* response.json;
            const said =
              typeof json === "object" && json !== null && "message" in json
                ? String(json.message)
                : `status ${response.status}`;
            return yield* new ThreadOffloadCliError({ detail: said });
          }
          const last = yield* response.stream.pipe(
            Stream.decodeText(),
            Stream.splitLines,
            Stream.filter((line) => line.trim().length > 0),
            Stream.mapEffect((line) => decodeLine(line)),
            Stream.filter((line) => line.type !== "heartbeat"),
            Stream.runHead,
          );
          if (Option.isNone(last)) {
            return yield* new ThreadOffloadCliError({
              detail: "T3 Code closed the connection before answering.",
            });
          }
          if (last.value.type === "error") {
            return yield* new ThreadOffloadCliError({ detail: last.value.detail });
          }
          return yield* Schema.decodeUnknownEffect(schema)(last.value.result);
        },
        Effect.mapError((error) =>
          isCliError(error)
            ? error
            : new ThreadOffloadCliError({ detail: `Could not reach T3 Code: ${String(error)}` }),
        ),
      );
      return yield* run(call);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.merge(
          EnvironmentAuth.layerRuntime.pipe(
            Layer.provide(ServerConfig.layer(config)),
            Layer.provide(Layer.succeed(References.MinimumLogLevel, config.logLevel)),
          ),
          FetchHttpClient.layer,
        ),
      ),
    );
  },
  Effect.tapError((error) => (isCliError(error) ? Console.error(error.message) : Effect.void)),
);

const reclaimLine = (reclaimableBytes: number) =>
  `t3 doctor can return ${formatBytes(reclaimableBytes)} to disk: quit T3 Code, then run t3 doctor --fix.`;

const offloadCommand = Command.make("offload", {
  ...projectLocationFlags,
  threads: Argument.String("thread").pipe(
    Argument.withDescription(
      "Offload only the units rooted at these threads. Default: every thread that qualifies.",
    ),
    Argument.variadic,
  ),
  idleDays: Flag.Int("idle-days").pipe(
    Flag.withDescription("Offload threads with no activity for this many days."),
    Flag.withDefault(7),
  ),
  limit: Flag.Int("limit").pipe(
    Flag.withDescription("Offload at most this many units (a thread with its children)."),
    Flag.optional,
  ),
  dryRun: Flag.Boolean("dry-run").pipe(
    Flag.withDescription("List what would be offloaded and how many bytes, changing nothing."),
    Flag.withDefault(false),
  ),
  json: Flag.Boolean("json").pipe(
    Flag.withDescription("Print JSON instead of text."),
    Flag.withDefault(false),
  ),
}).pipe(
  Command.withDescription(
    "Move threads idle for days, with their subagents and forks, out of the database into compressed files under the T3 home's offload folder. Pinned, snoozed, grouped and busy threads stay.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const threadIds = yield* Effect.forEach(flags.threads, (value) =>
        decodeThreadId(String(value).trim()).pipe(
          Effect.mapError(
            () => new ThreadOffloadCliError({ detail: `'${value}' is not a thread id.` }),
          ),
        ),
      );
      const now = yield* DateTime.now;
      const before = DateTime.formatIso(
        DateTime.subtractDuration(now, Duration.days(Math.max(0, flags.idleDays))),
      );
      yield* withServer(flags, "t3 thread offload", (call) =>
        Effect.gen(function* () {
          const plan = yield* call(
            THREAD_OFFLOAD_PLAN_ROUTE,
            {
              before,
              withSizes: flags.dryRun,
              ...(threadIds.length > 0 ? { threadIds } : {}),
              ...Option.match(flags.limit, {
                onNone: () => ({}),
                onSome: (limit) => ({ limit }),
              }),
            },
            ThreadOffloadPlan,
          );
          if (flags.dryRun) {
            if (flags.json) return yield* Console.log(encodeJson(plan));
            for (const unit of plan.units) {
              yield* Console.log(
                `${formatBytes(unit.bytes ?? 0).padStart(10)}  ${String(unit.threadIds.length).padStart(3)} thread(s)  ${unit.lastActivityAt.slice(0, 10)}  ${unit.title}  ${unit.rootThreadId}${unit.remainingParentThreadId === null ? "" : `  (its parent ${unit.remainingParentThreadId} stays)`}`,
              );
            }
            const bytes = plan.units.reduce((sum, unit) => sum + (unit.bytes ?? 0), 0);
            const threads = plan.units.reduce((sum, unit) => sum + unit.threadIds.length, 0);
            yield* Console.log(
              `\nWould offload ${plan.units.length} unit(s), ${threads} thread(s), ${formatBytes(bytes)} of rows, last active before ${before.slice(0, 10)}.${plan.moreUnitCount > 0 ? ` ${plan.moreUnitCount} more unit(s) qualify beyond --limit.` : ""}`,
            );
            for (const kept of plan.kept) {
              yield* Console.log(`Kept ${kept.threadCount} thread(s): ${kept.reason}.`);
            }
            return yield* Console.log(
              `Database: ${formatBytes(plan.storage.databaseBytes)}, ${formatBytes(plan.storage.reclaimableBytes)} reclaimable now. Dry run: nothing changed.`,
            );
          }
          const results: Array<ThreadOffloadResult> = [];
          let reclaimableBytes = plan.storage.reclaimableBytes;
          // Named threads go to the server even when the plan left them out, so it says why.
          const targets =
            threadIds.length > 0
              ? threadIds.map(
                  (threadId) =>
                    plan.units.find((unit) => unit.rootThreadId === threadId) ?? {
                      rootThreadId: threadId,
                      title: threadId,
                    },
                )
              : plan.units;
          for (const unit of targets) {
            const outcome = yield* call(
              THREAD_OFFLOAD_ROUTE,
              { threadId: unit.rootThreadId, before },
              ThreadOffloadResult,
            ).pipe(Effect.result);
            if (outcome._tag === "Failure") {
              yield* Console.error(
                `Kept ${unit.title === unit.rootThreadId ? unit.rootThreadId : `${unit.title} (${unit.rootThreadId})`}: ${outcome.failure.detail}`,
              );
              continue;
            }
            const result = outcome.success;
            results.push(result);
            reclaimableBytes = result.storage.reclaimableBytes;
            if (!flags.json) {
              yield* Console.log(
                `Offloaded ${unit.title} (${result.threadIds.length} thread(s), ${formatBytes(result.rowBytes)} of rows) to ${result.bundlePath} (${formatBytes(result.bundleBytes)}).`,
              );
            }
          }
          if (flags.json) yield* Console.log(encodeJson({ results, reclaimableBytes }));
          if (threadIds.length > 0 && results.length < targets.length) {
            return yield* new ThreadOffloadCliError({
              detail: `${targets.length - results.length} of the named thread(s) stayed.`,
            });
          }
          if (flags.json) return;
          const rowBytes = results.reduce((sum, result) => sum + result.rowBytes, 0);
          yield* Console.log(
            `\nOffloaded ${results.length} unit(s), ${formatBytes(rowBytes)} of rows. ${reclaimLine(reclaimableBytes)}`,
          );
        }),
      );
    }),
  ),
);

const listRecords = Effect.fn("cli.thread.restore.list")(function* (offloadDirectory: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = path.join(offloadDirectory, "records");
  const names = yield* fs.readDirectory(directory).pipe(Effect.orElseSucceed(() => []));
  const records: Array<ThreadOffloadRecord> = [];
  for (const name of names.filter((entry) => entry.endsWith(".json"))) {
    const text = yield* fs.readFileString(path.join(directory, name)).pipe(Effect.option);
    const record = Option.flatMap(text, decodeRecord);
    if (Option.isSome(record)) records.push(record.value);
  }
  return records.toSorted((left, right) => right.offloadedAt.localeCompare(left.offloadedAt));
});

const restoreCommand = Command.make("restore", {
  ...projectLocationFlags,
  thread: Argument.String("thread").pipe(
    Argument.withDescription(
      "An offloaded thread; its whole unit comes back. Without one, list the offloaded threads.",
    ),
    Argument.optional,
  ),
  bundle: Flag.String("bundle").pipe(
    Flag.withDescription(
      "Restore from this bundle file instead of a thread's record, for when the record is lost.",
    ),
    Flag.optional,
  ),
  json: Flag.Boolean("json").pipe(
    Flag.withDescription("Print JSON instead of text."),
    Flag.withDefault(false),
  ),
}).pipe(
  Command.withDescription(
    "Put an offloaded thread back exactly as it was, or list them. The bundle is kept under offload/restored until t3 thread prune-restored.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      if (Option.isSome(flags.bundle)) {
        const path = yield* Path.Path;
        const bundlePath = path.resolve(flags.bundle.value);
        return yield* withServer(flags, "t3 thread restore", (call) =>
          call(THREAD_RESTORE_ROUTE, { bundlePath }, ThreadRestoreResult).pipe(
            Effect.flatMap((result) => printRestored(result, flags.json)),
          ),
        );
      }
      if (Option.isNone(flags.thread)) {
        const logLevel = yield* GlobalFlag.LogLevel;
        const config = yield* resolveCliAuthConfig(flags, logLevel);
        const path = yield* Path.Path;
        const records = yield* listRecords(offloadDirectoryFor(config.baseDir, path));
        if (flags.json) return yield* Console.log(encodeJson({ records }));
        for (const record of records) {
          yield* Console.log(
            `${record.offloadedAt.slice(0, 10)}  ${record.threadId}  ${record.title}${record.parentThreadId === null ? "" : `  (child of ${record.parentThreadId})`}`,
          );
        }
        return;
      }
      const threadId = yield* decodeThreadId(flags.thread.value.trim()).pipe(
        Effect.mapError(
          () => new ThreadOffloadCliError({ detail: `'${flags.thread}' is not a thread id.` }),
        ),
      );
      yield* withServer(flags, "t3 thread restore", (call) =>
        call(THREAD_RESTORE_ROUTE, { threadId }, ThreadRestoreResult).pipe(
          Effect.flatMap((result) => printRestored(result, flags.json)),
        ),
      );
    }),
  ),
);

const printRestored = (result: ThreadRestoreResult, json: boolean) =>
  json
    ? Console.log(encodeJson(result))
    : Console.log(
        `Restored ${result.threadIds.length} thread(s) (${result.rowCount} rows). The bundle stays at ${result.bundlePath}.`,
      );

const pruneRestoredCommand = Command.make("prune-restored", {
  ...projectLocationFlags,
  olderThanDays: Flag.Int("older-than-days").pipe(
    Flag.withDescription("Delete bundles restored at least this many days ago."),
    Flag.withDefault(30),
  ),
  dryRun: Flag.Boolean("dry-run").pipe(
    Flag.withDescription("List what would be deleted, deleting nothing."),
    Flag.withDefault(false),
  ),
}).pipe(
  Command.withDescription(
    "Delete bundles under the offload folder's restored/ whose threads were restored long ago. Restore itself never deletes a bundle.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const logLevel = yield* GlobalFlag.LogLevel;
      const config = yield* resolveCliAuthConfig(flags, logLevel);
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = restoredDirectoryFor(offloadDirectoryFor(config.baseDir, path), path);
      const names = yield* fs.readDirectory(directory).pipe(Effect.orElseSucceed(() => []));
      const cutoff =
        DateTime.toEpochMillis(yield* DateTime.now) -
        Duration.toMillis(Duration.days(Math.max(0, flags.olderThanDays)));
      let count = 0;
      let bytes = 0;
      for (const name of names.filter((entry) => entry.endsWith(".jsonl.zst"))) {
        const file = path.join(directory, name);
        const info = yield* fs.stat(file).pipe(Effect.option);
        if (Option.isNone(info)) continue;
        const modified = Option.getOrUndefined(info.value.mtime)?.getTime();
        if (modified === undefined || modified > cutoff) continue;
        if (!flags.dryRun) {
          yield* fs
            .remove(file)
            .pipe(
              Effect.mapError(
                () => new ThreadOffloadCliError({ detail: `Could not delete ${file}.` }),
              ),
            );
        }
        count += 1;
        bytes += Number(info.value.size);
        yield* Console.log(`${flags.dryRun ? "Would delete" : "Deleted"} ${file}`);
      }
      yield* Console.log(
        `${flags.dryRun ? "Would delete" : "Deleted"} ${count} restored bundle(s), ${formatBytes(bytes)}.`,
      );
    }),
  ),
);

/** For `t3 thread show`: where an offloaded thread went, or nothing. */
export const offloadNotice = Effect.fn("cli.thread.offloadNotice")(function* (
  baseDir: string,
  threadId: string,
) {
  const path = yield* Path.Path;
  const record = yield* readRecord(offloadDirectoryFor(baseDir, path), threadId);
  return Option.map(
    record,
    (value) =>
      `Thread ${threadId} (${value.title}) was offloaded on ${value.offloadedAt.slice(0, 10)} to ${path.join(offloadDirectoryFor(baseDir, path), value.bundle)}. Bring it back with: t3 thread restore ${threadId}`,
  );
});

export const threadOffloadCommand = offloadCommand;
export const threadRestoreCommand = restoreCommand;
export const threadPruneRestoredCommand = pruneRestoredCommand;
