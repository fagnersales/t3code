import * as Schema from "effect/Schema";

import { IsoDateTime, NonNegativeInt, ThreadId } from "./baseSchemas.ts";

/**
 * Fork-only (fagnersales/t3code): moves idle threads out of the database into compressed bundles
 * under `<T3 home>/offload` and puts them back. A unit is a thread with every thread that reads
 * it (subagent children and forks, recursively), so no remaining thread reads a removed one.
 */
export const THREAD_OFFLOAD_PLAN_ROUTE = "/api/thread-offload/v1/plan";
export const THREAD_OFFLOAD_ROUTE = "/api/thread-offload/v1/offload";
export const THREAD_RESTORE_ROUTE = "/api/thread-offload/v1/restore";

/**
 * The routes answer with lines of JSON: heartbeats while the work runs (a unit can hold many
 * GiB, past fetch's five-minute wait for a response), then one result or error.
 */
export const ThreadOffloadResponseLine = Schema.Union([
  Schema.Struct({ type: Schema.Literal("heartbeat") }),
  /** The route's result, encoded with its own schema. */
  Schema.Struct({ type: Schema.Literal("result"), result: Schema.Unknown }),
  Schema.Struct({
    type: Schema.Literal("error"),
    reason: Schema.Literals(["not-found", "busy", "conflict", "failed"]),
    detail: Schema.String,
  }),
]);
export type ThreadOffloadResponseLine = typeof ThreadOffloadResponseLine.Type;

export const ThreadOffloadPlanRequest = Schema.Struct({
  /** Every thread of a unit must have had its last event before this. */
  before: IsoDateTime,
  limit: Schema.optional(NonNegativeInt),
  /** Only the units rooted at these threads. */
  threadIds: Schema.optional(Schema.Array(ThreadId)),
  /** Sum the bytes each unit holds; reads every row's header, so it takes a while. */
  withSizes: Schema.Boolean,
});
export type ThreadOffloadPlanRequest = typeof ThreadOffloadPlanRequest.Type;

export const ThreadOffloadUnit = Schema.Struct({
  rootThreadId: ThreadId,
  title: Schema.String,
  /** Root first, then its descendants; children are removed before their parents. */
  threadIds: Schema.Array(ThreadId),
  /** The root's parent when it stays behind, so it keeps a link to a thread that is offloaded. */
  remainingParentThreadId: Schema.NullOr(ThreadId),
  lastActivityAt: IsoDateTime,
  /** Bytes of row content, when sizes were asked for. */
  bytes: Schema.NullOr(NonNegativeInt),
});
export type ThreadOffloadUnit = typeof ThreadOffloadUnit.Type;

export const ThreadOffloadStorage = Schema.Struct({
  databaseBytes: NonNegativeInt,
  /** What `t3 doctor --fix` returns to disk. */
  reclaimableBytes: NonNegativeInt,
});
export type ThreadOffloadStorage = typeof ThreadOffloadStorage.Type;

export const ThreadOffloadPlan = Schema.Struct({
  units: Schema.Array(ThreadOffloadUnit),
  /** Units that qualify beyond `limit`. */
  moreUnitCount: NonNegativeInt,
  /** Threads kept because they or a thread of their unit are busy, by reason. */
  kept: Schema.Array(Schema.Struct({ reason: Schema.String, threadCount: NonNegativeInt })),
  storage: ThreadOffloadStorage,
});
export type ThreadOffloadPlan = typeof ThreadOffloadPlan.Type;

export const ThreadOffloadRequest = Schema.Struct({
  threadId: ThreadId,
  before: IsoDateTime,
});
export type ThreadOffloadRequest = typeof ThreadOffloadRequest.Type;

export const ThreadOffloadResult = Schema.Struct({
  rootThreadId: ThreadId,
  threadIds: Schema.Array(ThreadId),
  bundlePath: Schema.String,
  bundleBytes: NonNegativeInt,
  rowCount: NonNegativeInt,
  rowBytes: NonNegativeInt,
  storage: ThreadOffloadStorage,
});
export type ThreadOffloadResult = typeof ThreadOffloadResult.Type;

export const ThreadRestoreRequest = Schema.Struct({ threadId: ThreadId });
export type ThreadRestoreRequest = typeof ThreadRestoreRequest.Type;

export const ThreadRestoreResult = Schema.Struct({
  rootThreadId: ThreadId,
  /** Threads put back; members that were never removed are left as they are. */
  threadIds: Schema.Array(ThreadId),
  rowCount: NonNegativeInt,
});
export type ThreadRestoreResult = typeof ThreadRestoreResult.Type;

/** What `<T3 home>/offload/records/<key>.json` says about one offloaded thread. */
export const ThreadOffloadRecord = Schema.Struct({
  threadId: ThreadId,
  rootThreadId: ThreadId,
  title: Schema.String,
  parentThreadId: Schema.NullOr(ThreadId),
  /** Relative to the offload folder. */
  bundle: Schema.String,
  offloadedAt: IsoDateTime,
});
export type ThreadOffloadRecord = typeof ThreadOffloadRecord.Type;
