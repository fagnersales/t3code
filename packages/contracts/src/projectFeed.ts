import * as Schema from "effect/Schema";

import { OrchestrationV2RunStatus } from "./orchestrationV2.ts";

/**
 * A project's live thread list, one JSON object per line. Fork-only (fagnersales/t3code); the
 * shapes are documented in docs/fork/project-feed.md. Bump the version for any change a reader of
 * an older version would misread; adding an optional field, a `changes` value or a new `type`
 * keeps it.
 */
export const PROJECT_FEED_VERSION = 1 as const;
export const PROJECT_FEED_ROUTE = "/api/project-feed/v1";

const Version = Schema.Literal(PROJECT_FEED_VERSION);
/** Event-log sequence: resume with `--after` (or `after=`) set to the last one you processed. */
const Position = Schema.Int;

export const ProjectFeedRun = Schema.Struct({
  runId: Schema.String,
  status: OrchestrationV2RunStatus,
  requestedAt: Schema.NullOr(Schema.String),
  startedAt: Schema.NullOr(Schema.String),
  completedAt: Schema.NullOr(Schema.String),
});
export type ProjectFeedRun = typeof ProjectFeedRun.Type;

export const ProjectFeedPendingRequest = Schema.Struct({
  requestId: Schema.String,
  /** The runtime request kind, such as `user_input` or `command_approval`. */
  kind: Schema.String,
  createdAt: Schema.String,
  /** False for questions the run keeps working past. */
  blocking: Schema.Boolean,
});
export type ProjectFeedPendingRequest = typeof ProjectFeedPendingRequest.Type;

export const ProjectFeedThread = Schema.Struct({
  threadId: Schema.String,
  projectId: Schema.String,
  title: Schema.String,
  /** As in `t3 thread list`: the active run's status, else the latest run's, else `idle`. */
  status: Schema.Union([Schema.Literal("idle"), OrchestrationV2RunStatus]),
  /** The latest run, or null before the first. */
  run: Schema.NullOr(ProjectFeedRun),
  /** Open questions and approvals. */
  pendingRequests: Schema.Int,
  /** The newest open one, or null. */
  pendingRequest: Schema.NullOr(ProjectFeedPendingRequest),
  settled: Schema.Boolean,
  settledAt: Schema.NullOr(Schema.String),
  snoozed: Schema.Boolean,
  snoozedUntil: Schema.NullOr(Schema.String),
  archived: Schema.Boolean,
  archivedAt: Schema.NullOr(Schema.String),
  parentThreadId: Schema.NullOr(Schema.String),
  relationshipToParent: Schema.NullOr(Schema.Literals(["fork", "subagent"])),
  createdBy: Schema.String,
  creationSource: Schema.String,
  providerInstanceId: Schema.String,
  model: Schema.String,
  branch: Schema.NullOr(Schema.String),
  worktreePath: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
export type ProjectFeedThread = typeof ProjectFeedThread.Type;

/** What a live `thread` event changed, compared with the last line sent for that thread. */
export const ProjectFeedChange = Schema.Literals([
  "created",
  "title",
  "status",
  "run",
  "requests",
  "settled",
  "snoozed",
  "archived",
  "parent",
  "model",
  "branch",
  "worktree",
]);
export type ProjectFeedChange = typeof ProjectFeedChange.Type;

const ProjectEvent = <const Type extends string, Fields extends Schema.Struct.Fields>(
  type: Type,
  fields: Fields,
) =>
  Schema.Struct({
    v: Version,
    type: Schema.Literal(type),
    projectId: Schema.String,
    position: Position,
    ...fields,
  });

export const ProjectFeedEvent = Schema.Union([
  /**
   * Forget the threads you hold for this project: the `thread` lines from here to `ready` are all
   * of them. Only sent when a feed starts without a position.
   */
  ProjectEvent("snapshot", {
    /** The project as the caller named it (id or path). */
    ref: Schema.String,
    threads: Schema.Int,
  }),
  /** A thread's state now: upsert it by `thread.threadId`. */
  ProjectEvent("thread", {
    thread: ProjectFeedThread,
    /** Live updates only: what changed. Absent in snapshots and resumes. */
    changes: Schema.optionalKey(Schema.Array(ProjectFeedChange)),
  }),
  /** The thread was deleted. */
  ProjectEvent("removed", {
    threadId: Schema.String,
    reason: Schema.Literal("deleted"),
  }),
  /** Catch-up is done; live events follow. */
  ProjectEvent("ready", {
    ref: Schema.String,
    title: Schema.String,
    workspaceRoot: Schema.String,
    /** How many threads the reader should now hold (archived ones only with `--archived`). */
    threads: Schema.Int,
    /** True when the feed sent only what changed after the given position. */
    resumed: Schema.Boolean,
  }),
  /** This project cannot be followed (not found, or deleted). Its feed ends; others go on. */
  Schema.Struct({
    v: Version,
    type: Schema.Literal("error"),
    ref: Schema.String,
    projectId: Schema.NullOr(Schema.String),
    position: Schema.NullOr(Position),
    message: Schema.String,
  }),
  Schema.Struct({ v: Version, type: Schema.Literal("heartbeat") }),
  /** Printed by `t3 project follow` only: the connection dropped and is retried. */
  Schema.Struct({
    v: Version,
    type: Schema.Literal("disconnected"),
    reason: Schema.String,
    retryInMs: Schema.Int,
  }),
]);
export type ProjectFeedEvent = typeof ProjectFeedEvent.Type;
