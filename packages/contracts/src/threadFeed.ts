import * as Schema from "effect/Schema";

import { ProviderApprovalDecision, ProviderApprovalOption } from "./providerPolicy.ts";
import { OrchestrationV2RunStatus, OrchestrationV2UserInputQuestion } from "./orchestrationV2.ts";

/**
 * A thread's live feed, one JSON object per line. Fork-only (fagnersales/t3code); the shapes
 * are documented in docs/fork/thread-feed.md. Bump the version for any change a reader of an
 * older version would misread; adding an optional field or a new `type` keeps it.
 */
export const THREAD_FEED_VERSION = 1 as const;
export const THREAD_FEED_ROUTE = "/api/thread-feed/v1";

const Version = Schema.Literal(THREAD_FEED_VERSION);
/** Event-log sequence: resume with `--after` (or `after=`) set to the last one you processed. */
const Position = Schema.Int;

export const ThreadFeedRun = Schema.Struct({
  runId: Schema.String,
  ordinal: Schema.Int,
  status: OrchestrationV2RunStatus,
  requestedAt: Schema.String,
  startedAt: Schema.NullOr(Schema.String),
  completedAt: Schema.NullOr(Schema.String),
});
export type ThreadFeedRun = typeof ThreadFeedRun.Type;

export const ThreadFeedTool = Schema.Struct({
  name: Schema.NullOr(Schema.String),
  input: Schema.optionalKey(Schema.String),
  output: Schema.optionalKey(Schema.String),
  exitCode: Schema.optionalKey(Schema.Int),
  /** `input` or `output` was cut to the feed's limit. */
  truncated: Schema.optionalKey(Schema.Boolean),
});

export const ThreadFeedItem = Schema.Struct({
  itemId: Schema.String,
  runId: Schema.NullOr(Schema.String),
  ordinal: Schema.Int,
  /** The turn item's type, such as `user_message`, `assistant_message`, `command_execution`. */
  kind: Schema.String,
  status: Schema.String,
  title: Schema.NullOr(Schema.String),
  role: Schema.optionalKey(Schema.Literals(["user", "assistant"])),
  /** Messages, reasoning, plans and notices: the whole text so far. */
  text: Schema.optionalKey(Schema.String),
  streaming: Schema.optionalKey(Schema.Boolean),
  createdBy: Schema.optionalKey(Schema.String),
  tool: Schema.optionalKey(ThreadFeedTool),
  requestId: Schema.optionalKey(Schema.String),
  startedAt: Schema.NullOr(Schema.String),
  completedAt: Schema.NullOr(Schema.String),
  updatedAt: Schema.String,
});
export type ThreadFeedItem = typeof ThreadFeedItem.Type;

export const ThreadFeedRequest = Schema.Struct({
  requestId: Schema.String,
  runId: Schema.NullOr(Schema.String),
  state: Schema.Literals(["open", "closed"]),
  status: Schema.Literals(["pending", "resolved", "expired", "cancelled"]),
  /** `question` answers with answers keyed by question id; `approval` with a decision. */
  kind: Schema.Literals(["question", "approval", "other"]),
  requestKind: Schema.String,
  questions: Schema.optionalKey(Schema.Array(OrchestrationV2UserInputQuestion)),
  prompt: Schema.optionalKey(Schema.String),
  reason: Schema.optionalKey(Schema.String),
  options: Schema.optionalKey(Schema.Array(ProviderApprovalOption)),
  decision: Schema.optionalKey(ProviderApprovalDecision),
  createdAt: Schema.String,
});
export type ThreadFeedRequest = typeof ThreadFeedRequest.Type;

const ThreadEvent = <const Type extends string, Fields extends Schema.Struct.Fields>(
  type: Type,
  fields: Fields,
) =>
  Schema.Struct({
    v: Version,
    type: Schema.Literal(type),
    threadId: Schema.String,
    position: Position,
    ...fields,
  });

export const ThreadFeedEvent = Schema.Union([
  /** The thread's state now, after any catch-up. Live events follow. */
  ThreadEvent("ready", {
    thread: Schema.Struct({
      title: Schema.String,
      /** The latest run's status, or `idle` before the first run. */
      status: Schema.Union([Schema.Literal("idle"), OrchestrationV2RunStatus]),
      archived: Schema.Boolean,
    }),
    run: Schema.NullOr(ThreadFeedRun),
    requests: Schema.Array(ThreadFeedRequest),
  }),
  /** A new item, or an item whose status or content changed: upsert it by `itemId`. */
  ThreadEvent("item", { item: ThreadFeedItem }),
  /** Text appended to a streaming item since its last `item` or `text` event. */
  ThreadEvent("text", {
    itemId: Schema.String,
    runId: Schema.NullOr(Schema.String),
    delta: Schema.String,
    /** The item's text length after this delta, to detect a missed delta. */
    length: Schema.Int,
  }),
  ThreadEvent("run", { run: ThreadFeedRun }),
  ThreadEvent("request", { request: ThreadFeedRequest }),
  /** The thread cannot be followed (for example it does not exist); its feed ends. */
  ThreadEvent("error", { message: Schema.String }),
  Schema.Struct({ v: Version, type: Schema.Literal("heartbeat") }),
  /** Printed by `t3 thread follow` only: the connection dropped and is retried. */
  Schema.Struct({
    v: Version,
    type: Schema.Literal("disconnected"),
    reason: Schema.String,
    retryInMs: Schema.Int,
  }),
]);
export type ThreadFeedEvent = typeof ThreadFeedEvent.Type;
