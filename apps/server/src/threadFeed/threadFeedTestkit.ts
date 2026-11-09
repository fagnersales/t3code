import {
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type ProjectId,
  ProviderInstanceId,
  RunId,
  RuntimeRequestId,
  type ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import type * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadFeed from "./ThreadFeed.ts";

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;

/** The feed over a bare event sink and projection store, as tests and the benchmark need it. */
export const layerFeedOn = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>) => {
  const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(Layer.provide(database));
  const sink = EventSink.layer.pipe(Layer.provide(Layer.mergeAll(stores, database)));
  const feed = Layer.effect(
    ThreadFeed.ThreadFeed,
    Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const sourceError = (cause: unknown) => new ThreadFeed.ThreadFeedSourceError({ cause });
      return yield* ThreadFeed.make({
        latestSequence: (threadId) =>
          eventSink.latestSequence({ threadId }).pipe(Effect.mapError(sourceError)),
        eventsAfter: (threadId, afterSequence) =>
          eventSink
            .stream({ threadId, afterSequence, bounded: true })
            .pipe(Stream.mapError(sourceError)),
      });
    }),
  );
  return Layer.mergeAll(sink, stores, IdAllocator.layer, database).pipe(
    Layer.provideMerge(feed.pipe(Layer.provide(Layer.mergeAll(sink, stores, database)))),
  );
};

export const writeEvents = Effect.fn("threadFeedTestkit.writeEvents")(function* (
  threadId: ThreadId,
  events: ReadonlyArray<
    | { readonly type: "turn-item.updated"; readonly payload: OrchestrationV2TurnItem }
    | { readonly type: "run.updated"; readonly payload: OrchestrationV2Run }
    | { readonly type: "runtime-request.updated"; readonly payload: OrchestrationV2RuntimeRequest }
  >,
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const domain: Array<OrchestrationV2DomainEvent> = [];
  for (const event of events) {
    domain.push({
      id: yield* idAllocator.allocate.event({ threadId }),
      threadId,
      occurredAt: now,
      ...event,
    } as OrchestrationV2DomainEvent);
  }
  const stored = yield* eventSink.write({ events: domain });
  return stored.at(-1)?.sequence ?? 0;
});

export const seedThread = (title: string) =>
  seedThreadRecord(title).pipe(Effect.map((thread) => thread.id));

/** A new thread in its own project, or in `projectId`; a subagent of `parentThreadId`. */
export const seedThreadRecord = Effect.fn("threadFeedTestkit.seedThreadRecord")(function* (
  title: string,
  options?: { readonly projectId?: ProjectId; readonly parentThreadId?: ThreadId },
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const projectId =
    options?.projectId ?? (yield* idAllocator.allocate.project({ fixtureName: "thread-feed" }));
  const threadId = yield* idAllocator.allocate.thread({ fixtureName: "thread-feed", projectId });
  const parent = options?.parentThreadId;
  const thread: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId,
    title,
    providerInstanceId: modelSelection.instanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    branchPullRequest: null,
    activeOrderKey: null,
    activeProviderThreadId: null,
    lineage:
      parent === undefined
        ? { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId }
        : { parentThreadId: parent, relationshipToParent: "subagent", rootThreadId: parent },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  yield* eventSink.write({
    events: [
      {
        id: yield* idAllocator.allocate.event({ threadId }),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: thread,
      },
    ],
  });
  return thread;
});

/** Writes a thread lifecycle event (rename, settle, archive, delete…) with the changed thread. */
export const writeThreadEvent = Effect.fn("threadFeedTestkit.writeThreadEvent")(function* (
  type: Exclude<
    Extract<OrchestrationV2DomainEvent, { payload: OrchestrationV2AppThread }>["type"],
    "thread.created"
  >,
  thread: OrchestrationV2AppThread,
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const stored = yield* eventSink.write({
    events: [
      {
        id: yield* idAllocator.allocate.event({ threadId: thread.id }),
        type,
        threadId: thread.id,
        occurredAt: now,
        payload: { ...thread, updatedAt: now },
      } as OrchestrationV2DomainEvent,
    ],
  });
  return stored.at(-1)?.sequence ?? 0;
});

const baseItem = (threadId: ThreadId, ordinal: number, now: DateTime.Utc) => ({
  id: TurnItemId.make(`${threadId}:item-${ordinal}`),
  threadId,
  runId: null,
  nodeId: null,
  providerThreadId: null,
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  ordinal,
  title: null,
  startedAt: now,
  completedAt: null,
  updatedAt: now,
});

export const userMessage = (
  threadId: ThreadId,
  ordinal: number,
  text: string,
  now: DateTime.Utc,
): OrchestrationV2TurnItem => ({
  ...baseItem(threadId, ordinal, now),
  type: "user_message",
  status: "completed",
  completedAt: now,
  createdBy: "user",
  creationSource: "web",
  messageId: MessageId.make(`${threadId}:message-${ordinal}`),
  inputIntent: "turn_start",
  text,
  attachments: [],
});

export const assistantMessage = (
  threadId: ThreadId,
  ordinal: number,
  text: string,
  streaming: boolean,
  now: DateTime.Utc,
): OrchestrationV2TurnItem => ({
  ...baseItem(threadId, ordinal, now),
  type: "assistant_message",
  status: streaming ? "running" : "completed",
  completedAt: streaming ? null : now,
  messageId: MessageId.make(`${threadId}:message-${ordinal}`),
  text,
  streaming,
});

export const commandItem = (
  threadId: ThreadId,
  ordinal: number,
  output: string,
  now: DateTime.Utc,
): OrchestrationV2TurnItem => ({
  ...baseItem(threadId, ordinal, now),
  type: "command_execution",
  status: "completed",
  completedAt: now,
  input: `echo ${ordinal}`,
  output,
  exitCode: 0,
});

export const questionItem = (
  threadId: ThreadId,
  ordinal: number,
  requestId: RuntimeRequestId,
  nodeId: NodeId,
  now: DateTime.Utc,
): OrchestrationV2TurnItem => ({
  ...baseItem(threadId, ordinal, now),
  nodeId,
  type: "user_input_request",
  status: "waiting",
  requestId,
  questions: [
    {
      id: "deploy",
      header: "Deploy",
      question: "Deploy now?",
      options: [
        { label: "Yes", description: "Ship it" },
        { label: "No", description: "Wait" },
      ],
    },
  ],
});

export const runtimeRequest = (
  requestId: RuntimeRequestId,
  nodeId: NodeId,
  status: OrchestrationV2RuntimeRequest["status"],
  now: DateTime.Utc,
): OrchestrationV2RuntimeRequest => ({
  id: requestId,
  nodeId,
  providerTurnId: null,
  nativeRequestRef: null,
  kind: "user_input",
  status,
  responseCapability: { type: "message" },
  createdAt: now,
  resolvedAt: status === "pending" ? null : now,
});

export const run = (
  threadId: ThreadId,
  ordinal: number,
  status: OrchestrationV2Run["status"],
  now: DateTime.Utc,
): OrchestrationV2Run => ({
  id: RunId.make(`${threadId}:run-${ordinal}`),
  threadId,
  ordinal,
  providerInstanceId: modelSelection.instanceId,
  modelSelection,
  providerThreadId: null,
  userMessageId: MessageId.make(`${threadId}:message-run-${ordinal}`),
  rootNodeId: null,
  activeAttemptId: null,
  status,
  requestedAt: now,
  startedAt: now,
  completedAt: status === "completed" ? now : null,
  checkpointId: null,
  contextHandoffId: null,
});

export { NodeId, RuntimeRequestId };
