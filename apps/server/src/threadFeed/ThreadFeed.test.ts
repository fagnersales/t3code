import { assert, it } from "@effect/vitest";
import { type ThreadFeedEvent, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import { ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION } from "../orchestration-v2/ProjectionStore.ts";
import { EVENT_LOG_PRUNED_THROUGH } from "../persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ThreadFeed from "./ThreadFeed.ts";
import {
  assistantMessage,
  commandItem,
  layerFeedOn,
  NodeId,
  questionItem,
  run,
  RuntimeRequestId,
  runtimeRequest,
  seedThread,
  userMessage,
  writeEvents,
} from "./threadFeedTestkit.ts";

const layer = it.layer(layerFeedOn(SqlitePersistence.layerMemory));

const follow = Effect.fn("test.follow")(function* (input: ThreadFeed.ThreadFeedFollowInput) {
  const feed = yield* ThreadFeed.ThreadFeed;
  const events = yield* Queue.unbounded<ThreadFeedEvent>();
  yield* feed.follow(input).pipe(
    Stream.runForEach((event) => Queue.offer(events, event)),
    Effect.forkScoped,
  );
  return events;
});

/** Takes events up to and including the first one of `type`. */
const takeThrough = Effect.fn("test.takeThrough")(function* (
  events: Queue.Queue<ThreadFeedEvent>,
  type: ThreadFeedEvent["type"],
) {
  const taken: Array<ThreadFeedEvent> = [];
  while (true) {
    const event = yield* Queue.take(events);
    taken.push(event);
    if (event.type === type) return taken;
  }
});

const summary = (event: ThreadFeedEvent) => {
  switch (event.type) {
    case "item":
      return `item ${event.item.kind} ${event.item.status} ${event.item.text ?? ""}`.trim();
    case "text":
      return `text +${JSON.stringify(event.delta)} =${event.length}`;
    case "run":
      return `run ${event.run.status}`;
    case "request":
      return `request ${event.request.kind} ${event.request.state}`;
    default:
      return event.type;
  }
};

layer("ThreadFeed", (it) => {
  it.effect("after a position, sends only later changes, then ready, then live events", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const threadId = yield* seedThread("Resume");
        const items = Array.from({ length: 50 }, (_, index) =>
          index % 2 === 0
            ? userMessage(threadId, index, `question ${index}`, now)
            : commandItem(threadId, index, `output ${index}`, now),
        );
        yield* writeEvents(
          threadId,
          items.map((payload) => ({ type: "turn-item.updated" as const, payload })),
        );
        const position = yield* writeEvents(threadId, [
          { type: "run.updated", payload: run(threadId, 1, "running", now) },
        ]);
        yield* writeEvents(threadId, [
          {
            type: "turn-item.updated",
            payload: assistantMessage(threadId, 50, "Hel", true, now),
          },
        ]);

        const events = yield* follow({ threadId, after: position });
        const caughtUp = yield* takeThrough(events, "ready");
        assert.deepEqual(caughtUp.map(summary), ["item assistant_message running Hel", "ready"]);
        const ready = caughtUp.at(-1);
        assert.equal(ready?.type === "ready" ? ready.run?.status : null, "running");
        assert.isTrue(caughtUp.every((event) => "position" in event && event.position > position));

        yield* writeEvents(threadId, [
          {
            type: "turn-item.updated",
            payload: assistantMessage(threadId, 50, "Hello, wor", true, now),
          },
        ]);
        yield* writeEvents(threadId, [
          {
            type: "turn-item.updated",
            payload: assistantMessage(threadId, 50, "Hello, world.", false, now),
          },
          { type: "run.updated", payload: run(threadId, 1, "completed", now) },
        ]);
        const live = [
          yield* Queue.take(events),
          yield* Queue.take(events),
          yield* Queue.take(events),
        ];
        assert.deepEqual(live.map(summary), [
          'text +"lo, wor" =10',
          "item assistant_message completed Hello, world.",
          "run completed",
        ]);
      }),
    ),
  );

  it.effect("after a position whose events were pruned, starts over from the newest items", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const now = yield* DateTime.now;
        const threadId = yield* seedThread("Pruned");
        const position = yield* writeEvents(threadId, [
          { type: "turn-item.updated", payload: userMessage(threadId, 0, "old", now) },
        ]);
        const pruned = yield* writeEvents(threadId, [
          { type: "turn-item.updated", payload: userMessage(threadId, 1, "pruned", now) },
        ]);
        yield* writeEvents(threadId, [
          { type: "turn-item.updated", payload: userMessage(threadId, 2, "kept", now) },
        ]);
        yield* sql`
          INSERT INTO orchestration_v2_projection_metadata (
            projection_name, schema_version, last_sequence, updated_at
          )
          VALUES (
            ${EVENT_LOG_PRUNED_THROUGH},
            ${ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
            ${pruned},
            ${DateTime.formatIso(now)}
          )
          ON CONFLICT(projection_name) DO UPDATE SET last_sequence = excluded.last_sequence
        `;

        const events = yield* follow({ threadId, after: position });
        const start = yield* takeThrough(events, "ready");
        // The replay would have started after `position`; the pruned change still arrives.
        assert.deepEqual(start.map(summary), [
          "item user_message completed old",
          "item user_message completed pruned",
          "item user_message completed kept",
          "ready",
        ]);
      }),
    ),
  );

  it.effect("without a position, sends the tail and the current state, then live events", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const threadId = yield* seedThread("Fresh");
        yield* writeEvents(
          threadId,
          Array.from({ length: 20 }, (_, index) => ({
            type: "turn-item.updated" as const,
            payload: userMessage(threadId, index, `message ${index}`, now),
          })),
        );
        const events = yield* follow({ threadId, tail: 2 });
        const start = yield* takeThrough(events, "ready");
        assert.deepEqual(start.map(summary), [
          "item user_message completed message 18",
          "item user_message completed message 19",
          "ready",
        ]);
        const ready = start.at(-1);
        assert.equal(ready?.type === "ready" ? ready.thread.title : null, "Fresh");

        yield* writeEvents(threadId, [
          { type: "turn-item.updated", payload: userMessage(threadId, 20, "new", now) },
        ]);
        assert.equal(summary(yield* Queue.take(events)), "item user_message completed new");
      }),
    ),
  );

  it.effect("reports a question when it opens and when it is answered", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const threadId = yield* seedThread("Question");
        const requestId = RuntimeRequestId.make(`${threadId}:request`);
        const nodeId = NodeId.make(`${threadId}:node`);
        const events = yield* follow({ threadId });
        yield* takeThrough(events, "ready");

        yield* writeEvents(threadId, [
          {
            type: "runtime-request.updated",
            payload: runtimeRequest(requestId, nodeId, "pending", now),
          },
          { type: "turn-item.updated", payload: questionItem(threadId, 0, requestId, nodeId, now) },
        ]);
        const opened = yield* takeThrough(events, "request");
        const request = opened.at(-1);
        assert.deepEqual(
          request?.type === "request"
            ? {
                state: request.request.state,
                kind: request.request.kind,
                questions: request.request.questions?.map((question) => question.id),
              }
            : null,
          { state: "open", kind: "question", questions: ["deploy"] },
        );

        yield* writeEvents(threadId, [
          {
            type: "runtime-request.updated",
            payload: runtimeRequest(requestId, nodeId, "resolved", now),
          },
        ]);
        const answered = yield* takeThrough(events, "request");
        assert.deepEqual(answered.map(summary), [
          "item user_input_request waiting",
          "request question closed",
        ]);
      }),
    ),
  );

  it.effect("a reader that resumes while a question waits gets it in ready", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const threadId = yield* seedThread("Waiting");
        const requestId = RuntimeRequestId.make(`${threadId}:request`);
        const nodeId = NodeId.make(`${threadId}:node`);
        const position = yield* writeEvents(threadId, [
          {
            type: "runtime-request.updated",
            payload: runtimeRequest(requestId, nodeId, "pending", now),
          },
          { type: "turn-item.updated", payload: questionItem(threadId, 0, requestId, nodeId, now) },
        ]);
        const events = yield* follow({ threadId, after: position });
        const [ready] = yield* takeThrough(events, "ready");
        assert.deepEqual(
          ready?.type === "ready"
            ? ready.requests.map((entry) => [entry.requestId, entry.kind, entry.state])
            : null,
          [[requestId, "question", "open"]],
        );
      }),
    ),
  );

  it.effect("an unknown thread ends its feed with an error", () =>
    Effect.gen(function* () {
      const feed = yield* ThreadFeed.ThreadFeed;
      const events = yield* feed
        .follow({ threadId: ThreadId.make("thread-missing") })
        .pipe(Stream.runCollect);
      assert.deepEqual(
        Array.from(events).map((event) => event.type),
        ["error"],
      );
    }),
  );
});

it("a streaming item that rewrites its text is sent whole instead of as a delta", () => {
  const threadId = ThreadId.make("thread-rewrite");
  const now = DateTime.makeUnsafe(0);
  const translator = ThreadFeed.makeThreadFeedTranslator(threadId);
  const stored = (sequence: number, text: string) =>
    ({
      sequence,
      commandId: null,
      event: {
        id: `event-${sequence}`,
        type: "turn-item.updated",
        threadId,
        occurredAt: now,
        payload: assistantMessage(threadId, 0, text, true, now),
      },
    }) as unknown as Parameters<typeof translator.translate>[0];
  assert.deepEqual(translator.translate(stored(1, "Draft")).map(summary), [
    "item assistant_message running Draft",
  ]);
  assert.deepEqual(translator.translate(stored(2, "Draft two")).map(summary), ['text +" two" =9']);
  assert.deepEqual(translator.translate(stored(3, "Final")).map(summary), [
    "item assistant_message running Final",
  ]);
});
