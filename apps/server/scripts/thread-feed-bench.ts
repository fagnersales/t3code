// @effect-diagnostics nodeBuiltinImport:off globalDateInEffect:off - a benchmark measures wall time.
/**
 * The thread feed's cost on a synthetic thread, on a fresh SQLite file in a temp folder:
 *   node scripts/thread-feed-bench.ts [items=30000]
 * Never point it at real data; it creates and deletes its own folder.
 */
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";

import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type { OrchestrationV2TurnItem, ThreadFeedEvent, ThreadId } from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as ProjectionStore from "../src/orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../src/persistence/Sqlite.ts";
import * as ThreadFeed from "../src/threadFeed/ThreadFeed.ts";
import {
  assistantMessage,
  commandItem,
  layerFeedOn,
  seedThread,
  userMessage,
  writeEvents,
} from "../src/threadFeed/threadFeedTestkit.ts";

const ITEMS = Number(process.argv[2] ?? 30_000);
const SMALL_ITEMS = 1_000;
const RUNS = 20;
const BATCH = 500;

const median = (values: ReadonlyArray<number>) => {
  const sorted = values.toSorted((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
};
const ms = (value: number) => `${value.toFixed(2)} ms`;

const timed = Effect.fn("bench.timed")(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
  const start = NodePerfHooks.performance.now();
  const result = yield* effect;
  return { result, ms: NodePerfHooks.performance.now() - start };
});

const fillThread = Effect.fn("bench.fillThread")(function* (title: string, count: number) {
  const now = yield* DateTime.now;
  const threadId = yield* seedThread(title);
  let position = 0;
  for (let start = 0; start < count; start += BATCH) {
    const items: Array<OrchestrationV2TurnItem> = [];
    for (let ordinal = start; ordinal < Math.min(count, start + BATCH); ordinal++) {
      const text = `message ${ordinal} `.repeat(20);
      items.push(
        ordinal % 3 === 0
          ? userMessage(threadId, ordinal, text, now)
          : ordinal % 3 === 1
            ? assistantMessage(threadId, ordinal, text, false, now)
            : commandItem(threadId, ordinal, text.repeat(5), now),
      );
    }
    position = yield* writeEvents(
      threadId,
      items.map((payload) => ({ type: "turn-item.updated" as const, payload })),
    );
  }
  return { threadId, position };
});

/** Time from subscribing to the feed's `ready` event, and the events before it. */
const timeToReady = Effect.fn("bench.timeToReady")(function* (
  input: ThreadFeed.ThreadFeedFollowInput,
) {
  const feed = yield* ThreadFeed.ThreadFeed;
  const start = NodePerfHooks.performance.now();
  const events = yield* feed.follow(input).pipe(
    Stream.takeUntil((event) => event.type === "ready"),
    Stream.runCollect,
  );
  return { ms: NodePerfHooks.performance.now() - start, events: events.length };
});

const liveLatency = Effect.fn("bench.liveLatency")(function* (threadId: ThreadId, first: number) {
  const feed = yield* ThreadFeed.ThreadFeed;
  const now = yield* DateTime.now;
  const events = yield* Queue.unbounded<ThreadFeedEvent>();
  yield* feed.follow({ threadId }).pipe(
    Stream.runForEach((event) => Queue.offer(events, event)),
    Effect.forkScoped,
  );
  while ((yield* Queue.take(events)).type !== "ready");
  const samples: Array<number> = [];
  for (let index = 0; index < RUNS; index++) {
    const ordinal = first + index;
    const start = NodePerfHooks.performance.now();
    yield* writeEvents(threadId, [
      { type: "turn-item.updated", payload: userMessage(threadId, ordinal, "live", now) },
    ]);
    while ((yield* Queue.take(events)).type !== "item");
    samples.push(NodePerfHooks.performance.now() - start);
  }
  return median(samples);
});

const measure = Effect.fn("bench.measure")(function* (label: string, count: number) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const filled = yield* timed(fillThread(label, count));
  const { threadId, position } = filled.result;
  yield* Console.log(`\n${label}: ${count} items (written in ${ms(filled.ms)})`);

  const resume: Array<number> = [];
  const fresh: Array<number> = [];
  const tail: Array<number> = [];
  const timeline: Array<number> = [];
  const projection: Array<number> = [];
  let resumeEvents = 0;
  for (let run = 0; run < RUNS; run++) {
    // A reader that saw everything but the last 10 items: they are one write behind.
    const behind = yield* timeToReady({ threadId, after: position - 10 });
    resume.push(behind.ms);
    resumeEvents = behind.events;
    fresh.push((yield* timeToReady({ threadId })).ms);
    tail.push((yield* timeToReady({ threadId, tail: 20 })).ms);
    timeline.push(
      (yield* timed(
        projections.getTimelinePage(threadId, {
          view: "messages",
          limit: 10,
          beforePosition: Number.MAX_SAFE_INTEGER,
        }),
      )).ms,
    );
  }
  for (let run = 0; run < 3; run++) {
    projection.push((yield* timed(projections.getThreadProjection(threadId))).ms);
  }
  const latency = yield* Effect.scoped(liveLatency(threadId, count));
  yield* Console.log(
    [
      `  feed: resume after a position (10 newer items, ${resumeEvents} events) ${ms(median(resume))}`,
      `  feed: start fresh (state only)                                ${ms(median(fresh))}`,
      `  feed: start fresh with --tail 20                               ${ms(median(tail))}`,
      `  feed: write → event on the stream (live)                       ${ms(latency)}`,
      `  baseline: t3_thread_read newest 10 messages (thread show)      ${ms(median(timeline))}`,
      `  baseline: getThreadProjection (Headquarters' request read)     ${ms(median(projection))}`,
    ].join("\n"),
  );
});

const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-thread-feed-bench-"));
const program = Effect.gen(function* () {
  yield* Console.log(`SQLite file: ${directory}/state.sqlite (${RUNS} runs each, median)`);
  yield* measure("small thread", SMALL_ITEMS);
  yield* measure("large thread", ITEMS);
}).pipe(
  Effect.provide(
    layerFeedOn(SqlitePersistence.layerFromPath(NodePath.join(directory, "state.sqlite"))).pipe(
      Layer.provide(NodeServices.layer),
    ),
  ),
  Effect.ensuring(Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true }))),
);

NodeRuntime.runMain(program);
