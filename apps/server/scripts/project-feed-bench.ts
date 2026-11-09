// @effect-diagnostics nodeBuiltinImport:off globalDateInEffect:off - a benchmark measures wall time.
/**
 * The project feed's cost on synthetic projects, on a fresh SQLite file in a temp folder:
 *   node scripts/project-feed-bench.ts [threads=5000]
 * Never point it at real data; it creates and deletes its own folder.
 */
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";

import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import {
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  ProjectId,
  type ProjectFeedEvent,
  ProviderInstanceId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as EventSink from "../src/orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../src/orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../src/persistence/Sqlite.ts";
import * as ProjectFeed from "../src/projectFeed/ProjectFeed.ts";
import { layerProjectFeedOn } from "../src/projectFeed/projectFeedTestkit.ts";
import { commandItem, run, userMessage } from "../src/threadFeed/threadFeedTestkit.ts";

const THREADS = Number(process.argv[2] ?? 5_000);
const SMALL_THREADS = 1_000;
const RUNS = 10;
const BATCH = 100;

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;

const median = (values: ReadonlyArray<number>) => {
  const sorted = values.toSorted((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
};
const ms = (value: number) => `${value.toFixed(1)} ms`.padStart(10);

const timed = Effect.fn("bench.timed")(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
  const start = NodePerfHooks.performance.now();
  const result = yield* effect;
  return { result, ms: NodePerfHooks.performance.now() - start };
});

/** `count` threads, a tenth of them subagents, each with a finished run and a few items. */
const fillProject = Effect.fn("bench.fillProject")(function* (name: string, count: number) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const projectId = ProjectId.make(`bench-${name}`);
  const threadIds: Array<ThreadId> = [];
  for (let start = 0; start < count; start += BATCH) {
    const now = yield* DateTime.now;
    const events: Array<OrchestrationV2DomainEvent> = [];
    for (let index = start; index < Math.min(count, start + BATCH); index++) {
      const id = yield* idAllocator.allocate.thread({ fixtureName: "project-feed", projectId });
      const parent = index % 10 === 9 ? threadIds[index - 1] : undefined;
      const thread: OrchestrationV2AppThread = {
        createdBy: "user",
        creationSource: "web",
        id,
        projectId,
        title: `${name} thread ${index}`,
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
            ? { parentThreadId: null, relationshipToParent: null, rootThreadId: id }
            : { parentThreadId: parent, relationshipToParent: "subagent", rootThreadId: parent },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: index % 4 === 3 ? now : null,
        settledOverride: index % 2 === 0 ? "settled" : null,
        settledAt: index % 2 === 0 ? now : null,
        lastVisitedAt: null,
        deletedAt: null,
      };
      threadIds.push(id);
      const base = { threadId: id, occurredAt: now };
      events.push(
        {
          ...base,
          id: yield* idAllocator.allocate.event({ threadId: id }),
          type: "thread.created",
          payload: thread,
        },
        {
          ...base,
          id: yield* idAllocator.allocate.event({ threadId: id }),
          type: "run.updated",
          payload: run(id, 1, "completed", now),
        },
        {
          ...base,
          id: yield* idAllocator.allocate.event({ threadId: id }),
          type: "turn-item.updated",
          payload: userMessage(id, 1, "do it", now),
        },
        {
          ...base,
          id: yield* idAllocator.allocate.event({ threadId: id }),
          type: "turn-item.updated",
          payload: commandItem(id, 2, "ok", now),
        },
      );
    }
    yield* eventSink.write({ events });
  }
  return { projectId, threadIds };
});

const writeRuns = Effect.fn("bench.writeRuns")(function* (
  threadIds: ReadonlyArray<ThreadId>,
  ordinal: number,
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const events: Array<OrchestrationV2DomainEvent> = [];
  for (const threadId of threadIds) {
    events.push({
      id: yield* idAllocator.allocate.event({ threadId }),
      threadId,
      occurredAt: now,
      type: "run.updated",
      payload: run(threadId, ordinal, "running", now),
    });
  }
  const stored = yield* eventSink.write({ events });
  return stored.at(-1)?.sequence ?? 0;
});

/** Time from subscribing to the feed's `ready` event, and the events before it. */
const timeToReady = Effect.fn("bench.timeToReady")(function* (
  input: ProjectFeed.ProjectFeedFollowInput,
) {
  const feed = yield* ProjectFeed.ProjectFeed;
  const start = NodePerfHooks.performance.now();
  const events = yield* feed.follow(input).pipe(
    Stream.takeUntil((event) => event.type === "ready"),
    Stream.runCollect,
  );
  const bytes = Array.from(events).reduce(
    (sum, event) => sum + JSON.stringify(event).length + 1,
    0,
  );
  return { ms: NodePerfHooks.performance.now() - start, events: events.length, bytes };
});

const liveLatency = Effect.fn("bench.liveLatency")(function* (
  projectId: ProjectId,
  threadIds: ReadonlyArray<ThreadId>,
  noise: ReadonlyArray<ThreadId>,
) {
  const feed = yield* ProjectFeed.ProjectFeed;
  const events = yield* Queue.unbounded<ProjectFeedEvent>();
  yield* feed.follow({ ref: projectId }).pipe(
    Stream.runForEach((event) => Queue.offer(events, event)),
    Effect.forkScoped,
  );
  while ((yield* Queue.take(events)).type !== "ready");
  const samples: Array<number> = [];
  for (let index = 0; index < RUNS; index++) {
    // Another project's run changes first: the feed must skip it.
    yield* writeRuns([noise[index] as ThreadId], 100 + index);
    const start = NodePerfHooks.performance.now();
    yield* writeRuns([threadIds[index * 2] as ThreadId], 100 + index);
    while ((yield* Queue.take(events)).type !== "thread");
    samples.push(NodePerfHooks.performance.now() - start);
  }
  return median(samples);
});

const measure = Effect.fn("bench.measure")(function* (
  name: string,
  count: number,
  noise: ReadonlyArray<ThreadId>,
) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const filled = yield* timed(fillProject(name, count));
  const { projectId, threadIds } = filled.result;
  yield* Console.log(`\n${name}: ${count} threads (written in ${ms(filled.ms).trim()})`);

  const snapshot: Array<number> = [];
  const resume: Array<number> = [];
  const idle: Array<number> = [];
  const list: Array<number> = [];
  let snapshotInfo = { events: 0, bytes: 0 };
  let resumeEvents = 0;
  for (let index = 0; index < RUNS; index++) {
    const fresh = yield* timeToReady({ ref: projectId });
    snapshot.push(fresh.ms);
    snapshotInfo = fresh;
    // A reader one batch behind: 10 threads changed since its position.
    const position = yield* timeToReadyPosition(projectId);
    yield* writeRuns(threadIds.slice(index * 10, index * 10 + 10), 2 + index);
    const behind = yield* timeToReady({ ref: projectId, after: position });
    resume.push(behind.ms);
    resumeEvents = behind.events;
    const head = yield* timeToReadyPosition(projectId);
    idle.push((yield* timeToReady({ ref: projectId, after: head })).ms);
    list.push((yield* timed(projections.getShellSnapshot({ projectId, location: "active" }))).ms);
  }
  const latency = yield* Effect.scoped(liveLatency(projectId, threadIds, noise));
  yield* Console.log(
    [
      `  feed: snapshot (${snapshotInfo.events - 2} threads, ${(snapshotInfo.bytes / 1024).toFixed(0)} KB)   ${ms(median(snapshot))}`,
      `  feed: resume, 10 threads changed (${resumeEvents} events)    ${ms(median(resume))}`,
      `  feed: resume, nothing changed                     ${ms(median(idle))}`,
      `  feed: run change → event on the stream (live)     ${ms(latency)}`,
      `  baseline: shell read behind \`t3 thread list\`      ${ms(median(list))}`,
    ].join("\n"),
  );
});

/** The position a reader holds right after `ready`. */
const timeToReadyPosition = Effect.fn("bench.position")(function* (projectId: ProjectId) {
  const feed = yield* ProjectFeed.ProjectFeed;
  const events = yield* feed.follow({ ref: projectId, after: Number.MAX_SAFE_INTEGER }).pipe(
    Stream.takeUntil((event) => event.type === "ready"),
    Stream.runCollect,
  );
  const ready = Array.from(events).at(-1);
  return ready?.type === "ready" ? ready.position : 0;
});

const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-project-feed-bench-"));
const program = Effect.gen(function* () {
  yield* Console.log(`SQLite file: ${directory}/state.sqlite (${RUNS} runs each, median)`);
  // A busy neighbouring project, so the log holds more than the followed project.
  const neighbour = yield* fillProject("neighbour", 1_000);
  yield* measure("small project", SMALL_THREADS, neighbour.threadIds);
  yield* measure("large project", THREADS, neighbour.threadIds);
}).pipe(
  Effect.provide(
    layerProjectFeedOn(
      SqlitePersistence.layerFromPath(NodePath.join(directory, "state.sqlite")),
    ).pipe(Layer.provide(NodeServices.layer)),
  ),
  Effect.ensuring(Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true }))),
);

NodeRuntime.runMain(program);
