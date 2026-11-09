import { assert, it } from "@effect/vitest";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import {
  MessageId,
  type OrchestrationV2AppThread,
  type ProjectFeedEvent,
  type ProjectFeedThread,
  ProjectId,
  RuntimeRequestId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import {
  assistantMessage,
  NodeId,
  run,
  runtimeRequest,
  seedThreadRecord,
  writeEvents,
  writeThreadEvent,
} from "../threadFeed/threadFeedTestkit.ts";
import * as ProjectFeed from "./ProjectFeed.ts";
import { layerProjectFeedOn, MISSING_PROJECT } from "./projectFeedTestkit.ts";

const layer = it.layer(layerProjectFeedOn(SqlitePersistence.layerMemory));

/** The live feed batches with real timers. */
const live = <A, E, R>(effect: Effect.Effect<A, E, R>) => TestClock.withLive(Effect.scoped(effect));

let projectCount = 0;
const newProject = () => ProjectId.make(`project-feed-test-${++projectCount}`);

const follow = Effect.fn("test.follow")(function* (input: ProjectFeed.ProjectFeedFollowInput) {
  const feed = yield* ProjectFeed.ProjectFeed;
  const events = yield* Queue.unbounded<ProjectFeedEvent>();
  yield* feed.follow(input).pipe(
    Stream.runForEach((event) => Queue.offer(events, event)),
    Effect.forkScoped,
  );
  return events;
});

/** Takes events up to and including the first one of `type`. */
const takeThrough = Effect.fn("test.takeThrough")(function* (
  events: Queue.Queue<ProjectFeedEvent>,
  type: ProjectFeedEvent["type"],
) {
  const taken: Array<ProjectFeedEvent> = [];
  while (true) {
    const event = yield* Queue.take(events).pipe(
      Effect.timeoutOrElse({
        duration: Duration.seconds(5),
        orElse: () => Effect.die(`no ${type} after ${JSON.stringify(taken.map(summary))}`),
      }),
    );
    taken.push(event);
    if (event.type === type) return taken;
  }
});

let markerCount = 0;

/**
 * Everything the feed sent for changes written so far: renames the project's marker thread and
 * returns the events before that rename arrives, since the feed keeps the log's order.
 */
const drain = Effect.fn("test.drain")(function* (
  events: Queue.Queue<ProjectFeedEvent>,
  marker: OrchestrationV2AppThread,
) {
  yield* writeThreadEvent("thread.metadata-updated", {
    ...marker,
    title: `marker ${++markerCount}`,
  });
  const taken = yield* takeThrough(events, "thread");
  while (true) {
    const last = taken.at(-1);
    if (last?.type === "thread" && last.thread.threadId === marker.id) return taken.slice(0, -1);
    taken.push(...(yield* takeThrough(events, "thread")));
  }
});

const summary = (event: ProjectFeedEvent) => {
  switch (event.type) {
    case "snapshot":
      return `snapshot ${event.threads}`;
    case "thread":
      return `thread ${event.thread.title} ${event.thread.status}${
        event.changes === undefined ? "" : ` [${event.changes.join(",")}]`
      }`;
    case "removed":
      return `removed ${event.threadId}`;
    case "ready":
      return `ready ${event.threads}${event.resumed ? " resumed" : ""}`;
    case "error":
      return `error ${event.message}`;
    default:
      return event.type;
  }
};

const threadOf = (events: ReadonlyArray<ProjectFeedEvent>, title: string) =>
  events.flatMap((event): Array<ProjectFeedThread> =>
    event.type === "thread" && event.thread.title === title ? [event.thread] : [],
  )[0];

const now = DateTime.now;

/** What a launch's `message.dispatch` commits: the first message and its run, together. */
const writeFirstMessage = Effect.fn("test.writeFirstMessage")(function* (
  thread: OrchestrationV2AppThread,
  senderThreadId: OrchestrationV2AppThread["id"] | undefined,
) {
  const ids = yield* IdAllocator.IdAllocatorV2;
  const sink = yield* EventSink.EventSinkV2;
  const t = yield* now;
  yield* sink.write({
    events: [
      {
        id: yield* ids.allocate.event({ threadId: thread.id }),
        type: "message.updated",
        threadId: thread.id,
        occurredAt: t,
        payload: {
          id: MessageId.make(`${thread.id}:first`),
          threadId: thread.id,
          runId: null,
          nodeId: null,
          role: "user",
          text: "Build it",
          attachments: [],
          streaming: false,
          createdBy: "agent",
          creationSource: "mcp",
          ...(senderThreadId === undefined ? {} : { senderThreadId }),
          createdAt: t,
          updatedAt: t,
        },
      },
      {
        id: yield* ids.allocate.event({ threadId: thread.id }),
        type: "run.updated",
        threadId: thread.id,
        occurredAt: t,
        payload: run(thread.id, 1, "running", t),
      },
    ],
  });
});

const seed = Effect.fn("test.seed")(function* (projectId: ProjectId, title: string) {
  return yield* seedThreadRecord(title, { projectId });
});

layer("ProjectFeed", (it) => {
  it.effect("starts with a snapshot of the project's threads, subagents included", () =>
    live(
      Effect.gen(function* () {
        const projectId = newProject();
        const lead = yield* seed(projectId, "Lead");
        yield* seedThreadRecord("Worker", { projectId, parentThreadId: lead.id });
        const old = yield* seed(projectId, "Old");
        yield* writeThreadEvent("thread.archived", { ...old, archivedAt: yield* now });
        yield* seed(newProject(), "Elsewhere");

        const events = yield* takeThrough(yield* follow({ ref: projectId }), "ready");
        assert.deepEqual(events.map(summary), [
          "snapshot 2",
          "thread Lead idle",
          "thread Worker idle",
          "ready 2",
        ]);
        const worker = threadOf(events, "Worker");
        assert.equal(worker?.parentThreadId, lead.id);
        assert.equal(worker?.relationshipToParent, "subagent");
        const first = events[0];
        assert.equal(first?.type === "snapshot" ? first.ref : null, projectId);

        const withArchived = yield* takeThrough(
          yield* follow({ ref: projectId, includeArchived: true }),
          "ready",
        );
        assert.deepEqual(withArchived.map(summary).toSorted(), [
          "ready 3",
          "snapshot 3",
          "thread Lead idle",
          "thread Old idle",
          "thread Worker idle",
        ]);
        assert.equal(threadOf(withArchived, "Old")?.archived, true);
      }),
    ),
  );

  it.effect("sends each change with what changed, and nothing for other projects or items", () =>
    live(
      Effect.gen(function* () {
        const projectId = newProject();
        const marker = yield* seed(projectId, "Marker");
        const lead = yield* seed(projectId, "Lead");
        const events = yield* follow({ ref: projectId });
        yield* takeThrough(events, "ready");

        const job = yield* seed(projectId, "Job");
        yield* seed(newProject(), "Elsewhere");
        const t = yield* now;
        yield* writeEvents(lead.id, [
          { type: "turn-item.updated", payload: assistantMessage(lead.id, 1, "hi", true, t) },
        ]);
        assert.deepEqual((yield* drain(events, marker)).map(summary), [
          "thread Job idle [created]",
        ]);

        yield* writeThreadEvent("thread.metadata-updated", { ...job, title: "Job: build" });
        assert.deepEqual((yield* drain(events, marker)).map(summary), [
          "thread Job: build idle [title]",
        ]);

        yield* writeEvents(job.id, [
          { type: "run.updated", payload: run(job.id, 1, "running", t) },
        ]);
        const running = yield* drain(events, marker);
        assert.deepEqual(running.map(summary), ["thread Job: build running [status,run]"]);
        const runningThread = threadOf(running, "Job: build");
        assert.equal(runningThread?.run?.status, "running");
        assert.equal(runningThread?.run?.runId, `${job.id}:run-1`);

        const nodeId = NodeId.make(`${job.id}:node`);
        const requestId = RuntimeRequestId.make(`${job.id}:request`);
        yield* writeEvents(job.id, [
          {
            type: "runtime-request.updated",
            payload: runtimeRequest(requestId, nodeId, "pending", t),
          },
        ]);
        const asked = yield* drain(events, marker);
        assert.deepEqual(asked.map(summary), ["thread Job: build running [requests]"]);
        assert.equal(threadOf(asked, "Job: build")?.pendingRequests, 1);
        assert.equal(threadOf(asked, "Job: build")?.pendingRequest?.requestId, requestId);

        yield* writeEvents(job.id, [
          {
            type: "runtime-request.updated",
            payload: runtimeRequest(requestId, nodeId, "resolved", t),
          },
          { type: "run.updated", payload: run(job.id, 1, "completed", t) },
        ]);
        const done = yield* drain(events, marker);
        assert.deepEqual(done.map(summary), ["thread Job: build completed [status,run,requests]"]);
        assert.equal(threadOf(done, "Job: build")?.pendingRequests, 0);

        const renamed = { ...job, title: "Job: build" };
        yield* writeThreadEvent("thread.settled", {
          ...renamed,
          settledOverride: "settled",
          settledAt: t,
        });
        assert.deepEqual((yield* drain(events, marker)).map(summary), [
          "thread Job: build completed [settled]",
        ]);

        yield* writeThreadEvent("thread.archived", { ...renamed, archivedAt: t });
        const archived = yield* drain(events, marker);
        assert.deepEqual(archived.map(summary), ["thread Job: build completed [settled,archived]"]);
        assert.equal(threadOf(archived, "Job: build")?.archived, true);

        yield* writeThreadEvent("thread.deleted", { ...renamed, archivedAt: t, deletedAt: t });
        assert.deepEqual((yield* drain(events, marker)).map(summary), [`removed ${job.id}`]);
      }),
    ),
  );

  it.effect("names the thread whose agent launched it, from the first message's sender", () =>
    live(
      Effect.gen(function* () {
        const projectId = newProject();
        const marker = yield* seed(projectId, "Marker");
        const lead = yield* seed(projectId, "Lead");
        const events = yield* follow({ ref: projectId });
        yield* takeThrough(events, "ready");

        // A launch commits the thread first, then its message with the run.
        const job = yield* seed(projectId, "Job");
        const created = yield* drain(events, marker);
        assert.deepEqual(created.map(summary), ["thread Job idle [created]"]);
        assert.isNull(threadOf(created, "Job")?.launchedByThreadId);
        yield* writeFirstMessage(job, lead.id);
        const launched = yield* drain(events, marker);
        assert.deepEqual(launched.map(summary), ["thread Job running [status,run,launcher]"]);
        assert.equal(threadOf(launched, "Job")?.launchedByThreadId, lead.id);

        const byPerson = yield* seed(projectId, "By hand");
        yield* writeFirstMessage(byPerson, undefined);
        yield* drain(events, marker);

        const snapshot = yield* takeThrough(yield* follow({ ref: projectId }), "ready");
        assert.equal(threadOf(snapshot, "Job")?.launchedByThreadId, lead.id);
        assert.isNull(threadOf(snapshot, "Lead")?.launchedByThreadId);
        assert.isNull(threadOf(snapshot, "By hand")?.launchedByThreadId);
      }),
    ),
  );

  it.effect("resumes from a position with only the threads changed after it", () =>
    live(
      Effect.gen(function* () {
        const projectId = newProject();
        const marker = yield* seed(projectId, "Marker");
        const quiet = yield* seed(projectId, "Quiet");
        const busy = yield* seed(projectId, "Busy");
        const gone = yield* seed(projectId, "Gone");
        const ready = (yield* takeThrough(yield* follow({ ref: projectId }), "ready")).at(-1);
        const position = ready?.type === "ready" ? ready.position : -1;
        assert.isAbove(position, 0);

        const t = yield* now;
        yield* writeEvents(busy.id, [
          { type: "run.updated", payload: run(busy.id, 1, "running", t) },
        ]);
        yield* writeThreadEvent("thread.deleted", { ...gone, deletedAt: t });
        const fresh = yield* seed(projectId, "Fresh");

        const resumed = yield* takeThrough(
          yield* follow({ ref: projectId, after: position }),
          "ready",
        );
        assert.deepEqual(resumed.map(summary).toSorted(), [
          "ready 4 resumed",
          `removed ${gone.id}`,
          "thread Busy running",
          "thread Fresh idle",
        ]);
        assert.isUndefined(threadOf(resumed, quiet.title));
        assert.equal(threadOf(resumed, fresh.title)?.threadId, fresh.id);
        for (const event of resumed) {
          if (event.type === "thread") assert.isUndefined(event.changes);
        }

        // Live after a resume: a thread the reader may hold has no previous line to compare.
        const live = yield* follow({ ref: projectId, after: position });
        yield* takeThrough(live, "ready");
        yield* writeThreadEvent("thread.metadata-updated", { ...quiet, title: "Quiet now" });
        assert.deepEqual((yield* drain(live, marker)).map(summary), ["thread Quiet now idle"]);
      }),
    ),
  );

  it.effect("a project it cannot find ends with an error", () =>
    live(
      Effect.gen(function* () {
        const events = yield* takeThrough(yield* follow({ ref: MISSING_PROJECT }), "error");
        assert.deepEqual(events.map(summary), [
          `error No project ${MISSING_PROJECT} on this server.`,
        ]);
      }),
    ),
  );
});

it.effect("diffThreads names the fields a reader acts on, never updatedAt alone", () =>
  Effect.sync(() => {
    const base: ProjectFeedThread = {
      threadId: "t",
      projectId: "p",
      title: "A",
      status: "idle",
      run: null,
      pendingRequests: 0,
      pendingRequest: null,
      settled: false,
      settledAt: null,
      snoozed: false,
      snoozedUntil: null,
      archived: false,
      archivedAt: null,
      parentThreadId: null,
      relationshipToParent: null,
      launchedByThreadId: null,
      createdBy: "user",
      creationSource: "web",
      providerInstanceId: "codex",
      model: "gpt",
      branch: null,
      worktreePath: null,
      createdAt: "2026-11-09T00:00:00.000Z",
      updatedAt: "2026-11-09T00:00:00.000Z",
    };
    assert.deepEqual(ProjectFeed.diffThreads(base, { ...base, updatedAt: "later" }), []);
    assert.deepEqual(
      ProjectFeed.diffThreads(base, {
        ...base,
        title: "B",
        snoozed: true,
        branch: "fix",
        worktreePath: "/w",
        launchedByThreadId: "lead",
      }),
      ["title", "snoozed", "launcher", "branch", "worktree"],
    );
  }),
);

it.effect("a path resolves to the deepest project root that holds it", () =>
  Effect.gen(function* () {
    const path = yield* Path.Path;
    const projects = [
      { id: "outer", root: "/work/outer" },
      { id: "inner", root: "/work/outer/packages/inner" },
      { id: "sibling", root: "/work/outer-two" },
    ];
    const at = (target: string) => ProjectFeed.projectContaining(path, projects, target)?.id;
    assert.equal(at("/work/outer"), "outer");
    assert.equal(at("/work/outer/src/a.ts"), "outer");
    assert.equal(at("/work/outer/packages/inner/x"), "inner");
    assert.equal(at("/work/outer-two/x"), "sibling");
    assert.isUndefined(at("/work"));
  }).pipe(Effect.provide(Path.layer)),
);
