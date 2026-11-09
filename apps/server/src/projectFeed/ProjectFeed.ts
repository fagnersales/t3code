import {
  type ApplicationStoredEvent,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2ThreadShellSnapshot,
  PROJECT_FEED_VERSION,
  type ProjectFeedChange,
  type ProjectFeedEvent,
  type ProjectFeedThread,
  type ProjectId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import type { ShellSnapshotOptions } from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import { isSnoozed } from "../orchestration-v2/ThreadSettlementService.ts";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";

/** Live events are read in small batches, so a burst of run updates costs one shell read. */
const BATCH_SIZE = 256;
const BATCH_WINDOW = Duration.millis(50);
/** A snooze ends on its own when its time passes; this is how often the feed looks. */
const SNOOZE_CHECK_INTERVAL = Duration.seconds(30);
/** `IN (...)` lists stay well under SQLite's bound-parameter limit. */
const ID_CHUNK = 500;
/** Live-stream failures (a full buffer under load) resume this many times in a row, then end. */
const MAX_SILENT_RESUMES = 5;

/**
 * Domain events that can change what the feed shows. Others (timeline items, provider sessions,
 * visits, pins) never do, so they are dropped before the live buffer holds them.
 */
const THREAD_EVENT_TYPES: ReadonlySet<string> = new Set([
  "thread.created",
  "thread.archived",
  "thread.unarchived",
  "thread.deleted",
  "thread.settled",
  "thread.unsettled",
  "thread.snoozed",
  "thread.unsnoozed",
  "thread.auto-settle-set",
  "thread.metadata-updated",
  "thread.model-selection-updated",
  "thread.provider-switched",
  "run.created",
  "run.updated",
  "runtime-request.updated",
]);

export interface ProjectRef {
  readonly id: ProjectId;
  readonly title: string;
  readonly workspaceRoot: string;
}

/** What the live stream keeps of a committed event: enough to know which thread to re-read. */
export type ProjectFeedSignal =
  | {
      readonly kind: "thread";
      readonly sequence: number;
      readonly threadId: ThreadId;
      /** Set by thread lifecycle events, whose payload is the whole thread. */
      readonly projectId: string | undefined;
      readonly created: boolean;
    }
  | { readonly kind: "project-deleted"; readonly sequence: number; readonly projectId: string };

const toProjectFeedSignal = (stored: ApplicationStoredEvent): ProjectFeedSignal => {
  if ("aggregateKind" in stored) {
    return { kind: "project-deleted", sequence: stored.sequence, projectId: stored.aggregateId };
  }
  const { event } = stored;
  return {
    kind: "thread",
    sequence: stored.sequence,
    threadId: event.threadId,
    projectId: event.type.startsWith("thread.")
      ? (event.payload as { readonly projectId?: string }).projectId
      : undefined,
    created: event.type === "thread.created",
  };
};

const isProjectFeedEvent = (stored: ApplicationStoredEvent) =>
  "aggregateKind" in stored
    ? stored.type === "project.deleted"
    : THREAD_EVENT_TYPES.has(stored.event.type);

const iso = (value: DateTime.Utc | null | undefined) =>
  value == null ? null : DateTime.formatIso(value);

const toFeedThread = (
  shell: OrchestrationV2ThreadShell,
  pendingRequests: number,
  nowMs: number,
): ProjectFeedThread => {
  const snoozed = isSnoozed(shell, nowMs);
  const settled = shell.settledOverride === "settled";
  const pending = shell.pendingRuntimeRequest;
  return {
    threadId: shell.id,
    projectId: shell.projectId,
    title: shell.title,
    status: shell.activityRunStatus ?? shell.status,
    run:
      shell.latestRunId === null || shell.status === "idle"
        ? null
        : {
            runId: shell.latestRunId,
            status: shell.status,
            requestedAt: iso(shell.latestRunRequestedAt),
            startedAt: iso(shell.latestRunStartedAt),
            completedAt: iso(shell.latestRunCompletedAt),
          },
    // The shell always names the newest one; the count comes from the requests table.
    pendingRequests: Math.max(pendingRequests, pending === null ? 0 : 1),
    pendingRequest:
      pending === null
        ? null
        : {
            requestId: pending.id,
            kind: pending.kind,
            createdAt: DateTime.formatIso(pending.createdAt),
            blocking: pending.blocking !== false,
          },
    settled,
    settledAt: settled ? iso(shell.settledAt) : null,
    snoozed,
    snoozedUntil: snoozed ? iso(shell.snoozedUntil) : null,
    archived: shell.archivedAt !== null,
    archivedAt: iso(shell.archivedAt),
    parentThreadId: shell.lineage.parentThreadId,
    relationshipToParent: shell.lineage.relationshipToParent,
    createdBy: shell.createdBy,
    creationSource: shell.creationSource,
    providerInstanceId: shell.modelSelection.instanceId,
    model: shell.modelSelection.model,
    branch: shell.branch,
    worktreePath: shell.worktreePath,
    createdAt: DateTime.formatIso(shell.createdAt),
    updatedAt: DateTime.formatIso(shell.updatedAt),
  };
};

const sameJson = (left: unknown, right: unknown) => JSON.stringify(left) === JSON.stringify(right);

/** The fields a reader acts on, compared with the last line sent. `updatedAt` alone is no change. */
export const diffThreads = (
  previous: ProjectFeedThread,
  next: ProjectFeedThread,
): Array<ProjectFeedChange> => {
  const changes: Array<ProjectFeedChange> = [];
  if (previous.title !== next.title) changes.push("title");
  if (previous.status !== next.status) changes.push("status");
  if (!sameJson(previous.run, next.run)) changes.push("run");
  if (
    previous.pendingRequests !== next.pendingRequests ||
    !sameJson(previous.pendingRequest, next.pendingRequest)
  ) {
    changes.push("requests");
  }
  if (previous.settled !== next.settled || previous.settledAt !== next.settledAt) {
    changes.push("settled");
  }
  if (previous.snoozed !== next.snoozed || previous.snoozedUntil !== next.snoozedUntil) {
    changes.push("snoozed");
  }
  if (previous.archived !== next.archived) changes.push("archived");
  if (
    previous.parentThreadId !== next.parentThreadId ||
    previous.relationshipToParent !== next.relationshipToParent
  ) {
    changes.push("parent");
  }
  if (previous.model !== next.model || previous.providerInstanceId !== next.providerInstanceId) {
    changes.push("model");
  }
  if (previous.branch !== next.branch) changes.push("branch");
  if (previous.worktreePath !== next.worktreePath) changes.push("worktree");
  return changes;
};

/** The project whose root holds `target`, deepest root first; both sides are real paths. */
export const projectContaining = <P extends { readonly root: string }>(
  path: Pick<Path.Path, "relative" | "isAbsolute" | "sep">,
  projects: ReadonlyArray<P>,
  target: string,
): P | undefined =>
  projects
    .filter(({ root }) => {
      const relative = path.relative(root, target);
      return (
        relative === "" ||
        (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
      );
    })
    .toSorted((left, right) => right.root.length - left.root.length)[0];

export class ProjectFeedSourceError extends Schema.TaggedError<ProjectFeedSourceError>()(
  "ProjectFeedSourceError",
  { cause: Schema.Defect() },
) {}

/** What the feed reads besides its own index seeks. */
export interface ProjectFeedSource {
  /** A project id, or a path inside a project's workspace. */
  readonly resolveProject: (
    ref: string,
  ) => Effect.Effect<ProjectRef | null, ProjectFeedSourceError>;
  readonly latestPosition: Effect.Effect<number, ProjectFeedSourceError>;
  /** Thread shells of the project (not deleted), as the sidebar computes them. */
  readonly readShells: (input: {
    readonly projectId: ProjectId;
    readonly threadIds?: ReadonlyArray<ThreadId>;
    readonly includeArchived: boolean;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2ThreadShell>, ProjectFeedSourceError>;
  /**
   * Committed events after `position` that `isProjectFeedEvent` keeps, as signals, read from the
   * log and then live from the server's one event store.
   */
  readonly signalsAfter: (
    position: number,
  ) => Stream.Stream<ProjectFeedSignal, ProjectFeedSourceError>;
}

export interface ProjectFeedFollowInput {
  /** Project id, or a path inside the project. */
  readonly ref: string;
  /** Send only threads changed after this position; without it, a snapshot. */
  readonly after?: number | undefined;
  /** Also hold archived threads (snapshot, resume and `ready.threads`). */
  readonly includeArchived?: boolean | undefined;
}

export interface ProjectFeedShape {
  /** Never fails: a project it cannot follow ends with an `error` event. */
  readonly follow: (input: ProjectFeedFollowInput) => Stream.Stream<ProjectFeedEvent>;
}

export class ProjectFeed extends Context.Service<ProjectFeed, ProjectFeedShape>()(
  "t3/projectFeed/ProjectFeed",
) {}

const chunks = <A>(values: ReadonlyArray<A>) => {
  const out: Array<ReadonlyArray<A>> = [];
  for (let start = 0; start < values.length; start += ID_CHUNK) {
    out.push(values.slice(start, start + ID_CHUNK));
  }
  return out;
};

export const make = Effect.fn("ProjectFeed.make")(function* (source: ProjectFeedSource) {
  const sql = yield* SqlClient.SqlClient;

  // Every id the project ever had, deleted and archived ones included (index on project_id).
  const readThreadIds = Effect.fn("ProjectFeed.readThreadIds")(function* (projectId: ProjectId) {
    const rows = yield* sql<{ thread_id: string }>`
      SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id = ${projectId}`;
    return rows.map((row) => row.thread_id);
  });

  // Threads with any event after the position: one seek per thread on (stream_id, sequence).
  const readChangedThreadIds = Effect.fn("ProjectFeed.readChangedThreadIds")(function* (
    projectId: ProjectId,
    after: number,
  ) {
    const rows = yield* sql<{ thread_id: string }>`
      SELECT t.thread_id FROM orchestration_v2_projection_threads t
      WHERE t.project_id = ${projectId}
        AND EXISTS (
          SELECT 1 FROM orchestration_events e
            INDEXED BY idx_orchestration_events_agent_stream_sequence
          WHERE e.stream_id = t.thread_id
            AND e.application_event_version = 2
            AND e.aggregate_kind = 'thread'
            AND e.sequence > ${after}
        )`;
    return rows.map((row) => row.thread_id as ThreadId);
  });

  const readPendingCounts = Effect.fn("ProjectFeed.readPendingCounts")(function* (
    threadIds: ReadonlyArray<string>,
  ) {
    const counts = new Map<string, number>();
    for (const ids of chunks(threadIds)) {
      const rows = yield* sql<{ thread_id: string; count: number }>`
        SELECT thread_id, COUNT(*) AS count
        FROM orchestration_v2_projection_runtime_requests
        WHERE status = 'pending' AND thread_id IN ${sql.in(ids)}
        GROUP BY thread_id`;
      for (const row of rows) counts.set(row.thread_id, row.count);
    }
    return counts;
  });

  const readThreads = Effect.fn("ProjectFeed.readThreads")(function* (input: {
    readonly projectId: ProjectId;
    readonly threadIds?: ReadonlyArray<ThreadId>;
    readonly includeArchived: boolean;
  }) {
    const shells =
      input.threadIds === undefined
        ? yield* source.readShells(input)
        : (yield* Effect.forEach(chunks(input.threadIds), (threadIds) =>
            source.readShells({ ...input, threadIds }),
          )).flat();
    const pending = yield* readPendingCounts(
      shells.filter((shell) => shell.pendingRuntimeRequest !== null).map((shell) => shell.id),
    );
    const nowMs = yield* Clock.currentTimeMillis;
    return shells.map((shell) => ({
      shell,
      thread: toFeedThread(shell, pending.get(shell.id) ?? 0, nowMs),
    }));
  });

  const follow = (input: ProjectFeedFollowInput): Stream.Stream<ProjectFeedEvent> => {
    const includeArchived = input.includeArchived === true;
    let project: ProjectRef | null = null;
    let lastPosition: number | null = input.after ?? null;
    /** The last line sent per thread the reader holds (archived ones too, to see an unarchive). */
    const sent = new Map<string, ProjectFeedThread>();
    /** Shells of snoozed threads, to notice a snooze that runs out without an event. */
    const snoozedShells = new Map<string, OrchestrationV2ThreadShell>();
    let known = new Set<string>();

    const common = (position: number) => ({
      v: PROJECT_FEED_VERSION,
      projectId: project?.id ?? input.ref,
      position,
    });
    const errorEvent = (message: string): ProjectFeedEvent => ({
      v: PROJECT_FEED_VERSION,
      type: "error",
      ref: input.ref,
      projectId: project?.id ?? null,
      position: lastPosition,
      message,
    });
    const remember = (thread: ProjectFeedThread, shell: OrchestrationV2ThreadShell) => {
      sent.set(thread.threadId, thread);
      if (thread.snoozed) snoozedShells.set(thread.threadId, shell);
      else snoozedShells.delete(thread.threadId);
    };
    const forget = (threadId: string) => {
      sent.delete(threadId);
      snoozedShells.delete(threadId);
    };

    /** Snapshot (no position) or the threads changed after it, then `ready`. */
    const catchUp = Effect.fn("ProjectFeed.catchUp")(function* (after: number | null) {
      const resolved = project ?? (yield* source.resolveProject(input.ref));
      if (resolved === null) return null;
      project = resolved;
      const head = yield* source.latestPosition;
      known = new Set(yield* readThreadIds(resolved.id));
      const events: Array<ProjectFeedEvent> = [];
      if (after === null) {
        const threads = yield* readThreads({ projectId: resolved.id, includeArchived });
        sent.clear();
        snoozedShells.clear();
        events.push({ ...common(head), type: "snapshot", ref: input.ref, threads: threads.length });
        for (const { thread, shell } of threads) {
          remember(thread, shell);
          events.push({ ...common(head), type: "thread", thread });
        }
      } else {
        const changed = yield* readChangedThreadIds(resolved.id, after);
        const threads = yield* readThreads({
          projectId: resolved.id,
          threadIds: changed,
          includeArchived: true,
        });
        const present = new Set(threads.map(({ thread }) => thread.threadId));
        for (const { thread, shell } of threads) {
          remember(thread, shell);
          events.push({ ...common(head), type: "thread", thread });
        }
        for (const threadId of changed) {
          if (present.has(threadId)) continue;
          forget(threadId);
          events.push({ ...common(head), type: "removed", threadId, reason: "deleted" });
        }
      }
      const threadCount =
        after === null ? sent.size : yield* readThreadCount(resolved.id, includeArchived);
      events.push({
        ...common(head),
        type: "ready",
        ref: input.ref,
        title: resolved.title,
        workspaceRoot: resolved.workspaceRoot,
        threads: threadCount,
        resumed: after !== null,
      });
      return { head, events };
    });

    const readThreadCount = Effect.fn("ProjectFeed.readThreadCount")(function* (
      projectId: ProjectId,
      withArchived: boolean,
    ) {
      const rows = yield* sql<{ count: number }>`
        SELECT COUNT(*) AS count FROM orchestration_v2_projection_threads
        WHERE project_id = ${projectId} AND deleted_at IS NULL${
          withArchived ? sql`` : sql` AND archived_at IS NULL`
        }`;
      return rows[0]?.count ?? 0;
    });

    /** Re-reads the threads a batch of signals touched and sends what changed. */
    const applyBatch = Effect.fn("ProjectFeed.applyBatch")(function* (
      signals: ReadonlyArray<ProjectFeedSignal>,
    ) {
      const current = project;
      if (current === null) return [];
      const touched = new Map<string, { position: number; created: boolean }>();
      for (const signal of signals) {
        if (signal.kind === "project-deleted") {
          if (signal.projectId !== current.id) continue;
          lastPosition = signal.sequence;
          return [errorEvent("The project was deleted.")];
        }
        if (!known.has(signal.threadId)) {
          if (signal.projectId !== current.id) continue;
          known.add(signal.threadId);
        }
        touched.set(signal.threadId, {
          position: signal.sequence,
          created: signal.created || touched.get(signal.threadId)?.created === true,
        });
      }
      if (touched.size === 0) return [];
      const threads = yield* readThreads({
        projectId: current.id,
        threadIds: [...touched.keys()] as Array<ThreadId>,
        includeArchived: true,
      });
      const byId = new Map(threads.map((entry) => [entry.thread.threadId, entry]));
      const out: Array<Extract<ProjectFeedEvent, { type: "thread" | "removed" }>> = [];
      for (const [threadId, { position, created }] of touched) {
        const entry = byId.get(threadId);
        const previous = sent.get(threadId);
        if (entry === undefined) {
          if (previous === undefined) continue;
          forget(threadId);
          out.push({ ...common(position), type: "removed", threadId, reason: "deleted" });
          continue;
        }
        if (previous === undefined) {
          // Archived threads the reader never held stay out until they come back.
          if (entry.thread.archived && !includeArchived) {
            sent.delete(threadId);
            continue;
          }
          remember(entry.thread, entry.shell);
          // A thread the reader may hold from before a resume: no previous line to compare with.
          out.push({
            ...common(position),
            type: "thread",
            thread: entry.thread,
            ...(created ? { changes: ["created" as const] } : {}),
          });
          continue;
        }
        const changes = diffThreads(previous, entry.thread);
        remember(entry.thread, entry.shell);
        if (changes.length === 0) continue;
        out.push({ ...common(position), type: "thread", thread: entry.thread, changes });
      }
      return out.sort((left, right) => left.position - right.position);
    });

    /** A snooze that ran out changes `snoozed` without any event. */
    const checkSnoozes = Effect.fn("ProjectFeed.checkSnoozes")(function* () {
      if (snoozedShells.size === 0 || lastPosition === null) return [];
      const nowMs = yield* Clock.currentTimeMillis;
      const out: Array<ProjectFeedEvent> = [];
      for (const [threadId, shell] of snoozedShells) {
        if (isSnoozed(shell, nowMs)) continue;
        const previous = sent.get(threadId);
        snoozedShells.delete(threadId);
        if (previous === undefined) continue;
        const thread = { ...previous, snoozed: false, snoozedUntil: null };
        sent.set(threadId, thread);
        out.push({ ...common(lastPosition), type: "thread", thread, changes: ["snoozed"] });
      }
      return out;
    });

    type Tick = { readonly kind: "tick" };
    let failuresInRow = 0;
    const run = (after: number | null): Stream.Stream<ProjectFeedEvent> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const caught = yield* catchUp(after);
          if (caught === null) {
            return Stream.succeed(errorEvent(`No project ${input.ref} on this server.`));
          }
          const live = source.signalsAfter(caught.head).pipe(
            Stream.groupedWithin(BATCH_SIZE, BATCH_WINDOW),
            Stream.map((batch) => Array.from(batch) as ReadonlyArray<ProjectFeedSignal | Tick>),
            Stream.merge(
              Stream.tick(SNOOZE_CHECK_INTERVAL).pipe(
                Stream.map((): ReadonlyArray<Tick> => [{ kind: "tick" }]),
              ),
            ),
            Stream.mapEffect((batch) =>
              batch.length === 1 && batch[0]?.kind === "tick"
                ? checkSnoozes()
                : applyBatch(batch as ReadonlyArray<ProjectFeedSignal>),
            ),
            // Only a live batch read without failing counts as recovered; a catch-up alone does not.
            Stream.tap(() => Effect.sync(() => (failuresInRow = 0))),
            Stream.flattenIterable,
            Stream.takeUntil((event) => event.type === "error"),
          );
          return Stream.fromIterable(caught.events).pipe(Stream.concat(live));
        }),
      ).pipe(
        Stream.tap((event) =>
          Effect.sync(() => {
            if ("position" in event && event.position !== null) lastPosition = event.position;
          }),
        ),
        Stream.catchCause((cause) =>
          Stream.unwrap(
            Effect.gen(function* () {
              yield* Effect.logWarning("project feed lost its source", { ref: input.ref, cause });
              // A full live buffer or a failed read: resume from the last line sent.
              failuresInRow += 1;
              if (
                failuresInRow <= MAX_SILENT_RESUMES &&
                project !== null &&
                lastPosition !== null
              ) {
                return run(lastPosition);
              }
              return Stream.succeed(errorEvent("The server stopped reading this project."));
            }),
          ),
        ),
      );

    return run(lastPosition);
  };

  return ProjectFeed.of({ follow });
});

const sourceError = (cause: unknown) => new ProjectFeedSourceError({ cause });

/** The source over the server's own services; tests swap only `resolveProject`. */
export const sourceFrom = <E>(input: {
  readonly resolveProject: ProjectFeedSource["resolveProject"];
  readonly getShellSnapshot: (
    options: ShellSnapshotOptions,
  ) => Effect.Effect<OrchestrationV2ThreadShellSnapshot, E>;
  readonly events: OrchestrationEventStore.OrchestrationEventStore["Service"];
}): ProjectFeedSource => ({
  resolveProject: input.resolveProject,
  latestPosition: input.events.latestApplicationSequence.pipe(Effect.mapError(sourceError)),
  readShells: ({ projectId, threadIds, includeArchived }) =>
    input
      .getShellSnapshot({
        projectId,
        ...(includeArchived ? {} : { location: "active" as const }),
        ...(threadIds === undefined ? {} : { threadIds }),
      })
      .pipe(
        Effect.map((snapshot) => [...snapshot.threads, ...snapshot.archivedThreads]),
        Effect.mapError(sourceError),
      ),
  signalsAfter: (position) =>
    input.events
      .streamProjectedApplicationEvents({
        afterSequence: position,
        filter: isProjectFeedEvent,
        project: toProjectFeedSignal,
      })
      .pipe(Stream.mapError(sourceError)),
});

export const layer = Layer.effect(
  ProjectFeed,
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const events = yield* OrchestrationEventStore.OrchestrationEventStore;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const realPath = (value: string) =>
      fileSystem.realPath(value).pipe(Effect.orElseSucceed(() => value));
    const resolveProject = Effect.fn("ProjectFeed.resolveProject")(function* (ref: string) {
      const shells = yield* projects.listShells();
      const byId = shells.find((shell) => shell.id === ref);
      if (byId === undefined && !path.isAbsolute(ref)) return null;
      const found =
        byId ??
        projectContaining(
          path,
          yield* Effect.forEach(shells, (shell) =>
            Effect.map(realPath(shell.workspaceRoot), (root) => ({ shell, root })),
          ),
          yield* realPath(ref),
        )?.shell;
      return found === undefined
        ? null
        : { id: found.id, title: found.title, workspaceRoot: found.workspaceRoot };
    }, Effect.mapError(sourceError));
    return yield* make(
      sourceFrom({
        resolveProject,
        getShellSnapshot: orchestrator.getShellSnapshot,
        events,
      }),
    );
  }),
);
