import { PROJECT_FEED_ROUTE, PROJECT_FEED_VERSION, ProjectFeedEvent } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag, GlobalFlag } from "effect/cli";

import { expandHomePath } from "../os-jank.ts";
import { projectLocationFlags, resolveCliAuthConfig } from "./config.ts";
import { followFeed } from "./feedClient.ts";

const decodeEvent = Schema.decodeUnknownOption(Schema.fromJsonString(ProjectFeedEvent));
const encodeEvent = Schema.encodeSync(Schema.fromJsonString(ProjectFeedEvent));

export class ProjectFollowUsageError extends Schema.TaggedError<ProjectFollowUsageError>()(
  "ProjectFollowUsageError",
  { detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

const isUsageError = Schema.is(ProjectFollowUsageError);

export interface FollowedProject {
  /** What the caller passed: a project id, or a path the server resolves. */
  readonly ref: string;
  /** Known once the server answers; later connections name the project by id. */
  projectId: string | undefined;
  /** The last position received, or the one the caller asked to resume after. */
  position: number | undefined;
}

/** `ref@position` resumes that project alone; only a numeric suffix is a position. */
export const parseProjectArguments = (
  values: ReadonlyArray<string>,
  after: number | undefined,
): Array<FollowedProject> | string => {
  if (values.length === 0) return "Pass at least one project id or path.";
  const projects: Array<FollowedProject> = [];
  for (const value of values) {
    const at = value.lastIndexOf("@");
    const suffix = at === -1 ? "" : value.slice(at + 1);
    const own = /^\d+$/.test(suffix) ? Number(suffix) : undefined;
    const ref = (own === undefined ? value : value.slice(0, at)).trim();
    if (ref === "") return `'${value}' names no project.`;
    if (own !== undefined && !Number.isSafeInteger(own)) {
      return `'${value}' has no valid position after @.`;
    }
    if (!projects.some((project) => project.ref === ref)) {
      projects.push({ ref, projectId: undefined, position: own ?? after });
    }
  }
  return projects;
};

/** What a connection asks for; the same ref comes back in `snapshot`, `ready` and `error`. */
const wireRef = (project: FollowedProject) => project.projectId ?? project.ref;

export const projectFeedUrl = (
  origin: string,
  projects: ReadonlyArray<FollowedProject>,
  archived: boolean,
) => {
  const url = new URL(PROJECT_FEED_ROUTE, origin);
  for (const project of projects) {
    const ref = wireRef(project);
    url.searchParams.append(
      "project",
      project.position === undefined ? ref : `${ref}@${project.position}`,
    );
  }
  if (archived) url.searchParams.set("archived", "1");
  return url;
};

/** Updates what each followed project resumes from, and drops a project that ended. */
const byRef = (projects: ReadonlyArray<FollowedProject>, ref: string) =>
  projects.find((entry) => entry.ref === ref || wireRef(entry) === ref);

export const trackProjectEvent = (
  projects: Array<FollowedProject>,
  event: ProjectFeedEvent,
): void => {
  switch (event.type) {
    case "snapshot":
    case "ready": {
      const project = byRef(projects, event.ref);
      if (project === undefined) return;
      project.projectId = event.projectId;
      project.position = event.position;
      return;
    }
    case "thread":
    case "removed": {
      const project = projects.find((entry) => entry.projectId === event.projectId);
      if (project !== undefined) project.position = event.position;
      return;
    }
    case "error": {
      const project = byRef(projects, event.ref);
      if (project !== undefined) projects.splice(projects.indexOf(project), 1);
      return;
    }
    default:
      return;
  }
};

/** One line for a person reading along; scripts use --jsonl. */
const formatText = (event: ProjectFeedEvent): string | null => {
  switch (event.type) {
    case "snapshot":
      return `[${event.ref}] ${event.threads} threads`;
    case "thread": {
      const { thread } = event;
      const flags = [
        thread.pendingRequests > 0 ? `${thread.pendingRequests} pending` : null,
        thread.settled ? "settled" : null,
        thread.snoozed ? "snoozed" : null,
        thread.archived ? "archived" : null,
        thread.parentThreadId === null
          ? null
          : `${thread.relationshipToParent ?? "child"} of ${thread.parentThreadId}`,
        thread.launchedByThreadId === null ? null : `launched by ${thread.launchedByThreadId}`,
      ].filter((flag) => flag !== null);
      return `${thread.threadId}  ${thread.status.padEnd(11)}  ${thread.title}${
        flags.length === 0 ? "" : `  (${flags.join(", ")})`
      }${event.changes === undefined ? "" : `  [${event.changes.join(", ")}]`}`;
    }
    case "removed":
      return `${event.threadId}  ${event.reason}`;
    case "ready":
      return `[${event.title}] ready, ${event.threads} threads${event.resumed ? " (resumed)" : ""}`;
    case "error":
      return `[${event.ref}] error: ${event.message}`;
    case "disconnected":
      return `disconnected: ${event.reason} (retrying in ${event.retryInMs} ms)`;
    case "heartbeat":
      return null;
  }
};

/** Paths go to the server absolute; anything else is sent as a project id. */
const toWireRef = Effect.fn("cli.project.follow.toWireRef")(function* (ref: string) {
  const path = yield* Path.Path;
  const fileSystem = yield* FileSystem.FileSystem;
  const expanded = yield* expandHomePath(ref);
  const absolute = path.resolve(yield* HostProcess.WorkingDirectory, expanded);
  const looksLikePath = /^[./~]/.test(ref) || ref.includes(path.sep);
  if (
    !looksLikePath &&
    !(yield* fileSystem.exists(absolute).pipe(Effect.orElseSucceed(() => false)))
  ) {
    return ref;
  }
  return yield* fileSystem.realPath(absolute).pipe(Effect.orElseSucceed(() => absolute));
});

const followCommand = Command.make("follow", {
  ...projectLocationFlags,
  projects: Argument.String("project").pipe(
    Argument.withDescription(
      "Project id or a path inside the project; project@position resumes that project alone.",
    ),
    Argument.variadic,
  ),
  after: Flag.Int("after").pipe(
    Flag.withDescription(
      "Send only the threads changed after this position (a `position` from an earlier event).",
    ),
    Flag.optional,
  ),
  archived: Flag.Boolean("archived").pipe(
    Flag.withDescription("Also send archived threads in the snapshot and count them."),
    Flag.withDefault(false),
  ),
  jsonl: Flag.Boolean("jsonl").pipe(
    Flag.withDescription("Print one JSON event per line (docs/fork/project-feed.md)."),
    Flag.withDefault(false),
  ),
}).pipe(
  Command.withDescription(
    "Stream projects' threads: a snapshot, then threads created, renamed, running, waiting, settled, archived or deleted, until killed.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const after = Option.getOrUndefined(flags.after);
      if (after !== undefined && after < 0) {
        return yield* new ProjectFollowUsageError({ detail: "--after must be 0 or more." });
      }
      const parsed = parseProjectArguments(flags.projects.map(String), after);
      if (typeof parsed === "string") {
        return yield* new ProjectFollowUsageError({ detail: parsed });
      }
      const projects: Array<FollowedProject> = [];
      for (const project of parsed) {
        projects.push({ ...project, ref: yield* toWireRef(project.ref) });
      }
      const print = (event: ProjectFeedEvent) => {
        if (flags.jsonl) {
          return event.type === "heartbeat" ? Effect.void : Console.log(encodeEvent(event));
        }
        const line = formatText(event);
        return line === null ? Effect.void : Console.log(line);
      };

      const logLevel = yield* GlobalFlag.LogLevel;
      const config = yield* resolveCliAuthConfig(flags, logLevel);
      yield* followFeed({
        config,
        label: "t3 project follow",
        url: (origin) => projectFeedUrl(origin, projects, flags.archived),
        decodeLine: decodeEvent,
        receive: (event) =>
          Effect.suspend(() => {
            trackProjectEvent(projects, event);
            return print(event);
          }),
        disconnected: (reason, retryInMs) =>
          print({ v: PROJECT_FEED_VERSION, type: "disconnected", reason, retryInMs }),
        done: () => projects.length === 0,
      }).pipe(Effect.mapError((detail) => new ProjectFollowUsageError({ detail })));
    }).pipe(
      Effect.tapError((error) =>
        isUsageError(error) ? Console.error(error.message) : Effect.void,
      ),
    ),
  ),
);

export const projectFollowCommand = followCommand;
