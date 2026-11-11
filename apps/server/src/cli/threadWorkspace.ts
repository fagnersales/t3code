import {
  THREAD_WORKSPACES_ROUTE,
  type ThreadWorkspacesResponse as ThreadWorkspacesResponseType,
  ThreadWorkspacesResponse,
  threadWorkspaceMembership,
} from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag } from "effect/cli";

import { projectLocationFlags } from "./config.ts";
import { ThreadGroupError, callThreadGroupsServer } from "./threadGroup.ts";

/** The name of the default workspace: every thread no group or move puts elsewhere. */
const THREADS_WORKSPACE = "threads";

const decodeResponse = Schema.decodeUnknownEffect(ThreadWorkspacesResponse);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(ThreadWorkspacesResponse));
const isThreadGroupError = Schema.is(ThreadGroupError);

/** One block per group's workspace; a moved thread is marked. Threads lists only moved ones. */
const formatListing = ({ groups, choices }: ThreadWorkspacesResponseType): string => {
  const membership = threadWorkspaceMembership(groups, choices);
  const lines = (workspace: string | null) =>
    [...membership]
      .filter(([, member]) => member === workspace)
      .map(([threadId]) => `  ${threadId}${choices[threadId] === undefined ? "" : " (moved)"}`);
  const blocks = Object.keys(groups)
    .toSorted()
    .map((group) => [`${group}:`, ...lines(group)].join("\n"));
  const movedToThreads = lines(null);
  if (movedToThreads.length > 0) {
    blocks.push([`${THREADS_WORKSPACE} (moved out of a group):`, ...movedToThreads].join("\n"));
  }
  return blocks.length === 0 ? "No workspaces: no thread group exists." : blocks.join("\n\n");
};

const workspaceCommand = Command.make("workspace", {
  ...projectLocationFlags,
  workspace: Argument.String("workspace").pipe(
    Argument.withDescription(
      `The workspace to move the threads to: a group's name, such as Headquarters, or ${THREADS_WORKSPACE}.`,
    ),
    Argument.optional,
  ),
  threads: Argument.String("thread").pipe(
    Argument.withDescription("The threads to move."),
    Argument.variadic,
  ),
  reset: Flag.Boolean("reset").pipe(
    Flag.withDescription(
      "Forget the move of every thread named (no workspace argument), so each follows its group again.",
    ),
    Flag.withDefault(false),
  ),
  json: Flag.Boolean("json").pipe(
    Flag.withDescription("Print the groups and the moves as JSON."),
    Flag.withDefault(false),
  ),
}).pipe(
  Command.withDescription(
    `Move threads between sidebar workspaces: ${THREADS_WORKSPACE} or a thread group's. A move wins over the group, and t3 thread group never undoes it. Without threads, list each workspace.`,
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const named = [...Option.toArray(flags.workspace), ...flags.threads]
        .map((value) => String(value).trim())
        .filter(Boolean);
      const [workspace, ...moved] = named;
      const threadIds = flags.reset ? named : moved;
      if (!flags.reset && workspace !== undefined && moved.length === 0) {
        return yield* new ThreadGroupError({
          detail: `Name the threads to move to ${workspace}, or run without arguments to list.`,
        });
      }
      const body: Option.Option<unknown> =
        threadIds.length === 0
          ? Option.none()
          : Option.some(
              flags.reset
                ? { reset: true, threadIds }
                : {
                    workspace:
                      workspace?.toLowerCase() === THREADS_WORKSPACE ? null : (workspace ?? null),
                    threadIds,
                  },
            );
      const response = yield* callThreadGroupsServer(flags, {
        label: "t3 thread workspace",
        route: THREAD_WORKSPACES_ROUTE,
        body,
        decode: decodeResponse,
      });
      if (flags.json) return yield* Console.log(encodeJson(response));
      if (Option.isNone(body)) return yield* Console.log(formatListing(response));
      const count = `${threadIds.length} thread${threadIds.length === 1 ? "" : "s"}`;
      yield* Console.log(
        flags.reset
          ? `${count} follow their group again.`
          : `${count} moved to ${workspace?.toLowerCase() === THREADS_WORKSPACE ? THREADS_WORKSPACE : workspace}.`,
      );
    }).pipe(
      Effect.tapError((error) =>
        isThreadGroupError(error) ? Console.error(error.message) : Effect.void,
      ),
    ),
  ),
);

export const threadWorkspaceCommand = workspaceCommand;
