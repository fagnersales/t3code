import * as Schema from "effect/Schema";

import { ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ThreadGroups, ThreadWorkspaceChoices } from "./settings.ts";

/**
 * Fork-only (fagnersales/t3code): sets the threads of one group in `threadGroups`, the threads
 * another app runs here. Each group is a sidebar workspace. An empty list removes the group.
 */
export const THREAD_GROUPS_ROUTE = "/api/thread-groups/v1";

export const ThreadGroupSetRequest = Schema.Struct({
  group: TrimmedNonEmptyString,
  threadIds: Schema.Array(ThreadId),
});
export type ThreadGroupSetRequest = typeof ThreadGroupSetRequest.Type;

export const ThreadGroupsResponse = Schema.Struct({ groups: ThreadGroups });
export type ThreadGroupsResponse = typeof ThreadGroupsResponse.Type;

/**
 * Fork-only: moves threads by hand into a group's workspace, or into Threads (`workspace: null`),
 * in `threadWorkspaceChoices`. `reset` forgets the move, so the threads follow their group again.
 */
export const THREAD_WORKSPACES_ROUTE = "/api/thread-workspaces/v1";

export const ThreadWorkspaceMoveRequest = Schema.Union([
  Schema.Struct({
    workspace: Schema.NullOr(TrimmedNonEmptyString),
    threadIds: Schema.Array(ThreadId),
  }),
  Schema.Struct({ reset: Schema.Literal(true), threadIds: Schema.Array(ThreadId) }),
]);
export type ThreadWorkspaceMoveRequest = typeof ThreadWorkspaceMoveRequest.Type;

export const ThreadWorkspacesResponse = Schema.Struct({
  groups: ThreadGroups,
  choices: ThreadWorkspaceChoices,
});
export type ThreadWorkspacesResponse = typeof ThreadWorkspacesResponse.Type;

/**
 * The workspace each named thread is in: its hand-made choice when it has one, else its group. A
 * choice naming a group that is gone puts the thread in Threads (null).
 */
export function threadWorkspaceMembership(
  groups: ThreadGroups,
  choices: ThreadWorkspaceChoices,
): ReadonlyMap<ThreadId, string | null> {
  const membership = new Map<ThreadId, string | null>();
  for (const [group, threadIds] of Object.entries(groups)) {
    for (const threadId of threadIds) membership.set(threadId, group);
  }
  for (const [threadId, choice] of Object.entries(choices) as Array<
    [ThreadId, ThreadWorkspaceChoices[ThreadId]]
  >) {
    membership.set(
      threadId,
      choice.workspace !== null && groups[choice.workspace] !== undefined ? choice.workspace : null,
    );
  }
  return membership;
}
