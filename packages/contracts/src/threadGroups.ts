import * as Schema from "effect/Schema";

import { ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ThreadGroups } from "./settings.ts";

/**
 * Fork-only (fagnersales/t3code): sets the threads of one group in `threadGroups`, which the
 * sidebar hides until the user shows them. An empty list removes the group.
 */
export const THREAD_GROUPS_ROUTE = "/api/thread-groups/v1";

export const ThreadGroupSetRequest = Schema.Struct({
  group: TrimmedNonEmptyString,
  threadIds: Schema.Array(ThreadId),
});
export type ThreadGroupSetRequest = typeof ThreadGroupSetRequest.Type;

export const ThreadGroupsResponse = Schema.Struct({ groups: ThreadGroups });
export type ThreadGroupsResponse = typeof ThreadGroupsResponse.Type;
