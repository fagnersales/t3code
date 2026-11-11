import {
  AuthOrchestrationReadScope,
  AuthSettingsWriteScope,
  THREAD_WORKSPACES_ROUTE,
  type ServerSettings,
  type ThreadId,
  type ThreadWorkspaceChoice,
  ThreadWorkspaceMoveRequest,
  ThreadWorkspacesResponse,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";

import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import { forkParked } from "../serverActivation.ts";
import * as Settings from "../serverSettings.ts";
import { problem, refusal } from "./http.ts";

const NO_STORE = { "cache-control": "no-store" };
const SQL_CHUNK = 500;

const encodeResponse = Schema.encodeSync(ThreadWorkspacesResponse);
const decodeRequest = Schema.decodeUnknownOption(ThreadWorkspaceMoveRequest);

const respond = (settings: ServerSettings) =>
  HttpServerResponse.jsonUnsafe(
    encodeResponse({ groups: settings.threadGroups, choices: settings.threadWorkspaceChoices }),
    { headers: NO_STORE },
  );

/** The ids among `threadIds` that name a thread the server still has. */
const existingThreadIds = Effect.fn("ThreadWorkspaces.existingThreadIds")(function* (
  threadIds: ReadonlyArray<string>,
) {
  const sql = yield* SqlClient.SqlClient;
  const existing = new Set<string>();
  for (let start = 0; start < threadIds.length; start += SQL_CHUNK) {
    const rows = yield* sql<{ thread_id: string }>`
      SELECT thread_id FROM orchestration_v2_projection_threads
      WHERE deleted_at IS NULL AND thread_id IN ${sql.in(threadIds.slice(start, start + SQL_CHUNK))}`;
    for (const row of rows) existing.add(row.thread_id);
  }
  return existing;
});

const readRoute = Effect.gen(function* () {
  const refused = yield* refusal(AuthOrchestrationReadScope);
  if (Option.isSome(refused)) return refused.value;
  const settings = yield* Settings.ServerSettingsService;
  const current = yield* settings.getSettings.pipe(Effect.option);
  if (Option.isNone(current)) return problem(500, "Could not read the settings.");
  return respond(current.value);
});

const moveRoute = Effect.gen(function* () {
  const refused = yield* refusal(AuthSettingsWriteScope);
  if (Option.isSome(refused)) return refused.value;
  const request = yield* HttpServerRequest.HttpServerRequest;
  const body = yield* request.json.pipe(Effect.option);
  const input = Option.flatMap(body, decodeRequest);
  if (Option.isNone(input)) {
    return problem(
      400,
      "Send { workspace, threadIds } (workspace null for Threads) or { reset: true, threadIds }.",
    );
  }
  const threadIds = [...new Set(input.value.threadIds)];
  if (threadIds.length === 0) return problem(400, "Name at least one thread.");
  const settings = yield* Settings.ServerSettingsService;
  const current = yield* settings.getSettings.pipe(Effect.option);
  if (Option.isNone(current)) return problem(500, "Could not read the settings.");
  const choice: ThreadWorkspaceChoice | null =
    "reset" in input.value ? null : { workspace: input.value.workspace };
  if (choice?.workspace != null && current.value.threadGroups[choice.workspace] === undefined) {
    const names = Object.keys(current.value.threadGroups).toSorted();
    return problem(
      400,
      `No workspace is called ${choice.workspace}. Workspaces: threads${names.map((name) => `, ${name}`).join("")}.`,
    );
  }
  // A reset only forgets; a move must name threads that exist, or the choice would never be dropped.
  if (choice !== null) {
    const existing = yield* existingThreadIds(threadIds).pipe(Effect.option);
    if (Option.isNone(existing)) return problem(500, "Could not read the threads.");
    const unknown = threadIds.filter((threadId) => !existing.value.has(threadId));
    if (unknown.length > 0) return problem(400, `No such thread: ${unknown.join(", ")}.`);
  }
  const next = yield* settings
    .updateSettings({
      threadWorkspaceChoices: Object.fromEntries(threadIds.map((threadId) => [threadId, choice])),
    })
    .pipe(Effect.option);
  if (Option.isNone(next)) return problem(500, "Could not save the settings.");
  return respond(next.value);
});

export const routesLayer = Layer.mergeAll(
  HttpRouter.add("GET", THREAD_WORKSPACES_ROUTE, readRoute),
  HttpRouter.add("POST", THREAD_WORKSPACES_ROUTE, moveRoute),
);

/**
 * Drops the hand-made workspace choices of deleted threads: once at start, for deletions this
 * server missed (offloads too), then on each `thread.deleted`.
 */
export const prunerLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const settings = yield* Settings.ServerSettingsService;
    const engine = yield* Orchestrator.OrchestratorV2;
    const sql = yield* SqlClient.SqlClient;
    const forget = (threadIds: ReadonlyArray<ThreadId>) =>
      threadIds.length === 0
        ? Effect.void
        : settings
            .updateSettings({
              threadWorkspaceChoices: Object.fromEntries(
                threadIds.map((threadId) => [threadId, null]),
              ),
            })
            .pipe(Effect.asVoid);
    const pruneMissing = Effect.gen(function* () {
      const current = yield* settings.getSettings;
      const chosen = Object.keys(current.threadWorkspaceChoices) as ThreadId[];
      if (chosen.length === 0) return;
      const existing = yield* existingThreadIds(chosen).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
      yield* forget(chosen.filter((threadId) => !existing.has(threadId)));
    });
    yield* forkParked(
      pruneMissing.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Could not prune thread workspace choices", { cause }),
        ),
        Effect.andThen(
          Stream.runForEach(engine.streamDomainEvents, (event) =>
            event.type !== "thread.deleted"
              ? Effect.void
              : settings.getSettings.pipe(
                  Effect.flatMap((current) =>
                    current.threadWorkspaceChoices[event.threadId] === undefined
                      ? Effect.void
                      : forget([event.threadId]),
                  ),
                  Effect.catchCause((cause) =>
                    Effect.logWarning("Could not drop a deleted thread's workspace choice", {
                      cause,
                    }),
                  ),
                ),
          ),
        ),
      ),
    );
  }),
);
