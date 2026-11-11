import {
  type AuthEnvironmentScope,
  AuthOrchestrationReadScope,
  AuthSettingsWriteScope,
  THREAD_GROUPS_ROUTE,
  ThreadGroupSetRequest,
  type ThreadGroups,
  ThreadGroupsResponse,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerSettings from "../serverSettings.ts";

const NO_STORE = { "cache-control": "no-store" };

export const problem = (status: number, message: string) =>
  HttpServerResponse.jsonUnsafe({ message }, { status, headers: NO_STORE });

const encodeResponse = Schema.encodeSync(ThreadGroupsResponse);
const decodeRequest = Schema.decodeUnknownOption(ThreadGroupSetRequest);

const respond = (groups: ThreadGroups) =>
  HttpServerResponse.jsonUnsafe(encodeResponse({ groups }), { headers: NO_STORE });

/** The response to send instead, when the request's session lacks `scope`. */
export const refusal = Effect.fn("ThreadGroupsHttp.refusal")(function* (
  scope: AuthEnvironmentScope,
) {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const session = yield* auth.authenticateHttpRequest(request).pipe(Effect.option);
  if (Option.isNone(session)) return Option.some(problem(401, "Sign in with a bearer session."));
  if (!session.value.scopes.includes(scope)) {
    return Option.some(problem(403, `This session needs the ${scope} scope.`));
  }
  return Option.none();
});

const readRoute = Effect.gen(function* () {
  const refused = yield* refusal(AuthOrchestrationReadScope);
  if (Option.isSome(refused)) return refused.value;
  const settings = yield* ServerSettings.ServerSettingsService;
  const current = yield* settings.getSettings.pipe(Effect.option);
  if (Option.isNone(current)) return problem(500, "Could not read the settings.");
  return respond(current.value.threadGroups);
});

const setRoute = Effect.gen(function* () {
  const refused = yield* refusal(AuthSettingsWriteScope);
  if (Option.isSome(refused)) return refused.value;
  const request = yield* HttpServerRequest.HttpServerRequest;
  const body = yield* request.json.pipe(Effect.option);
  const input = Option.flatMap(body, decodeRequest);
  if (Option.isNone(input)) return problem(400, "Send { group, threadIds }.");
  const { group, threadIds } = input.value;
  const unique = [...new Set(threadIds)];
  const settings = yield* ServerSettings.ServerSettingsService;
  const next = yield* settings
    .updateSettings({ threadGroups: { [group]: unique.length === 0 ? null : unique } })
    .pipe(Effect.option);
  if (Option.isNone(next)) return problem(500, "Could not save the settings.");
  return respond(next.value.threadGroups);
});

export const layer = Layer.mergeAll(
  HttpRouter.add("GET", THREAD_GROUPS_ROUTE, readRoute),
  HttpRouter.add("POST", THREAD_GROUPS_ROUTE, setRoute),
);
