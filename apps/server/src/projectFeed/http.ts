import {
  AuthOrchestrationReadScope,
  PROJECT_FEED_ROUTE,
  PROJECT_FEED_VERSION,
  ProjectFeedEvent,
} from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import { ProjectFeed, type ProjectFeedFollowInput } from "./ProjectFeed.ts";

const HEARTBEAT_INTERVAL = Duration.seconds(20);
const MAX_PROJECTS = 16;
const NO_STORE = { "cache-control": "no-store, no-transform", "x-accel-buffering": "no" };

const encodeLine = Schema.encodeSync(Schema.fromJsonString(ProjectFeedEvent));

const problem = (status: number, message: string) =>
  HttpServerResponse.jsonUnsafe({ message }, { status, headers: NO_STORE });

const nonNegativeInt = (value: string | null) => {
  if (value === null || value === "") return undefined;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
};

/**
 * `project=<id or absolute path>` once per project, or `project=<ref>@<position>` to resume that
 * project alone. `after=<position>` resumes every project without its own; `archived=1` also holds
 * archived threads. A path may hold `@`: only a numeric suffix is read as a position.
 */
export const parseProjectFollowQuery = (
  params: URLSearchParams,
): ReadonlyArray<ProjectFeedFollowInput> | string => {
  const after = nonNegativeInt(params.get("after"));
  if (after === null) return "after must be a non-negative integer.";
  const archived = params.get("archived");
  if (archived !== null && archived !== "0" && archived !== "1") return "archived must be 0 or 1.";
  const refs = params.getAll("project");
  if (refs.length === 0) return "Pass at least one project.";
  if (refs.length > MAX_PROJECTS) return `Pass at most ${MAX_PROJECTS} projects.`;
  const inputs: Array<ProjectFeedFollowInput> = [];
  for (const entry of refs) {
    const at = entry.lastIndexOf("@");
    const suffix = at === -1 ? "" : entry.slice(at + 1);
    const own = /^\d+$/.test(suffix) ? nonNegativeInt(suffix) : undefined;
    const ref = (own === undefined ? entry : entry.slice(0, at)).trim();
    if (ref === "" || own === null) return `'${entry}' is not a project or project@position.`;
    inputs.push({ ref, after: own ?? after, includeArchived: archived === "1" });
  }
  return inputs;
};

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const feed = yield* ProjectFeed;
    return HttpRouter.add("GET", PROJECT_FEED_ROUTE, followRoute(feed));
  }),
);

const followRoute = Effect.fn("ProjectFeedHttp.follow")(function* (feed: ProjectFeed["Service"]) {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const session = yield* auth.authenticateHttpRequest(request).pipe(Effect.option);
  if (Option.isNone(session)) return problem(401, "Sign in with a bearer session.");
  if (!session.value.scopes.includes(AuthOrchestrationReadScope)) {
    return problem(403, `This session needs the ${AuthOrchestrationReadScope} scope.`);
  }
  const url = HttpServerRequest.toURL(request);
  if (Option.isNone(url)) return problem(400, "Bad request.");
  const inputs = parseProjectFollowQuery(url.value.searchParams);
  if (typeof inputs === "string") return problem(400, inputs);

  const events = Stream.mergeAll(inputs.map(feed.follow), { concurrency: "unbounded" });
  const heartbeat = Stream.tick(HEARTBEAT_INTERVAL).pipe(
    Stream.map((): ProjectFeedEvent => ({ v: PROJECT_FEED_VERSION, type: "heartbeat" })),
  );
  return HttpServerResponse.stream(
    events.pipe(
      Stream.merge(heartbeat, { haltStrategy: "left" }),
      Stream.map((event) => `${encodeLine(event)}\n`),
      Stream.encodeText,
    ),
    { contentType: "application/x-ndjson", headers: NO_STORE },
  );
});
