import {
  AuthOrchestrationReadScope,
  THREAD_FEED_ROUTE,
  THREAD_FEED_VERSION,
  ThreadFeedEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import { ThreadFeed, type ThreadFeedFollowInput } from "./ThreadFeed.ts";

const HEARTBEAT_INTERVAL = Duration.seconds(20);
const MAX_THREADS = 64;
const NO_STORE = { "cache-control": "no-store, no-transform", "x-accel-buffering": "no" };

const encodeLine = Schema.encodeSync(Schema.fromJsonString(ThreadFeedEvent));
const isThreadId = Schema.is(ThreadId);

const problem = (status: number, message: string) =>
  HttpServerResponse.jsonUnsafe({ message }, { status, headers: NO_STORE });

const nonNegativeInt = (value: string | null) => {
  if (value === null || value === "") return undefined;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
};

/**
 * `thread=<id>` once per thread, or `thread=<id>@<position>` to resume that thread alone.
 * `after=<position>` resumes every thread without its own; `tail=<n>` sends the last n items
 * of a thread that starts fresh.
 */
export const parseFollowQuery = (
  params: URLSearchParams,
): ReadonlyArray<ThreadFeedFollowInput> | string => {
  const after = nonNegativeInt(params.get("after"));
  const tail = nonNegativeInt(params.get("tail"));
  if (after === null) return "after must be a non-negative integer.";
  if (tail === null) return "tail must be a non-negative integer.";
  const threads = params.getAll("thread");
  if (threads.length === 0) return "Pass at least one thread.";
  if (threads.length > MAX_THREADS) return `Pass at most ${MAX_THREADS} threads.`;
  const inputs: Array<ThreadFeedFollowInput> = [];
  for (const entry of threads) {
    const at = entry.lastIndexOf("@");
    const id = at === -1 ? entry : entry.slice(0, at);
    const own = at === -1 ? undefined : nonNegativeInt(entry.slice(at + 1));
    if (!isThreadId(id) || own === null || (at !== -1 && own === undefined)) {
      return `'${entry}' is not a thread id or thread@position.`;
    }
    inputs.push({ threadId: id, after: own ?? after, tail });
  }
  return inputs;
};

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const feed = yield* ThreadFeed;
    return HttpRouter.add("GET", THREAD_FEED_ROUTE, followRoute(feed));
  }),
);

const followRoute = Effect.fn("ThreadFeedHttp.follow")(function* (feed: ThreadFeed["Service"]) {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const session = yield* auth.authenticateHttpRequest(request).pipe(Effect.option);
  if (Option.isNone(session)) return problem(401, "Sign in with a bearer session.");
  if (!session.value.scopes.includes(AuthOrchestrationReadScope)) {
    return problem(403, `This session needs the ${AuthOrchestrationReadScope} scope.`);
  }
  const url = HttpServerRequest.toURL(request);
  if (Option.isNone(url)) return problem(400, "Bad request.");
  const inputs = parseFollowQuery(url.value.searchParams);
  if (typeof inputs === "string") return problem(400, inputs);

  const events = Stream.mergeAll(inputs.map(feed.follow), { concurrency: "unbounded" });
  const heartbeat = Stream.tick(HEARTBEAT_INTERVAL).pipe(
    Stream.map((): ThreadFeedEvent => ({ v: THREAD_FEED_VERSION, type: "heartbeat" })),
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
