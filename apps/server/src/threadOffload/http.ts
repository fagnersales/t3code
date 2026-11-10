import {
  type AuthEnvironmentScope,
  AuthEnvironmentMaintainScope,
  AuthOrchestrationReadScope,
  THREAD_OFFLOAD_PLAN_ROUTE,
  THREAD_OFFLOAD_ROUTE,
  THREAD_RESTORE_ROUTE,
  ThreadOffloadPlan,
  ThreadOffloadPlanRequest,
  ThreadOffloadRequest,
  ThreadOffloadResponseLine,
  ThreadOffloadResult,
  ThreadRestoreRequest,
  ThreadRestoreResult,
} from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ThreadOffload from "./ThreadOffload.ts";

const NO_STORE = { "cache-control": "no-store" };

const problem = (status: number, message: string) =>
  HttpServerResponse.jsonUnsafe({ message }, { status, headers: NO_STORE });

const refusal = Effect.fn("ThreadOffloadHttp.refusal")(function* (scope: AuthEnvironmentScope) {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const session = yield* auth.authenticateHttpRequest(request).pipe(Effect.option);
  if (Option.isNone(session)) return Option.some(problem(401, "Sign in with a bearer session."));
  if (!session.value.scopes.includes(scope)) {
    return Option.some(problem(403, `This session needs the ${scope} scope.`));
  }
  return Option.none();
});

/** Threads another app follows through a thread group stay, like pinned ones. */
const groupedThreadIds = Effect.gen(function* () {
  const settings = yield* ServerSettings.ServerSettingsService;
  const current = yield* settings.getSettings.pipe(Effect.option);
  return new Set<string>(
    Option.match(current, {
      onNone: () => [],
      onSome: (value) => Object.values(value.threadGroups).flat(),
    }),
  );
});

const HEARTBEAT_INTERVAL = Duration.seconds(15);

const encodeLine = Schema.encodeSync(Schema.fromJsonString(ThreadOffloadResponseLine));

const route = <I, O>(input: {
  readonly scope: AuthEnvironmentScope;
  readonly request: Schema.Decoder<I>;
  readonly response: Schema.Encoder<O>;
  readonly usage: string;
  readonly run: (
    request: I,
    keepThreadIds: ReadonlySet<string>,
  ) => Effect.Effect<O, ThreadOffload.ThreadOffloadError>;
}) => {
  const decode = Schema.decodeUnknownOption(input.request);
  const encodeResult = Schema.encodeSync(input.response);
  return Effect.gen(function* () {
    const refused = yield* refusal(input.scope);
    if (Option.isSome(refused)) return refused.value;
    const request = yield* HttpServerRequest.HttpServerRequest;
    const body = Option.flatMap(yield* request.json.pipe(Effect.option), decode);
    if (Option.isNone(body)) return problem(400, input.usage);
    const keepThreadIds = yield* groupedThreadIds;
    // A client that leaves early does not stop the work halfway; the next run sees its outcome.
    const outcome = input.run(body.value, keepThreadIds).pipe(
      Effect.map((result): ThreadOffloadResponseLine => ({
        type: "result",
        result: encodeResult(result),
      })),
      Effect.catchTags({
        ThreadOffloadError: (error) =>
          Effect.succeed<ThreadOffloadResponseLine>({
            type: "error",
            reason: error.reason,
            detail: error.detail,
          }),
      }),
      Effect.uninterruptible,
    );
    const heartbeat = Stream.tick(HEARTBEAT_INTERVAL).pipe(
      Stream.map((): ThreadOffloadResponseLine => ({ type: "heartbeat" })),
    );
    return HttpServerResponse.stream(
      Stream.fromEffect(outcome).pipe(
        Stream.merge(heartbeat, { haltStrategy: "left" }),
        Stream.map((line) => `${encodeLine(line)}\n`),
        Stream.encodeText,
      ),
      { contentType: "application/x-ndjson", headers: NO_STORE },
    );
  });
};

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const offload = yield* ThreadOffload.ThreadOffload;
    return Layer.mergeAll(
      HttpRouter.add(
        "POST",
        THREAD_OFFLOAD_PLAN_ROUTE,
        route({
          scope: AuthOrchestrationReadScope,
          request: ThreadOffloadPlanRequest,
          response: ThreadOffloadPlan,
          usage: "Send { before, withSizes, limit?, threadIds? }.",
          run: (request, keepThreadIds) => offload.plan({ ...request, keepThreadIds }),
        }),
      ),
      HttpRouter.add(
        "POST",
        THREAD_OFFLOAD_ROUTE,
        route({
          scope: AuthEnvironmentMaintainScope,
          request: ThreadOffloadRequest,
          response: ThreadOffloadResult,
          usage: "Send { threadId, before }.",
          run: (request, keepThreadIds) => offload.offload({ ...request, keepThreadIds }),
        }),
      ),
      HttpRouter.add(
        "POST",
        THREAD_RESTORE_ROUTE,
        route({
          scope: AuthEnvironmentMaintainScope,
          request: ThreadRestoreRequest,
          response: ThreadRestoreResult,
          usage: "Send { threadId }.",
          run: (request) => offload.restore(request),
        }),
      ),
    );
  }),
);
