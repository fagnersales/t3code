import { AuthOrchestrationReadScope } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import { readLiveServerRuntimeState } from "../serverRuntimeState.ts";

const SESSION_TTL = Duration.minutes(15);
const RETRY_MIN_MS = 1_000;
const RETRY_MAX_MS = 30_000;

class FeedConnectionError extends Schema.TaggedError<FeedConnectionError>()("FeedConnectionError", {
  reason: Schema.String,
  retry: Schema.Boolean,
}) {}

const isConnectionError = Schema.is(FeedConnectionError);

export interface FeedClientInput<Event> {
  readonly config: ServerConfig.ServerConfig["Service"];
  /** The session's label in `t3 auth session list`. */
  readonly label: string;
  /** Built again for every connection, from what the reader has received so far. */
  readonly url: (origin: string) => URL;
  readonly decodeLine: (line: string) => Option.Option<Event>;
  readonly receive: (event: Event) => Effect.Effect<void>;
  readonly disconnected: (reason: string, retryInMs: number) => Effect.Effect<void>;
  /** Checked after each connection: nothing left to follow. */
  readonly done: () => boolean;
  /** After a connection that delivered its stream, before the next one. */
  readonly afterConnection?: () => void;
}

/**
 * Reads an ndjson feed from the local server with a short-lived read-only session, and connects
 * again (1 s, doubling to 30 s) whenever the server restarts or the connection drops. Fails with
 * the reason only when retrying cannot help: 4xx answers, such as a server without the feed.
 */
export const followFeed = Effect.fn("cli.feed.follow")(function* <Event>(
  input: FeedClientInput<Event>,
) {
  const { config } = input;
  let retryMs = RETRY_MIN_MS;
  const connectOnce = Effect.gen(function* () {
    const runtimeState = yield* readLiveServerRuntimeState(config.serverRuntimeStatePath);
    if (Option.isNone(runtimeState)) {
      return yield* new FeedConnectionError({
        reason: `No running T3 Code server uses ${config.baseDir}.`,
        retry: true,
      });
    }
    const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;
    const issued = yield* Effect.acquireRelease(
      environmentAuth.issueSession({
        ttl: SESSION_TTL,
        label: input.label,
        scopes: [AuthOrchestrationReadScope],
      }),
      (session) =>
        environmentAuth.revokeSession(session.sessionId).pipe(Effect.ignore({ log: true })),
    );
    const unreachable = (cause: unknown) =>
      new FeedConnectionError({
        reason: `Lost ${runtimeState.value.origin}: ${String(cause)}`,
        retry: true,
      });
    const response = yield* HttpClient.execute(
      HttpClientRequest.get(input.url(runtimeState.value.origin)).pipe(
        HttpClientRequest.bearerToken(issued.token),
        HttpClientRequest.accept("application/x-ndjson"),
      ),
    ).pipe(Effect.mapError(unreachable));
    if (response.status !== 200) {
      const detail = yield* response.text.pipe(Effect.orElseSucceed(() => ""));
      return yield* new FeedConnectionError({
        reason: `The server answered ${response.status}${detail ? `: ${detail}` : ""}`,
        // 404: a server without the feed (upstream T3 Code); 4xx: bad arguments or access.
        retry: response.status >= 500,
      });
    }
    yield* response.stream.pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.filter((line) => line.trim() !== ""),
      Stream.runForEach((line) =>
        Option.match(input.decodeLine(line), {
          onNone: () => Effect.void,
          onSome: (event) =>
            Effect.suspend(() => {
              retryMs = RETRY_MIN_MS;
              return input.receive(event);
            }),
        }),
      ),
      Effect.mapError(unreachable),
    );
    input.afterConnection?.();
    return yield* new FeedConnectionError({ reason: "The server closed the feed.", retry: true });
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.merge(
        EnvironmentAuth.layerRuntime.pipe(
          Layer.provide(ServerConfig.layer(config)),
          Layer.provide(Layer.succeed(References.MinimumLogLevel, config.logLevel)),
        ),
        FetchHttpClient.layer,
      ),
    ),
  );

  while (!input.done()) {
    const outcome = yield* connectOnce.pipe(
      Effect.as(null),
      Effect.catch((error) =>
        Effect.succeed(
          isConnectionError(error)
            ? error
            : new FeedConnectionError({ reason: String(error), retry: true }),
        ),
      ),
    );
    if (outcome === null || input.done()) return;
    if (!outcome.retry) return yield* Effect.fail(outcome.reason);
    yield* input.disconnected(outcome.reason, retryMs);
    yield* Effect.sleep(Duration.millis(retryMs));
    retryMs = Math.min(retryMs * 2, RETRY_MAX_MS);
  }
});
