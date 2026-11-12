import {
  AuthOrchestrationReadScope,
  AuthSettingsWriteScope,
  THREAD_GROUPS_ROUTE,
  ThreadGroupsResponse,
} from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag, GlobalFlag } from "effect/cli";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import { readLiveServerRuntimeState } from "../serverRuntimeState.ts";
import { type CliAuthLocationFlags, projectLocationFlags, resolveCliAuthConfig } from "./config.ts";

export class ThreadGroupError extends Schema.TaggedError<ThreadGroupError>()("ThreadGroupError", {
  detail: Schema.String,
}) {
  override get message(): string {
    return this.detail;
  }
}

const decodeResponse = Schema.decodeUnknownEffect(ThreadGroupsResponse);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(ThreadGroupsResponse));
const isThreadGroupError = Schema.is(ThreadGroupError);

/**
 * Calls a fork route of the running server with a session made for the call: a GET without a
 * body, a POST (which needs settings:write) with one.
 */
export const callThreadGroupsServer = Effect.fn("cli.callThreadGroupsServer")(function* <A>(
  flags: CliAuthLocationFlags,
  input: {
    readonly label: string;
    readonly route: string;
    readonly body: Option.Option<unknown>;
    readonly decode: (body: unknown) => Effect.Effect<A, Schema.SchemaError>;
  },
) {
  const writes = Option.isSome(input.body);
  const logLevel = yield* GlobalFlag.LogLevel;
  const config = yield* resolveCliAuthConfig(flags, logLevel);
  return yield* Effect.gen(function* () {
    const runtimeState = yield* readLiveServerRuntimeState(config.serverRuntimeStatePath);
    if (Option.isNone(runtimeState)) {
      return yield* new ThreadGroupError({
        detail: `No running T3 Code server uses ${config.baseDir}. Start T3 Code, then retry.`,
      });
    }
    const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;
    const issued = yield* Effect.acquireRelease(
      environmentAuth.issueSession({
        label: input.label,
        scopes: [writes ? AuthSettingsWriteScope : AuthOrchestrationReadScope],
      }),
      (session) =>
        environmentAuth.revokeSession(session.sessionId).pipe(Effect.ignore({ log: true })),
    );
    const url = new URL(input.route, runtimeState.value.origin);
    const request = Option.match(input.body, {
      onNone: () => HttpClientRequest.get(url),
      onSome: (body) => HttpClientRequest.post(url).pipe(HttpClientRequest.bodyJsonUnsafe(body)),
    });
    const response = yield* HttpClient.execute(
      request.pipe(HttpClientRequest.bearerToken(issued.token)),
    );
    const body = yield* response.json;
    if (response.status !== 200) {
      const said =
        typeof body === "object" && body !== null && "message" in body
          ? String(body.message)
          : `status ${response.status}`;
      return yield* new ThreadGroupError({ detail: `The server refused: ${said}` });
    }
    return yield* input.decode(body);
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
    Effect.mapError((error) =>
      isThreadGroupError(error)
        ? error
        : new ThreadGroupError({ detail: `Could not reach T3 Code: ${String(error)}` }),
    ),
  );
});

const groupCommand = Command.make("group", {
  ...projectLocationFlags,
  group: Argument.String("group").pipe(
    Argument.withDescription("The group's name, such as Headquarters."),
  ),
  threads: Argument.String("thread").pipe(
    Argument.withDescription("Every thread of the group: the list replaces the saved one."),
    Argument.variadic,
  ),
  clear: Flag.Boolean("clear").pipe(
    Flag.withDescription("Remove the group, so the sidebar shows its threads again."),
    Flag.withDefault(false),
  ),
  json: Flag.Boolean("json").pipe(
    Flag.withDescription("Print every group as JSON."),
    Flag.withDefault(false),
  ),
}).pipe(
  Command.withDescription(
    "Set the threads of a group the sidebar hides until you show it, such as the threads another app runs here. Without threads, print the group.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const threadIds = flags.threads.map((thread) => String(thread).trim()).filter(Boolean);
      if (flags.clear && threadIds.length > 0) {
        return yield* new ThreadGroupError({ detail: "Pass threads or --clear, not both." });
      }
      const writes = flags.clear || threadIds.length > 0;
      const { groups } = yield* callThreadGroupsServer(flags, {
        label: "t3 thread group",
        route: THREAD_GROUPS_ROUTE,
        body: writes ? Option.some({ group: flags.group, threadIds }) : Option.none(),
        decode: decodeResponse,
      });
      if (flags.json) return yield* Console.log(encodeJson({ groups }));
      const members = groups[flags.group] ?? [];
      yield* Console.log(
        writes
          ? `${flags.group}: ${members.length} thread${members.length === 1 ? "" : "s"}.`
          : members.join("\n"),
      );
    }).pipe(
      Effect.tapError((error) =>
        isThreadGroupError(error) ? Console.error(error.message) : Effect.void,
      ),
    ),
  ),
);

export const threadGroupCommand = groupCommand;
