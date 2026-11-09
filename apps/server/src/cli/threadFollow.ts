import {
  THREAD_FEED_ROUTE,
  THREAD_FEED_VERSION,
  ThreadFeedEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag, GlobalFlag } from "effect/cli";

import { projectLocationFlags, resolveCliAuthConfig } from "./config.ts";
import { followFeed } from "./feedClient.ts";

const decodeEvent = Schema.decodeUnknownOption(Schema.fromJsonString(ThreadFeedEvent));
const encodeEvent = Schema.encodeSync(Schema.fromJsonString(ThreadFeedEvent));
const isThreadId = Schema.is(ThreadId);

export class ThreadFollowUsageError extends Schema.TaggedError<ThreadFollowUsageError>()(
  "ThreadFollowUsageError",
  { detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

const isUsageError = Schema.is(ThreadFollowUsageError);

interface FollowedThread {
  readonly threadId: ThreadId;
  /** The last position received, or the one the caller asked to resume after. */
  position: number | undefined;
}

/** `thread@position` resumes that thread alone; a bare id uses `--after`. */
export const parseThreadArguments = (
  values: ReadonlyArray<string>,
  after: number | undefined,
): Array<FollowedThread> | string => {
  if (values.length === 0) return "Pass at least one thread id.";
  const threads: Array<FollowedThread> = [];
  for (const value of values) {
    const at = value.lastIndexOf("@");
    const id = (at === -1 ? value : value.slice(0, at)).trim();
    const own = at === -1 ? undefined : Number(value.slice(at + 1));
    if (!isThreadId(id)) return `'${value}' is not a thread id.`;
    if (own !== undefined && !(Number.isSafeInteger(own) && own >= 0)) {
      return `'${value}' has no valid position after @.`;
    }
    if (!threads.some((thread) => thread.threadId === id)) {
      threads.push({ threadId: id, position: own ?? after });
    }
  }
  return threads;
};

const feedUrl = (
  origin: string,
  threads: ReadonlyArray<FollowedThread>,
  tail: number | undefined,
) => {
  const url = new URL(THREAD_FEED_ROUTE, origin);
  for (const thread of threads) {
    url.searchParams.append(
      "thread",
      thread.position === undefined ? thread.threadId : `${thread.threadId}@${thread.position}`,
    );
  }
  if (tail !== undefined && tail > 0) url.searchParams.set("tail", String(tail));
  return url;
};

/** One line for a person reading along; scripts use --jsonl. */
const formatText = (event: ThreadFeedEvent): string | null => {
  switch (event.type) {
    case "ready":
      return `[${event.threadId}] ready  ${event.thread.status}  ${event.thread.title}`;
    case "item":
      return `[${event.threadId}] ${event.item.role ?? event.item.kind} (${event.item.status})${
        event.item.text === undefined ? "" : `\n${event.item.text}`
      }`;
    case "text":
      return event.delta;
    case "run":
      return `[${event.threadId}] run ${event.run.ordinal} ${event.run.status}`;
    case "request":
      return `[${event.threadId}] ${event.request.kind} ${event.request.state}: ${
        event.request.prompt ?? event.request.questions?.map((q) => q.question).join(" / ") ?? ""
      }`;
    case "error":
      return `[${event.threadId}] error: ${event.message}`;
    case "disconnected":
      return `disconnected: ${event.reason} (retrying in ${event.retryInMs} ms)`;
    case "heartbeat":
      return null;
  }
};

const followCommand = Command.make("follow", {
  ...projectLocationFlags,
  threads: Argument.String("thread").pipe(
    Argument.withDescription("Thread id, or thread@position to resume that thread alone."),
    Argument.variadic,
  ),
  after: Flag.Int("after").pipe(
    Flag.withDescription(
      "Send only what came after this position (a `position` from an earlier event).",
    ),
    Flag.optional,
  ),
  tail: Flag.Int("tail").pipe(
    Flag.withDescription("Without --after, first send each thread's last N items (max 200)."),
    Flag.optional,
  ),
  jsonl: Flag.Boolean("jsonl").pipe(
    Flag.withDescription("Print one JSON event per line (docs/fork/thread-feed.md)."),
    Flag.withDefault(false),
  ),
}).pipe(
  Command.withDescription(
    "Stream threads' new items, assistant text, run status and requests as they happen, until killed.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const after = Option.getOrUndefined(flags.after);
      if (after !== undefined && after < 0) {
        return yield* new ThreadFollowUsageError({ detail: "--after must be 0 or more." });
      }
      const parsed = parseThreadArguments(flags.threads.map(String), after);
      if (typeof parsed === "string") {
        return yield* new ThreadFollowUsageError({ detail: parsed });
      }
      const threads = parsed;
      let tail = Option.getOrUndefined(flags.tail);
      const print = (event: ThreadFeedEvent) => {
        if (flags.jsonl) {
          return event.type === "heartbeat" ? Effect.void : Console.log(encodeEvent(event));
        }
        const line = formatText(event);
        return line === null ? Effect.void : Console.log(line);
      };

      const receive = Effect.fn("cli.thread.follow.receive")(function* (event: ThreadFeedEvent) {
        if ("threadId" in event) {
          const thread = threads.find((entry) => entry.threadId === event.threadId);
          if (thread !== undefined) {
            thread.position = event.position;
            if (event.type === "error") threads.splice(threads.indexOf(thread), 1);
          }
        }
        yield* print(event);
      });

      const logLevel = yield* GlobalFlag.LogLevel;
      const config = yield* resolveCliAuthConfig(flags, logLevel);
      yield* followFeed({
        config,
        label: "t3 thread follow",
        url: (origin) => feedUrl(origin, threads, tail),
        decodeLine: decodeEvent,
        receive,
        disconnected: (reason, retryInMs) =>
          print({ v: THREAD_FEED_VERSION, type: "disconnected", reason, retryInMs }),
        done: () => threads.length === 0,
        // Resumed connections read from each thread's position, never the tail again.
        afterConnection: () => {
          tail = undefined;
        },
      }).pipe(Effect.mapError((detail) => new ThreadFollowUsageError({ detail })));
    }).pipe(
      Effect.tapError((error) =>
        isUsageError(error) ? Console.error(error.message) : Effect.void,
      ),
    ),
  ),
);

export const threadFollowCommand = followCommand;
