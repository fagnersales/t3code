import {
  OrchestrationV2ProviderTurnTokenUsage,
  OrchestrationV2RunJson,
  OrchestrationV2RuntimeRequestJson,
  OrchestrationV2TurnItemJson,
  THREAD_FEED_VERSION,
  ThreadTokenUsageSnapshot,
  type OrchestrationV2Run,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2TurnItem,
  type ThreadFeedEvent,
  type ThreadFeedItem,
  type ThreadFeedRequest,
  type ThreadFeedRun,
  type ThreadFeedUsage,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import { isQueuedProviderThreadPlaceholder } from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import { EVENT_LOG_PRUNED_THROUGH } from "../persistence/OrchestrationEventStore.ts";

/** Tool input and output are previews; the full text stays readable through `t3 thread show`. */
const TOOL_TEXT_LIMIT = 4_000;
const MAX_TAIL_ITEMS = 200;
/** A provider can report usage many times a second; a reader gets about one line a second. */
const USAGE_INTERVAL_MS = 1_000;
/** How far back the newest turn that reported usage is looked for. */
const USAGE_TURNS_READ = 32;

type FeedEventOf<T extends ThreadFeedEvent["type"]> = Extract<ThreadFeedEvent, { type: T }>;

const iso = (value: DateTime.Utc | null | undefined) =>
  value == null ? null : DateTime.formatIso(value);

const truncate = (value: string | undefined) =>
  value === undefined || value.length <= TOOL_TEXT_LIMIT
    ? { value, truncated: false }
    : { value: value.slice(0, TOOL_TEXT_LIMIT), truncated: true };

const stringify = (value: unknown) =>
  value === undefined ? undefined : typeof value === "string" ? value : JSON.stringify(value);

const toolOf = (name: string | null, input?: string, output?: string, exitCode?: number) => {
  const shownInput = truncate(input);
  const shownOutput = truncate(output);
  return {
    name,
    ...(shownInput.value === undefined ? {} : { input: shownInput.value }),
    ...(shownOutput.value === undefined ? {} : { output: shownOutput.value }),
    ...(exitCode === undefined ? {} : { exitCode }),
    ...(shownInput.truncated || shownOutput.truncated ? { truncated: true } : {}),
  };
};

/** The text a reader follows as it grows, for the items that stream. */
const streamedText = (item: OrchestrationV2TurnItem): string | undefined => {
  switch (item.type) {
    case "assistant_message":
    case "reasoning":
      return item.text;
    case "proposed_plan":
      return item.markdown;
    default:
      return undefined;
  }
};

const isStreaming = (item: OrchestrationV2TurnItem) =>
  (item.type === "assistant_message" ||
    item.type === "reasoning" ||
    item.type === "proposed_plan") &&
  item.streaming;

const toFeedItem = (item: OrchestrationV2TurnItem): ThreadFeedItem => {
  const base = {
    itemId: item.id,
    runId: item.runId,
    ordinal: item.ordinal,
    kind: item.type,
    status: item.status,
    title: item.title,
    startedAt: iso(item.startedAt),
    completedAt: iso(item.completedAt),
    updatedAt: DateTime.formatIso(item.updatedAt),
  };
  switch (item.type) {
    case "user_message":
      return { ...base, role: "user", text: item.text, createdBy: item.createdBy };
    case "assistant_message":
      return { ...base, role: "assistant", text: item.text, streaming: item.streaming };
    case "reasoning":
      return { ...base, text: item.text, streaming: item.streaming };
    case "proposed_plan":
      return { ...base, text: item.markdown, streaming: item.streaming };
    case "system_notice":
    case "run_interrupt_request":
    case "run_interrupt_result":
      return { ...base, text: item.message };
    case "error":
      return { ...base, text: item.failure.message };
    case "compaction":
    case "handoff":
      return item.summary === undefined ? base : { ...base, text: item.summary };
    case "command_execution":
      return { ...base, tool: toolOf("command", item.input, item.output, item.exitCode) };
    case "dynamic_tool":
      return {
        ...base,
        tool: toolOf(item.toolName, stringify(item.input), stringify(item.output)),
      };
    case "file_change":
      return { ...base, tool: toolOf("file_change", item.fileName, item.diffStr) };
    case "file_search":
      return { ...base, tool: toolOf("file_search", item.pattern) };
    case "web_search":
      return { ...base, tool: toolOf("web_search", item.patterns?.join("\n")) };
    case "subagent":
      return {
        ...base,
        tool: toolOf("subagent", item.prompt, item.result ?? item.progress),
      };
    case "approval_request":
    case "user_input_request":
      return { ...base, requestId: item.requestId };
    default:
      return base;
  }
};

const toFeedRun = (run: OrchestrationV2Run): ThreadFeedRun => ({
  runId: run.id,
  ordinal: run.ordinal,
  status: run.status,
  requestedAt: DateTime.formatIso(run.requestedAt),
  startedAt: iso(run.startedAt),
  completedAt: iso(run.completedAt),
});

const toFeedUsage = (
  usage: Pick<ThreadFeedUsage, "usedTokens" | "updatedAt"> & {
    readonly maxTokens?: number | null | undefined;
    readonly autoCompactThreshold?: number | undefined;
  },
): ThreadFeedUsage => ({
  usedTokens: usage.usedTokens,
  maxTokens: usage.maxTokens ?? null,
  ...(usage.autoCompactThreshold === undefined
    ? {}
    : { autoCompactThreshold: usage.autoCompactThreshold }),
  updatedAt: usage.updatedAt,
});

/** A line is a change when one of these moved; `updatedAt` moves with every report. */
const usageSignature = (usage: ThreadFeedUsage) =>
  `${usage.usedTokens}:${usage.maxTokens ?? ""}:${usage.autoCompactThreshold ?? ""}`;

/** Which conversation's usage counts for the thread, and whether its turns report it. */
interface UsageSeed {
  readonly activeProviderThreadId: string | null;
  /** A conversation whose turns report usage is never read from its own `contextUsage`. */
  readonly turnsReport: boolean;
  readonly usage: ThreadFeedUsage | null;
}

type RequestDetail = Pick<
  ThreadFeedRequest,
  "kind" | "questions" | "prompt" | "reason" | "options"
> & { readonly requestId: string; readonly runId: string | null; readonly requestKind?: string };

const requestDetailOf = (item: OrchestrationV2TurnItem): RequestDetail | undefined => {
  if (item.type === "user_input_request") {
    return {
      kind: "question",
      requestId: item.requestId,
      questions: item.questions,
      runId: item.runId,
    };
  }
  if (item.type === "approval_request") {
    return {
      kind: "approval",
      requestId: item.requestId,
      requestKind: item.requestKind,
      runId: item.runId,
      ...(item.prompt === undefined ? {} : { prompt: item.prompt }),
      ...(item.reason === undefined ? {} : { reason: item.reason }),
      ...(item.options === undefined ? {} : { options: item.options }),
    };
  }
  return undefined;
};

const toFeedRequest = (
  request: OrchestrationV2RuntimeRequest,
  detail: RequestDetail | undefined,
  runId: string | null,
): ThreadFeedRequest => ({
  requestId: request.id,
  runId: detail?.runId ?? runId,
  state: request.status === "pending" ? "open" : "closed",
  status: request.status,
  kind: detail?.kind ?? "other",
  requestKind: detail?.requestKind ?? request.kind,
  ...(detail?.questions === undefined ? {} : { questions: detail.questions }),
  ...(detail?.prompt === undefined ? {} : { prompt: detail.prompt }),
  ...(detail?.reason === undefined ? {} : { reason: detail.reason }),
  ...(detail?.options === undefined ? {} : { options: detail.options }),
  ...(request.decision === undefined ? {} : { decision: request.decision }),
  createdAt: DateTime.formatIso(request.createdAt),
});

/**
 * Turns one thread's stored events into feed events. It keeps only what the next event needs:
 * each item's status, the text of items still streaming, and the open requests.
 */
export const makeThreadFeedTranslator = (threadId: ThreadId) => {
  const items = new Map<string, { status: string; text: string | undefined }>();
  const runs = new Map<string, string>();
  const details = new Map<string, RequestDetail>();
  const requests = new Map<
    string,
    { request: OrchestrationV2RuntimeRequest; runId: string | null }
  >();
  const emittedRequests = new Map<string, string>();
  let activeProviderThreadId: string | null = null;
  let turnsReport = false;
  /** Until `ready`, usage changes are only counted: the reader gets the newest once. */
  let usageLive = false;
  let usageChangedAt: number | undefined;
  let sentUsage: string | undefined;
  let lastUsageMs = Number.NEGATIVE_INFINITY;
  let heldUsage: { usage: ThreadFeedUsage; position: number; atMs: number } | undefined;
  let lastSequence = 0;

  const common = (position: number) => ({ v: THREAD_FEED_VERSION, threadId, position });

  const rememberItem = (item: OrchestrationV2TurnItem) => {
    items.set(item.id, {
      status: item.status,
      text: isStreaming(item) ? streamedText(item) : undefined,
    });
    const detail = requestDetailOf(item);
    if (detail !== undefined) details.set(detail.requestId, detail);
  };

  const requestEvent = (requestId: string, position: number): Array<ThreadFeedEvent> => {
    const known = requests.get(requestId);
    if (known === undefined) return [];
    const request = toFeedRequest(known.request, details.get(requestId), known.runId);
    const signature = `${request.status}:${request.kind}`;
    if (emittedRequests.get(requestId) === signature) return [];
    emittedRequests.set(requestId, signature);
    if (request.state === "closed") {
      requests.delete(requestId);
      details.delete(requestId);
    }
    return [{ ...common(position), type: "request", request } satisfies FeedEventOf<"request">];
  };

  const itemEvents = (item: OrchestrationV2TurnItem, position: number): Array<ThreadFeedEvent> => {
    const previous = items.get(item.id);
    rememberItem(item);
    const detail = requestDetailOf(item);
    const requestEvents = detail === undefined ? [] : requestEvent(detail.requestId, position);
    const full = { ...common(position), type: "item", item: toFeedItem(item) } as const;
    if (previous === undefined || previous.status !== item.status) return [full, ...requestEvents];
    const text = streamedText(item);
    if (text !== undefined && previous.text !== undefined && isStreaming(item)) {
      if (!text.startsWith(previous.text)) return [full];
      if (text.length === previous.text.length) return [];
      return [
        {
          ...common(position),
          type: "text",
          itemId: item.id,
          runId: item.runId,
          delta: text.slice(previous.text.length),
          length: text.length,
        },
      ];
    }
    // Running tools rewrite their output often; their final state comes with the status change.
    if (item.status === "running" && full.item.tool !== undefined) return requestEvents;
    return [full, ...requestEvents];
  };

  /** The usage this event reports for the thread's own conversation, never a subagent's. */
  const usageOf = (event: OrchestrationV2StoredEvent["event"]): ThreadFeedUsage | undefined => {
    if (event.type === "provider-thread.updated") {
      const providerThread = event.payload;
      if (
        providerThread.appThreadId === threadId &&
        !isQueuedProviderThreadPlaceholder(providerThread) &&
        providerThread.id !== activeProviderThreadId
      ) {
        activeProviderThreadId = providerThread.id;
        turnsReport = false;
      }
      return providerThread.id === activeProviderThreadId &&
        !turnsReport &&
        providerThread.contextUsage != null
        ? toFeedUsage({
            ...providerThread.contextUsage,
            updatedAt: DateTime.formatIso(providerThread.updatedAt),
          })
        : undefined;
    }
    if (
      event.type === "provider-turn.updated" &&
      event.payload.providerThreadId === activeProviderThreadId &&
      event.payload.tokenUsage !== undefined
    ) {
      turnsReport = true;
      return toFeedUsage(event.payload.tokenUsage);
    }
    return undefined;
  };

  const usageEvent = (usage: ThreadFeedUsage, position: number, atMs: number) => {
    sentUsage = usageSignature(usage);
    lastUsageMs = atMs;
    heldUsage = undefined;
    return [{ ...common(position), type: "usage", usage } satisfies FeedEventOf<"usage">];
  };

  const usageEvents = (stored: OrchestrationV2StoredEvent): Array<ThreadFeedEvent> => {
    const usage = usageOf(stored.event);
    if (usage === undefined) return [];
    if (!usageLive) {
      usageChangedAt = stored.sequence;
      return [];
    }
    const known = heldUsage === undefined ? sentUsage : usageSignature(heldUsage.usage);
    if (known === usageSignature(usage)) return [];
    const atMs = DateTime.toEpochMillis(stored.event.occurredAt);
    if (atMs - lastUsageMs >= USAGE_INTERVAL_MS) return usageEvent(usage, stored.sequence, atMs);
    heldUsage = { usage, position: stored.sequence, atMs };
    return [];
  };

  return {
    /**
     * What catch-up found: the position of the newest usage change it passed, if any. From here
     * on usage changes are sent as they happen.
     */
    startUsage: (seed: UsageSeed) => {
      const changedAt = usageChangedAt;
      usageChangedAt = undefined;
      activeProviderThreadId = seed.activeProviderThreadId;
      turnsReport = seed.turnsReport;
      sentUsage = seed.usage === null ? undefined : usageSignature(seed.usage);
      usageLive = true;
      return changedAt;
    },
    /** Before catch-up: which conversation's usage to count. */
    seedUsage: (seed: Pick<UsageSeed, "activeProviderThreadId" | "turnsReport">) => {
      activeProviderThreadId = seed.activeProviderThreadId;
      turnsReport = seed.turnsReport;
    },
    /** The usage held back to keep to one line a second, once it is due. Positions never go back. */
    flushUsage: (): Array<ThreadFeedEvent> => {
      if (heldUsage === undefined) return [];
      const { usage, position, atMs } = heldUsage;
      if (usageSignature(usage) === sentUsage) {
        heldUsage = undefined;
        return [];
      }
      return usageEvent(usage, Math.max(position, lastSequence), atMs);
    },
    /** A pending request this event opens whose question or approval item is not known yet. */
    missingDetail: (stored: OrchestrationV2StoredEvent) =>
      stored.event.type === "runtime-request.updated" &&
      stored.event.payload.status === "pending" &&
      !details.has(stored.event.payload.id)
        ? stored.event.payload
        : undefined,
    /** Items and requests the reader already holds, from the snapshot. */
    seed: (input: {
      readonly items?: ReadonlyArray<OrchestrationV2TurnItem>;
      readonly runs?: ReadonlyArray<OrchestrationV2Run>;
      readonly requests?: ReadonlyArray<{
        readonly request: OrchestrationV2RuntimeRequest;
        readonly feed: ThreadFeedRequest;
        readonly detailItem?: OrchestrationV2TurnItem;
      }>;
    }) => {
      for (const item of input.items ?? []) rememberItem(item);
      for (const run of input.runs ?? []) runs.set(run.id, run.status);
      for (const entry of input.requests ?? []) {
        if (entry.detailItem !== undefined) rememberItem(entry.detailItem);
        requests.set(entry.request.id, { request: entry.request, runId: entry.feed.runId });
        emittedRequests.set(entry.request.id, `${entry.feed.status}:${entry.feed.kind}`);
      }
    },
    translate: (stored: OrchestrationV2StoredEvent): Array<ThreadFeedEvent> => {
      const { event, sequence } = stored;
      if (event.threadId !== threadId) return [];
      lastSequence = Math.max(lastSequence, sequence);
      switch (event.type) {
        case "turn-item.updated":
          return itemEvents(event.payload, sequence);
        case "run.created":
        case "run.updated": {
          const run = event.payload;
          if (runs.get(run.id) === run.status) return [];
          runs.set(run.id, run.status);
          return [{ ...common(sequence), type: "run", run: toFeedRun(run) }];
        }
        case "runtime-request.updated":
          requests.set(event.payload.id, { request: event.payload, runId: event.runId ?? null });
          return requestEvent(event.payload.id, sequence);
        case "provider-thread.updated":
        case "provider-turn.updated":
          return usageEvents(stored);
        default:
          return [];
      }
    },
  };
};

const decodeRun = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2RunJson));
const decodeRequest = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2RuntimeRequestJson),
);
const decodeItem = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2TurnItemJson));
const decodeTurnUsage = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderTurnTokenUsage),
);
const decodeContextUsage = Schema.decodeUnknownEffect(
  Schema.fromJsonString(ThreadTokenUsageSnapshot),
);

export interface ThreadFeedFollowInput {
  readonly threadId: ThreadId;
  /** Send only what came after this position; without it, start at the thread's state now. */
  readonly after?: number | undefined;
  /** Without `after`, first send the thread's last `tail` items. */
  readonly tail?: number | undefined;
}

export interface ThreadFeedShape {
  /** Never fails: a thread it cannot follow ends with an `error` event. */
  readonly follow: (input: ThreadFeedFollowInput) => Stream.Stream<ThreadFeedEvent>;
}

export class ThreadFeed extends Context.Service<ThreadFeed, ThreadFeedShape>()(
  "t3/threadFeed/ThreadFeed",
) {}

export class ThreadFeedSourceError extends Schema.TaggedError<ThreadFeedSourceError>()(
  "ThreadFeedSourceError",
  { cause: Schema.Defect() },
) {}

/**
 * What the feed reads besides its own index seeks. The live stream must come from the server's
 * one event sink: its subscribers hear commits through that instance's in-memory PubSub.
 */
export interface ThreadFeedSource {
  readonly latestSequence: (threadId: ThreadId) => Effect.Effect<number, ThreadFeedSourceError>;
  /** The thread's stored events after `afterSequence`, read by keyset, then live. */
  readonly eventsAfter: (
    threadId: ThreadId,
    afterSequence: number,
  ) => Stream.Stream<OrchestrationV2StoredEvent, ThreadFeedSourceError>;
}

export const make = Effect.fn("ThreadFeed.make")(function* (source: ThreadFeedSource) {
  const sql = yield* SqlClient.SqlClient;

  // Each read seeks an index, so none grows with the thread's length.
  const readTail = Effect.fn("ThreadFeed.readTail")(function* (threadId: ThreadId, tail: number) {
    if (tail <= 0) return [];
    const rows = yield* sql<{ payload_json: string }>`
      SELECT payload_json FROM orchestration_v2_projection_turn_items
      WHERE thread_id = ${threadId}
      ORDER BY ordinal DESC, turn_item_id DESC
      LIMIT ${Math.min(tail, MAX_TAIL_ITEMS)}`;
    const decoded = yield* Effect.forEach(rows, (row) => decodeItem(row.payload_json));
    return decoded.toReversed();
  });

  // Questions and approvals are items of the request's node; a node holds one turn's items.
  const readDetailItem = Effect.fn("ThreadFeed.readDetailItem")(function* (
    request: OrchestrationV2RuntimeRequest,
  ) {
    const rows = yield* sql<{ payload_json: string }>`
      SELECT payload_json FROM orchestration_v2_projection_turn_items
      WHERE node_id = ${request.nodeId}
        AND type IN ('approval_request', 'user_input_request')
        AND json_extract(payload_json, '$.requestId') = ${request.id}
      LIMIT 1`;
    return rows[0] === undefined ? undefined : yield* decodeItem(rows[0].payload_json);
  });

  // The server prunes events the projections cover once they are a day old, keeping only each
  // thread's newest, so a replay from below this boundary would miss changes.
  const prunedThrough = sql<{ last_sequence: number }>`
    SELECT last_sequence FROM orchestration_v2_projection_metadata
    WHERE projection_name = ${EVENT_LOG_PRUNED_THROUGH}`.pipe(
    Effect.map((rows) => rows[0]?.last_sequence ?? 0),
  );

  // The usage T3's window shows: the newest report among the active conversation's turns, else
  // that conversation's own. The turns are read from the newest through their
  // (provider_thread_id, ordinal) index, a fixed number of them however long the thread is.
  const readUsage = Effect.fn("ThreadFeed.readUsage")(function* (
    activeProviderThreadId: string | null,
  ) {
    if (activeProviderThreadId === null) return { turnsReport: false, usage: null };
    const turnRows = yield* sql<{ usage: string }>`
      SELECT usage FROM (
        SELECT json_extract(payload_json, '$.tokenUsage') AS usage, ordinal
        FROM orchestration_v2_projection_provider_turns
        WHERE provider_thread_id = ${activeProviderThreadId}
        ORDER BY ordinal DESC
        LIMIT ${USAGE_TURNS_READ}
      )
      WHERE usage IS NOT NULL
      ORDER BY ordinal ASC`;
    let latest: ThreadFeedUsage | null = null;
    for (const row of turnRows) {
      const usage = yield* decodeTurnUsage(row.usage);
      // Ties go to the later turn, as in the window's own pick.
      if (latest === null || Date.parse(usage.updatedAt) >= Date.parse(latest.updatedAt)) {
        latest = toFeedUsage(usage);
      }
    }
    if (latest !== null) return { turnsReport: true, usage: latest };
    const threadRows = yield* sql<{ usage: string | null; updated_at: string }>`
      SELECT json_extract(payload_json, '$.contextUsage') AS usage, updated_at
      FROM orchestration_v2_projection_provider_threads
      WHERE provider_thread_id = ${activeProviderThreadId}`;
    const row = threadRows[0];
    if (row === undefined || row.usage === null) return { turnsReport: false, usage: null };
    const usage = yield* decodeContextUsage(row.usage);
    return { turnsReport: false, usage: toFeedUsage({ ...usage, updatedAt: row.updated_at }) };
  });

  const readState = Effect.fn("ThreadFeed.readState")(function* (threadId: ThreadId) {
    const threadRows = yield* sql<{
      title: string;
      archived_at: string | null;
      active_provider_thread_id: string | null;
    }>`
      SELECT title, archived_at, active_provider_thread_id
      FROM orchestration_v2_projection_threads
      WHERE thread_id = ${threadId} AND deleted_at IS NULL`;
    const thread = threadRows[0];
    if (thread === undefined) return null;
    const runRows = yield* sql<{ payload_json: string }>`
      SELECT payload_json FROM orchestration_v2_projection_runs
      WHERE thread_id = ${threadId}
      ORDER BY ordinal DESC
      LIMIT 1`;
    const run = runRows[0] === undefined ? null : yield* decodeRun(runRows[0].payload_json);
    const requestRows = yield* sql<{ payload_json: string }>`
      SELECT payload_json FROM orchestration_v2_projection_runtime_requests
      WHERE thread_id = ${threadId} AND status = 'pending'
      ORDER BY created_at ASC`;
    const requests = yield* Effect.forEach(requestRows, (row) =>
      Effect.gen(function* () {
        const request = yield* decodeRequest(row.payload_json);
        const detailItem = yield* readDetailItem(request);
        const feed = toFeedRequest(
          request,
          detailItem === undefined ? undefined : requestDetailOf(detailItem),
          detailItem?.runId ?? null,
        );
        return { request, feed, ...(detailItem === undefined ? {} : { detailItem }) };
      }),
    );
    return {
      thread: {
        title: thread.title,
        status: run?.status ?? ("idle" as const),
        archived: thread.archived_at !== null,
      },
      run,
      requests,
      usage: {
        activeProviderThreadId: thread.active_provider_thread_id,
        ...(yield* readUsage(thread.active_provider_thread_id)),
      },
    };
  });

  const follow = (input: ThreadFeedFollowInput): Stream.Stream<ThreadFeedEvent> => {
    const { threadId } = input;
    let lastPosition = input.after ?? 0;
    const errorEvent = (message: string): ThreadFeedEvent => ({
      v: THREAD_FEED_VERSION,
      type: "error",
      threadId,
      position: lastPosition,
      message,
    });
    const events = Stream.unwrap(
      Effect.gen(function* () {
        const head = yield* source.latestSequence(threadId);
        const initial = yield* readState(threadId);
        if (initial === null) {
          lastPosition = input.after ?? head;
          return Stream.succeed(errorEvent(`No thread ${threadId} on this server.`));
        }
        // A position whose events were pruned starts over as a fresh follow with the longest tail.
        const after =
          input.after !== undefined && input.after < (yield* prunedThrough)
            ? undefined
            : input.after;
        const tail = after === input.after ? (input.tail ?? 0) : MAX_TAIL_ITEMS;
        const translator = makeThreadFeedTranslator(threadId);
        const tailItems = after === undefined ? yield* readTail(threadId, tail) : [];
        translator.seed({ items: tailItems });
        translator.seedUsage(initial.usage);
        const tailEvents = tailItems.map((item): ThreadFeedEvent => ({
          v: THREAD_FEED_VERSION,
          type: "item",
          threadId,
          position: head,
          item: toFeedItem(item),
        }));
        // After catch-up the state is read again, so `ready` describes the thread as it is now.
        const readyEvents = Effect.fn("ThreadFeed.readyEvents")(function* (fresh: boolean) {
          const state = fresh ? yield* readState(threadId) : initial;
          if (state === null) return [errorEvent(`Thread ${threadId} was deleted.`)];
          translator.seed({
            ...(state.run === null ? {} : { runs: [state.run] }),
            requests: state.requests,
          });
          // A resume that passed usage changes gets the newest once, not each one in between.
          const usageChangedAt = translator.startUsage(state.usage);
          const usage: Array<ThreadFeedEvent> =
            usageChangedAt === undefined || state.usage.usage === null
              ? []
              : [
                  {
                    v: THREAD_FEED_VERSION,
                    type: "usage",
                    threadId,
                    position: usageChangedAt,
                    usage: state.usage.usage,
                  },
                ];
          const ready: ThreadFeedEvent = {
            v: THREAD_FEED_VERSION,
            type: "ready",
            threadId,
            position: head,
            thread: state.thread,
            run: state.run === null ? null : toFeedRun(state.run),
            requests: state.requests.map((entry) => entry.feed),
            usage: state.usage.usage,
          };
          return [...usage, ready];
        });
        const start = after ?? head;
        const caughtUp = start >= head;
        let readySent = caughtUp;
        const initialReady = caughtUp ? yield* readyEvents(false) : [];
        // The thread's newest event is never compacted away, so catch-up always reaches `head`.
        const translated = source.eventsAfter(threadId, start).pipe(
          Stream.mapEffect((stored) =>
            Effect.gen(function* () {
              const pending = translator.missingDetail(stored);
              const detailItem = pending === undefined ? undefined : yield* readDetailItem(pending);
              if (detailItem !== undefined) translator.seed({ items: [detailItem] });
              const out = translator.translate(stored);
              if (readySent || stored.sequence < head) return out;
              readySent = true;
              return [...out, ...(yield* readyEvents(true))];
            }),
          ),
          Stream.flattenIterable,
        );
        // A change held back to keep to one line a second goes out by itself, with no later event.
        const heldUsage = Stream.tick(USAGE_INTERVAL_MS).pipe(
          Stream.map(() => translator.flushUsage()),
          Stream.flattenIterable,
        );
        return Stream.fromIterable([...tailEvents, ...initialReady]).pipe(
          Stream.concat(translated.pipe(Stream.merge(heldUsage, { haltStrategy: "left" }))),
        );
      }),
    );
    return events.pipe(
      Stream.tap((event) =>
        Effect.sync(() => {
          if ("position" in event) lastPosition = event.position;
        }),
      ),
      Stream.catchCause((cause) =>
        Stream.unwrap(
          Effect.logWarning("thread feed ended with an error", { threadId, cause }).pipe(
            Effect.as(Stream.succeed(errorEvent("The server stopped reading this thread."))),
          ),
        ),
      ),
    );
  };

  return ThreadFeed.of({ follow });
});

export const layer = Layer.effect(
  ThreadFeed,
  Effect.gen(function* () {
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const sourceError = (cause: unknown) => new ThreadFeedSourceError({ cause });
    return yield* make({
      latestSequence: (threadId) =>
        threads.getThreadEventSequence(threadId).pipe(Effect.mapError(sourceError)),
      eventsAfter: (threadId, afterSequence) =>
        threads
          .streamStoredEventsFrom({ threadId, afterSequence })
          .pipe(Stream.mapError(sourceError)),
    });
  }),
);
