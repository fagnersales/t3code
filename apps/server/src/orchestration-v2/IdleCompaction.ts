import type {
  IdleCompactionMode,
  OrchestrationV2ProviderTurnTokenUsage,
  OrchestrationV2Run,
  OrchestrationV2ThreadProjection,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { isListedThreadLineage, OrchestrationV2RunStatus } from "@t3tools/contracts";
import {
  COMPACTION_SUGGESTION_MIN_TOKENS,
  hasDismissedResumeCompaction,
  isCompactCommand,
  shouldOfferResumeCompaction,
} from "@t3tools/shared/claudeCompaction";
import { derivePendingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";

// Claude Code's idleCompaction defaults to 200K and won't go below 100K. Below
// that a cold re-read is cheap, so compacting mostly loses context for little
// saving.
const MIN_CONTEXT_TOKENS = COMPACTION_SUGGESTION_MIN_TOKENS;
// Start this long before the cache expires: enough for the session to resume
// and send the compaction while the cache is still warm.
const LEAD_MS = 2 * 60_000;

export type IdleCompactionRecords = Pick<
  OrchestrationV2ThreadProjection,
  | "thread"
  | "runs"
  | "messages"
  | "providerThreads"
  | "providerTurns"
  | "runtimeRequests"
  | "turnItems"
  | "contextTransfers"
>;

/** Work the next turn must see: a pending request, a pending merge-back, or background work. */
function hasPendingWork(
  {
    thread,
    runs,
    providerThreads,
    runtimeRequests,
    turnItems,
    contextTransfers,
  }: Omit<IdleCompactionRecords, "messages" | "providerTurns">,
  latestRun: OrchestrationV2Run,
): boolean {
  return (
    runtimeRequests.some((request) => request.status === "pending") ||
    // A pending merge-back belongs to the user's next message.
    contextTransfers.some(
      (transfer) =>
        transfer.type === "merge_back" &&
        transfer.targetThreadId === thread.id &&
        transfer.status === "pending",
    ) ||
    // Background work reports into the next turn; compaction would swallow it.
    derivePendingBackgroundWork({
      latestRun,
      runs,
      providerThreads,
      turnItems,
      activeProviderThreadId: thread.activeProviderThreadId,
    }).length > 0
  );
}

/**
 * The latest token usage of the Claude session that ran `latestRun`, or null when the thread's
 * current selection is no longer that session (it receives the /compact) or it reported none.
 */
function activeClaudeSessionUsage(
  {
    thread,
    providerThreads,
    providerTurns,
  }: Pick<IdleCompactionRecords, "thread" | "providerThreads" | "providerTurns">,
  latestRun: OrchestrationV2Run,
): OrchestrationV2ProviderTurnTokenUsage | null {
  const providerThread = providerThreads.find(
    (candidate) => candidate.id === thread.activeProviderThreadId,
  );
  if (
    providerThread?.driver !== "claudeAgent" ||
    providerThread.id !== latestRun.providerThreadId ||
    providerThread.providerInstanceId !== thread.providerInstanceId ||
    latestRun.providerInstanceId !== thread.providerInstanceId
  )
    return null;
  return (
    providerTurns
      .filter(
        (turn) => turn.providerThreadId === providerThread.id && turn.tokenUsage !== undefined,
      )
      .map((turn) => turn.tokenUsage!)
      .toSorted((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt))[0] ??
    null
  );
}

function latestRunOf(runs: IdleCompactionRecords["runs"]): OrchestrationV2Run | null {
  // A removed or steered queued message leaves a cancelled run that never started.
  return runs
    .filter((run) => run.status !== "cancelled" || run.startedAt !== null)
    .reduce<OrchestrationV2Run | null>(
      (latest, run) => (latest === null || run.ordinal > latest.ordinal ? run : latest),
      null,
    );
}

const IDLE_RUN_STATUSES: ReadonlySet<OrchestrationV2Run["status"]> = new Set([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
  "rolled_back",
]);

/** With the latest run, the runs `shouldCompactBeforeSend` and steering look at. */
export const UNFINISHED_RUN_STATUSES = OrchestrationV2RunStatus.literals.filter(
  (status) => !IDLE_RUN_STATUSES.has(status),
);

/**
 * Whether a message sent to this thread should go behind a `/compact` run, the rule T3's window
 * applies when the user returns to a big Claude chat whose prompt cache has expired. Only for an
 * idle thread: a busy one queues or steers as before, and its cache is warm anyway.
 */
export function shouldCompactBeforeSend(
  records: Omit<IdleCompactionRecords, "messages">,
  input: {
    readonly text: string;
    readonly mode: IdleCompactionMode;
    /** The instance the message asks for, when it names one. */
    readonly instanceId?: ProviderInstanceId | undefined;
    readonly now: string;
  },
): boolean {
  const { thread, runs, runtimeRequests } = records;
  if (input.mode === "off" || isCompactCommand(input.text)) return false;
  if (thread.archivedAt !== null || thread.deletedAt !== null) return false;
  if (input.instanceId !== undefined && input.instanceId !== thread.providerInstanceId)
    return false;
  if (hasDismissedResumeCompaction(runtimeRequests)) return false;
  const latestRun = latestRunOf(runs);
  if (latestRun === null || runs.some((run) => !IDLE_RUN_STATUSES.has(run.status))) return false;
  if (hasPendingWork(records, latestRun)) return false;
  const usage = activeClaudeSessionUsage(records, latestRun);
  return (
    usage !== null &&
    shouldOfferResumeCompaction({
      provider: "claudeAgent",
      usedTokens: usage.usedTokens,
      updatedAt: usage.updatedAt,
      promptCacheTtlMs: usage.promptCacheTtlMs,
      now: input.now,
    })
  );
}

/**
 * When an idle Claude thread should compact, or null when it should not. The
 * window ends when the prompt cache of Claude's last request expires (1h on
 * subscriptions, 5m on API keys); compacting after that pays a full uncached
 * read.
 */
export function idleCompactionWindow(
  {
    thread,
    runs,
    messages,
    providerThreads,
    providerTurns,
    runtimeRequests,
    turnItems,
    contextTransfers,
  }: IdleCompactionRecords,
  nowMs: number,
): {
  readonly runId: (typeof runs)[number]["id"];
  readonly startMs: number;
  readonly endMs: number;
} | null {
  if (thread.deletedAt !== null || thread.archivedAt !== null) return null;
  if (thread.settledAt !== null || thread.settledOverride === "settled") return null;
  if (thread.snoozedUntil != null && DateTime.toEpochMillis(thread.snoozedUntil) > nowMs)
    return null;
  if (!isListedThreadLineage(thread.lineage)) return null;
  const latestRun = latestRunOf(runs);
  if (latestRun?.status !== "completed" || latestRun.completedAt === null) return null;
  const message = messages.find((candidate) => candidate.id === latestRun.userMessageId);
  // Never compact a compaction, so one idle spell compacts at most once.
  if (message === undefined || message.text.trim().toLowerCase() === "/compact") return null;
  if (
    hasPendingWork(
      { thread, runs, providerThreads, runtimeRequests, turnItems, contextTransfers },
      latestRun,
    )
  )
    return null;
  const usage = activeClaudeSessionUsage({ thread, providerThreads, providerTurns }, latestRun);
  if (usage === null || usage.usedTokens < MIN_CONTEXT_TOKENS) return null;
  // A request served fully from cache creates no cache entry and reports no
  // TTL, but it still refreshes the cache. Treat it as unknown and skip.
  const expiresAt = usage.promptCacheExpiresAt;
  const endMs = expiresAt === undefined ? Number.NaN : Date.parse(expiresAt);
  if (!Number.isFinite(endMs) || nowMs >= endMs) return null;
  return { runId: latestRun.id, startMs: endMs - LEAD_MS, endMs };
}
