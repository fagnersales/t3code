/** Adapter and client match these dialog strings; reword them only here. */
export const CLAUDE_RESUME_COMPACTION_NEVER_ANSWER = "Don't ask again";
export const CLAUDE_RESUME_COMPACTION_COMPACT_ANSWER = "Compact and continue";

export function formatClaudeResumeCompactionQuestion(input: {
  readonly ageMinutes: number;
  readonly estimatedTokens: number;
}): string {
  const ageLabel =
    input.ageMinutes >= 60
      ? `${Math.floor(input.ageMinutes / 60)}h ${input.ageMinutes % 60}m`
      : `${input.ageMinutes}m`;
  return `This session is ${ageLabel} old and uses ${input.estimatedTokens.toLocaleString("en-US")} tokens. Compact it before continuing?`;
}

const CLAUDE_RESUME_COMPACTION_QUESTION_PATTERN =
  /^This session is (\d+h \d+m|\d+m) old and uses \d{1,3}(?:,\d{3})* tokens\. Compact it before continuing\?$/u;

export function isClaudeResumeCompactionQuestion(question: string): boolean {
  return CLAUDE_RESUME_COMPACTION_QUESTION_PATTERN.test(question);
}

/**
 * The session age from an answered resume question whose answer was to compact, or null. Answers
 * are keyed by question id, and the id may itself be the question text.
 */
export function claudeResumeCompactionAge(questionAnswer: {
  readonly questionTextById?: Readonly<Record<string, string>> | undefined;
  readonly answers: Readonly<Record<string, unknown>>;
}): string | null {
  for (const [id, answer] of Object.entries(questionAnswer.answers)) {
    const age = CLAUDE_RESUME_COMPACTION_QUESTION_PATTERN.exec(
      questionAnswer.questionTextById?.[id] ?? id,
    )?.[1];
    const chosen = Array.isArray(answer) ? answer : [answer];
    if (age !== undefined && chosen.includes(CLAUDE_RESUME_COMPACTION_COMPACT_ANSWER)) return age;
  }
  return null;
}

/**
 * Context size below which T3 does not suggest or run a Claude compaction:
 * the stale-thread Compact chip and idle compaction. Matches Claude Code's
 * idleCompaction default of 200K.
 */
export const COMPACTION_SUGGESTION_MIN_TOKENS = 200_000;

/** Prompt cache lifetime assumed when Claude reports none: its 1h cache plus slack. */
const CLAUDE_RESUME_COMPACTION_FALLBACK_MS = 70 * 60_000;

// A typed `/compact`, with or without instructions, is already the compaction. Queueing a
// bare one ahead of it would run an uninstructed compaction first and charge for two.
export const COMPACT_COMMAND_PATTERN = /^\/compact(?:\s|$)/i;

/** A message that is itself a compaction, so nothing should compact before it. */
export function isCompactCommand(text: string): boolean {
  return COMPACT_COMMAND_PATTERN.test(text);
}

/** Whether the user answered "never" to Claude's native resume-compaction question. */
export function hasDismissedResumeCompaction(
  requests: ReadonlyArray<{
    readonly kind: string;
    readonly status: string;
    readonly answers?: unknown;
  }>,
): boolean {
  return requests.some(({ kind, status, answers }) => {
    if (kind !== "user_input" || status !== "resolved") return false;
    if (!answers || typeof answers !== "object" || Array.isArray(answers)) return false;
    return Object.entries(answers).some(
      ([question, answer]) =>
        isClaudeResumeCompactionQuestion(question) &&
        answer === CLAUDE_RESUME_COMPACTION_NEVER_ANSWER,
    );
  });
}

/**
 * Whether a Claude thread is big and cold enough that its next message should compact first:
 * at least {@link COMPACTION_SUGGESTION_MIN_TOKENS} in context, and its last usage report older
 * than the prompt cache, so the next turn would re-read the whole history uncached.
 */
export function shouldOfferResumeCompaction(input: {
  readonly provider: string | null | undefined;
  readonly usedTokens: number | null | undefined;
  readonly updatedAt: string | null | undefined;
  readonly promptCacheTtlMs?: number | undefined;
  readonly now: string;
}): boolean {
  if (
    input.provider !== "claudeAgent" ||
    (input.usedTokens ?? 0) < COMPACTION_SUGGESTION_MIN_TOKENS
  ) {
    return false;
  }

  const updatedAt = Date.parse(input.updatedAt ?? "");
  const now = Date.parse(input.now);
  return (
    Number.isFinite(updatedAt) &&
    Number.isFinite(now) &&
    now - updatedAt >= (input.promptCacheTtlMs ?? CLAUDE_RESUME_COMPACTION_FALLBACK_MS)
  );
}
