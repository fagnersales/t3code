import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";

import {
  hasDismissedResumeCompaction,
  isCompactCommand,
  shouldOfferResumeCompaction,
} from "./claudeCompaction.ts";

const NOW = "2026-10-19T12:00:00.000Z";
const minutesAgo = (minutes: number) =>
  DateTime.formatIso(DateTime.makeUnsafe(Date.parse(NOW) - minutes * 60_000));

describe("shouldOfferResumeCompaction", () => {
  const cold = {
    provider: "claudeAgent",
    usedTokens: 250_000,
    updatedAt: minutesAgo(71),
    now: NOW,
  };

  it("offers for a big Claude context whose prompt cache has expired", () => {
    expect(shouldOfferResumeCompaction(cold)).toBe(true);
    expect(
      shouldOfferResumeCompaction({ ...cold, updatedAt: minutesAgo(6), promptCacheTtlMs: 300_000 }),
    ).toBe(true);
  });

  it.each([
    ["another provider", { provider: "codex" }],
    ["a small context", { usedTokens: 199_999 }],
    ["a warm cache", { updatedAt: minutesAgo(69) }],
    ["a warm reported cache", { updatedAt: minutesAgo(4), promptCacheTtlMs: 300_000 }],
    ["an unknown update time", { updatedAt: null }],
  ])("skips %s", (_, override) => {
    expect(shouldOfferResumeCompaction({ ...cold, ...override })).toBe(false);
  });
});

describe("isCompactCommand", () => {
  it.each([
    ["/compact", true],
    ["/COMPACT keep the plan", true],
    ["/compaction", false],
    ["please /compact", false],
  ])("%s → %s", (text, expected) => {
    expect(isCompactCommand(text)).toBe(expected);
  });
});

describe("hasDismissedResumeCompaction", () => {
  const resolved = (answers: Record<string, unknown>) => ({
    kind: "user_input",
    status: "resolved",
    answers,
  });

  it("recognizes the native resume dialog's permanent dismissal", () => {
    expect(
      hasDismissedResumeCompaction([
        resolved({
          "This session is 2h 0m old and uses 250,000 tokens. Compact it before continuing?":
            "Don't ask again",
        }),
      ]),
    ).toBe(true);
  });

  it("ignores unrelated questions that end with Claude's compaction prompt", () => {
    expect(
      hasDismissedResumeCompaction([
        resolved({ "The build cache is large. Compact it before continuing?": "Don't ask again" }),
      ]),
    ).toBe(false);
  });
});
