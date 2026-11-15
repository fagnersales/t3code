import { ThreadId } from "@t3tools/contracts";
import { assert, it } from "vite-plus/test";

import { parseFollowQuery } from "../threadFeed/http.ts";
import { formatText, parseThreadArguments } from "./threadFollow.ts";

const threadA = ThreadId.make("thread-a");
const threadB = ThreadId.make("thread-b");

it("thread arguments take a shared --after, an own @position, and drop repeats", () => {
  assert.deepEqual(parseThreadArguments(["thread-a", "thread-b@42", "thread-a"], 7), [
    { threadId: threadA, position: 7 },
    { threadId: threadB, position: 42 },
  ]);
  assert.deepEqual(parseThreadArguments(["thread-a"], undefined), [
    { threadId: threadA, position: undefined },
  ]);
  assert.typeOf(parseThreadArguments(["thread-a@x"], undefined), "string");
  assert.typeOf(parseThreadArguments([], undefined), "string");
});

it("the text view prints a short line for the thread's usage", () => {
  const usage = {
    usedTokens: 412_000,
    maxTokens: 1_000_000,
    updatedAt: "2026-11-15T21:00:00.000Z",
  };
  assert.equal(
    formatText({ v: 1, type: "usage", threadId: threadA, position: 9, usage }),
    "[thread-a] context 412,000 of 1,000,000 tokens (41%)",
  );
  assert.equal(
    formatText({
      v: 1,
      type: "usage",
      threadId: threadA,
      position: 9,
      usage: { ...usage, maxTokens: null },
    }),
    "[thread-a] context 412,000 tokens",
  );
});

it("the feed route reads the same thread@position form from its query", () => {
  const parsed = parseFollowQuery(
    new URLSearchParams("thread=thread-a&thread=thread-b@42&after=7&tail=5"),
  );
  assert.deepEqual(parsed, [
    { threadId: threadA, after: 7, tail: 5 },
    { threadId: threadB, after: 42, tail: 5 },
  ]);
  assert.typeOf(parseFollowQuery(new URLSearchParams("after=1")), "string");
  assert.typeOf(parseFollowQuery(new URLSearchParams("thread=a&after=-1")), "string");
});
