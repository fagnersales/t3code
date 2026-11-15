# Thread feed, version 1

Fork-only (`fagnersales/t3code`). A live feed of a thread's events: new timeline items, the
assistant's text as it is written, run status, questions and approvals as they open and
close, and how full the thread's context is. Schemas: [`packages/contracts/src/threadFeed.ts`](../../packages/contracts/src/threadFeed.ts).

## Reading it

From a shell, one JSON object per line on stdout, until killed:

```sh
t3 thread follow <thread>... [--after <position>] [--tail <n>] --jsonl
```

- Several threads in one process; every event names its `threadId`.
- `--after <position>`: send only what changed after that position, then `ready`, then live
  events. `<thread>@<position>` resumes one thread from its own position.
- Without `--after`: send the thread's last `--tail` items (default 0, max 200), then `ready`.
- It reconnects by itself when the server restarts or the connection drops (printing a
  `disconnected` line), resuming each thread from the last position it printed.
- Without `--jsonl` it prints a short text view for a person.
- Exit status: 0 when every thread ended with `error`; 1 for bad arguments, or a server
  without the feed (upstream T3 Code answers 404).

Over HTTP, the same events (the CLI is a thin client of this):

```
GET /api/thread-feed/v1?thread=<id>[@<position>]&thread=…[&after=<position>][&tail=<n>]
Authorization: Bearer <session token with the orchestration:read scope>
```

The response is `application/x-ndjson`, with a `heartbeat` line every 20 s. Tokens come from
`t3 auth session issue --scope orchestration:read`.

## Positions

`position` is the event-log sequence of the change an event reports. It only grows. Save the last
`position` you processed per thread and pass it back with `--after` (or `thread@position`). A
resume reads only the thread's events after it, through an index, so it costs the same on a thread
with 20,000 items as on a new one. Several events can share a position (one commit can change
several items).

The log keeps each item's newest version, so a resume from an old position still gets the current
state of every item changed since, but not every intermediate text delta.

## Events

Every line has `v: 1` and a `type`. All but `heartbeat` and `disconnected` have `threadId` and
`position`. Ignore types you do not know; new types and optional fields can appear within
version 1.

| `type`         | Meaning                                                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------ |
| `item`         | A new item, or one whose status or content changed. Upsert by `item.itemId`.                           |
| `text`         | `delta` was appended to a streaming item's text. `length` is the text length after it.                 |
| `run`          | A run's status changed.                                                                                |
| `request`      | A question or approval opened (`state: "open"`) or closed (`"closed"`).                                |
| `usage`        | The thread's context usage changed. Replace what you hold with `usage`.                                |
| `ready`        | Catch-up is done. The thread, its latest run, its open requests and its usage now. Live events follow. |
| `error`        | This thread cannot be followed (for example, it does not exist). Its feed ends; others go on.          |
| `heartbeat`    | HTTP only, every 20 s.                                                                                 |
| `disconnected` | CLI only: the connection dropped and is retried in `retryInMs`.                                        |

`item.item` fields: `itemId`, `runId`, `ordinal`, `kind` (the item type: `user_message`,
`assistant_message`, `reasoning`, `proposed_plan`, `command_execution`, `file_change`,
`dynamic_tool`, `subagent`, `user_input_request`, `approval_request`, `error`, …), `status`,
`title`, and when they apply: `role` (`user` or `assistant`), `text` (the whole text so far),
`streaming`, `createdBy`, `requestId`, and `tool` (`name`, `input`, `output`, `exitCode`,
`truncated`; input and output are cut at 4,000 characters, read the full text with
`t3 thread show`). Times are ISO strings: `startedAt`, `completedAt`, `updatedAt`.

Streaming text: the first event for an item is an `item` with the text so far. Later chunks are
`text` deltas. When the item finishes, an `item` with the full final text arrives. If a delta's
`length` does not match `your text length + delta.length`, you missed one; take the next `item`.
Running tools send their final state with the status change, not every output update.

`run.run`: `runId`, `ordinal`, `status` (`preparing`, `queued`, `starting`, `running`, `waiting`,
`completed`, `interrupted`, `failed`, `cancelled`, `rolled_back`), `requestedAt`, `startedAt`,
`completedAt`.

`request.request`: `requestId`, `runId`, `state`, `status` (`pending`, `resolved`, `expired`,
`cancelled`), `kind` (`question`, `approval`, `other`), `requestKind`, `createdAt`, and for
questions `questions` (`id`, `header`, `question`, `options`, `multiSelect`, …), for approvals
`prompt`, `reason`, `options` (`decision`, `label`), and once answered `decision`. Answer with the
`runtime-request.respond` command as before.

`ready`: `thread` (`title`, `status`: the latest run's status or `idle`, `archived`), `run` (the
latest run, or null), `requests` (the open ones), `usage` (the thread's usage now, or null when
no provider reported one).

`usage.usage` and `ready.usage`: `usedTokens`, `maxTokens` (the context window's size; null
when the provider gave none), `autoCompactThreshold` (where the provider compacts by itself;
absent when it has no such point) and `updatedAt` (when the provider reported it). It is what
T3's own window shows for the thread: the newest report among the turns of the thread's active
conversation with the provider, or for a provider that reports per conversation (the ACP agents),
that conversation's own. A subagent's turns never count. Two cases the window covers are left
out: a fork that has not run yet (the window shows its source's usage; the feed says null), and
a conversation whose newest 32 turns reported nothing.

- A `usage` line is sent when `usedTokens`, `maxTokens` or `autoCompactThreshold` changed, not
  for a report that repeats them.
- At most about one line a second per thread: a change within a second of the last line is held
  and the newest one goes out up to a second later. Such a held line carries the position of the
  newest event sent before it, so positions never go back.
- A resume sends at most one `usage` line, before `ready`, with the usage now, when a report
  came after the position; its position is that of the newest report.
- After a compaction the next report is a smaller number, sent like any other change. Until the
  provider reports it, the usage stays the one from before: the feed makes none up.

## Example

`t3 thread follow thread-1 --after 81234 --jsonl` while the thread answers a message (ids
shortened, one line per event in the real stream):

```json
{"v":1,"type":"item","threadId":"thread-1","position":81240,"item":{"itemId":"item-90","runId":"run-7","ordinal":90,"kind":"user_message","status":"completed","title":null,"role":"user","text":"Ship the fix?","createdBy":"user","startedAt":"2026-11-09T10:00:00.000Z","completedAt":"2026-11-09T10:00:00.000Z","updatedAt":"2026-11-09T10:00:00.000Z"}}
{"v":1,"type":"run","threadId":"thread-1","position":81241,"run":{"runId":"run-7","ordinal":7,"status":"running","requestedAt":"2026-11-09T10:00:00.000Z","startedAt":"2026-11-09T10:00:01.000Z","completedAt":null}}
{"v":1,"type":"ready","threadId":"thread-1","position":81241,"thread":{"title":"Auto Delivery","status":"running","archived":false},"run":{"runId":"run-7","ordinal":7,"status":"running","requestedAt":"2026-11-09T10:00:00.000Z","startedAt":"2026-11-09T10:00:01.000Z","completedAt":null},"requests":[],"usage":{"usedTokens":411200,"maxTokens":1000000,"autoCompactThreshold":835000,"updatedAt":"2026-11-09T09:58:40.000Z"}}
{"v":1,"type":"usage","threadId":"thread-1","position":81245,"usage":{"usedTokens":412000,"maxTokens":1000000,"autoCompactThreshold":835000,"updatedAt":"2026-11-09T10:00:01.500Z"}}
{"v":1,"type":"item","threadId":"thread-1","position":81250,"item":{"itemId":"item-91","runId":"run-7","ordinal":91,"kind":"assistant_message","status":"running","title":null,"role":"assistant","text":"Running the","streaming":true,"startedAt":"2026-11-09T10:00:02.000Z","completedAt":null,"updatedAt":"2026-11-09T10:00:02.000Z"}}
{"v":1,"type":"text","threadId":"thread-1","position":81251,"itemId":"item-91","runId":"run-7","delta":" tests first.","length":24}
{"v":1,"type":"item","threadId":"thread-1","position":81260,"item":{"itemId":"item-92","runId":"run-7","ordinal":92,"kind":"command_execution","status":"completed","title":null,"tool":{"name":"command","input":"vp test run","output":"Tests 42 passed","exitCode":0},"startedAt":"2026-11-09T10:00:03.000Z","completedAt":"2026-11-09T10:00:09.000Z","updatedAt":"2026-11-09T10:00:09.000Z"}}
{"v":1,"type":"request","threadId":"thread-1","position":81262,"request":{"requestId":"request-3","runId":"run-7","state":"open","status":"pending","kind":"question","requestKind":"user_input","questions":[{"id":"deploy","header":"Deploy","question":"Deploy now?","options":[{"label":"Yes","description":"Ship it"},{"label":"No","description":"Wait"}]}],"createdAt":"2026-11-09T10:00:10.000Z"}}
{"v":1,"type":"run","threadId":"thread-1","position":81262,"run":{"runId":"run-7","ordinal":7,"status":"waiting","requestedAt":"2026-11-09T10:00:00.000Z","startedAt":"2026-11-09T10:00:01.000Z","completedAt":null}}
{"v":1,"type":"request","threadId":"thread-1","position":81270,"request":{"requestId":"request-3","runId":"run-7","state":"closed","status":"resolved","kind":"question","requestKind":"user_input","questions":[{"id":"deploy","header":"Deploy","question":"Deploy now?","options":[{"label":"Yes","description":"Ship it"},{"label":"No","description":"Wait"}]}],"createdAt":"2026-11-09T10:00:10.000Z"}}
{"v":1,"type":"item","threadId":"thread-1","position":81280,"item":{"itemId":"item-91","runId":"run-7","ordinal":91,"kind":"assistant_message","status":"completed","title":null,"role":"assistant","text":"Running the tests first. All 42 pass; deployed.","streaming":false,"startedAt":"2026-11-09T10:00:02.000Z","completedAt":"2026-11-09T10:00:20.000Z","updatedAt":"2026-11-09T10:00:20.000Z"}}
{"v":1,"type":"run","threadId":"thread-1","position":81281,"run":{"runId":"run-7","ordinal":7,"status":"completed","requestedAt":"2026-11-09T10:00:00.000Z","startedAt":"2026-11-09T10:00:01.000Z","completedAt":"2026-11-09T10:00:20.000Z"}}
{"v":1,"type":"heartbeat"}
```

## Cost

`node scripts/thread-feed-bench.ts` in `apps/server` (a synthetic thread in a temp SQLite file,
median of 20 runs, Apple Silicon):

| Read                                               | 1,000 items | 30,000 items |
| -------------------------------------------------- | ----------: | -----------: |
| feed: resume, 10 items behind                      |      4.7 ms |       4.9 ms |
| feed: start fresh, state only                      |      0.6 ms |       1.0 ms |
| feed: start fresh, `--tail 20`                     |      1.1 ms |       1.7 ms |
| feed: write to event on the stream                 |      4.6 ms |       1.1 ms |
| `t3_thread_read`, newest 10 messages (thread show) |      9.9 ms |       469 ms |
| `getThreadProjection` (whole thread)               |       24 ms |       823 ms |

`ready` reads the usage with two more seeks: the active conversation's newest 32 turns through
their `(provider_thread_id, ordinal)` index, and for a provider that reports per conversation,
that conversation's row. A live `usage` line reads nothing: it comes from the event itself.
With a provider turn (and its usage) every 10 items:

| Read                                       | 1,000 items, 100 turns | 30,000 items, 3,000 turns |
| ------------------------------------------ | ---------------------: | ------------------------: |
| feed: start fresh, state only (`ready`)    |                 1.3 ms |                    1.2 ms |
| feed: usage write to `usage` on the stream |                 4.5 ms |                    5.1 ms |

The feed adds no database migration: its reads use indexes the Nightly already has.
