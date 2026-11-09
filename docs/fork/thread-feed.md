# Thread feed, version 1

Fork-only (`fagnersales/t3code`). A live feed of a thread's events: new timeline items, the
assistant's text as it is written, run status, and questions and approvals as they open and
close. Schemas: [`packages/contracts/src/threadFeed.ts`](../../packages/contracts/src/threadFeed.ts).

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

| `type`         | Meaning                                                                                       |
| -------------- | --------------------------------------------------------------------------------------------- |
| `item`         | A new item, or one whose status or content changed. Upsert by `item.itemId`.                  |
| `text`         | `delta` was appended to a streaming item's text. `length` is the text length after it.        |
| `run`          | A run's status changed.                                                                       |
| `request`      | A question or approval opened (`state: "open"`) or closed (`"closed"`).                       |
| `ready`        | Catch-up is done. The thread, its latest run, and its open requests now. Live events follow.  |
| `error`        | This thread cannot be followed (for example, it does not exist). Its feed ends; others go on. |
| `heartbeat`    | HTTP only, every 20 s.                                                                        |
| `disconnected` | CLI only: the connection dropped and is retried in `retryInMs`.                               |

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
latest run, or null), `requests` (the open ones).

## Example

`t3 thread follow thread-1 --after 81234 --jsonl` while the thread answers a message (ids
shortened, one line per event in the real stream):

```json
{"v":1,"type":"item","threadId":"thread-1","position":81240,"item":{"itemId":"item-90","runId":"run-7","ordinal":90,"kind":"user_message","status":"completed","title":null,"role":"user","text":"Ship the fix?","createdBy":"user","startedAt":"2026-11-09T10:00:00.000Z","completedAt":"2026-11-09T10:00:00.000Z","updatedAt":"2026-11-09T10:00:00.000Z"}}
{"v":1,"type":"run","threadId":"thread-1","position":81241,"run":{"runId":"run-7","ordinal":7,"status":"running","requestedAt":"2026-11-09T10:00:00.000Z","startedAt":"2026-11-09T10:00:01.000Z","completedAt":null}}
{"v":1,"type":"ready","threadId":"thread-1","position":81241,"thread":{"title":"Auto Delivery","status":"running","archived":false},"run":{"runId":"run-7","ordinal":7,"status":"running","requestedAt":"2026-11-09T10:00:00.000Z","startedAt":"2026-11-09T10:00:01.000Z","completedAt":null},"requests":[]}
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

The feed adds no database migration: its reads use indexes the Nightly already has.
