# Project feed, version 1

Fork-only (`fagnersales/t3code`). A live list of a project's threads: a snapshot, then each thread
created, renamed, starting or ending a run, asking a question, settled, snoozed, archived or deleted,
as it happens. It replaces polling `t3 thread list --project …`. Schemas:
[`packages/contracts/src/projectFeed.ts`](../../packages/contracts/src/projectFeed.ts).

## Reading it

From a shell, one JSON object per line on stdout, until killed:

```sh
t3 project follow <project>... [--after <position>] [--archived] --jsonl
```

- `<project>` is a project id, or a path inside the project (`.`, `~/work/blox-survivors`), as in
  `thread list --project`. Several projects in one process; every line names its `projectId`.
- Without `--after`: a `snapshot` line, one `thread` line per thread, then `ready`, then live
  events.
- `--after <position>`: only the threads that changed after that position, then `ready`, then
  live events. `<project>@<position>` resumes one project from its own position.
- `--archived`: the snapshot and `ready.threads` include archived threads. Without it they hold
  the project's non-archived threads (subagents included).
- It reconnects by itself when the server restarts or the connection drops: it prints a
  `disconnected` line, retries after 1 s (doubling up to 30 s), and resumes each project from
  the last position it printed, so the reconnect sends only what changed meanwhile.
- No session to manage: it issues a short-lived read-only session for each connection and
  revokes it when the connection ends, like `thread follow`.
- Without `--jsonl` it prints a short text view for a person.
- Exit status: 0 when every project ended with `error`; 1 for bad arguments, or a server without
  the feed (upstream T3 Code answers 404).

Over HTTP, the same events (the CLI is a thin client of this):

```
GET /api/project-feed/v1?project=<id or absolute path>[@<position>]&project=…[&after=<position>][&archived=1]
Authorization: Bearer <session token with the orchestration:read scope>
```

The response is `application/x-ndjson`, with a `heartbeat` line every 20 s. Tokens come from
`t3 auth session issue --scope orchestration:read`. A path may contain `@`; only a numeric suffix
is read as a position.

### Why reconnect instead of exiting

A restart of T3 (an update, `install --quit`, a crash) is routine, and a reader that exits on it
must restart itself and resume anyway. The CLI already does exactly that, with the resume cost
of a few milliseconds, so a caller only needs to treat `disconnected` as "the list may be
stale until the next `ready`". A caller that wants to own the retry policy can kill the process
when it sees `disconnected`, or read the HTTP route directly.

## Positions

`position` is the event-log sequence of the change a line reports, shared by every thread and
project on the server. It only grows. Save the last `position` you processed per project and
pass it back with `--after` (or `project@position`).

A resume finds the project's threads with any event after the position, one index seek per
thread on the log's `(stream_id, sequence)` index, and sends each one's state now. Its cost
depends on the project's thread count, not on how long ago the position is or how busy other
projects were. Threads deleted since then come as `removed`. Snapshot and resume lines carry the
position of `ready` (the log's head when the feed started); live lines carry their own.

## Events

Every line has `v: 1` and a `type`. Ignore types, fields and `changes` values you do not know;
they can appear within version 1.

| `type`         | Meaning                                                                                         |
| -------------- | ----------------------------------------------------------------------------------------------- |
| `snapshot`     | Forget the threads you hold for this project; the `thread` lines up to `ready` are all of them. |
| `thread`       | A thread's state now. Upsert it by `thread.threadId`.                                           |
| `removed`      | The thread was deleted (`reason: "deleted"`). Drop it.                                          |
| `ready`        | Catch-up is done; live events follow. `threads` is how many you should hold now.                |
| `error`        | This project cannot be followed (not found, or deleted). Its feed ends; others go on.           |
| `heartbeat`    | HTTP only, every 20 s.                                                                          |
| `disconnected` | CLI only: the connection dropped and is retried in `retryInMs`.                                 |

`snapshot`, `ready` and `error` carry `ref`: the project as the caller named it (for the CLI, the
real path or id it sent).

`thread.thread`:

| Field                                    | Meaning                                                                                                                                                                                                                                                                                                                                                                                              |
| ---------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `threadId`, `projectId`, `title`         |                                                                                                                                                                                                                                                                                                                                                                                                      |
| `status`                                 | As in `t3 thread list`: the active run's status, else the latest run's, else `idle`.                                                                                                                                                                                                                                                                                                                 |
| `run`                                    | The latest run: `runId`, `status`, `requestedAt`, `startedAt`, `completedAt`; null before the first run.                                                                                                                                                                                                                                                                                             |
| `pendingRequests`                        | Open questions and approvals.                                                                                                                                                                                                                                                                                                                                                                        |
| `pendingRequest`                         | The newest open one: `requestId`, `kind` (`user_input`, `command_approval`, …), `createdAt`, `blocking`.                                                                                                                                                                                                                                                                                             |
| `settled`, `settledAt`                   | Settled by the user, the agent or auto-settle.                                                                                                                                                                                                                                                                                                                                                       |
| `snoozed`, `snoozedUntil`                | True while the snooze holds (it ends at `snoozedUntil`, or when a request opens).                                                                                                                                                                                                                                                                                                                    |
| `archived`, `archivedAt`                 |                                                                                                                                                                                                                                                                                                                                                                                                      |
| `parentThreadId`, `relationshipToParent` | Set for subagents (`subagent`) and forks (`fork`).                                                                                                                                                                                                                                                                                                                                                   |
| `launchedByThreadId`                     | The thread whose agent opened this one: the sender of its first message, set when a thread's agent calls `t3_thread_launch`. Null for a person's thread, `t3 thread start`, T3's own threads, and on the `created` line of a launch, whose message commits just after (a `launcher` change follows). Subagents and forks use `parentThreadId`. Absent from builds before it: read absent as unknown. |
| `createdBy`, `creationSource`            | Who opened it (`user`, `agent`, `system`) and from where (`web`, `mobile`, `mcp`, `provider`, `server`). `t3 thread start` and agents' `t3_thread_launch` both show as `agent` from `mcp`.                                                                                                                                                                                                           |
| `providerInstanceId`, `model`            |                                                                                                                                                                                                                                                                                                                                                                                                      |
| `branch`, `worktreePath`                 | Null when the thread works in the project folder.                                                                                                                                                                                                                                                                                                                                                    |
| `createdAt`, `updatedAt`                 | ISO times. `updatedAt` is as of the line; a change of it alone sends no line.                                                                                                                                                                                                                                                                                                                        |

`thread.changes` (live lines only) names what changed since the last line for that thread:
`created`, `title`, `status`, `run`, `requests`, `settled`, `snoozed`, `archived`, `parent`,
`launcher`, `model`, `branch`, `worktree`. It is absent on snapshot and resume lines, and on the first live
line for a thread the feed had not sent before on this connection (after a resume you may hold it
from earlier: compare with your copy).

Archiving sends a `thread` line with `archived: true` (drop it if you do not show archived
threads). Without `--archived`, a thread archived before the feed started and later unarchived
arrives as a `thread` line too.

A run going `running` → `waiting` with a question sends one line with
`changes: ["status", "run", "requests"]` or two lines, depending on whether the two changes were
committed together. Read the state, not the number of lines.

## Example

`t3 project follow ~/work/blox-survivors --jsonl` while the lead agent opens a job and it asks a
question (ids shortened, some fields left out; one line per event in the real stream):

```json
{"v":1,"type":"snapshot","projectId":"p-blox","position":91200,"ref":"/Users/fagnersales/work/blox-survivors","threads":2}
{"v":1,"type":"thread","projectId":"p-blox","position":91200,"thread":{"threadId":"t-lead","projectId":"p-blox","title":"Blox Survivors lead","status":"running","run":{"runId":"r-40","status":"running","requestedAt":"2026-11-09T12:00:00.000Z","startedAt":"2026-11-09T12:00:01.000Z","completedAt":null},"pendingRequests":0,"pendingRequest":null,"settled":false,"settledAt":null,"snoozed":false,"snoozedUntil":null,"archived":false,"archivedAt":null,"parentThreadId":null,"relationshipToParent":null,"createdBy":"user","creationSource":"web","providerInstanceId":"codex","model":"gpt-5.4","branch":null,"worktreePath":null,"createdAt":"2026-11-01T09:00:00.000Z","updatedAt":"2026-11-09T12:00:01.000Z"}}
{"v":1,"type":"thread","projectId":"p-blox","position":91200,"thread":{"threadId":"t-old","title":"Boss wave tuning","status":"completed","settled":true,"…":"…"}}
{"v":1,"type":"ready","projectId":"p-blox","position":91200,"ref":"/Users/fagnersales/work/blox-survivors","title":"blox-survivors","workspaceRoot":"/Users/fagnersales/work/blox-survivors","threads":2,"resumed":false}
{"v":1,"type":"thread","projectId":"p-blox","position":91234,"thread":{"threadId":"t-job","title":"Fix XP orb magnet","status":"starting","run":{"runId":"r-1","status":"starting","…":"…"},"createdBy":"agent","creationSource":"mcp","…":"…"},"changes":["created"]}
{"v":1,"type":"thread","projectId":"p-blox","position":91240,"thread":{"threadId":"t-job","status":"running","…":"…"},"changes":["status","run"]}
{"v":1,"type":"thread","projectId":"p-blox","position":91302,"thread":{"threadId":"t-job","status":"waiting","pendingRequests":1,"pendingRequest":{"requestId":"q-1","kind":"user_input","createdAt":"2026-11-09T12:03:10.000Z","blocking":true},"…":"…"},"changes":["status","run","requests"]}
{"v":1,"type":"thread","projectId":"p-blox","position":91320,"thread":{"threadId":"t-job","status":"running","pendingRequests":0,"pendingRequest":null,"…":"…"},"changes":["status","run","requests"]}
{"v":1,"type":"thread","projectId":"p-blox","position":91410,"thread":{"threadId":"t-job","status":"completed","…":"…"},"changes":["status","run"]}
{"v":1,"type":"thread","projectId":"p-blox","position":91415,"thread":{"threadId":"t-job","settled":true,"…":"…"},"changes":["settled"]}
{"v":1,"type":"disconnected","reason":"The server closed the feed.","retryInMs":1000}
{"v":1,"type":"ready","projectId":"p-blox","position":91420,"ref":"p-blox","title":"blox-survivors","workspaceRoot":"/Users/fagnersales/work/blox-survivors","threads":3,"resumed":true}
```

The last two lines are a T3 restart: nothing changed while it was down, so the resume sends only
`ready` (note `ref` is now the id the CLI resumed with).

## Guarantees

- **Order:** lines for one project come in log order; a thread's lines never go back in time.
- **Nothing missed:** after `ready`, every committed change to a thread's fields above reaches
  you, or the feed reconnects or resumes and sends the thread's state again. Several changes
  within 50 ms are read together, so you get the latest state, not every intermediate one.
- **Idempotent:** a `thread` line is the whole state; applying it twice is harmless. A resume may
  resend threads whose only change since your position was a timeline item.
- **Counts:** `ready.threads` is how many threads you should hold. If yours differs (say, a deleted
  thread was purged by storage cleanup before you resumed), start again without `--after`.
- **Server-side recovery:** if the server's live buffer overflows under a burst of writes, the
  feed resumes itself from the last line it sent (you see another `ready` with `resumed: true`).
  After 5 failures in a row it ends the project with `error`.
- **Snooze timeouts:** a snooze that runs out sends `changes: ["snoozed"]` within 30 s.

What it does not send: timeline items and text (use `t3 thread follow` for one thread), visits,
pins and unread marks, pull request links, secret and link requests (they are not in
`pendingRequests`).

## Cost

`node scripts/project-feed-bench.ts [threads]` in `apps/server` (synthetic projects in a temp
SQLite file, a busy neighbouring project of 1,000 threads in the same log, a tenth of the threads
subagents, a quarter archived, median of 10 runs, Apple Silicon):

| Read                                               | 1,000 threads |  5,000 threads |
| -------------------------------------------------- | ------------: | -------------: |
| feed: snapshot (750 / 3,750 non-archived threads)  | 42 ms, 0.7 MB | 338 ms, 3.5 MB |
| feed: resume, 10 threads changed                   |        4.3 ms |          36 ms |
| feed: resume, nothing changed                      |        1.3 ms |          22 ms |
| feed: a run change → its line on the stream (live) |         53 ms |          57 ms |
| baseline: the shell read behind `t3 thread list`   |         36 ms |         309 ms |

The live latency is the 50 ms batching window plus one read of the changed threads. A follower
costs nothing while the project is quiet: other projects' events are dropped by type and thread
before any read. Blox Survivors has under 100 threads, so its snapshot is a few milliseconds.

The feed adds no database migration: it reads the shell projection the sidebar uses, the
runtime-requests `(thread_id, status)` index and the event log's `(stream_id, sequence)` index,
all already in the Nightly.
