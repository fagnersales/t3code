# Thread offload

`t3 thread offload` moves threads that have been idle for days out of the database into compressed
files, and `t3 thread restore` puts one back exactly as it was. Most of a large database is old
subagent threads that read screenshots: each screenshot is stored inline, once in its turn item and
again in the event log on every update of that item.

```sh
t3 thread offload --dry-run               # what would go, and how many bytes (reads every row header)
t3 thread offload                         # every qualifying thread, idle 7+ days
t3 thread offload --idle-days 14 --limit 20
t3 thread offload <thread>…               # only these threads and what they own
t3 thread restore                         # list offloaded threads
t3 thread restore <thread>                # bring it back, with the rest of its unit
t3 thread restore --bundle <file>         # from a bundle whose records are lost
t3 thread prune-restored [--older-than-days 30] [--dry-run]   # delete bundles restored long ago
```

It runs inside the running server (start T3 Code first): each thread is offloaded under the same
per-thread command lock the orchestrator takes (the server hands it the orchestrator's own
`ThreadCommandExecutor`; `runtimeLayer.test.ts` checks it), so no command writes to it meanwhile,
and reads go through the server's reader connection.

Some writers do not take that lock (provider sessions, command output, checkpoints, runs, the
session importers). So the export notes each thread's event count, last event sequence and thread
row `updated_at`, and the delete checks them again inside its transaction, with the exact rows
exported: any difference rolls that thread back and stops the unit.

## What goes and what stays

A **unit** is a thread plus every thread that reads it: its subagent children and its forks,
recursively. A fork reads its source's history, so a source never goes without its forks. A unit
goes only when every thread in it qualifies:

- no event for `--idle-days` (default 7);
- nothing running, queued, waiting for an answer, with a provider session or provider thread still
  open, a tool or subagent still running, effects pending, or a context transfer pending from or
  to it (the parent's next message would read a merge-back from an offloaded fork);
- not pinned, not snoozed into the future, not in a thread group (Headquarters follows those), not
  the target of a scheduled task. If the settings cannot be read, thread groups are unknown and
  nothing goes;
- not an imported session (id `import:…`, or a `provider_session_runtime` row with
  `importedTranscripts`): that row is how the importer knows a session is already imported, so
  removing it would let the next import recreate the thread under the same id;
- not holding the newest event, which the projection records as how far it has applied events.

A subagent child can go alone while its parent stays. Then:

- **App:** the parent's transcript is unchanged (it comes from the parent's own events). Its
  "Previous agents" list still shows the child, dimmed and not clickable. A link to the child opens a
  new draft, as for a deleted thread.
- **`t3 thread show <child>`** says when and where it was offloaded and how to restore it.

Open windows drop offloaded threads from the sidebar at once, and show restored ones; a window that
was closed gets a full snapshot when it reconnects.

## Files

Under the T3 home (`~/.t3/offload/`):

- `bundles/<time>-<key>.jsonl.zst`: one per unit. zstd with a 128 MiB window, so the copies of a
  screenshot in successive updates compress to almost nothing (a real review thread: 250 MiB of
  rows to 26 MiB). Lines of JSON: a header (format version 2, the database's latest migration and
  projection schema version, threads, titles, the columns of every table), then one line per row
  `[table, thread, rowid, values]` (blobs as `{"$blob": base64}`), then a trailer with the SHA-256
  and count of every line before it.
- `records/<key>.json`: one per offloaded thread: its title, parent and bundle. `<key>` is a hash of
  the thread id, which can be long.
- `restored/`: bundles whose threads were restored. Restore never deletes a bundle;
  `t3 thread prune-restored` deletes those restored more than `--older-than-days` (30) ago.
- `shell-resync-sequence`: clients resuming from before the last offload or restore get a snapshot.

The bundle holds every row of the thread in every table that has one: the event log (both
versions), command receipts, effects, every `orchestration_v2_projection_*` table, turn item
positions, and the v1 rows (the legacy importer would otherwise recreate the thread from them).
Provider threads and context transfers go with the thread whose events wrote them, which is not
always the thread they name.

The tables are a fixed list (`THREAD_TABLES`). Before each offload, every table with a
`thread_id`, `*_thread_id`, `stream_id` or `aggregate_id` column must be in it or in
`IGNORED_THREAD_TABLES` with a reason, and every listed table must exist with its column;
otherwise nothing is offloaded until the list is updated. A migration that adds a thread table
therefore stops offloading instead of leaving that table's rows behind.

## Order of work

For each unit: write the bundle, read it back and check it against what was written, write the
records, then delete the rows one thread per transaction, children before parents, checking that
each delete removed exactly the rows written and that the thread did not change since its export.
A failure before the deletes leaves the database as it was; a failed delete removes the records of
the threads still in the database (and the bundle, when none was removed); a crash between threads
leaves the removed ones recorded, and `restore` puts back only the threads that are missing. No row
is changed in the threads that stay, so their projections are what T3 would rebuild from the
events left.

Durability: the bundle and each record are fsynced, renamed into place, and their folder fsynced,
all before the first delete. On macOS plain `fsync` leaves data in the drive's write cache; Node's
fsync already calls `fcntl(F_FULLFSYNC)` there (libuv falls back to `F_BARRIERFSYNC`, then
`fsync`), so nothing else is needed.

Restore checks the bundle, and refuses it if the database's latest migration or projection schema
version differs from the one in its header. Then, before inserting anything, it checks each thread
of the unit:

- not in the database (no event, and no row in any table of the list): it goes back;
- in the database with exactly the bundle's events (same count, same `event_id`s): it was never
  removed (an offload stopped partway) and is left as it is;
- anything else (the thread was recreated under its id, or changed): restore stops with a
  conflict naming the thread and both event counts, and changes nothing. Bundle and records stay.

Each missing thread goes back in one transaction, parents first, with its original rowids (a rowid
another row took since makes SQLite give the row a new one, past the table's largest). The records
pointing at the bundle are removed and the bundle moves to `restored/`. Restoring a bundle again
(`--bundle`) finds every thread present and changes nothing.

## Space

Deleting rows frees pages inside the file; the file does not shrink. `t3 thread offload` ends by
saying how much `t3 doctor` can return. **Quit T3 Code, then run `t3 doctor --fix`**: it compacts the
event log, VACUUMs (needs free disk about the size of what stays) and turns on incremental
auto-vacuum. From then on the running server returns freed pages to disk by itself, and `doctor`
only recommends a VACUUM again past 1 GiB free.

## Rollback

No migration: the schema is untouched. An upstream Nightly opens the database and simply does not
see offloaded threads; bring them back with the fork's `t3 thread restore` first if they should
come along. The same holds for any update that adds a migration: a bundle only goes back into the
schema it was written from, so restore what you want to keep before updating.

## R2 (later)

Uploading adds a remote copy of each bundle: after the local check, upload it, verify its checksum
remotely, record the object key in the thread's record, and optionally delete the local file.
Restore downloads the bundle when the local file is missing, then proceeds as now. The bundle
already carries its own checksum and needs nothing else to be restored.
