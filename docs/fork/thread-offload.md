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
```

It runs inside the running server (start T3 Code first): each thread is offloaded under the same
per-thread command lock the orchestrator takes, so no command writes to it meanwhile, and reads go
through the server's reader connection.

## What goes and what stays

A **unit** is a thread plus every thread that reads it: its subagent children and its forks,
recursively. A fork reads its source's history, so a source never goes without its forks. A unit
goes only when every thread in it qualifies:

- no event for `--idle-days` (default 7);
- nothing running, queued, waiting for an answer, with a provider session or provider thread still
  open, a tool or subagent still running, or effects pending;
- not pinned, not snoozed into the future, not in a thread group (Headquarters follows those), not
  the target of a scheduled task;
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
  rows to 26 MiB). Lines of JSON: a header (format, threads, titles, the columns of every table),
  then one line per row `[table, thread, rowid, values]` (blobs as `{"$blob": base64}`), then a
  trailer with the SHA-256 and count of every line before it.
- `records/<key>.json`: one per offloaded thread: its title, parent and bundle. `<key>` is a hash of
  the thread id, which can be long.
- `shell-resync-sequence`: clients resuming from before the last offload or restore get a snapshot.

The bundle holds every row of the thread in every table that has one: the event log (both
versions), command receipts, effects, every `orchestration_v2_projection_*` table, turn item
positions, and the v1 rows (the legacy importer would otherwise recreate the thread from them).
Provider threads and context transfers go with the thread whose events wrote them, which is not
always the thread they name.

## Order of work

For each unit: write the bundle, read it back and check it against what was written, write the
records, then delete the rows one thread per transaction, children before parents, checking that
each delete removed exactly the rows written. A failure before the deletes leaves the database as it
was; a crash between threads leaves the removed ones recorded, and `restore` puts back only the
threads that are missing. No row is changed in the threads that stay, so their projections are what
T3 would rebuild from the events left.

Restore checks the bundle, then inserts each missing thread in one transaction, parents first, with
its original rowids (a rowid taken since gets a new one). Then it deletes the records and the bundle.

## Space

Deleting rows frees pages inside the file; the file does not shrink. `t3 thread offload` ends by
saying how much `t3 doctor` can return. **Quit T3 Code, then run `t3 doctor --fix`**: it compacts the
event log, VACUUMs (needs free disk about the size of what stays) and turns on incremental
auto-vacuum. From then on the running server returns freed pages to disk by itself, and `doctor`
only recommends a VACUUM again past 1 GiB free.

## Rollback

No migration: the schema is untouched. An upstream Nightly opens the database and simply does not
see offloaded threads; bring them back with the fork's `t3 thread restore` first if they should
come along.

## R2 (later)

Uploading adds a remote copy of each bundle: after the local check, upload it, verify its checksum
remotely, record the object key in the thread's record, and optionally delete the local file.
Restore downloads the bundle when the local file is missing, then proceeds as now. The bundle
already carries its own checksum and needs nothing else to be restored.
