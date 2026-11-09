# fagnersales/t3code

Fagner's T3 Code: the Nightly he ran, plus a live thread feed for Headquarters. Branch
`fagner/main`.

## What it is

- **Base:** upstream commit `6d450a4` (tag `v0.1.5-nightly.20261029.3877`), exactly the Nightly
  that was installed when the fork was made, so the fork reads his database as that Nightly
  left it. No newer upstream code, so no newer migrations.
- **Faster thread reads:** `t3 thread show` reads from the end of a thread instead of paging
  through it (backport of upstream PR #33079, branch `perf/thread-read-tail`).
- **Live feed:** `t3 thread follow` and `GET /api/thread-feed/v1`, see
  [thread-feed.md](thread-feed.md).
- **Build identity:** versions `<nightly>-fagner.N`, app name `T3 Code (Fagner)`, no update feed
  (the updater reports updates as unavailable, so nothing can replace it with upstream).

**Database migrations added by the fork: none.** Its migrations are the Nightly's own
(`001` to `072` and the `OrchestrationV2` set). That is what keeps rollback possible without a
backup.

## Scripts (`scripts/fagner/`)

| Script            | Does                                                                                                                                                     |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `build`           | Builds `T3 Code (Fagner).app` with its `t3` CLI into `~/work/t3code-fagner-builds.noindex/<version>/`, ad-hoc signs it, points `latest` at it.           |
| `install --quit`  | Quits T3 gracefully, keeps the running Nightly for rollback, puts the newest build in `/Applications`, points `~/.t3/bin/t3` at it, relaunches.          |
| `rollback --quit` | The reverse: puts the kept Nightly back, points `t3` at it, relaunches it.                                                                               |
| `update`          | Fetches upstream, merges the newest Nightly tag into `fagner/main` (stops if it adds migrations, unless `--accept-migrations`), installs, tests, builds. |

`install` and `rollback` never copy or open the database. Run without `--quit`, they refuse while
T3 is running. With it, they detach into their own session (so quitting T3 and its terminals
cannot stop them), send T3 the same SIGTERM its own Quit path handles, wait up to 3 minutes for
it to exit (never force), wait for a pending Nightly update install (ShipIt) to finish, then swap.
Logs: `~/Library/Logs/t3code-fagner/`.

## Switching to the fork

```sh
~/work/t3code-worktrees/fagner-main/scripts/fagner/install --quit
```

It prints its log path and returns; T3 quits a few seconds later, and **T3 Code (Fagner)** opens
within about half a minute. Every chat in T3 stops with it (they resume when the app is back), so
start it when nothing important is mid-run. Follow it with
`tail -f ~/Library/Logs/t3code-fagner/install-*.log`.

What changes on disk:

- `/Applications/T3 Code (Nightly).app` moves to
  `~/work/t3code-fagner-builds.noindex/rollback/T3 Code (Nightly) 0.1.5-nightly.20261029.3877.app` (an
  APFS clone, no extra space). If Nightly had downloaded an update, it installs on quit; that
  copy never ran and is kept separately as `… (never ran).app`.
- `/Applications/T3 Code (Fagner).app` is the new app.
- `~/.t3/bin/t3` runs the fork's CLI (`t3 --version` ends in `-fagner.1`).
- `~/.t3/userdata` and `~/Library/Application Support/t3code-v2` are untouched and shared, so
  chats, settings, window state and drafts carry over.

## First launch

- **No Gatekeeper dialog.** The app is built on this Mac, so it has no quarantine attribute;
  `install` removes one anyway. It is ad-hoc signed, which is enough to run locally.
- **Keychain prompt, once:** macOS may ask to let "T3 Code (Fagner)" use "T3 Code Nightly Safe
  Storage" (saved remote environments are encrypted with it). Choose **Always Allow** (it asks
  for your login password). Deny and saved environment logins must be entered again. An ad-hoc
  signature changes with every build, so this prompt can come back after each `update`.
- **Privacy prompts again:** permissions granted to the Nightly's signature (Local Network,
  Screen Recording for window capture, Camera, Microphone, Desktop/Documents access for agents)
  are asked for again the first time they are used. Allow them as before.
- **Dock:** a pinned Nightly icon goes blank; pin T3 Code (Fagner) instead.
- **About T3 Code** and `t3 --version` show `0.1.5-nightly.20261029.3877-fagner.1`. The window
  title and sidebar artwork still say Nightly; Finder and the Dock say T3 Code (Fagner).
  Automatic updates report "not available" (no update feed).

## Rolling back to Nightly

```sh
~/work/t3code-worktrees/fagner-main/scripts/fagner/rollback --quit
```

It quits the fork, moves it to `~/work/t3code-fagner-builds.noindex/rollback/T3 Code (Fagner)
uninstalled.app`, puts the kept Nightly 3877 back in `/Applications`, points `t3` at it and
opens it. The data needs nothing: the fork never changed its schema.

Once back on Nightly its updater returns. When it updates to a newer Nightly, that build migrates
the database forward (upstream had added migrations `073` to `080` by 2026-11-09); from then on,
come back to the fork only through `scripts/fagner/update --accept-migrations` (a fork based on
that newer Nightly).

## Updating the fork

```sh
cd ~/work/t3code-worktrees/fagner-main
scripts/fagner/update            # newest upstream Nightly tag; stops if it adds migrations
scripts/fagner/update --accept-migrations --push
scripts/fagner/install --quit
```

Taking migrations is a one-way door for rollback: after the new build runs, only Nightlies at
least as new as its base can open the data. `update` names the migrations before it stops.
