# fagnersales/t3code

Fagner's T3 Code: the Nightly he ran, plus live thread and project feeds for Headquarters. Branch
`fagner/main`.

## What it is

- **Base:** upstream commit `6d450a4` (tag `v0.1.5-nightly.20261029.3877`), exactly the Nightly
  that was installed when the fork was made, so the fork reads his database as that Nightly
  left it. No newer upstream code, so no newer migrations.
- **Faster thread reads:** `t3 thread show` reads from the end of a thread instead of paging
  through it (backport of upstream PR #33079, branch `perf/thread-read-tail`).
- **Live feed:** `t3 thread follow` and `GET /api/thread-feed/v1`, see
  [thread-feed.md](thread-feed.md).
- **Project feed:** `t3 project follow` and `GET /api/project-feed/v1`, a project's threads live
  (created, running, waiting, settled, archived, deleted), see [project-feed.md](project-feed.md).
- **Thread groups:** `t3 thread group <name> <thread>…` (and `POST /api/thread-groups/v1`) sets
  the threads of a named group in the server settings' `threadGroups`; each call replaces the
  group's list. Headquarters keeps its group, `Headquarters`, equal to the threads it follows.
- **Workspaces:** each thread group is a sidebar workspace beside Threads (every other thread).
  The switch in the sidebar header flips between them (a menu when there are several groups),
  remembered per window, with a badge counting threads in another workspace that wait on an
  approval or a question. The open thread always stays listed, and search looks in every
  workspace. A thread moved by hand ("Move to Headquarters" / "Move to Threads" in the thread
  menu, also for a selection, or `t3 thread workspace <name|threads> <thread>…`) is kept in
  `threadWorkspaceChoices`, which wins over the group both ways and which `t3 thread group` never
  touches. `t3 thread workspace` lists every workspace, `--reset` forgets a move. A thread the
  user starts while a workspace is shown joins it, and a subagent, delegated task or fork follows
  its parent; a top-level thread an agent launches does not (the launcher is only in its first
  message). The server drops the move of a deleted thread, and at start of any thread it no
  longer has (offloaded ones too). Thread offload still keeps only `threadGroups` threads. Both are
  settings keys, not migrations: an older build ignores them.
- **Thread offload:** `t3 thread offload` moves threads idle for 7+ days, with their subagents
  and forks, out of the database into compressed files under `~/.t3/offload/`; `t3 thread restore`
  puts one back exactly and keeps the bundle (`t3 thread prune-restored` deletes old ones). Runs
  inside the running server. See [thread-offload.md](thread-offload.md).
- **Build identity:** versions `<nightly>-fagner.N`, app name `T3 Code (Fagner)`, no update feed
  (the updater reports updates as unavailable, so nothing can replace it with upstream).

**Database migrations added by the fork: none.** Its migrations are the Nightly's own
(`001` to `072` and the `OrchestrationV2` set). That is what keeps rollback possible without a
backup.

## Scripts (`scripts/fagner/`)

| Script            | Does                                                                                                                                                          |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `build`           | Builds `T3 Code (Fagner).app` with its `t3` CLI into `~/work/t3code-fagner-builds.noindex/<version>/`, signs it, points `latest` at it.                       |
| `install --quit`  | Quits T3 gracefully, keeps the running Nightly for rollback, puts the newest build in `/Applications`, points `~/.t3/bin/t3` at it, relaunches.               |
| `rollback --quit` | The reverse: puts the kept Nightly back, points `t3` at it, relaunches it.                                                                                    |
| `setup-signing`   | Makes the local certificate `build` signs with, once. macOS then takes every build for the same app and keeps the permissions granted to it.                  |
| `update`          | Fetches upstream, merges the newest Nightly tag into `fagner/main` (stops if it adds migrations, unless `--accept-migrations`), installs, tests, builds.      |
| `linux …`         | The same for the Linux PC (the Acer), run from the Mac: `build`, `install --quit`, `rollback --quit`, `status`, `deploy`. See [Linux PC](#linux-pc-the-acer). |
| `shrink --quit`   | Quits T3, pauses Headquarters' device program, runs `t3 doctor --fix` (gives the database's free space back), then starts both again.                         |

`install` and `rollback` never copy or open the database. Run without `--quit`, they refuse while
T3 is running. They reopen T3 with the environment the Dock gives it and open it again if it quits
within seconds. With `--quit`, they detach into their own session (so quitting T3 and its terminals
cannot stop them), send T3 the same SIGTERM its own Quit path handles, wait up to 3 minutes for
it to exit (never force), wait for a pending Nightly update install (ShipIt) to finish, then swap.
Logs: `~/Library/Logs/t3code-fagner/`.

Quitting T3 ends every chat, including the one that ran the script. From a chat, pass
`--after 25` (`install` and `shrink`) so its reply goes out first, and schedule a wake-up for the
chat to check the log once T3 is back.

`t3 doctor --fix` refuses while any process has the database open. Headquarters' device program
keeps `t3 thread follow` and `t3 project follow` running, which read it, so quitting T3 is not
enough: `shrink` pauses its launchd job (`ink.fagner.headquarters.daemon`) and waits until nothing
holds the file. At about 5 GiB live, the repair took 2 minutes.

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

## Linux PC (the Acer)

The Acer (`acer@omarchy-acer`, Arch with Omarchy and Hyprland) runs the same fork. Everything is
driven from the Mac with `scripts/fagner/linux`; the PC is `$T3_FAGNER_LINUX_HOST` (default
`acer@omarchy-acer`).

```sh
cd ~/work/t3code-worktrees/fagner-main
scripts/fagner/linux deploy            # build this commit on the Acer, then install --quit
scripts/fagner/linux status            # what runs there, the launcher, the CLI, busy threads
scripts/fagner/linux rollback --quit   # back to the Nightly
```

`deploy` is `build` then `install --quit`; each also runs alone.

- **Built on the Acer, not cross-built.** A Linux build must run on Linux: node-pty is compiled
  from source there and the browser secret helper links against libsecret
  (`build-desktop-artifact.ts` refuses another host). `build` sends the commit as a git bundle (only
  the commits the Acer lacks), checks it out in `~/.cache/t3code-fagner/repo` there, takes node,
  pnpm and Rust from mise at the versions `package.json` pins (installed, never activated), and
  builds the AppImage into `~/.cache/t3code-fagner/builds/<version>/`, extracted to `app/` the way
  the AUR package installs it. It builds the commit, so it refuses uncommitted app changes. It
  takes about 5 minutes. Besides the Arch base it needs `libxcrypt-compat` (electron-builder's
  bundled fpm, which also makes a .deb, links against `libcrypt.so.1`).
- **One version for both machines.** A build folder on the Mac records its commit
  (`~/work/t3code-fagner-builds.noindex/<version>/commit`). Mac and Linux builds of the same app
  sources (only `scripts/fagner` and `docs/fork` may differ) get the same `-fagner.N`, whichever
  machine builds first.
- **install** refuses while any thread on the Acer is running, starting, waiting or queued (read
  with its own `t3 thread list`; `--force` overrides). Otherwise it detaches on the Acer
  (`systemd-run --user`, log in `~/.local/state/t3code-fagner/`), stages the build, keeps a copy of
  the Nightly that ran on the data, quits T3 with SIGTERM and waits for its backend, swaps, and
  opens the fork with `uwsm app -- t3code.desktop`, as Walker does.
- **Rollback** quits the fork, removes the launcher override and points `t3` at the Nightly. If
  pacman has updated the Nightly since, it opens the kept copy instead, because a newer Nightly
  would migrate the data.

What lives where on the Acer:

| Path                                         | What                                                                                                   |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `/opt/t3code-fagner`                         | The fork (root-owned, setuid `chrome-sandbox`, like the package). `.previous` is the one before.       |
| `/opt/t3code-nightly-bin`                    | The AUR package `t3code-nightly-bin`, left installed as the rollback.                                  |
| `/opt/t3code-fagner-rollback/nightly-<ver>`  | A copy of the Nightly that ran on the data, made at the first install.                                 |
| `~/.local/share/applications/t3code.desktop` | The launcher: same id as the package's, so it wins in Walker and `gtk-launch`. Marked `X-T3-Fagner=1`. |
| `~/.t3/bin/t3`                               | The CLI shim, pointed at `/opt/t3code-fagner`.                                                         |
| `~/.t3/userdata`, `~/.config/t3code-v2`      | Untouched and shared: same app id, same data, same threads.                                            |

pacman and yay only write `/usr/share/applications/t3code.desktop` and `/opt/t3code-nightly-bin`,
so updating `t3code-nightly-bin` never takes the launcher or the CLI back. It does change what
rollback finds in `/opt`, which is why the copy is kept. Nothing on the Acer autostarts T3; it
opens from Walker. `--render-node-override=/dev/dri/renderD129` (the Intel GPU, which Sunshine also
uses) is not configured anywhere: Chromium's Wayland backend passes the compositor's render node to
its GPU process, so the fork gets it like the Nightly and every other Electron app there.

To see T3's window without raising it (someone else may be using the screen), capture it by its
toplevel id: `hyprctl clients -j` gives the window's `stableId`, and `grim -T <stableId> -` captures
it even under a fullscreen window.
