# Shared by scripts/fagner/linux-host/*, which run on the Linux PC (scripts/fagner/linux copies
# them to ~/.local/share/t3code-fagner/scripts there). Bash, GNU userland, a Wayland session
# (Hyprland on the Acer, GNOME on the Lenovo).
# shellcheck shell=bash

FAGNER_CACHE="${T3_FAGNER_CACHE:-$HOME/.cache/t3code-fagner}"
FAGNER_SRC="$FAGNER_CACHE/repo"
FAGNER_LINUX_BUILDS="$FAGNER_CACHE/builds"
FAGNER_STATE="$HOME/.local/state/t3code-fagner"
NIGHTLY_PACKAGE="t3code-nightly-bin"
# Where T3 lives on this PC. "package": the Nightly is the pacman package in /opt and the fork goes
# beside it, root-owned (the Acer). "home": the Nightly is an extracted AppImage in ~/Applications
# and the fork goes beside it, owned by the user and run without Chromium's sandbox, as that
# Nightly is (the Lenovo, Ubuntu with GNOME).
if [ -d "/opt/$NIGHTLY_PACKAGE" ]; then
  LAYOUT="package"
  FAGNER_APP="/opt/t3code-fagner"
  NIGHTLY_APP="/opt/$NIGHTLY_PACKAGE"
  NIGHTLY_KEPT="/opt/t3code-fagner-rollback"
  LAUNCH_ARGS=""
else
  LAYOUT="home"
  FAGNER_APP="$HOME/Applications/T3-Code-Fagner"
  NIGHTLY_APP="$HOME/Applications/T3-Code-Nightly"
  NIGHTLY_KEPT=""
  LAUNCH_ARGS=" --no-sandbox"
fi
FAGNER_APP_PREVIOUS="$FAGNER_APP.previous"
# A user service some PCs have, which reopens T3 after a crash; it names the app folder it opens.
WATCHDOG="$HOME/.local/bin/t3code-watchdog"
WATCHDOG_UNIT="t3code-watchdog.service"
DATABASE_BACKUPS="$HOME/t3-backup"
# Same desktop id as the package's /usr/share/applications/t3code.desktop: the copy in the user's
# folder wins, and pacman never writes there, so an update of the package cannot take it back.
LAUNCHER="$HOME/.local/share/applications/t3code.desktop"
LAUNCHER_ID="t3code.desktop"
LAUNCHER_MARK="X-T3-Fagner=1"
T3_HOME_DIR="$HOME/.t3"
CLI_SHIM="$T3_HOME_DIR/bin/t3"
STATE_DB="$T3_HOME_DIR/userdata/statev2.sqlite"
BUSY_STATUSES="running starting waiting queued"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
die() {
  log "ERROR: $*"
  exit 1
}

# The version an extracted AppImage folder carries.
app_version() { sed -n 's/^X-AppImage-Version=//p' "$1/t3code.desktop" 2>/dev/null | head -1; }

# Hyprland, the user's systemd and D-Bus, as an ssh login lacks them.
session_env() {
  export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
  export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=$XDG_RUNTIME_DIR/bus}"
  if [ -z "${HYPRLAND_INSTANCE_SIGNATURE:-}" ] && [ -d "$XDG_RUNTIME_DIR/hypr" ]; then
    HYPRLAND_INSTANCE_SIGNATURE="$(ls -t "$XDG_RUNTIME_DIR/hypr" | head -1)"
    export HYPRLAND_INSTANCE_SIGNATURE
  fi
  export WAYLAND_DISPLAY="${WAYLAND_DISPLAY:-wayland-1}"
}

# A T3 process: its executable is a t3code binary (Nightly, fork or a kept copy).
is_t3_exe() {
  local exe="${1% (deleted)}"
  case "$exe" in
    "$NIGHTLY_APP/t3code" | "$FAGNER_APP/t3code" | "$FAGNER_APP_PREVIOUS/t3code") return 0 ;;
  esac
  [ -n "$NIGHTLY_KEPT" ] || return 1
  case "$exe" in "$NIGHTLY_KEPT/"*/t3code) return 0 ;; esac
  return 1
}

# Run a command that writes the app folders: as root where they are root's.
as_owner() {
  if [ "$LAYOUT" = "package" ]; then sudo "$@"; else "$@"; fi
}

# PIDs of T3's main (window) processes: no Chromium --type and not a CLI run as Node.
running_app_pids() {
  local dir pid exe
  for dir in /proc/[0-9]*; do
    pid="${dir#/proc/}"
    exe="$(readlink "$dir/exe" 2>/dev/null)" || continue
    is_t3_exe "$exe" || continue
    # Chromium rewrites its helpers' arguments into one string, so search the whole line.
    grep -qa -- '--type=' "$dir/cmdline" 2>/dev/null && continue
    tr '\0' '\n' <"$dir/environ" 2>/dev/null | grep -qx 'ELECTRON_RUN_AS_NODE=1' && continue
    echo "$pid"
  done
}

# PIDs of T3's backend servers (the app's child that owns the database).
running_server_pids() {
  local dir exe
  for dir in /proc/[0-9]*; do
    exe="$(readlink "$dir/exe" 2>/dev/null)" || continue
    is_t3_exe "$exe" || continue
    grep -qa -- '--bootstrap-fd' "$dir/cmdline" 2>/dev/null && echo "${dir#/proc/}"
  done
}

# The binary of the running T3 window, if any.
running_app_exe() {
  local pid
  pid="$(running_app_pids | head -1)"
  [ -n "$pid" ] || return 0
  readlink "/proc/$pid/exe" | sed 's/ (deleted)$//'
}

wait_for_exit() {
  local timeout="$1" waited=0 pid alive
  shift
  while :; do
    alive=""
    for pid in "$@"; do
      if kill -0 "$pid" 2>/dev/null; then alive="$alive $pid"; fi
    done
    [ -z "$alive" ] && return 0
    [ "$waited" -ge "$timeout" ] && return 1
    sleep 1
    waited=$((waited + 1))
  done
}

# Every thread of every project, as "status<TAB>id<TAB>title" lines, read with the T3 CLI. The CLI
# lists one project at a time and has no project list, so the ids come from the database, opened
# read-only.
list_threads() {
  local project
  [ -x "$CLI_SHIM" ] || die "No t3 CLI at $CLI_SHIM"
  [ -f "$STATE_DB" ] || die "No T3 database at $STATE_DB"
  for project in $(python3 -c 'import sqlite3, sys
db = sqlite3.connect("file:%s?mode=ro" % sys.argv[1], uri=True)
for (project,) in db.execute("select project_id from projection_projects where deleted_at is null"):
    print(project)' "$STATE_DB"); do
    "$CLI_SHIM" thread list --project "$project" --json |
      python3 -c 'import json, sys
for t in json.load(sys.stdin):
    print("%s\t%s\t%s" % (t["status"], t["threadId"], t["title"]))'
  done
}

# Threads whose status means an agent is working or about to.
busy_threads() {
  local statuses
  statuses="$(echo "$BUSY_STATUSES" | tr ' ' '|')"
  list_threads | grep -E "^($statuses)	" || true
}

# Refuse to quit T3 while a thread is busy, unless --force.
check_not_busy() {
  local force="$1" busy
  [ -n "$(running_app_pids)" ] || return 0
  busy="$(busy_threads)"
  [ -z "$busy" ] && return 0
  log "Busy T3 threads:"
  echo "$busy" | sed 's/^/    /'
  [ "$force" = 1 ] && {
    log "--force: quitting T3 anyway."
    return 0
  }
  die "Restarting T3 now would interrupt them. Nothing was changed. Run again when they finish (or with --force)."
}

# Quit T3 gracefully: SIGTERM is its Quit path (DesktopLifecycle.ts). Waits for the backend too, so
# the database is closed before the swap.
quit_t3_gracefully() {
  local pids servers
  pids="$(running_app_pids)"
  servers="$(running_server_pids)"
  if [ -z "$pids" ]; then
    log "T3 Code is not running."
    return 0
  fi
  log "Quitting T3 Code (pids: $(echo $pids))..."
  # shellcheck disable=SC2086
  kill -TERM $pids 2>/dev/null || true
  # shellcheck disable=SC2086
  wait_for_exit 180 $pids $servers ||
    die "T3 Code did not quit within 180 s. Nothing was changed; quit it yourself and run this again."
  log "T3 Code has quit."
}

# The launcher, pointed at an app folder. Marked, so rollback knows it may remove it.
write_launcher() {
  local dir="$1" name="$2"
  mkdir -p "$(dirname "$LAUNCHER")"
  cat >"$LAUNCHER.tmp" <<EOF
[Desktop Entry]
Name=$name
Comment=Desktop control surface for local coding agents
Exec=$dir/AppRun$LAUNCH_ARGS %U
TryExec=$dir/AppRun
Terminal=false
Type=Application
Icon=$dir/t3code.png
StartupWMClass=com.t3tools.T3Code
Categories=Development;
MimeType=x-scheme-handler/t3code;
$LAUNCHER_MARK
EOF
  mv "$LAUNCHER.tmp" "$LAUNCHER"
  command -v update-desktop-database >/dev/null && update-desktop-database "$(dirname "$LAUNCHER")" 2>/dev/null || true
  log "Launcher $LAUNCHER opens $dir"
}

remove_launcher() {
  if [ -f "$LAUNCHER" ] && grep -qx "$LAUNCHER_MARK" "$LAUNCHER"; then
    rm -f "$LAUNCHER"
    command -v update-desktop-database >/dev/null && update-desktop-database "$(dirname "$LAUNCHER")" 2>/dev/null || true
    log "Removed $LAUNCHER; the package's launcher opens Nightly again."
  fi
}

# The launcher T3 writes for itself (DesktopCliShim.ts), pointed at an app folder.
write_cli_shim() {
  local dir="$1"
  [ -x "$dir/t3code" ] || die "No executable at $dir/t3code"
  mkdir -p "$(dirname "$CLI_SHIM")"
  cat >"$CLI_SHIM.fagner.tmp" <<EOF
#!/bin/sh
# Written by T3 Code: runs the desktop app's bundled t3 CLI.
export T3CODE_CLI_PATH='$CLI_SHIM'
home='$T3_HOME_DIR'
export T3CODE_HOME="\${T3CODE_HOME:-\$home}"
export ELECTRON_RUN_AS_NODE=1
app='$dir/t3code'
if [ ! -x "\$app" ]; then
  echo "T3 Code has moved or been removed. Open the app once to update this command." >&2
  exit 127
fi
exec "\$app" '$dir/resources/app.asar/apps/server/dist/bin.mjs' "\$@"
EOF
  chmod 755 "$CLI_SHIM.fagner.tmp"
  mv "$CLI_SHIM.fagner.tmp" "$CLI_SHIM"
  log "t3 CLI now runs $dir"
}

# Open T3 the way the app launcher (Walker) does, in its own systemd scope inside the session, and
# make sure it stays open: right after the old copy quits, a new one can quit again within seconds.
launch_app() {
  local dir="$1" attempt
  session_env
  for attempt in 1 2 3 4 5; do
    if command -v uwsm >/dev/null; then
      # uwsm app waits for the app to exit, so it gets its own session.
      setsid -f uwsm app -- "$LAUNCHER_ID" >/dev/null 2>&1 </dev/null
    else
      # The user's systemd has the desktop session's environment, which an ssh login lacks.
      # shellcheck disable=SC2086
      systemd-run --user --quiet --collect --slice=app.slice \
        --unit="app-t3code-$(date +%s).service" "$dir/AppRun" $LAUNCH_ARGS
    fi
    sleep 10
    if [ "$(running_app_exe)" = "$dir/t3code" ]; then
      log "Launched $dir (pid $(running_app_pids | head -1))"
      return 0
    fi
    log "T3 from $dir is not running after launch (attempt $attempt of 5)."
    sleep $((attempt * 3))
  done
  die "T3 from $dir did not stay open after 5 launches. Open it from the launcher."
}

# The watchdog would reopen the app it names while the swap runs, so it is stopped first and
# started again pointed at the app that was just opened. A PC without one skips both.
stop_watchdog() {
  [ -f "$WATCHDOG" ] || return 0
  session_env
  systemctl --user stop "$WATCHDOG_UNIT" 2>/dev/null || true
  # A script that stops half way leaves the watchdog as it found it.
  trap 'systemctl --user start "$WATCHDOG_UNIT" 2>/dev/null || true' EXIT
}

start_watchdog() {
  local dir="$1"
  [ -f "$WATCHDOG" ] || return 0
  session_env
  trap - EXIT
  sed "s#^APP_DIR=.*#APP_DIR=\"$dir\"#" "$WATCHDOG" >"$WATCHDOG.fagner.tmp"
  grep -qx "APP_DIR=\"$dir\"" "$WATCHDOG.fagner.tmp" || die "$WATCHDOG has no APP_DIR line to point at $dir."
  chmod 755 "$WATCHDOG.fagner.tmp"
  mv "$WATCHDOG.fagner.tmp" "$WATCHDOG"
  systemctl --user start "$WATCHDOG_UNIT"
  log "The crash watchdog reopens $dir"
}

# Put a copy of the database back (a folder holding statev2.sqlite, as kept before a build with
# newer migrations first ran). The database in use is set aside, never deleted. T3 must have quit.
restore_database() {
  local from="$1" aside
  [ -f "$from/statev2.sqlite" ] || die "No statev2.sqlite in $from."
  [ -z "$(running_app_pids)$(running_server_pids)" ] || die "T3 Code still runs; the database stays as it is."
  aside="$DATABASE_BACKUPS/statev2-replaced-$(date '+%Y%m%d-%H%M%S')"
  mkdir -p "$aside"
  mv "$STATE_DB"* "$aside/"
  cp -a "$from"/statev2.sqlite* "$(dirname "$STATE_DB")/"
  log "Database from $from is back; the one it replaced is in $aside"
}

# Start a copy of this script in its own systemd unit, so quitting T3 (and the ssh session that
# asked) cannot stop it.
detach_self() {
  local script="$1" name="$2" log_file
  shift 2
  mkdir -p "$FAGNER_STATE"
  log_file="$FAGNER_STATE/$name-$(date '+%Y%m%d-%H%M%S').log"
  session_env
  systemd-run --user --quiet --collect --unit "t3code-fagner-$name-$(date +%s)" \
    -p "StandardOutput=append:$log_file" -p "StandardError=append:$log_file" \
    /bin/bash "$script" --foreground "$@"
  echo "Running detached. Log: $log_file"
}
