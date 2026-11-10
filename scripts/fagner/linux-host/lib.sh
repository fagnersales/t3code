# Shared by scripts/fagner/linux-host/*, which run on the Linux PC (scripts/fagner/linux copies
# them to ~/.local/share/t3code-fagner/scripts there). Bash, GNU userland, a Hyprland session.
# shellcheck shell=bash

FAGNER_CACHE="${T3_FAGNER_CACHE:-$HOME/.cache/t3code-fagner}"
FAGNER_SRC="$FAGNER_CACHE/repo"
FAGNER_LINUX_BUILDS="$FAGNER_CACHE/builds"
FAGNER_STATE="$HOME/.local/state/t3code-fagner"
FAGNER_OPT="/opt/t3code-fagner"
FAGNER_OPT_PREVIOUS="/opt/t3code-fagner.previous"
FAGNER_OPT_ROLLBACK="/opt/t3code-fagner-rollback"
NIGHTLY_OPT="/opt/t3code-nightly-bin"
NIGHTLY_PACKAGE="t3code-nightly-bin"
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
  case "$1" in
    "$NIGHTLY_OPT/t3code" | "$FAGNER_OPT/t3code" | "$FAGNER_OPT_ROLLBACK/"*/t3code) return 0 ;;
    *" (deleted)") is_t3_exe "${1% (deleted)}" ;;
    *) return 1 ;;
  esac
}

# PIDs of T3's main (window) processes: no Chromium --type and not a CLI run as Node.
running_app_pids() {
  local dir pid exe
  for dir in /proc/[0-9]*; do
    pid="${dir#/proc/}"
    exe="$(readlink "$dir/exe" 2>/dev/null)" || continue
    is_t3_exe "$exe" || continue
    tr '\0' '\n' <"$dir/cmdline" 2>/dev/null | grep -q -- '^--type=' && continue
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
    tr '\0' '\n' <"$dir/cmdline" 2>/dev/null | grep -qx -- '--bootstrap-fd' && echo "${dir#/proc/}"
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
  for project in $(sqlite3 -readonly "$STATE_DB" \
    'select project_id from projection_projects where deleted_at is null'); do
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
Exec=$dir/AppRun %U
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
      uwsm app -- "$LAUNCHER_ID" >/dev/null 2>&1 || log "uwsm app -- $LAUNCHER_ID failed"
    else
      setsid -f "$dir/AppRun" >/dev/null 2>&1 </dev/null
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
