# Shared by scripts/fagner/*. Bash 3.2 (macOS /bin/bash).
# shellcheck shell=bash

FAGNER_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FAGNER_BUILDS_DIR="${T3_FAGNER_BUILDS_DIR:-$HOME/work/t3code-fagner-builds.noindex}"
FAGNER_ROLLBACK_DIR="$FAGNER_BUILDS_DIR/rollback"
FAGNER_APP_NAME="T3 Code (Fagner)"
# Overrides exist so the scripts can be tested on a scratch folder and a bundle id nothing runs.
APPS_DIR="${T3_FAGNER_APPS_DIR:-/Applications}"
FAGNER_APP="$APPS_DIR/$FAGNER_APP_NAME.app"
NIGHTLY_APP_NAME="T3 Code (Nightly)"
NIGHTLY_APP="$APPS_DIR/$NIGHTLY_APP_NAME.app"
APP_BUNDLE_ID="${T3_FAGNER_BUNDLE_ID:-com.t3tools.t3code}"
T3_HOME_DIR="${T3_FAGNER_T3_HOME:-$HOME/.t3}"
CLI_SHIM="$T3_HOME_DIR/bin/t3"
SIGNING_IDENTITY="T3 Code Fagner Local Signing"
SIGNING_KEYCHAIN="$HOME/Library/Keychains/t3code-fagner-signing.keychain-db"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
die() {
  log "ERROR: $*"
  exit 1
}

plist_version() { /usr/bin/defaults read "$1/Contents/Info.plist" CFBundleShortVersionString 2>/dev/null; }

# Paths whose changes leave the app itself unchanged.
APP_NEUTRAL_PATHS=(':(exclude)scripts/fagner' ':(exclude)docs/fork')

# True when the working tree's app sources equal those of commit $1.
same_app_sources() {
  git -C "$FAGNER_REPO_ROOT" cat-file -e "$1^{commit}" 2>/dev/null &&
    git -C "$FAGNER_REPO_ROOT" diff --quiet "$1" -- . "${APP_NEUTRAL_PATHS[@]}"
}

# The version this checkout builds as. The Mac and Linux builds of the same app sources share one:
# a build folder whose `commit` file names a commit with the same app sources keeps its number.
# Otherwise the next free -fagner.N past every build folder and the installed Mac app.
version_for_checkout() {
  local dir base_tag base installed n
  for dir in "$FAGNER_BUILDS_DIR"/*-fagner.*; do
    [ -f "$dir/commit" ] || continue
    if same_app_sources "$(cat "$dir/commit")"; then
      basename "$dir"
      return 0
    fi
  done
  base_tag="$(git -C "$FAGNER_REPO_ROOT" describe --tags --abbrev=0 --match 'v*-nightly.*' HEAD)"
  base="${base_tag#v}"
  # Builds from before the .noindex move have no folder; the installed app still counts.
  installed="$(plist_version "/Applications/$FAGNER_APP_NAME.app" || true)"
  n=1
  case "$installed" in "$base-fagner."[0-9]*) n=$((${installed##*-fagner.} + 1)) ;; esac
  while [ -e "$FAGNER_BUILDS_DIR/$base-fagner.$n" ]; do n=$((n + 1)); done
  echo "$base-fagner.$n"
}

# Record the commit a build folder was made from, when the working tree's app sources are that
# commit's (so a build of uncommitted changes is never reused for another platform).
record_build_commit() {
  local head
  head="$(git -C "$FAGNER_REPO_ROOT" rev-parse HEAD)"
  if same_app_sources "$head"; then
    echo "$head" >"$1/commit"
  else
    log "The app sources have uncommitted changes; $1 is not recorded as commit $head."
  fi
}

# Remote names differ between clones, so find them by URL.
remote_for() {
  git -C "$FAGNER_REPO_ROOT" remote -v | awk -v repo="$1" '$2 ~ repo"(\\.git)?$" && $3 == "(fetch)" { print $1; exit }'
}
upstream_remote() { remote_for "pingdotgg/t3code"; }
fork_remote() { remote_for "fagnersales/t3code"; }

# PIDs of running apps with T3's bundle id (Nightly and Fagner builds share it). Needs no permission.
running_app_pids() {
  local asn
  for asn in $(/usr/bin/lsappinfo find "bundleid=$APP_BUNDLE_ID" 2>/dev/null); do
    /usr/bin/lsappinfo info -only pid "$asn" 2>/dev/null | sed -n 's/^"pid"=\([0-9][0-9]*\)$/\1/p'
  done
}

# T3's backend server, the app's child. It can outlive the app while it closes its database, and an
# app opened meanwhile finds the data directory owned ("This T3 home is unavailable") and quits.
# macOS's pgrep -f does not match it, so ps.
backend_pids() { /bin/ps -axo pid=,args= | /usr/bin/awk '/app\.asar\/apps\/desktop\/dist-electron\/compileCache\.cjs/ && !/awk/ {print $1}'; }

# Squirrel's installer finishes a staged Nightly update after the app quits.
shipit_pids() { /usr/bin/pgrep -x ShipIt || true; }

wait_for_exit() {
  local timeout="$1"
  shift
  local waited=0 pid alive
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

# Quit T3 gracefully, and wait for it and for any pending Nightly update install.
quit_t3_gracefully() {
  local pids
  pids="$(running_app_pids)"
  if [ -z "$pids" ]; then
    log "T3 Code is not running."
  else
    log "Quitting T3 Code (pids: $(echo $pids))..."
    # T3 treats SIGTERM like Quit: it stops its backend and closes windows (DesktopLifecycle.ts).
    # shellcheck disable=SC2086
    kill -TERM $pids 2>/dev/null || true
    # shellcheck disable=SC2086
    if ! wait_for_exit 180 $pids; then
      die "T3 Code did not quit within 180 s. Nothing was changed; quit it yourself and run this again."
    fi
    log "T3 Code has quit."
  fi
  local backends
  backends="$(backend_pids)"
  if [ -n "$backends" ]; then
    log "Waiting for T3's server to close (pids: $(echo $backends))..."
    # shellcheck disable=SC2086
    wait_for_exit 180 $backends || die "T3's server is still running after 180 s. Nothing was changed; quit it and run this again."
    log "T3's server has closed."
  fi
  local shipit
  shipit="$(shipit_pids)"
  if [ -n "$shipit" ]; then
    log "Waiting for a pending Nightly update install (ShipIt) to finish..."
    # shellcheck disable=SC2086
    wait_for_exit 600 $shipit || die "ShipIt is still running after 600 s. Nothing was changed."
  fi
}

# `open` hands the app this script's environment. Run from a T3 chat, that has
# ELECTRON_RUN_AS_NODE=1, and the app starts as a bare Node and exits at once; so give it only what
# launchd gives an app opened from the Dock.
open_like_dock() {
  local agent
  agent="$(/bin/launchctl getenv SSH_AUTH_SOCK 2>/dev/null || true)"
  /usr/bin/env -i HOME="$HOME" USER="$USER" LOGNAME="${LOGNAME:-$USER}" SHELL="${SHELL:-/bin/zsh}" \
    TMPDIR="${TMPDIR:-/tmp/}" PATH=/usr/bin:/bin:/usr/sbin:/sbin ${agent:+SSH_AUTH_SOCK="$agent"} \
    /usr/bin/open "$1"
}

# Open the app and make sure it stays open: right after the old copy quits, a new one can quit
# again within seconds, so try again a few times.
LAUNCH_SETTLE_SECONDS=10
LAUNCH_ATTEMPTS=5
launch_app() {
  local app="$1" name attempt
  name="$(basename "$app" .app)"
  for attempt in $(seq 1 "$LAUNCH_ATTEMPTS"); do
    open_like_dock "$app"
    sleep "$LAUNCH_SETTLE_SECONDS"
    if [ -n "$(running_app_pids)" ] && [ -n "$(backend_pids)" ]; then
      log "Launched $name"
      return 0
    fi
    # An app without its server is showing an error dialog; quit it before trying again.
    if [ -n "$(running_app_pids)" ]; then
      log "$name is open without its server (an error dialog); quitting it."
      # shellcheck disable=SC2046
      kill -TERM $(running_app_pids) 2>/dev/null || true
      # shellcheck disable=SC2046
      wait_for_exit 60 $(running_app_pids) || true
    fi
    log "$name quit right after launch (attempt $attempt of $LAUNCH_ATTEMPTS). Its processes still running:"
    /bin/ps -axo pid,etime,command | grep -F "/$name.app/" | grep -v -e grep -e ' follow ' | cut -c1-200 || true
    sleep $((attempt * 3))
  done
  die "$name did not stay open after $LAUNCH_ATTEMPTS launches. Open it yourself."
}

# The launcher T3 writes for itself (DesktopCliShim.ts), pointed at an app bundle.
write_cli_shim() {
  local app="$1" exe
  exe="$app/Contents/MacOS/$(/usr/bin/defaults read "$app/Contents/Info.plist" CFBundleExecutable)"
  [ -x "$exe" ] || die "No executable at $exe"
  mkdir -p "$(dirname "$CLI_SHIM")"
  cat >"$CLI_SHIM.fagner.tmp" <<EOF
#!/bin/sh
# Written by T3 Code: runs the desktop app's bundled t3 CLI.
export T3CODE_CLI_PATH='$CLI_SHIM'
home='$T3_HOME_DIR'
export T3CODE_HOME="\${T3CODE_HOME:-\$home}"
export ELECTRON_RUN_AS_NODE=1
app='$exe'
if [ ! -x "\$app" ]; then
  echo "T3 Code has moved or been removed. Open the app once to update this command." >&2
  exit 127
fi
exec "\$app" '$app/Contents/Resources/app.asar/apps/server/dist/bin.mjs' "\$@"
EOF
  chmod 755 "$CLI_SHIM.fagner.tmp"
  mv "$CLI_SHIM.fagner.tmp" "$CLI_SHIM"
  log "t3 CLI now runs $app"
}

# Start a copy of this script in its own session, so quitting T3 (and its terminals) cannot stop it.
detach_self() {
  local script="$1" log_file="$2"
  shift 2
  mkdir -p "$(dirname "$log_file")"
  /usr/bin/nohup /usr/bin/perl -e 'use POSIX qw(setsid); setsid(); exec @ARGV or die "exec: $!"' \
    "$script" --foreground "$@" >>"$log_file" 2>&1 </dev/null &
  echo "Running detached (pid $!). Log: $log_file"
}

# macOS forgets this copy of the app, so it never opens it in place of the installed one.
forget_app() {
  /System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -u "$1" 2>/dev/null || true
}
