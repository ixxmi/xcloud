#!/usr/bin/env sh
set -eu

usage() {
  cat >&2 <<'EOF'
usage:
  xcloud-watchdog.sh start server|client [xcloud args...]
  xcloud-watchdog.sh stop server|client
  xcloud-watchdog.sh restart server|client [xcloud args...]
  xcloud-watchdog.sh upgrade server|client [xcloud args...]
  xcloud-watchdog.sh status server|client
  xcloud-watchdog.sh foreground server|client [xcloud args...]

compat:
  xcloud-watchdog.sh server|client [xcloud args...]  # alias for start
EOF
}

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SCRIPT_PATH="$SCRIPT_DIR/$(basename "$0")"
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
RUNTIME_DIR="${XCLOUD_RUNTIME_DIR:-$ROOT_DIR/xcloud-runtime}"
BIN_DIR="${XCLOUD_BIN_DIR:-$RUNTIME_DIR/bin}"
RUN_DIR="${XCLOUD_RUN_DIR:-$RUNTIME_DIR/run}"
LOG_DIR="${XCLOUD_LOG_DIR:-$RUNTIME_DIR/logs}"
GOCACHE="${GOCACHE:-$RUNTIME_DIR/go-build-cache}"
RESTART_DELAY="${XCLOUD_RESTART_DELAY:-3}"
STOP_TIMEOUT="${XCLOUD_STOP_TIMEOUT:-15}"
export GOCACHE

ACTION="${1:-start}"
if [ "$#" -gt 0 ]; then
  shift
fi

case "$ACTION" in
  server|client)
    MODE="$ACTION"
    ACTION="start"
    ;;
  start|stop|restart|upgrade|status|foreground|watchdog)
    MODE="${1:-}"
    if [ "$#" -gt 0 ]; then
      shift
    fi
    ;;
  *)
    usage
    exit 2
    ;;
esac

case "${MODE:-}" in
  server)
    PROCESS_NAME="xclouds"
    SUBCOMMAND="server"
    ;;
  client)
    PROCESS_NAME="xcloudc"
    SUBCOMMAND="client"
    ;;
  *)
    usage
    exit 2
    ;;
esac

BIN="$BIN_DIR/$PROCESS_NAME"
PID_FILE="$RUN_DIR/$PROCESS_NAME.watchdog.pid"
CHILD_PID_FILE="$RUN_DIR/$PROCESS_NAME.pid"
ARGS_FILE="$RUN_DIR/$PROCESS_NAME.args"
LOG_FILE="$LOG_DIR/$PROCESS_NAME.log"
LOCK_DIR="$RUN_DIR/$PROCESS_NAME.lock"

mkdir -p "$BIN_DIR" "$RUN_DIR" "$LOG_DIR" "$GOCACHE"

is_running() {
  pid="$1"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

read_pid() {
  file="$1"
  if [ -f "$file" ]; then
    read -r pid < "$file" || pid=""
    printf '%s' "$pid"
  fi
}

quote_arg() {
  printf "'"
  printf '%s' "$1" | sed "s/'/'\\\\''/g"
  printf "'"
}

save_args() {
  printf 'set --' > "$ARGS_FILE"
  for arg do
    printf ' ' >> "$ARGS_FILE"
    quote_arg "$arg" >> "$ARGS_FILE"
  done
  printf '\n' >> "$ARGS_FILE"
}

build_binary() {
  tmp="$BIN.tmp.$$"
  echo "building $PROCESS_NAME -> $BIN"
  (cd "$ROOT_DIR" && go build -o "$tmp" ./cmd/xcloud)
  mv "$tmp" "$BIN"
}

ensure_binary() {
  if [ ! -x "$BIN" ]; then
    build_binary
  fi
}

cleanup_lock() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
}

cleanup_watchdog() {
  child_pid="$(read_pid "$CHILD_PID_FILE")"
  if is_running "$child_pid"; then
    kill "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
  fi
  rm -f "$PID_FILE" "$CHILD_PID_FILE"
  cleanup_lock
}

fallback_watchdog_pids() {
  if command -v pgrep >/dev/null 2>&1; then
    pgrep -f "xcloud-watchdog[.]sh (watchdog |foreground )?$MODE([[:space:]]|$)" 2>/dev/null || true
  fi
}

fallback_process_pids() {
  if command -v pgrep >/dev/null 2>&1; then
    pgrep -x "$PROCESS_NAME" 2>/dev/null || true
  fi
}

has_fallback_processes() {
  for pid in $(fallback_watchdog_pids) $(fallback_process_pids); do
    if [ "$pid" != "$$" ] && is_running "$pid"; then
      return 0
    fi
  done
  return 1
}

stop_pid() {
  pid="$1"
  label="$2"
  if [ "$pid" = "$$" ]; then
    return 0
  fi
  if ! is_running "$pid"; then
    return 0
  fi

  echo "stopping $PROCESS_NAME $label pid $pid"
  kill "$pid" 2>/dev/null || true

  elapsed=0
  while is_running "$pid"; do
    if [ "$elapsed" -ge "$STOP_TIMEOUT" ]; then
      echo "forcing $PROCESS_NAME $label pid $pid"
      kill -KILL "$pid" 2>/dev/null || true
      break
    fi
    elapsed=$((elapsed + 1))
    sleep 1
  done
}

do_status() {
  watchdog_pid="$(read_pid "$PID_FILE")"
  child_pid="$(read_pid "$CHILD_PID_FILE")"
  watchdog_running=0
  process_running=0

  if is_running "$watchdog_pid"; then
    echo "$PROCESS_NAME watchdog running: pid $watchdog_pid"
    watchdog_running=1
  else
    for pid in $(fallback_watchdog_pids); do
      if [ "$pid" != "$$" ] && is_running "$pid"; then
        echo "$PROCESS_NAME watchdog running without pid file: pid $pid"
        watchdog_running=1
      fi
    done
    if [ "$watchdog_running" -eq 0 ]; then
      echo "$PROCESS_NAME watchdog stopped"
    fi
  fi

  if is_running "$child_pid"; then
    echo "$PROCESS_NAME process running: pid $child_pid"
    process_running=1
  else
    for pid in $(fallback_process_pids); do
      if [ "$pid" != "$$" ] && is_running "$pid"; then
        echo "$PROCESS_NAME process running without pid file: pid $pid"
        process_running=1
      fi
    done
    if [ "$process_running" -eq 0 ]; then
      echo "$PROCESS_NAME process stopped"
    fi
  fi

  echo "log: $LOG_FILE"
}

do_stop() {
  watchdog_pid="$(read_pid "$PID_FILE")"
  child_pid="$(read_pid "$CHILD_PID_FILE")"

  if is_running "$watchdog_pid"; then
    stop_pid "$watchdog_pid" "watchdog"
  fi
  if is_running "$child_pid"; then
    stop_pid "$child_pid" "process"
  fi
  for pid in $(fallback_watchdog_pids); do
    stop_pid "$pid" "watchdog"
  done
  for pid in $(fallback_process_pids); do
    stop_pid "$pid" "process"
  done

  rm -f "$PID_FILE" "$CHILD_PID_FILE"
  cleanup_lock
}

do_start() {
  ensure_binary

  current_pid="$(read_pid "$PID_FILE")"
  if is_running "$current_pid"; then
    echo "$PROCESS_NAME watchdog is already running: pid $current_pid"
    echo "use '$0 upgrade $MODE [xcloud args...]' to rebuild and restart it"
    exit 1
  fi
  if [ -d "$LOCK_DIR" ] && has_fallback_processes; then
    echo "$PROCESS_NAME watchdog appears to be running without a pid file. lock: $LOCK_DIR" >&2
    echo "use '$0 upgrade $MODE [xcloud args...]' to rebuild and restart it" >&2
    exit 1
  fi

  cleanup_lock
  save_args "$@"
  nohup "$SCRIPT_PATH" watchdog "$MODE" "$@" >> "$LOG_FILE" 2>&1 &
  watchdog_pid="$!"
  echo "$watchdog_pid" > "$PID_FILE"
  echo "$PROCESS_NAME watchdog started: pid $watchdog_pid, log: $LOG_FILE"
}

do_restart() {
  do_stop
  do_start "$@"
}

do_upgrade() {
  build_binary
  if [ "$#" -eq 0 ] && [ -f "$ARGS_FILE" ]; then
    . "$ARGS_FILE"
  fi
  do_restart "$@"
}

run_watchdog() {
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$PROCESS_NAME watchdog is already running. lock: $LOCK_DIR" >&2
    exit 1
  fi

  echo "$$" > "$PID_FILE"
  trap 'cleanup_watchdog; exit 0' INT TERM
  trap 'cleanup_watchdog' EXIT

  echo "$(date '+%Y-%m-%d %H:%M:%S') $PROCESS_NAME watchdog started" >> "$LOG_FILE"
  while true; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') starting $PROCESS_NAME $SUBCOMMAND $*" >> "$LOG_FILE"
    "$BIN" "$SUBCOMMAND" "$@" >> "$LOG_FILE" 2>&1 &
    child_pid="$!"
    echo "$child_pid" > "$CHILD_PID_FILE"

    set +e
    wait "$child_pid"
    status="$?"
    set -e
    rm -f "$CHILD_PID_FILE"

    echo "$(date '+%Y-%m-%d %H:%M:%S') $PROCESS_NAME exited with status $status; restarting in ${RESTART_DELAY}s" >> "$LOG_FILE"
    sleep "$RESTART_DELAY"
  done
}

case "$ACTION" in
  start)
    do_start "$@"
    ;;
  stop)
    do_stop
    ;;
  restart)
    do_restart "$@"
    ;;
  upgrade)
    do_upgrade "$@"
    ;;
  status)
    do_status
    ;;
  foreground)
    ensure_binary
    run_watchdog "$@"
    ;;
  watchdog)
    run_watchdog "$@"
    ;;
esac
