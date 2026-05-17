#!/usr/bin/env sh
set -eu

MODE="${1:-server}"
if [ "$#" -gt 0 ]; then
  shift
fi

case "$MODE" in
  server)
    PROCESS_NAME="xclouds"
    SUBCOMMAND="server"
    ;;
  client)
    PROCESS_NAME="xcloudc"
    SUBCOMMAND="client"
    ;;
  *)
    echo "usage: $0 server|client [xcloud args...]" >&2
    exit 2
    ;;
esac

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
RUNTIME_DIR="${XCLOUD_RUNTIME_DIR:-$ROOT_DIR/xcloud-runtime}"
BIN_DIR="${XCLOUD_BIN_DIR:-$RUNTIME_DIR/bin}"
RUN_DIR="${XCLOUD_RUN_DIR:-$RUNTIME_DIR/run}"
LOG_DIR="${XCLOUD_LOG_DIR:-$RUNTIME_DIR/logs}"
RESTART_DELAY="${XCLOUD_RESTART_DELAY:-3}"
BIN="$BIN_DIR/$PROCESS_NAME"
LOCK_DIR="$RUN_DIR/$PROCESS_NAME.lock"
LOG_FILE="$LOG_DIR/$PROCESS_NAME.log"

mkdir -p "$BIN_DIR" "$RUN_DIR" "$LOG_DIR"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "$PROCESS_NAME watchdog is already running. lock: $LOCK_DIR" >&2
  exit 1
fi

cleanup() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

if [ ! -x "$BIN" ]; then
  echo "building $PROCESS_NAME -> $BIN"
  (cd "$ROOT_DIR" && go build -o "$BIN" ./cmd/xcloud)
fi

echo "$PROCESS_NAME watchdog started. log: $LOG_FILE"
while true; do
  echo "$(date '+%Y-%m-%d %H:%M:%S') starting $PROCESS_NAME $SUBCOMMAND $*" >> "$LOG_FILE"
  set +e
  "$BIN" "$SUBCOMMAND" "$@" >> "$LOG_FILE" 2>&1
  STATUS="$?"
  set -e
  echo "$(date '+%Y-%m-%d %H:%M:%S') $PROCESS_NAME exited with status $STATUS; restarting in ${RESTART_DELAY}s" >> "$LOG_FILE"
  sleep "$RESTART_DELAY"
done
