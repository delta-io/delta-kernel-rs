#!/usr/bin/env bash
# Start or stop the Unity Catalog OSS server built by setup_unitycatalog.sh, for the gated
# delta-kernel-unity-catalog live integration tests.
#
# Usage: uc_oss_server.sh {start|stop}
# Overridable via env: UC_DIR, UC_HOST, UC_PID_FILE, UC_LOG_FILE.
set -euo pipefail

UC_DIR="${UC_DIR:-$HOME/unitycatalog}"
UC_HOST="${UC_HOST:-http://localhost:8080}"
PID_FILE="${UC_PID_FILE:-$UC_DIR/uc-server.pid}"
LOG_FILE="${UC_LOG_FILE:-$UC_DIR/uc-server.log}"
# Any always-available REST endpoint works as a readiness probe; listing catalogs returns 200
# once the server is serving (auth is disabled by default in the OSS server config).
HEALTH_URL="$UC_HOST/api/2.1/unity-catalog/catalogs"

start() {
  cd "$UC_DIR"

  # Managed (CREATE) tables need a storage root; a fresh server has none, so the staging-tables
  # endpoint 400s. Point the server-wide managed location at a local dir for CI. Kept outside
  # UC_DIR so created tables don't end up in the build cache. Rewrite the line so a cached
  # server.properties with a stale path gets corrected.
  local managed_dir="${UC_MANAGED_DIR:-${TMPDIR:-/tmp}/uc-managed-tables}"
  local props="$UC_DIR/etc/conf/server.properties"
  mkdir -p "$managed_dir"
  sed -i '/^storage-root\.tables=/d' "$props"
  echo "storage-root.tables=file://$managed_dir" >>"$props"

  echo "Starting Unity Catalog server (logs: $LOG_FILE)"
  # Launch in a new session so stop() can kill the whole process group (the forked JVM child),
  # not just the wrapper. Record the session leader's own PID ($$) as the PGID; capturing $! here
  # would yield the wrapper PID, not the new session leader.
  setsid bash -c 'echo $$ >"'"$PID_FILE"'"; exec bin/start-uc-server >"'"$LOG_FILE"'" 2>&1' &

  # Wait up to ~5 minutes: covers an incremental sbt build on first boot plus JVM startup.
  for i in $(seq 1 150); do
    if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
      echo "Unity Catalog server is up (after $((i * 2))s)."
      return 0
    fi
    if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      echo "Server process exited before becoming healthy. Logs:" >&2
      cat "$LOG_FILE" >&2
      return 1
    fi
    sleep 2
  done

  echo "Server did not become healthy within the timeout. Logs:" >&2
  cat "$LOG_FILE" >&2
  return 1
}

stop() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE")"
    # Kill the whole process group (negative pid) to take down the forked JVM child.
    kill -- -"$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
  fi
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  *)
    echo "usage: $0 {start|stop}" >&2
    exit 2
    ;;
esac
