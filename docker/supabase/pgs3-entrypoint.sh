#!/bin/sh
# Wait for the demo to capture the dictionary and declare the slot lost, then
# serve the wal-g archive. pgs3 has no route to the primary: every segment it
# decodes is fetched from S3 by `wal-g wal-fetch`.
#
# The demo touches $STATE/restart after re-capturing the dictionary for a
# schema change; pgs3 is then restarted on the new one.
set -eu

STATE=${PGS3_STATE:-/state}
LOG="$STATE/pgs3.log"

echo "pgs3: waiting for $STATE/ready"
while [ ! -f "$STATE/ready" ]; do sleep 0.5; done

: > "$LOG"
tail -F "$LOG" 2>/dev/null &

while :; do
  rm -f "$STATE/restart"
  # The start LSN comes from the dictionary `pgs3 init` wrote. The subscriber's
  # own origin tells pgs3 which of those transactions it already has.
  pgs3 serve \
    --catalog "$STATE/catalog.json" \
    --wal /var/lib/pgs3/wal \
    --restore-command 'wal-g --config /etc/wal-g/config.json wal-fetch %f %p' \
    --listen 0.0.0.0:5439 \
    --status-file "$STATE/status.json" \
    --confirmed-lsn-file "$STATE/confirmed.lsn" >> "$LOG" 2>&1 &
  PID=$!
  while kill -0 "$PID" 2>/dev/null && [ ! -f "$STATE/restart" ]; do sleep 0.5; done
  if [ ! -f "$STATE/restart" ]; then
    wait "$PID" || exit $?   # pgs3 exited on its own
    exit 0
  fi
  echo "pgs3: restarting on the dictionary starting at" \
    "$(sed -nE 's/.*"start_lsn" *: *"([^"]+)".*/\1/p' "$STATE/catalog.json")" >> "$LOG"
  kill "$PID" 2>/dev/null || true
  wait "$PID" || true
done
