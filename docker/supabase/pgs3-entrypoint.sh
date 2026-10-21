#!/bin/sh
# Wait for the demo to capture the dictionary and declare the slot lost, then
# serve the wal-g archive. pgs3 has no route to the primary: every segment it
# decodes is fetched from S3 by `wal-g wal-fetch`.
set -eu

STATE=${PGS3_STATE:-/state}
LOG="$STATE/pgs3.log"

echo "pgs3: waiting for $STATE/ready"
while [ ! -f "$STATE/ready" ]; do sleep 0.5; done

# Logged to the state volume as well, so the demo can show what pgs3 did.
: > "$LOG"
tail -F "$LOG" 2>/dev/null &

# Everything else is in the config: the archive, and the replica (`target`)
# that schema changes are applied to. The start LSN comes from the dictionary
# `pgs3 init` wrote at T0; the subscriber's own origin tells pgs3 which of
# those transactions it already has.
exec pgs3 serve --config /etc/pgs3.yaml >> "$LOG" 2>&1
