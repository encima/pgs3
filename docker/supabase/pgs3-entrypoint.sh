#!/bin/sh
# Wait for the demo to capture the dictionary and declare the slot lost, then
# serve the wal-g archive. pgs3 has no route to the primary: every segment it
# decodes is fetched from S3 by `wal-g wal-fetch`.
set -eu

STATE=${PGS3_STATE:-/state}

echo "pgs3: waiting for $STATE/ready"
while [ ! -f "$STATE/ready" ]; do sleep 0.5; done

# The start LSN comes from the dictionary `pgs3 init` wrote at T0. The
# subscriber's own origin tells pgs3 which of those transactions it already has.
exec pgs3 serve \
  --catalog "$STATE/catalog.json" \
  --wal /var/lib/pgs3/wal \
  --restore-command 'wal-g --config /etc/wal-g/config.json wal-fetch %f %p' \
  --listen 0.0.0.0:5439 \
  --status-file "$STATE/status.json" \
  --confirmed-lsn-file "$STATE/confirmed.lsn"
