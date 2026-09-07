#!/bin/sh
# Wait for the demo to publish the catalog dictionary and the slot's
# restart_lsn, then serve the archive.
set -eu

STATE=${PGS3_STATE:-/state}
ARCHIVE=${PGS3_ARCHIVE:-/archive}

echo "pgs3: waiting for $STATE/ready (catalog dictionary + restart_lsn)"
while [ ! -f "$STATE/ready" ]; do sleep 0.5; done

START=$(cat "$STATE/start.lsn")
echo "pgs3: archive=$ARCHIVE start=$START"

# FFFFFFFF/FFFFFFFF means "no end": the reader stops when it reaches unwritten
# WAL or a segment that has not been archived yet, then retries. That is what
# makes this follow the archive rather than run once.
exec pgs3 serve \
  --wal "$ARCHIVE" \
  --catalog "$STATE/catalog.json" \
  --start "$START" \
  --end FFFFFFFF/FFFFFFFF \
  --listen 0.0.0.0:5439 \
  --confirmed-lsn-file "$STATE/confirmed.lsn"
