#!/usr/bin/env bash
# Advance the primary's logical slot to whatever the subscriber has confirmed,
# so the primary retains only the WAL pgs3 has yet to hand over.
#
#   advance-slot.sh <conninfo> <slot> <confirmed-lsn-file> [interval-seconds]
#
# The LSN comes from the file pgs3 writes, which is fed solely by the
# subscriber's own standby status updates. That is the whole safety argument:
# this can never advance past what the subscriber has flushed, so a cutover
# can never ask the primary for WAL behind its slot.
set -euo pipefail
CONN="${1:?conninfo}"; SLOT="${2:?slot name}"; FILE="${3:?confirmed lsn file}"
INTERVAL="${4:-10}"

while true; do
  if [ -r "$FILE" ]; then
    LSN=$(tr -d '[:space:]' < "$FILE")
    if [ -n "$LSN" ]; then
      # pg_replication_slot_advance decodes internally from restart_lsn to the
      # target, so advance often in small steps rather than in rare big jumps.
      OUT=$(psql -X -q -t -A -d "$CONN" -v ON_ERROR_STOP=1 -c "
        select case
                 when confirmed_flush_lsn >= '$LSN'::pg_lsn then 'already at ' || confirmed_flush_lsn
                 else (select 'advanced to ' || end_lsn
                       from pg_replication_slot_advance('$SLOT', '$LSN'::pg_lsn))
               end
        from pg_replication_slots where slot_name = '$SLOT';")
      echo "$(date -u +%FT%TZ) $SLOT $OUT"
    fi
  fi
  sleep "$INTERVAL"
done
