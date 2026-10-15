#!/usr/bin/env bash
# Run the check against every Postgres major available in the nix store.
# The WAL record format is stable since 9.5, but the block-image header changed
# in PG15, so the range must straddle it to prove anything.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mapfile -t BINS < <(ls -d /nix/store/*-postgresql-*/bin 2>/dev/null \
  | grep -v -- '-dev/bin' \
  | while read -r d; do echo "$("$d/initdb" --version | awk '{print $3}')|$d"; done \
  | sort -t. -k1,1n -u)
[ ${#BINS[@]} -eq 0 ] && { echo "no postgres builds found"; exit 1; }

PORT=55600; FAILED=()
for entry in "${BINS[@]}"; do
  ver="${entry%%|*}"; bin="${entry#*|}"
  PORT=$((PORT+10))
  echo
  ok=1
  for script in check.sh check-subscriber.sh check-simdu.sh; do
    PGS3_PG_BIN="$bin" PGS3_CHECK_PORT="$PORT" "$ROOT/scripts/$script" 2>&1 \
      | grep -vE '^initdb:|^NOTICE:' || ok=0
  done
  [ $ok -eq 1 ] || FAILED+=("$ver")
done
echo
if [ ${#FAILED[@]} -eq 0 ]; then
  echo "ALL PASS across: $(printf '%s ' "${BINS[@]%%|*}")"
else
  echo "FAILED on: ${FAILED[*]}"; exit 1
fi
