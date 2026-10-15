#!/usr/bin/env bash
# One-command setup: build, capture catalog, start serve, print subscriber SQL.
#
#   ./scripts/quickstart.sh <source-conninfo> [options]
#
# Examples:
#   ./scripts/quickstart.sh 'host=primary dbname=mydb'
#   ./scripts/quickstart.sh 'host=primary dbname=mydb' --publication mypub
#   ./scripts/quickstart.sh 'host=primary dbname=mydb' --port 15439
#
# What it does:
#   1. Builds pgs3 (release)
#   2. Captures the catalog dictionary at T0
#   3. Starts serve in the background
#   4. Prints the SQL to create the subscription on the replica
#
# After it runs, create the subscription on the replica using the printed SQL,
# then repoint it at pgs3:
#   ALTER SUBSCRIPTION sub CONNECTION 'host=127.0.0.1 port=5439 ...';
#   ALTER SUBSCRIPTION sub ENABLE;
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SOURCE="${1:?source conninfo, e.g. 'host=primary dbname=mydb'}"
shift

PUBLICATION="p"
PGS3_DIR="${PGS3_DIR:-/tmp/pgs3}"
LISTEN="127.0.0.1:5439"
WAL_DIR="${PGS3_DIR}/walcache"
CATALOG="${PGS3_DIR}/catalog.json"
CONFIRMED_FILE="${PGS3_DIR}/confirmed.lsn"
STATUS_FILE="${PGS3_DIR}/status"
PORT_REPLICATION=5439
EXTRA_ARGS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --publication)  PUBLICATION="$2"; shift 2 ;;
    --dir)          PGS3_DIR="$2"; shift 2
                    WAL_DIR="${PGS3_DIR}/walcache"
                    CATALOG="${PGS3_DIR}/catalog.json"
                    CONFIRMED_FILE="${PGS3_DIR}/confirmed.lsn"
                    STATUS_FILE="${PGS3_DIR}/status" ;;
    --port)         LISTEN="127.0.0.1:$2"; PORT_REPLICATION="$2"; shift 2 ;;
    --wal-dir)      WAL_DIR="$2"; shift 2 ;;
    --restore-cmd)  EXTRA_ARGS+=(--restore-command "$2"); shift 2 ;;
    --source-for-switch) EXTRA_ARGS+=(--source "$2"); shift 2 ;;
    -h|--help)      sed -n '2,20p' "$0"; exit 0 ;;
    *)              echo "unknown option: $1" >&2; exit 1 ;;
  esac
done

mkdir -p "$PGS3_DIR" "$WAL_DIR"

echo "==> Building pgs3"
( cd "$ROOT" && cargo build --release --quiet )
PGS3="$ROOT/target/release/pgs3"

echo "==> Ensuring publication '$PUBLICATION' exists on source"
if psql "$SOURCE" -tAc "SELECT 1 FROM pg_publication WHERE pubname = '$PUBLICATION'" | grep -q 1; then
  echo "    already exists"
else
  psql "$SOURCE" -c "CREATE PUBLICATION $PUBLICATION FOR ALL TABLES;" 2>&1 | sed 's/^/    /'
  echo "    created (FOR ALL TABLES)"
fi

CONFIG_FILE="${PGS3_DIR}/config.yaml"
cat > "$CONFIG_FILE" <<EOF
catalog: $CATALOG
publication: $PUBLICATION
wal: $WAL_DIR
listen: $LISTEN
confirmed-lsn-file: $CONFIRMED_FILE
status-file: $STATUS_FILE
EOF

echo "==> Capturing catalog dictionary at T0"
echo "    source: $SOURCE"
echo "    config:   $CONFIG_FILE"
"$PGS3" init --config "$CONFIG_FILE" --source "$SOURCE" 2>&1 | sed 's/^/    /'

echo "==> Starting pgs3 serve on $LISTEN"
echo "    wal dir:    $WAL_DIR"
echo "    catalog:    $CATALOG"
echo "    confirmed:  $CONFIRMED_FILE"
echo "    status:     $STATUS_FILE"
echo "    extra:      ${EXTRA_ARGS[*]:-(none)}"

"$PGS3" serve --config "$CONFIG_FILE" "${EXTRA_ARGS[@]}" \
  > "${PGS3_DIR}/pgs3.log" 2>&1 &

SERVE_PID=$!
echo "$SERVE_PID" > "${PGS3_DIR}/serve.pid"

# Wait for listener
for _ in $(seq 100); do
  grep -q 'listening on' "${PGS3_DIR}/pgs3.log" 2>/dev/null && break
  kill -0 "$SERVE_PID" 2>/dev/null || { echo "serve died:"; cat "${PGS3_DIR}/pgs3.log"; exit 1; }
  sleep 0.1
done

grep -q 'listening on' "${PGS3_DIR}/pgs3.log" || { echo "serve never listened:"; cat "${PGS3_DIR}/pgs3.log"; exit 1; }

echo "==> Checking WAL directory"
SEG_COUNT=$(find "$WAL_DIR" -type f -name '*.wal' 2>/dev/null | wc -l | tr -d ' ')
if [ "$SEG_COUNT" -gt 0 ]; then
  echo "    found $SEG_COUNT segment(s) already staged in $WAL_DIR"
  find "$WAL_DIR" -type f -name '*.wal' | sort | sed 's/^/      /'
else
  echo "    no segments staged in $WAL_DIR"
  if [ -n "${EXTRA_ARGS[*]:-}" ] && echo "${EXTRA_ARGS[*]}" | grep -q 'restore-command'; then
    echo "    serve will fetch on demand via --restore-command"
  else
    echo "    WARNING: no --restore-command set and WAL dir is empty."
    echo "    serve will block until segments appear. Either:"
    echo "      1. Pre-stage WAL: cp \$PGDATA/pg_wal/*.wal $WAL_DIR/"
    echo "      2. Re-run with --restore-command '...' to fetch on demand"
  fi
fi

echo ""
echo "==> pgs3 is serving (PID $SERVE_PID). On the replica, run:"
echo ""
echo "  -- Establish the subscription against the PRIMARY first (T0):"
echo "  CREATE SUBSCRIPTION sub"
echo "    CONNECTION 'host=<primary-host> port=<primary-port> user=<user> dbname=<db>'"
echo "    PUBLICATION $PUBLICATION"
echo "    WITH (binary = true, streaming = off, copy_data = false, slot_name = 'sub');"
echo ""
echo "  -- After the upgrade, repoint at pgs3:"
echo "  ALTER SUBSCRIPTION sub CONNECTION 'host=127.0.0.1 port=$PORT_REPLICATION dbname=<db>';"
echo "  ALTER SUBSCRIPTION sub ENABLE;"
echo ""
echo "==> Monitor:"
echo "  tail -f ${PGS3_DIR}/pgs3.log"
echo "  cat ${PGS3_DIR}/status"
echo "  cat ${CONFIRMED_FILE} 2>/dev/null"
echo ""
echo "==> Stop:"
echo "  kill \$(cat ${PGS3_DIR}/serve.pid)"
