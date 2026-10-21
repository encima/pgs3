#!/usr/bin/env bash
# One command from a primary to a running pgs3: build, capture the catalog at
# T0, start serving, and print the one command that subscribes the replica.
#
#   scripts/quickstart.sh <primary-conninfo> --restore-command '<cmd>' [options]
#
#   --restore-command <cmd>  how to fetch one segment: %f name, %p destination
#                            e.g. 'aws s3 cp s3://bucket/wal/%f %p'
#                                 'wal-g wal-fetch %f %p'
#                            (or set PGS3_WALG_CONFIG to use wal-g's config)
#   --publication <name>     default p; created FOR ALL TABLES if missing
#   --target <conninfo>      the replica: serve applies schema changes there
#                            as they happen on the primary
#   --port <n>               listen port, default 5439
#   --listen <addr:port>     listen address, default 127.0.0.1:<port>
#   --dir <path>             state directory, default ./pgs3-state
#   --reinit                 capture a new dictionary even if one exists
#   stop                     stop a pgs3 started by this script
#
# Everything lands in <dir>: config.yaml (rerun pgs3 with --config), the
# dictionary, the segment cache, the log, the status file and the pid file.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
usage() { sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

DIR="${PGS3_DIR:-$PWD/pgs3-state}"
SOURCE="" TARGET="" PUBLICATION="p" PORT=5439 LISTEN="" RESTORE="${PGS3_RESTORE_COMMAND:-}" REINIT="" STOP=""
while [ $# -gt 0 ]; do
  case "$1" in
    --restore-command|--restore-cmd) RESTORE="$2"; shift 2 ;;
    --publication)     PUBLICATION="$2"; shift 2 ;;
    --target)          TARGET="$2"; shift 2 ;;
    --port)            PORT="$2"; shift 2 ;;
    --listen)          LISTEN="$2"; shift 2 ;;
    --dir)             DIR="$2"; shift 2 ;;
    --reinit)          REINIT=1; shift ;;
    stop)              STOP=1; shift ;;
    -h|--help)         usage ;;
    -*)                echo "unknown option: $1" >&2; usage 1 ;;
    *)                 SOURCE="$1"; shift ;;
  esac
done
LISTEN="${LISTEN:-127.0.0.1:$PORT}"
PIDFILE="$DIR/pgs3.pid"

running() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

if [ -n "$STOP" ]; then
  if running; then kill "$(cat "$PIDFILE")"; echo "stopped pgs3 (PID $(cat "$PIDFILE"))"
  else echo "pgs3 is not running"; fi
  rm -f "$PIDFILE"; exit 0
fi

[ -n "$SOURCE" ] || { echo "error: give the primary's conninfo, e.g. 'host=primary dbname=app'" >&2; usage 1; }
command -v psql >/dev/null || { echo "error: psql is needed in PATH" >&2; exit 1; }
running && { echo "error: pgs3 is already running (PID $(cat "$PIDFILE")); '$0 stop' first" >&2; exit 1; }
if [ -z "$RESTORE" ] && [ -z "${PGS3_WALG_CONFIG:-}${WALG_S3_PREFIX:-}" ]; then
  echo "error: tell pgs3 how to fetch WAL from the archive:" >&2
  echo "  --restore-command 'aws s3 cp s3://<bucket>/<prefix>/%f %p'   (or wal-g, pgbackrest, ...)" >&2
  echo "  or export PGS3_WALG_CONFIG=/path/to/walg.json" >&2
  exit 1
fi

mkdir -p "$DIR/wal"
DIR="$(cd "$DIR" && pwd)"
CONFIG="$DIR/config.yaml"

echo "==> building pgs3"
( cd "$ROOT" && cargo build --release --quiet )
PGS3="$ROOT/target/release/pgs3"

echo "==> publication '$PUBLICATION' on the primary"
if [ "$(psql -X -tAq -d "$SOURCE" -v p="$PUBLICATION" <<<"select count(*) from pg_publication where pubname = :'p';")" = 1 ]; then
  echo "    exists"
else
  psql -X -q -d "$SOURCE" -v ON_ERROR_STOP=1 -v p="$PUBLICATION" <<<'create publication :"p" for all tables;'
  echo "    created FOR ALL TABLES"
fi

# Single-quoted YAML: only ' needs escaping, as ''.
yq() { printf "'%s'" "${1//\'/\'\'}"; }
{
  echo "# Written by scripts/quickstart.sh. Use with: pgs3 <command> --config $CONFIG"
  echo "source: $(yq "$SOURCE")"
  echo "publication: $(yq "$PUBLICATION")"
  echo "catalog: $(yq "$DIR/catalog.json")"
  echo "wal: $(yq "$DIR/wal")"
  echo "listen: $(yq "$LISTEN")"
  echo "status-file: $(yq "$DIR/status.json")"
  [ -n "$RESTORE" ] && echo "restore-command: $(yq "$RESTORE")"
} > "$CONFIG"

# Re-capturing moves T0, which must not happen under a running subscription.
if [ -s "$DIR/catalog.json" ] && [ -z "$REINIT" ]; then
  echo "==> reusing the dictionary in $DIR/catalog.json (--reinit to capture a new T0)"
else
  echo "==> capturing the catalog dictionary at T0"
  "$PGS3" init --config "$CONFIG" 2>&1 | sed 's/^/    /'
fi

# After init, which would otherwise copy the whole schema to a `target`: here
# the replica gets its data from a snapshot, and serve only needs it for DDL.
[ -n "$TARGET" ] && ! grep -q '^target:' "$CONFIG" && echo "target: $(yq "$TARGET")" >> "$CONFIG"

echo "==> checking the archive"
"$PGS3" check --config "$CONFIG" 2>&1 | sed 's/^/    /'

echo "==> starting pgs3 on $LISTEN"
nohup "$PGS3" serve --config "$CONFIG" >> "$DIR/pgs3.log" 2>&1 &
echo $! > "$PIDFILE"
for _ in $(seq 50); do
  grep -q 'listening on' "$DIR/pgs3.log" && break
  running || { tail -20 "$DIR/pgs3.log"; echo "error: pgs3 exited" >&2; exit 1; }
  sleep 0.1
done
START=$(sed -nE 's/.*"start_lsn" *: *"([^"]+)".*/\1/p' "$DIR/catalog.json")

cat <<EOF

pgs3 is serving (PID $(cat "$PIDFILE")), reading WAL from $START onwards.

Once the replica holds the data as of some LSN X >= $START (restored from a
snapshot taken after this point, or pg_upgraded from one), subscribe it with:

  $PGS3 subscribe --config $CONFIG \\
      --target '${TARGET:-host=<replica> dbname=<db>}' --at <X>

That creates no slot on the primary. Then watch:

  tail -f $DIR/pgs3.log
  cat $DIR/status.json
  $0 stop --dir $DIR
EOF
