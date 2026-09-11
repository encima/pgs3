#!/usr/bin/env bash
# The shape pgs3 exists for, on one machine, with a real object store and real
# wal-g on both ends:
#
#   pg17 primary --wal-g wal-push--> minio --wal-g wal-fetch--> pgs3 --pgoutput--> pg18
#
# pgs3 gets an *empty* --wal directory, so every segment it decodes has to come
# out of the bucket. The two majors differ on purpose: 17 is the source being
# upgraded, 18 is the replica it is upgrading to.
#
# minio, mc and wal-g come from devbox; the Postgres majors are resolved
# straight out of the nix store, because two majors cannot share one PATH.
#
#   devbox run demo-up       # build the stack, verify it replicated, leave it up
#   devbox run demo-status
#   devbox run demo-down
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIR="${PGS3_DEMO_DIR:-/tmp/pgs3-demo}"
OLD_PG="${PGS3_OLD_PG:-postgresql_17}"
NEW_PG="${PGS3_NEW_PG:-postgresql_18}"

PORT_OLD=55417; PORT_NEW=55418; PORT_PGS3=55439; PORT_S3=59000; PORT_UI=59001
BUCKET=pgs3; S3_KEY=minioadmin; S3_SECRET=minioadmin

export MC_CONFIG_DIR="$DIR/mc"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()  { printf '   \033[32mok\033[0m  %s\n' "$*"; }
die() { printf '   \033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }

# Re-evaluates the expression each round, so command substitution inside it is
# re-run rather than frozen at call time.
retry() { # seconds  shell-expression
  local deadline=$(( SECONDS + $1 )); shift
  while [ "$SECONDS" -lt "$deadline" ]; do
    if eval "$*" >/dev/null 2>&1; then return 0; fi
    sleep 0.3
  done
  return 1
}

pgbin() { nix build --no-link "nixpkgs#$1" >&2; echo "$(nix eval --raw "nixpkgs#$1.outPath")/bin"; }
OLD_BIN=$(pgbin "$OLD_PG"); NEW_BIN=$(pgbin "$NEW_PG")

pri() { "$OLD_BIN/psql" -X -q -t -A -h 127.0.0.1 -p "$PORT_OLD" -U postgres -d postgres "$@"; }
rep() { "$NEW_BIN/psql" -X -q -t -A -h 127.0.0.1 -p "$PORT_NEW" -U postgres -d postgres "$@"; }
slot_field() { pri -c "select $1 from pg_replication_slots where slot_name='sub';"; }

kill_pidfile() {
  [ -f "$DIR/$1.pid" ] || return 0
  kill "$(cat "$DIR/$1.pid")" 2>/dev/null || true
  rm -f "$DIR/$1.pid"
}

down() {
  kill_pidfile pgs3
  [ -d "$DIR/pg-old" ] && "$OLD_BIN/pg_ctl" -D "$DIR/pg-old" stop -m immediate >/dev/null 2>&1 || true
  [ -d "$DIR/pg-new" ] && "$NEW_BIN/pg_ctl" -D "$DIR/pg-new" stop -m immediate >/dev/null 2>&1 || true
  kill_pidfile minio
}

SCHEMA="create table t (
  id      int primary key,
  name    text,
  big     text,
  amount  numeric(12,2),
  flag    bool,
  uid     uuid,
  doc     jsonb,
  tags    text[],
  stamps  timestamptz[],
  ts      timestamptz);"

# The digest every row is compared on: TOAST, jsonb, arrays and numeric all in.
ROWS="select id||'|'||coalesce(name,'')||'|'||coalesce(md5(big),'')||'|'||coalesce(amount::text,'')
      ||'|'||coalesce(flag::text,'')||'|'||coalesce(uid::text,'')||'|'||coalesce(doc::text,'')
      ||'|'||coalesce(tags::text,'')||'|'||coalesce(stamps::text,'')||'|'||coalesce(ts::text,'')
      from t order by id;"

up() {
  say "0. clean slate"
  down; rm -rf "$DIR"
  mkdir -p "$DIR"/{minio-data,walcache,state,mc}
  ok "$DIR"

  say "1. minio on 127.0.0.1:$PORT_S3 (console :$PORT_UI)"
  MINIO_ROOT_USER="$S3_KEY" MINIO_ROOT_PASSWORD="$S3_SECRET" \
    minio server --quiet --address "127.0.0.1:$PORT_S3" \
    --console-address "127.0.0.1:$PORT_UI" "$DIR/minio-data" \
    > "$DIR/minio.log" 2>&1 &
  echo $! > "$DIR/minio.pid"
  retry 30 "mc alias set demo http://127.0.0.1:$PORT_S3 $S3_KEY $S3_SECRET" \
    || die "minio never came up; see $DIR/minio.log"
  mc mb --ignore-existing "demo/$BUCKET" >/dev/null
  ok "bucket s3://$BUCKET"

  # One config, used by both ends: the primary's archive_command pushes with it
  # and pgs3's wal-fetch pulls with it. It carries no Postgres connection
  # details, so nothing pgs3 runs can reach the primary.
  cat > "$DIR/walg.json" <<JSON
{
  "WALG_S3_PREFIX": "s3://$BUCKET/wal",
  "AWS_ENDPOINT": "http://127.0.0.1:$PORT_S3",
  "AWS_ACCESS_KEY_ID": "$S3_KEY",
  "AWS_SECRET_ACCESS_KEY": "$S3_SECRET",
  "AWS_REGION": "us-east-1",
  "AWS_S3_FORCE_PATH_STYLE": "true"
}
JSON

  say "2. two clusters: $("$OLD_BIN/initdb" --version | awk '{print $3}') primary, $("$NEW_BIN/initdb" --version | awk '{print $3}') replica"
  "$OLD_BIN/initdb" -D "$DIR/pg-old" -U postgres --no-sync -A trust >/dev/null 2>&1
  "$NEW_BIN/initdb" -D "$DIR/pg-new" -U postgres --no-sync -A trust >/dev/null 2>&1
  cat >> "$DIR/pg-old/postgresql.conf" <<CONF
port = $PORT_OLD
listen_addresses = '127.0.0.1'
unix_socket_directories = '$DIR/pg-old'
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8
wal_compression = pglz
fsync = off
archive_mode = on
archive_command = '$(command -v wal-g) --config $DIR/walg.json wal-push %p'
archive_timeout = 10s
CONF
  cat >> "$DIR/pg-new/postgresql.conf" <<CONF
port = $PORT_NEW
listen_addresses = '127.0.0.1'
unix_socket_directories = '$DIR/pg-new'
fsync = off
max_logical_replication_workers = 8
max_worker_processes = 16
CONF
  "$OLD_BIN/pg_ctl" -D "$DIR/pg-old" -l "$DIR/pg-old.log" start -w >/dev/null
  "$NEW_BIN/pg_ctl" -D "$DIR/pg-new" -l "$DIR/pg-new.log" start -w >/dev/null
  ok "primary :$PORT_OLD, replica :$PORT_NEW"

  say "3. the shared schema, published from the primary"
  pri -c "$SCHEMA alter table t replica identity full; create publication p for table t;" >/dev/null
  rep -c "$SCHEMA" >/dev/null
  ok "table t on both"

  say "4. the replica subscribes to the primary (T0), then goes away"
  # This first, direct subscription is not decoration: it is what fills
  # pg_subscription_rel on the replica. pgs3 speaks only IDENTIFY_SYSTEM and
  # START_REPLICATION, so ALTER SUBSCRIPTION ... REFRESH PUBLICATION cannot be
  # answered against it, and a subscription that never learned its table list
  # silently discards every change. T0 is established against the primary; the
  # catch-up is not.
  rep -c "create subscription sub
          connection 'host=127.0.0.1 port=$PORT_OLD user=postgres dbname=postgres'
          publication p
          with (binary = true, streaming = off, copy_data = false, slot_name = 'sub');" >/dev/null
  pri -c "insert into t (id, name, ts) values (1, 'before-the-window', now());" >/dev/null
  retry 60 "[ \"\$(rep -c 'select count(*) from t;')\" = '1' ]" \
    || die "the subscription never came up against the primary"
  rep -c "alter subscription sub disable;" >/dev/null
  retry 30 "[ \"\$(pri -c 'select count(*) from pg_stat_replication;')\" = '0' ]" \
    || die "the primary still shows a replication connection"
  # restart_lsn, not confirmed_flush_lsn: a transaction can begin before the
  # confirmed point and commit after it, so decoding has to start further back.
  START=$(slot_field restart_lsn)
  echo "$START" > "$DIR/state/start.lsn"
  ok "1 row via the primary, then disconnected at restart_lsn = $START"

  say "5. workload on the primary, archived to minio by wal-g"
  pri >/dev/null <<'SQL'
insert into t values (2,'alice',null,10.50,true,'11111111-1111-1111-1111-111111111111',
  '{"a":1,"b":[1,2.5,null,true,"s"]}', array['x','yy'],
  array['2026-01-01 00:00:00+00'::timestamptz,'2026-02-02 02:02:02+00'], now());
insert into t values (3,'bob',null,-3.25,false,null,'[]'::jsonb,array['a',null,'c'],'{}',now());
-- Incompressible, so it is pushed out to the TOAST relation in chunks.
insert into t values (4,'carol',
  (select string_agg(md5(random()::text||g),'') from generate_series(1,300) g),
  0.00,null,null,'"scalar"'::jsonb,array[]::text[],null,now());
-- Compressible, so it is pglz'd and kept inline.
insert into t values (5,'dave',repeat('abcdefgh',2000),1.00,true,null,'12345.678'::jsonb,null,null,now());
update t set name='alice2', amount=99.99 where id=2;
update t set id=30 where id=3;
delete from t where id=5;
begin; insert into t (id,name) values (99,'rolled back'); rollback;
copy t (id,name,amount) from stdin with (format csv);
6,eve,5.55
7,frank,6.66
\.
SQL
  pri -c "select pg_switch_wal();" >/dev/null
  retry 60 "[ \"\$(mc ls --recursive demo/$BUCKET/wal/ | wc -l | tr -d ' ')\" != 0 ]" \
    || die "nothing reached the bucket; check archive_command in $DIR/pg-old.log"
  ok "$(mc ls --recursive "demo/$BUCKET/wal/" | wc -l | tr -d ' ') object(s) in s3://$BUCKET/wal, $(pri -c 'select count(*) from t;') rows on the primary"

  say "6. freeze the catalog dictionary"
  pri -v pubname=p -f "$ROOT/sql/dump_catalog.sql" > "$DIR/state/catalog.json"
  ok "$(wc -c < "$DIR/state/catalog.json" | tr -d ' ') bytes of dictionary"

  say "7. pgs3 serves the bucket on :$PORT_PGS3"
  ( cd "$ROOT" && cargo build --release --quiet )
  # --wal is an empty cache dir: with nothing in it, every segment has to be
  # fetched from minio by wal-g before it can be decoded.
  PGS3_WALG_CONFIG="$DIR/walg.json" "$ROOT/target/release/pgs3" serve \
    --wal "$DIR/walcache" \
    --catalog "$DIR/state/catalog.json" \
    --start "$START" \
    --end FFFFFFFF/FFFFFFFF \
    --listen "127.0.0.1:$PORT_PGS3" \
    --confirmed-lsn-file "$DIR/state/confirmed.lsn" \
    > "$DIR/pgs3.log" 2>&1 &
  echo $! > "$DIR/pgs3.pid"
  # In a subshell so the socket is closed again: pgs3 serves one connection at
  # a time, and a probe left open would hold the listener shut.
  retry 30 "(exec 3<>/dev/tcp/127.0.0.1/$PORT_PGS3)" \
    || die "pgs3 never listened; see $DIR/pgs3.log"
  ok "serving from $START with an empty segment cache"

  say "8. repoint the replica at pgs3"
  rep -c "alter subscription sub
          connection 'host=127.0.0.1 port=$PORT_PGS3 user=postgres dbname=postgres';" >/dev/null
  rep -c "alter subscription sub enable;" >/dev/null
  EXPECTED=$(pri -c 'select count(*) from t;')
  retry 120 "[ \"\$(rep -c 'select count(*) from t;')\" = '$EXPECTED' ]" \
    || die "stalled at $(rep -c 'select count(*) from t;') of $EXPECTED rows; see $DIR/pgs3.log"
  ok "caught up: $EXPECTED rows"

  say "9. did any of that come from the primary?"
  [ "$(slot_field active)" = "f" ] || die "the primary's slot went active"
  [ "$(pri -c 'select count(*) from pg_stat_replication;')" = "0" ] \
    || die "the primary served a replication connection"
  FETCHED=$(ls -1 "$DIR/walcache" | wc -l | tr -d ' ')
  [ "$FETCHED" != 0 ] || die "the segment cache is empty; nothing was fetched from minio"
  ok "slot still inactive, zero replication connections"
  ok "$FETCHED segment(s) in the cache, every one pulled by wal-g from minio"

  say "10. compare the two databases"
  if [ "$(pri -c "$ROWS")" != "$(rep -c "$ROWS")" ]; then
    echo "--- primary ---"; pri -c "$ROWS"
    echo "--- replica ---"; rep -c "$ROWS"
    die "contents differ"
  fi
  ok "every row identical across the major version boundary"

  say "11. it keeps following the archive"
  pri -c "insert into t (id,name,ts) values (100,'after-catch-up',now());" >/dev/null
  pri -c "select pg_switch_wal();" >/dev/null
  retry 90 "[ \"\$(rep -c 'select count(*) from t where id=100;')\" = '1' ]" \
    || die "pgs3 did not pick up the newly archived segment"
  ok "new writes arrive without the primary being read"

  printf '\n\033[1;32mUp and replicating.\033[0m '
  printf 'Stack left running; scripts/minio-demo.sh status | down\n'
  status
}

status() {
  printf '\nminio    http://127.0.0.1:%s  (console :%s, %s/%s)\n' \
    "$PORT_S3" "$PORT_UI" "$S3_KEY" "$S3_SECRET"
  printf 'primary  psql -h 127.0.0.1 -p %s -U postgres  (%s)\n' "$PORT_OLD" "$OLD_PG"
  printf 'replica  psql -h 127.0.0.1 -p %s -U postgres  (%s)\n' "$PORT_NEW" "$NEW_PG"
  printf 'pgs3     127.0.0.1:%s  logs %s/pgs3.log\n' "$PORT_PGS3" "$DIR"
  printf 'archive  %s objects in s3://%s/wal, %s segment(s) cached locally\n' \
    "$(mc ls --recursive "demo/$BUCKET/wal/" 2>/dev/null | wc -l | tr -d ' ')" \
    "$BUCKET" "$(ls -1 "$DIR/walcache" 2>/dev/null | wc -l | tr -d ' ')"
  printf 'rows     primary %s, replica %s\n' \
    "$(pri -c 'select count(*) from t;' 2>/dev/null || echo '-')" \
    "$(rep -c 'select count(*) from t;' 2>/dev/null || echo '-')"
  printf 'slot     restart_lsn %s, confirmed_flush_lsn %s, active %s\n' \
    "$(slot_field restart_lsn 2>/dev/null || echo '-')" \
    "$(slot_field confirmed_flush_lsn 2>/dev/null || echo '-')" \
    "$(slot_field active 2>/dev/null || echo '-')"
  printf 'subscriber confirmed %s\n\n' "$(cat "$DIR/state/confirmed.lsn" 2>/dev/null || echo '-')"
}

case "${1:-up}" in
  up)     up ;;
  down)   down; echo "stopped (state kept in $DIR)" ;;
  status) status ;;
  *)      die "usage: $0 [up|down|status]" ;;
esac
