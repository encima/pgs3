#!/usr/bin/env bash
# End-to-end against a real Postgres subscriber.
#
# Mirrors the production shape: a subscription is established against the
# primary, goes away while the replica is "upgraded", and is then repointed at
# pgs3 to catch up from archived WAL. The primary is never read during catch-up.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[ -n "${PGS3_PG_BIN:-}" ] && PATH="$PGS3_PG_BIN:$PATH"
PGVER=$(initdb --version | awk '{print $3}'); PGMAJOR=${PGVER%%.*}
BASE="${TMPDIR:-/tmp}/pgs3-sub-$PGMAJOR-$$"
PRI="$BASE/pri"; SUB="$BASE/sub"; PSOCK="$BASE/ps"; SSOCK="$BASE/ss"
PPORT=$((${PGS3_CHECK_PORT:-55432} + 100)); SPORT=$((PPORT + 1)); LPORT=$((PPORT + 2))
cleanup() {
  [ -n "${PGS3_PID:-}" ] && kill "$PGS3_PID" 2>/dev/null
  pg_ctl -D "$SUB" stop -m immediate >/dev/null 2>&1
  pg_ctl -D "$PRI" stop -m immediate >/dev/null 2>&1
  rm -rf "$BASE"
}
trap cleanup EXIT
p() { psql -X -h "$PSOCK" -p "$PPORT" -U postgres -q -t -A "$@"; }
s() { psql -X -h "$SSOCK" -p "$SPORT" -U postgres -q -t -A "$@"; }
fail() { echo "    FAIL: $*"; exit 1; }

boot() { # dir sock port
  mkdir -p "$1" "$2"; chmod 700 "$1"
  initdb -D "$1" -U postgres --no-sync -E UTF8 -A trust >/dev/null 2>&1
  cat >> "$1/postgresql.conf" <<EOF
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8
max_logical_replication_workers = 8
port = $3
listen_addresses = ''
unix_socket_directories = '$2'
fsync = off
autovacuum = off
wal_compression = $([ "$PGMAJOR" -ge 15 ] && echo pglz || echo on)
EOF
  pg_ctl -D "$1" -l "$1/log" start -w >/dev/null
}

# pgoutput's protocol version ceiling moves with the server: 2 in PG14
# (streaming), 3 in PG15 (two-phase), 4 in PG16+ (parallel apply). The message
# formats this encoder produces are unchanged across all of them.
if   [ "$PGMAJOR" -ge 16 ]; then PROTO=4
elif [ "$PGMAJOR" -ge 15 ]; then PROTO=3
else                             PROTO=2
fi

echo "### Postgres $PGVER — real subscriber (pgoutput proto v$PROTO)"
( cd "$ROOT" && cargo build --quiet )
mkdir -p "$BASE"
boot "$PRI" "$PSOCK" "$PPORT"
boot "$SUB" "$SSOCK" "$SPORT"

DDL="create table s (
  id int primary key, name text, big text, amount numeric(12,2),
  flag bool, uid uuid, weight float8, blob bytea, ts timestamptz,
  doc jsonb, tags text[], nums int4[], stamps timestamptz[], grid int4[]);"
p -c "$DDL alter table s replica identity full; create publication ps for table s;" >/dev/null
s -c "$DDL" >/dev/null

echo "==> subscription established against the primary (T0)"
s -c "create subscription sub
      connection 'host=$PSOCK port=$PPORT user=postgres dbname=postgres'
      publication ps
      with (binary = true, streaming = off, copy_data = false, slot_name = 'sub');" >/dev/null
# Prove the subscription is live before treating its origin as T0.
p -c "insert into s values (1,'t0',null,1.00,true,null,1.0,null,now());" >/dev/null
for _ in $(seq 100); do
  [ "$(s -c 'select count(*) from s;')" = "1" ] && break; sleep 0.1
done
[ "$(s -c 'select count(*) from s;')" = "1" ] || fail "subscription never came up against the primary"

echo "==> replica goes away for its upgrade"
s -c "alter subscription sub disable;" >/dev/null
for _ in $(seq 100); do
  [ "$(p -c "select count(*) from pg_stat_replication where application_name='sub';")" = "0" ] && break
  sleep 0.1
done
# restart_lsn, not confirmed_flush_lsn: decoding has to start far enough back
# to catch transactions that straddle the confirmed point.
T0=$(p -c "select restart_lsn from pg_replication_slots where slot_name='sub';")
CONF0=$(s -c "select remote_lsn from pg_replication_origin_status;")
[ -n "$T0" ] || fail "no restart_lsn on the primary's slot"
[ -n "$CONF0" ] || fail "subscriber has no replication origin to resume from"
echo "    restart_lsn = $T0, subscriber origin = $CONF0"

# A pgoutput slot over the same window gives byte-exact expected output.
p -c "select pg_create_logical_replication_slot('po','pgoutput');" >/dev/null
POSTART=$(p -c "select restart_lsn from pg_replication_slots where slot_name='po';")

echo "==> primary keeps writing while the replica is down"
p >/dev/null <<'EOF'
insert into s values (2,'alice',null,10.50,true,'11111111-1111-1111-1111-111111111111',2.5,'\xdeadbeef','2026-01-01 00:00:00+00',
  '{"a":1,"b":[1,2.5,null,true,"s"],"c":{"d":"e"}}',
  array['x','yy','zzz'], array[1,2,3],
  array['2026-01-01 00:00:00+00'::timestamptz,'2026-02-02 02:02:02+00'], '{{1,2},{3,4}}');
insert into s values (3,'bob',null,-3.25,false,'22222222-2222-2222-2222-222222222222',-0.5,null,'2026-06-15 12:34:56+00',
  '[]'::jsonb, array['a',null,'c'], array[null,7]::int4[], '{}'::timestamptz[], null);
-- incompressible: goes out to the TOAST relation in chunks
insert into s values (4,'carol',(select string_agg(md5(random()::text||g),'') from generate_series(1,300) g),0.00,null,null,0,null,null,
  '"just a string"'::jsonb, array[]::text[], null, null, null);
-- compressible: pglz'd inline
insert into s values (5,'dave',repeat('abcdefgh',2000),1.00,true,null,1e10,null,null,
  '12345.678'::jsonb, null, null, null, null);
update s set name='alice2', amount=99.99 where id=2;
update s set id=30 where id=3;
delete from s where id=5;
begin; insert into s values (99,'ghost',null,0,null,null,0,null,null,null,null,null,null,null); rollback;
copy s (id,name,amount) from stdin with (format csv);
6,eve,5.55
7,frank,6.66
\.
EOF
p -c "select pg_switch_wal();" >/dev/null
END=$(p -c 'select pg_current_wal_lsn();')
p -v pubname=ps -f "$ROOT/sql/dump_catalog.sql" > "$BASE/catalog.json"

echo "==> pgoutput bytes vs pgoutput itself"
# The strongest check on the encoder: Postgres' own pgoutput plugin decoding the
# same WAL, compared message for message as raw bytes.
p -c "select encode(data,'hex') from pg_logical_slot_peek_binary_changes(
        'po', null, null,
        'proto_version','$PROTO','publication_names','ps','binary','true');" > "$BASE/po_expected.hex"
"$ROOT/target/debug/pgs3" pgoutput --wal "$PRI/pg_wal" --catalog "$BASE/catalog.json" \
  --start "$POSTART" --end "$END" > "$BASE/po_mine.hex"
if ! diff -q "$BASE/po_expected.hex" "$BASE/po_mine.hex" >/dev/null; then
  python3 "$ROOT/tests/hexdiff.py" "$BASE/po_expected.hex" "$BASE/po_mine.hex" | sed 's/^/    /'
  fail "pgoutput bytes differ from Postgres"
fi
echo "    OK: $(wc -l < "$BASE/po_mine.hex" | tr -d ' ') pgoutput messages byte-identical to Postgres"

echo "==> pgs3 serves the archived WAL"
"$ROOT/target/debug/pgs3" serve \
  --wal "$PRI/pg_wal" --catalog "$BASE/catalog.json" \
  --start "$T0" --end "$END" \
  --listen "127.0.0.1:$LPORT" \
  --confirmed-lsn-file "$BASE/confirmed.lsn" \
  --exit-when-caught-up > "$BASE/pgs3.log" 2>&1 &
PGS3_PID=$!
for _ in $(seq 100); do grep -q 'listening on' "$BASE/pgs3.log" && break; sleep 0.1; done

# Stop the primary outright: nothing may read from it during catch-up.
pg_ctl -D "$PRI" stop -m fast >/dev/null 2>&1
echo "    primary stopped — catch-up must come from WAL alone"

s -c "alter subscription sub connection 'host=127.0.0.1 port=$LPORT user=postgres dbname=pgs3';" >/dev/null
s -c "alter subscription sub enable;" >/dev/null

for _ in $(seq 300); do kill -0 "$PGS3_PID" 2>/dev/null || break; sleep 0.1; done
if kill -0 "$PGS3_PID" 2>/dev/null; then
  kill "$PGS3_PID" 2>/dev/null; sed 's/^/      /' "$BASE/pgs3.log"; fail "pgs3 never caught up"
fi
wait "$PGS3_PID" 2>/dev/null || true
PGS3_PID=""
sed 's/^/    /' "$BASE/pgs3.log"

echo "==> verify"
s -c "alter subscription sub disable;" >/dev/null
pg_ctl -D "$PRI" -l "$PRI/log" start -w >/dev/null
EXP=$(p -c "select id||'|'||coalesce(name,'')||'|'||coalesce(md5(big),'')||'|'||coalesce(amount::text,'')||'|'||coalesce(flag::text,'')||'|'||coalesce(uid::text,'')||'|'||coalesce(weight::text,'')||'|'||coalesce(encode(blob,'hex'),'')||'|'||coalesce(ts::text,'')||'|'||coalesce(doc::text,'')||'|'||coalesce(tags::text,'')||'|'||coalesce(nums::text,'')||'|'||coalesce(stamps::text,'')||'|'||coalesce(grid::text,'') from s order by id;")
GOT=$(s -c "select id||'|'||coalesce(name,'')||'|'||coalesce(md5(big),'')||'|'||coalesce(amount::text,'')||'|'||coalesce(flag::text,'')||'|'||coalesce(uid::text,'')||'|'||coalesce(weight::text,'')||'|'||coalesce(encode(blob,'hex'),'')||'|'||coalesce(ts::text,'')||'|'||coalesce(doc::text,'')||'|'||coalesce(tags::text,'')||'|'||coalesce(nums::text,'')||'|'||coalesce(stamps::text,'')||'|'||coalesce(grid::text,'') from s order by id;")
if [ "$EXP" != "$GOT" ]; then
  echo "--- primary ---"; echo "$EXP"; echo "--- subscriber ---"; echo "$GOT"
  echo "--- subscriber log ---"; grep -iE 'error|fatal|warning' "$SUB/log" | tail -10
  fail "subscriber contents differ from the primary"
fi
echo "    OK: $(echo "$EXP" | wc -l | tr -d ' ') rows identical on both sides"
CONF=$(cat "$BASE/confirmed.lsn" 2>/dev/null || echo "")
[ -n "$CONF" ] || fail "no confirmed-LSN file was published for the slot advancer"
echo "    OK: published confirmed flush LSN $CONF for the advancer"
