#!/usr/bin/env bash
# End-to-end check against a real Postgres: build a throwaway cluster, run a
# workload that exercises every decode path, then verify pgs3 produces the same
# logical changes as Postgres' own logical decoding of the same WAL.
#
# Set PGS3_PG_BIN to a Postgres bin directory to check a specific major.
# scripts/check-all.sh runs this across every major in the nix store.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[ -n "${PGS3_PG_BIN:-}" ] && PATH="$PGS3_PG_BIN:$PATH"
PGVER=$(initdb --version | awk '{print $3}')
PGMAJOR=${PGVER%%.*}
PORT="${PGS3_CHECK_PORT:-55432}"
PGD="${TMPDIR:-/tmp}/pgs3-check-$PGMAJOR-$$"
SOCK="$PGD-sock"
cleanup() { pg_ctl -D "$PGD" stop -m immediate >/dev/null 2>&1 || true; rm -rf "$PGD" "$SOCK"; }
trap cleanup EXIT
psql_() { psql -X -h "$SOCK" -p "$PORT" -U postgres -q -t -A "$@"; }
fail() { echo "    FAIL: $*"; exit 1; }

echo "### Postgres $PGVER"

echo "==> unit tests"
( cd "$ROOT" && cargo test --quiet 2>&1 | grep -E 'test result|FAILED' )
( cd "$ROOT" && cargo build --quiet )

# wal_compression became an enum of codecs in PG15; before that it is a boolean.
# That is the same release that moved the BKPIMAGE_* bits, which is exactly what
# this setting exists to exercise.
if [ "$PGMAJOR" -ge 15 ]; then WALCOMP=pglz; else WALCOMP=on; fi
# pgoutput's protocol ceiling: 2 in PG14, 3 in PG15, 4 in PG16+.
if   [ "$PGMAJOR" -ge 16 ]; then PROTO=4
elif [ "$PGMAJOR" -ge 15 ]; then PROTO=3
else                             PROTO=2
fi

echo "==> initdb (wal_compression=$WALCOMP)"
mkdir -p "$PGD" "$SOCK"; chmod 700 "$PGD"
initdb -D "$PGD" -U postgres --no-sync -E UTF8 -A trust >/dev/null 2>&1
cat >> "$PGD/postgresql.conf" <<EOF
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8
port = $PORT
listen_addresses = ''
unix_socket_directories = '$SOCK'
fsync = off
autovacuum = off
full_page_writes = on
wal_compression = $WALCOMP
EOF
pg_ctl -D "$PGD" -l "$PGD/log" start -w >/dev/null

echo "==> workload"
psql_ -c "select pg_create_logical_replication_slot('ref','test_decoding');" >/dev/null
psql_ -c "select pg_switch_wal();" >/dev/null
psql_ >/dev/null <<'EOF'
create table t (
  id int primary key, name text, big text, amount numeric(12,2),
  flag bool, uid uuid, doc jsonb, ts timestamptz,
  -- arrays: 4-byte and 8-byte aligned elements, varlena elements, and a
  -- multi-dimensional one. Nulls inside an array exercise the null bitmap,
  -- which also shifts where the data area starts.
  tags text[], nums int4[], stamps timestamptz[], grid int4[]);
alter table t replica identity full;
create publication p for table t;
-- Rows that exist before the checkpoint, so updating them later forces
-- full-page images on a *published* relation.
insert into t select g, 'row'||g, null, g, false, null, null, null,
       null, null, null, null from generate_series(100,300) g;
EOF
psql_ -c "checkpoint;" >/dev/null      # pages are now clean; next touch emits an FPI
# A pgoutput slot created here gives byte-exact expected output for the same
# window pgs3 decodes.
psql_ -c "select pg_create_logical_replication_slot('po','pgoutput');" >/dev/null
POSTART=$(psql_ -c "select restart_lsn from pg_replication_slots where slot_name='po';")
START=$(psql_ -c 'select pg_current_wal_lsn();')
psql_ >/dev/null <<'EOF'
insert into t values (1,'alice',null,10.50,true,'11111111-1111-1111-1111-111111111111','{"a":1}','2026-01-01 00:00:00+00',
  array['x','yy','zzz'], array[1,2,3],
  array['2026-01-01 00:00:00+00'::timestamptz,'2026-02-02 02:02:02+00'],
  '{{1,2},{3,4}}');
insert into t values (2,'bob',null,-3.25,false,'22222222-2222-2222-2222-222222222222','{"b":[1,2]}','2026-06-15 12:34:56+00',
  array['a',null,'c'], array[null,7]::int4[], '{}'::timestamptz[], null);
-- incompressible: forced out to the TOAST relation as chunks
insert into t values (3,'carol',(select string_agg(md5(random()::text||g),'') from generate_series(1,300) g),0.00,null,null,
  '{"nested":{"k":[1,2.5,null,true,"s"]}}','2026-03-03 03:03:03+00',
  array[]::text[], array[-2147483648,2147483647], null, null);
-- compressible: pglz'd and kept inline
insert into t values (4,'dave',repeat('abcdefgh',2000),1.00,true,null,null,null,null,null,null,null);
update t set name='alice2', amount=99.99 where id=1;   -- HOT_UPDATE
update t set id=20 where id=2;                          -- key update
delete from t where id=4;
begin; insert into t values (99,'ghost',null,0,null,null,null,null,null,null,null,null); rollback;
copy t (id,name,amount) from stdin with (format csv);
5,eve,5.55
6,frank,6.66
\.
-- First touch of the pre-checkpoint pages: these records carry a compressed
-- full-page image *and* tuple data, so a mis-sized image header corrupts the row.
update t set name = name || '!' where id between 100 and 300;
-- a rolled-back savepoint: its insert must not be replicated, while the
-- surrounding transaction's inserts must be (PG14+ tags the subxact's first
-- record with the top-level xid, which must not be taken as attribution).
begin;
insert into t values (7,'gina',null,7.77,true,null,null,null,null,null,null,null);
savepoint s1;
insert into t values (8,'ghost-sub',null,8.88,false,null,null,null,null,null,null,null);
rollback to savepoint s1;
insert into t values (9,'survivor',null,9.99,true,null,null,null,null,null,null,null);
commit;
EOF
psql_ -c "select pg_switch_wal();" >/dev/null
END=$(psql_ -c 'select pg_current_wal_lsn();')

psql_ -c "select lsn||'|'||xid||'|'||data from pg_logical_slot_peek_changes('ref',null,null);" > "$PGD/expected.txt"
psql_ -v pubname=p -f "$ROOT/sql/dump_catalog.sql" > "$PGD/catalog.json"
CAT="--catalog $PGD/catalog.json"

echo "==> full-page images"
FPW=$(pg_waldump -p "$PGD/pg_wal" -s "$START" -e "$END" 2>/dev/null | grep -c 'FPW' || true)
[ "$FPW" -gt 0 ] || fail "no full-page images; the block-image header path, the one thing
          that differs across majors, would go untested"
echo "    OK: $FPW records carry a compressed full-page image"

echo "==> framing vs pg_waldump"
"$ROOT/target/debug/pgs3" frame --wal "$PGD/pg_wal" $CAT --start "$START" --end "$END" \
  | awk '{gsub(/[a-z_]+:/,"",$0); print $1"|"$2"|"$4"|"$5}' > "$PGD/mine.csv"
pg_waldump -p "$PGD/pg_wal" -s "$START" -e "$END" 2>/dev/null \
  | python3 "$ROOT/tests/waldump_canon.py" > "$PGD/theirs.csv"
diff -q "$PGD/mine.csv" "$PGD/theirs.csv" >/dev/null \
  || { diff "$PGD/mine.csv" "$PGD/theirs.csv" | head -20; fail "framing diverges from pg_waldump"; }
echo "    OK: $(wc -l < "$PGD/mine.csv" | tr -d ' ') records match on lsn/rmgr/xid/xl_tot_len"

echo "==> logical decode vs test_decoding"
"$ROOT/target/debug/pgs3" decode --wal "$PGD/pg_wal" $CAT --start "$START" --end "$END" \
  > "$PGD/decoded.txt"
python3 "$ROOT/tests/compare.py" "$PGD/expected.txt" "$PGD/decoded.txt" | sed 's/^/    /'

echo "==> pgoutput bytes vs pgoutput itself"
# The strongest check on the encoder: Postgres' own pgoutput plugin decoding the
# same WAL, compared message for message as raw bytes.
psql_ -c "select encode(data,'hex') from pg_logical_slot_peek_binary_changes(
            'po', null, null,
            'proto_version','$PROTO','publication_names','p','binary','true');" > "$PGD/po_expected.hex"
"$ROOT/target/debug/pgs3" pgoutput --wal "$PGD/pg_wal" $CAT \
  --start "$POSTART" --end "$END" > "$PGD/po_mine.hex"
if ! diff -q "$PGD/po_expected.hex" "$PGD/po_mine.hex" >/dev/null; then
  python3 "$ROOT/tests/hexdiff.py" "$PGD/po_expected.hex" "$PGD/po_mine.hex" | sed 's/^/    /'
  fail "pgoutput bytes differ from Postgres"
fi
echo "    OK: $(wc -l < "$PGD/po_mine.hex" | tr -d ' ') pgoutput messages byte-identical to Postgres"

echo "==> unencodable type guard"
# Geometric types have no on-disk-to-send shortcut, so the encoder refuses the
# whole publication up front rather than emitting a wrong datum mid-stream.
psql_ -c "create table g(id int primary key, where_at point);
          create publication pg_ for table g;" >/dev/null
psql_ -v pubname=pg_ -f "$ROOT/sql/dump_catalog.sql" > "$PGD/catalog_g.json"
set +e
OUT=$("$ROOT/target/debug/pgs3" pgoutput --wal "$PGD/pg_wal" --catalog "$PGD/catalog_g.json" \
      --start "$POSTART" --end "$END" 2>&1); RC=$?
set -e
[ $RC -ne 0 ] || fail "encoded a point column instead of refusing"
grep -q "public.g.where_at is point" <<<"$OUT" || fail "refused, but not on the type allowlist"
echo "    OK: refused to encode point rather than guess"

echo "==> wrong-archive guard"
# The dictionary records the cluster's system_identifier; a segment from another
# cluster must be rejected rather than silently decoded against it.
# json_build_object writes `"key" : "value"`, with spaces around the colon.
sed -E 's/"system_identifier" *: *"[0-9]+"/"system_identifier" : "1234567890123456789"/' \
  "$PGD/catalog.json" > "$PGD/catalog_wrong.json"
cmp -s "$PGD/catalog.json" "$PGD/catalog_wrong.json" \
  && fail "test bug: could not rewrite system_identifier in the dictionary"
if "$ROOT/target/debug/pgs3" decode --wal "$PGD/pg_wal" --catalog "$PGD/catalog_wrong.json" \
     --start "$START" --end "$END" >/dev/null 2>"$PGD/err.txt"; then
  fail "decoded WAL from a different cluster than the dictionary"
fi
grep -q "Wrong archive for this dictionary" "$PGD/err.txt" \
  || { cat "$PGD/err.txt"; fail "rejected, but not on the system_identifier check"; }
echo "    OK: refused a dictionary from another cluster"

echo "==> routine maintenance does not trip the DDL guard"
# ANALYZE and VACUUM write statistics and touch pg_class, and their commits
# carry catalog invalidations -- but they change no rowtype. An autovacuum in
# the decode window must not stop replication.
# Separate -c calls: psql wraps multiple statements in one transaction, and
# VACUUM cannot run inside one.
psql_ -c "analyze t;" >/dev/null
psql_ -c "vacuum t;" >/dev/null
psql_ -c "create table unrelated(x int);" >/dev/null
psql_ -c "analyze unrelated;" >/dev/null
psql_ -c "drop table unrelated;" >/dev/null
psql_ -c "select pg_switch_wal();" >/dev/null
MEND=$(psql_ -c 'select pg_current_wal_lsn();')
"$ROOT/target/debug/pgs3" decode --wal "$PGD/pg_wal" $CAT --start "$START" --end "$MEND" >/dev/null \
  || fail "refused to decode across ANALYZE/VACUUM and unrelated DDL"
echo "    OK: decoded across ANALYZE, VACUUM and DDL on an unpublished table"

echo "==> DDL guard"
# A schema change after T0 makes the frozen dictionary wrong. Decoding the rows
# that follow it would silently misread them, so the decoder must stop instead.
psql_ -c "create table d(id int primary key, a text);
          alter table d replica identity full;
          create publication pd for table d;" >/dev/null
psql_ -v pubname=pd -f "$ROOT/sql/dump_catalog.sql" > "$PGD/catalog_d.json"
DSTART=$(psql_ -c 'select pg_current_wal_lsn();')
psql_ -c "insert into d values (1,'before');" >/dev/null
psql_ -c "alter table d add column b int;" >/dev/null
psql_ -c "insert into d values (2,'after',42);" >/dev/null
psql_ -c "select pg_switch_wal();" >/dev/null
DEND=$(psql_ -c 'select pg_current_wal_lsn();')
set +e
OUT=$("$ROOT/target/debug/pgs3" decode --wal "$PGD/pg_wal" --catalog "$PGD/catalog_d.json" \
      --start "$DSTART" --end "$DEND" 2>&1); RC=$?
set -e
[ $RC -ne 0 ]                          || fail "decoded across a post-T0 ALTER TABLE"
grep -q "published relation public.d" <<<"$OUT" \
  || { echo "$OUT" | tail -3 | sed "s/^/      /"; fail "stopped, but not on the rowtype-catalog guard"; }
grep -q "id\[int4\]:1"          <<<"$OUT" || fail "did not emit the change preceding the DDL"
! grep -q "'after'"             <<<"$OUT" || fail "decoded a row written after the schema changed"
echo "    OK: emitted the pre-DDL change, then refused at the ALTER's commit"

echo "==> truncate of a published table is refused"
# TRUNCATE assigns a new relfilenode that the frozen dictionary cannot follow,
# so the decoder must stop rather than silently drop the rows written after.
psql_ -c "create table tr(id int primary key); create publication ptr for table tr;" >/dev/null
psql_ -v pubname=ptr -f "$ROOT/sql/dump_catalog.sql" > "$PGD/catalog_tr.json"
TSTART=$(psql_ -c 'select pg_current_wal_lsn();')
psql_ -c "insert into tr values (1); truncate tr;" >/dev/null
psql_ -c "select pg_switch_wal();" >/dev/null
TEND=$(psql_ -c 'select pg_current_wal_lsn();')
set +e
OUT=$("$ROOT/target/debug/pgs3" decode --wal "$PGD/pg_wal" --catalog "$PGD/catalog_tr.json" \
      --start "$TSTART" --end "$TEND" 2>&1); RC=$?
set -e
[ $RC -ne 0 ]                 || fail "decoded across a TRUNCATE of a published table"
grep -q "public.tr" <<<"$OUT" || { echo "$OUT" | tail -3 | sed 's/^/      /'; fail "stopped, but not on a published-relation guard"; }
echo "    OK: refused to follow a TRUNCATE"
