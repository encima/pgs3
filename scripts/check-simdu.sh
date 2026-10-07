#!/usr/bin/env bash
# End-to-end in the SIMDU shape: no slot on the primary, WAL only from the
# archive, a FOR ALL TABLES publication, and the tail of the live segment
# brought out by pgs3's own pg_switch_wal().
#
#   init     -> dictionary + start LSN from a temporary slot, gone at once
#   snapshot -> X; the subscriber already holds everything committed by X
#   serve    -> subscriber origin at X; a transaction straddling X, and writes
#               after the subscriber connects, must all arrive
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[ -n "${PGS3_PG_BIN:-}" ] && PATH="$PGS3_PG_BIN:$PATH"
PGMAJOR=$(initdb --version | awk '{print $3}'); PGMAJOR=${PGMAJOR%%.*}
BASE="${TMPDIR:-/tmp}/pgs3-simdu-$PGMAJOR-$$"
PRI="$BASE/pri"; SUB="$BASE/sub"; PSOCK="$BASE/ps"; SSOCK="$BASE/ss"; ARCH="$BASE/archive"
PPORT=$((${PGS3_CHECK_PORT:-55432} + 200)); SPORT=$((PPORT + 1)); LPORT=$((PPORT + 2))
cleanup() {
  [ -n "${PGS3_PID:-}" ] && kill "$PGS3_PID" 2>/dev/null
  pg_ctl -D "$SUB" stop -m immediate >/dev/null 2>&1
  pg_ctl -D "$PRI" stop -m immediate >/dev/null 2>&1
  [ -n "${KEEP:-}" ] || rm -rf "${BASE:?}"
}
trap cleanup EXIT
p() { psql -X -h "$PSOCK" -p "$PPORT" -U postgres -q -t -A "$@"; }
s() { psql -X -h "$SSOCK" -p "$SPORT" -U postgres -q -t -A "$@"; }
fail() { echo "    FAIL: $*"; sed 's/^/      /' "$BASE/pgs3.log" 2>/dev/null | tail -20; exit 1; }

boot() { # dir sock port extra-conf
  mkdir -p "$1" "$2"; chmod 700 "$1"
  initdb -D "$1" -U postgres --no-sync -E UTF8 -A trust >/dev/null 2>&1
  printf 'wal_level = logical\nport = %s\nlisten_addresses = %s\nunix_socket_directories = %s\nfsync = off\n%s\n' \
    "$3" "''" "'$2'" "$4" >> "$1/postgresql.conf"
  pg_ctl -D "$1" -l "$1/log" start -w >/dev/null
}

echo "### Postgres $PGMAJOR — SIMDU shape"
( cd "$ROOT" && cargo build --quiet )
mkdir -p "$ARCH" "$BASE/wal"
boot "$PRI" "$PSOCK" "$PPORT" "archive_mode = on
archive_command = 'test ! -f $ARCH/%f && cp %p $ARCH/%f'"
boot "$SUB" "$SSOCK" "$SPORT" ""

# t carries the types Supabase's own schemas use: enums (auth), inet, regclass
# and an array of a composite holding an enum (realtime.subscription.filters).
DDL="create table s (id int primary key, name text, g int generated always as (id * 2) stored);
create type aal as enum ('aal1', 'aal2', 'aal3');
create type eq_op as enum ('eq', 'neq', 'lt');
create type filt as (column_name text, op eq_op, value text);
create domain posint as int check (value > 0);
create table t (id int primary key, a aal, ip inet, net cidr, rc regclass, d posint, f filt[]);"
p -c "$DDL create publication ps for all tables;" >/dev/null
s -c "$DDL" >/dev/null
T1="insert into t values (1, 'aal1', '10.0.0.1', '10.0.0.0/8', 's', 5, array[('id','eq','1')::filt]);"
p -c "insert into s values (1,'before init'); $T1" >/dev/null

echo "==> init"
"$ROOT/target/debug/pgs3" init --source "host=$PSOCK port=$PPORT user=postgres dbname=postgres" \
  --catalog "$BASE/catalog.json" --publication ps 2>&1 | sed 's/^/    /'
[ "$(p -c "select count(*) from pg_replication_slots;")" = 0 ] || fail "init left a slot behind"
grep -q '"start_lsn" *: *"[0-9A-F]*/' "$BASE/catalog.json" || fail "no start_lsn in the dictionary"
grep -q '"relname" *: *"s"' "$BASE/catalog.json" || fail "FOR ALL TABLES published nothing"

echo "==> snapshot at X, with a transaction straddling it"
p -c "insert into s values (2,'after init, before X');" >/dev/null
p -c "begin; insert into s values (3,'straddles X'); select pg_sleep(3); commit;" >/dev/null &
STRADDLE=$!
sleep 1
X=$(p -c "select pg_current_wal_lsn();")
s -c "insert into s values (1,'before init'), (2,'after init, before X'); $T1" >/dev/null
wait "$STRADDLE"
echo "    X = $X"

echo "==> subscriber repointed at pgs3 from X"
s -c "create subscription sub
      connection 'host=$PSOCK port=$PPORT user=postgres dbname=postgres'
      publication ps
      with (enabled = false, create_slot = false, slot_name = 'unused', copy_data = false,
            binary = true, streaming = off);" >/dev/null
s -c "select pg_replication_origin_advance('pg_' || oid, '$X') from pg_subscription where subname = 'sub';" >/dev/null
"$ROOT/target/debug/pgs3" serve --wal "$BASE/wal" --catalog "$BASE/catalog.json" \
  --listen "127.0.0.1:$LPORT" --restore-command "cp $ARCH/%f %p" \
  --source "host=$PSOCK port=$PPORT user=postgres dbname=postgres" > "$BASE/pgs3.log" 2>&1 &
PGS3_PID=$!
for _ in $(seq 100); do grep -q 'listening on' "$BASE/pgs3.log" && break; sleep 0.1; done
s -c "alter subscription sub connection 'host=127.0.0.1 port=$LPORT user=postgres dbname=postgres';
      alter subscription sub enable;" >/dev/null
p -c "insert into s values (4,'after the subscriber connected'); update s set name='updated' where id=1;" >/dev/null
p >/dev/null <<'EOF'
insert into t values
  (2, 'aal2', '2001:db8::1/64', '2001:db8::/32', 't', 7,
   array[('name','neq','x')::filt, ('n',null,null)::filt, null]),
  (3, null, null, null, null, null, '{}'),
  (4, 'aal3', '192.168.1.1', '192.168.0.0/16', 's', 1, array[('a','lt','9')::filt]);
update t set a = 'aal3', f = array[('id','lt','2')::filt] where id = 1;
delete from t where id = 4;
EOF

# Nobody switches WAL here but pgs3: rows 3 and 4 sit in the live segment.
Q="select string_agg(id||':'||name||':'||g, ',' order by id) from s;"
for _ in $(seq 900); do [ "$(s -c "$Q")" = "$(p -c "$Q")" ] && break; sleep 0.1; done
[ "$(s -c "$Q")" = "$(p -c "$Q")" ] || fail "subscriber has '$(s -c "$Q")', primary '$(p -c "$Q")'"
echo "    OK: $(s -c "$Q")"
# regclass travels as an Oid, which only pg_upgrade keeps stable across
# clusters; compare the Oid, not the name.
Q="select string_agg(concat_ws(':', id, a, ip, net, rc::oid, d, f), ',' order by id) from t;"
for _ in $(seq 300); do [ "$(s -c "$Q")" = "$(p -c "$Q")" ] && break; sleep 0.1; done
[ "$(s -c "$Q")" = "$(p -c "$Q")" ] || fail "t: subscriber has '$(s -c "$Q")', primary '$(p -c "$Q")'"
echo "    OK: $(s -c "$Q")"
[ -z "$(ls "$BASE/wal")" ] || fail "fetched segments left in the cache: $(ls "$BASE/wal")"
echo "    OK: segment cache empty"
