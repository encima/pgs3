#!/usr/bin/env bash
# Walks the upgrade shape end to end and checks it actually happened:
#
#   1. replica subscribes to the primary            -> establishes T0
#   2. replica goes away                            -> the "upgrade window"
#   3. primary keeps writing, WAL is archived
#   4. replica is repointed at pgs3 and catches up  -> from the archive alone
#   5. the primary's slot is advanced               -> it retains almost nothing
#   6. new writes keep flowing through pgs3         -> it follows the archive
set -euo pipefail

pri() { psql -X -q -t -A -h primary -U postgres -d postgres "$@"; }
rep() { psql -X -q -t -A -h replica -U postgres -d postgres "$@"; }
say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()  { printf '   \033[32mok\033[0m  %s\n' "$*"; }
die() { printf '   \033[31mFAIL\033[0m %s\n' "$*"; exit 1; }

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

say "1. schema and publication"
pri -c "$SCHEMA alter table t replica identity full; create publication p for table t;" >/dev/null
rep -c "$SCHEMA" >/dev/null
ok "table t on both, published from the primary"

say "2. replica subscribes to the primary (establishes T0)"
rep -c "create subscription sub
        connection 'host=primary user=postgres dbname=postgres'
        publication p
        with (binary = true, streaming = off, copy_data = false, slot_name = 'sub');" >/dev/null
pri -c "insert into t (id, name, ts) values (1, 'before-the-window', now());" >/dev/null
retry 30 '[ "$(rep -c "select count(*) from t;")" = "1" ]' \
  || die "subscription never came up against the primary"
ok "streaming from the primary; 1 row replicated"

say "3. the replica goes away for its upgrade"
rep -c "alter subscription sub disable;" >/dev/null
retry 30 '[ "$(pri -c "select count(*) from pg_stat_replication;")" = "0" ]' \
  || die "the primary still shows a replication connection"
# restart_lsn, not confirmed_flush_lsn: a transaction can begin before the
# confirmed point and commit after it, so decoding has to start further back.
START=$(pri -c "select restart_lsn from pg_replication_slots where slot_name='sub';")
echo "$START" > /state/start.lsn
ok "disconnected; slot restart_lsn = $START"

say "4. the primary keeps writing while the replica is down"
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
ok "$(pri -c 'select count(*) from t;') rows on the primary, WAL rolled to the archive"

say "5. hand pgs3 the frozen catalog dictionary"
pri -v pubname=p -f /sql/dump_catalog.sql > /state/catalog.json
retry 60 '[ -n "$(ls -A /archive)" ]' || die "nothing reached the archive"
ok "$(ls /archive | wc -l | tr -d ' ') segments archived, dictionary captured"
touch /state/ready
# In a subshell so the socket is closed again: pgs3 serves one connection at a
# time, and a probe left open would hold the listener shut against the replica.
retry 60 '(exec 3<>/dev/tcp/pgs3/5439)' || die "pgs3 never started listening"
ok "pgs3 is serving the archive on pgs3:5439"

say "6. repoint the replica at pgs3"
rep -c "alter subscription sub connection 'host=pgs3 port=5439 user=postgres dbname=pgs3';" >/dev/null
rep -c "alter subscription sub enable;" >/dev/null
EXPECTED=$(pri -c 'select count(*) from t;')
retry 90 '[ "$(rep -c "select count(*) from t;")" = "'"$EXPECTED"'" ]' \
  || die "replica stalled at $(rep -c 'select count(*) from t;') of $EXPECTED rows"
ok "caught up: $EXPECTED rows"

say "7. did the primary serve any of that?"
# The slot has sat inactive the whole time and no walsender ever attached: the
# catch-up came from archived WAL. pgs3 is not even on the primary's network.
ACTIVE=$(pri -c "select active from pg_replication_slots where slot_name='sub';")
CONNS=$(pri -c "select count(*) from pg_stat_replication;")
[ "$ACTIVE" = "f" ] || die "the primary's slot went active during catch-up"
[ "$CONNS" = "0" ]  || die "the primary served $CONNS replication connections"
ok "slot still inactive, zero replication connections on the primary"

say "8. compare the two databases"
Q="select id||'|'||coalesce(name,'')||'|'||coalesce(md5(big),'')||'|'||coalesce(amount::text,'')
   ||'|'||coalesce(flag::text,'')||'|'||coalesce(uid::text,'')||'|'||coalesce(doc::text,'')
   ||'|'||coalesce(tags::text,'')||'|'||coalesce(stamps::text,'')||'|'||coalesce(ts::text,'')
   from t order by id;"
if [ "$(pri -c "$Q")" != "$(rep -c "$Q")" ]; then
  echo "--- primary ---";    pri -c "$Q"
  echo "--- replica ---";    rep -c "$Q"
  die "contents differ"
fi
ok "every row identical, including TOAST, jsonb, arrays and numeric"

say "9. advance the primary's slot to what the subscriber confirmed"
# The LSN the advancer uses comes only from the subscriber's own standby status
# updates, by way of the file pgs3 writes. That is the safety argument: it
# cannot overtake what has actually been applied, so a cutover can never ask
# the primary for WAL behind its slot.
slot_field() { pri -c "select $1 from pg_replication_slots where slot_name='sub';"; }
RESTART_BEFORE=$(slot_field restart_lsn)
/scripts/advance-slot.sh "host=primary user=postgres dbname=postgres" sub /state/confirmed.lsn 2 \
  > /tmp/advance.log 2>&1 &
ADVANCER=$!
trap 'kill $ADVANCER 2>/dev/null || true' EXIT

CONFIRMED=$(cat /state/confirmed.lsn)
retry 60 '[ "$(slot_field confirmed_flush_lsn)" = "'"$CONFIRMED"'" ]' \
  || die "slot confirmed_flush_lsn is $(slot_field confirmed_flush_lsn), subscriber confirmed $CONFIRMED"
AHEAD=$(pri -c "select confirmed_flush_lsn > '$CONFIRMED'::pg_lsn
                from pg_replication_slots where slot_name='sub';")
[ "$AHEAD" = "f" ] || die "the slot was advanced past what the subscriber confirmed"
ok "confirmed_flush_lsn = $CONFIRMED, exactly what the subscriber reported, never beyond"
ok "restart_lsn still $RESTART_BEFORE; it follows at the next checkpoint"

say "10. new writes keep flowing, and the primary finally releases WAL"
# restart_lsn only moves once decoding passes a running-xacts record, which a
# checkpoint emits, AND the slot is then confirmed beyond it. So: checkpoint
# first, then write, so the next advance decodes across it.
pri -c "checkpoint;" >/dev/null
pri -c "insert into t (id,name,ts) values (100,'after-catch-up',now());" >/dev/null
pri -c "select pg_switch_wal();" >/dev/null
retry 60 '[ "$(rep -c "select count(*) from t where id=100;")" = "1" ]' \
  || die "pgs3 did not pick up the newly archived segment"
ok "pgs3 followed the archive; the row arrived without the primary being read"

retry 90 '[ "$(slot_field restart_lsn)" != "'"$RESTART_BEFORE"'" ]' \
  || die "restart_lsn never moved past $RESTART_BEFORE after a checkpoint"
RESTART_AFTER=$(slot_field restart_lsn)
FREED=$(pri -c "select pg_size_pretty(pg_wal_lsn_diff('$RESTART_AFTER'::pg_lsn, '$RESTART_BEFORE'::pg_lsn));")
sed 's/^/   /' /tmp/advance.log | tail -1
ok "restart_lsn $RESTART_BEFORE -> $RESTART_AFTER, releasing $FREED of WAL"

printf '\n\033[1;32mAll steps passed.\033[0m The replica caught up and stayed current\n'
printf 'entirely from archived WAL, with the primary never serving replication.\n\n'
