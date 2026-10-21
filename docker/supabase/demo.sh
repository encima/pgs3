#!/usr/bin/env bash
# A subscriber outlives its replication slot, and pgs3 brings it back from the
# wal-g archive:
#
#   1. T0: pgs3 init captures the catalog dictionary from the primary
#   2. the replica subscribes to the primary the ordinary way, with a slot
#   3. the replica goes offline
#   4. the primary takes a large workload: bulk inserts, TOAST, wide updates,
#      deletes -- far more WAL than max_slot_wal_keep_size
#   5. Postgres invalidates the slot and recycles the WAL; it cannot serve it
#   6. the replica is repointed at pgs3, which decodes the wal-g archive
#   7. the two databases are compared
#   8. new writes keep flowing; the dead slot is dropped
#   9. schema changes on the primary while writes keep running: pgs3 init
#      installed an event trigger that writes a new dictionary into WAL with
#      each change to a published table; pgs3 switches to it at exactly that
#      point and applies the change to the replica (its `target`) itself
set -euo pipefail

SCALE=${SCALE:-1}
PRI="host=primary user=supabase_admin dbname=postgres"
pri() { psql -X -q -t -A -v ON_ERROR_STOP=1 -d "$PRI" "$@"; }
rep() { psql -X -q -t -A -v ON_ERROR_STOP=1 -h replica -d postgres "$@"; }
walg() { wal-g --config /etc/wal-g/config.json "$@" 2>/dev/null; }
say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()  { printf '   \033[32mok\033[0m  %s\n' "$*"; }
info() { printf '        %s\n' "$*"; }
die() { printf '   \033[31mFAIL\033[0m %s\n' "$*"; exit 1; }

# Re-evaluates the expression each round, so command substitution inside it is
# re-run rather than frozen at call time.
retry() { # seconds  shell-expression
  local deadline=$(( SECONDS + $1 )); shift
  while [ "$SECONDS" -lt "$deadline" ]; do
    if eval "$*" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  return 1
}

slot() { pri -c "select $1 from pg_replication_slots where slot_name = 'sub';"; }
archived() { walg st ls wal_005/ | grep -c '^obj' || true; }
pretty() { pri -c "select pg_size_pretty($1::numeric);"; }

# Order-independent digest of every row's text form: cheap on millions of rows.
digest() { # psql-fn table
  "$1" -c "select count(*) || ' rows, ' || coalesce(sum(hashtextextended(t::text, 0)), 0) from $2 t;"
}

SCHEMA="
create table accounts (
  id         bigint primary key,
  owner      text not null,
  balance    numeric(14,2) not null,
  active     bool not null default true,
  tags       text[],
  updated_at timestamptz not null default now());
create table events (
  id         bigint primary key,
  account_id bigint not null,
  kind       text not null,
  payload    jsonb,
  ref        uuid,
  created_at timestamptz not null);
create table documents (
  id         bigint primary key,
  title      text not null,
  version    int not null default 1,
  body       text not null);"

rm -f /state/ready /state/confirmed.lsn /state/status.json /state/catalog*.json

say "1. schema on both, publication on the primary"
pri -c "$SCHEMA create publication p for table accounts, events, documents;" >/dev/null
rep -c "$SCHEMA" >/dev/null
ok "accounts, events, documents (supabase/postgres $(pri -c 'show server_version;'))"
ok "primary: max_slot_wal_keep_size = $(pri -c 'show max_slot_wal_keep_size;'), archive_command = wal-g wal-push"

say "2. T0: pgs3 captures the catalog dictionary"
pgs3 init --source "$PRI" --catalog /state/catalog.json --publication p 2>&1 | sed 's/^/        /'
T0=$(sed -nE 's/.*"start_lsn" *: *"([^"]+)".*/\1/p' /state/catalog.json)
ok "start LSN $T0; no slot left behind on the primary"
ok "event trigger $(pri -c "select evtname from pg_event_trigger where evtname = 'pgs3_dictionary';") installed: schema changes are recorded in WAL"

say "3. the replica subscribes to the primary, the ordinary way"
rep -c "create subscription sub
        connection '$PRI password=postgres'
        publication p
        with (binary = true, streaming = off, copy_data = false, slot_name = 'sub');" >/dev/null
pri >/dev/null <<SQL
insert into accounts (id, owner, balance, tags)
  select g, 'owner-' || g, (g % 1000) * 1.25, array['seed', 'tier-' || (g % 3)]
  from generate_series(1, 10000 * $SCALE) g;
SQL
retry 120 '[ "$(rep -c "select count(*) from accounts;")" = "$(( 10000 * SCALE ))" ]' \
  || die "the subscription never came up against the primary"
ok "$(rep -c 'select count(*) from accounts;') accounts replicated through slot 'sub'"
info "slot: active=$(slot active) wal_status=$(slot wal_status) restart_lsn=$(slot restart_lsn)"

say "4. the replica goes offline"
rep -c "alter subscription sub disable;" >/dev/null
retry 60 '[ "$(slot active)" = "f" ]' || die "the slot is still active"
ok "subscription disabled; the slot now holds WAL for it"

say "5. a large workload on the primary while it is away"
WAL_BEFORE=$(pri -c "select pg_current_wal_lsn();")
STARTED=$SECONDS
EVENTS=$(( 1000000 * SCALE )); BATCH=100000
for (( lo = 1; lo <= EVENTS; lo += BATCH )); do
  hi=$(( lo + BATCH - 1 ))
  pri -c "insert into events
            select g, 1 + g % (10000 * $SCALE),
                   (array['login','purchase','refund','logout'])[1 + g % 4],
                   jsonb_build_object('n', g, 'amount', round((g % 997) * 0.37, 2),
                                      'items', jsonb_build_array(g % 7, g % 11, 'sku-' || g % 13),
                                      'note', case when g % 5 = 0 then null else md5(g::text) end),
                   md5(g::text)::uuid,
                   timestamptz '2026-01-01' + g * interval '1 second'
            from generate_series($lo, $hi) g;" >/dev/null
  printf '\r        events: %d / %d' "$hi" "$EVENTS"
done
echo
ok "inserted $EVENTS events in $(( EVENTS / BATCH )) transactions of $BATCH"

# Incompressible bodies are moved out of line into TOAST, in ~2kB chunks.
DOCS=$(( 2000 * SCALE ))
for (( lo = 1; lo <= DOCS; lo += 250 )); do
  pri -c "insert into documents
            select g, 'doc-' || g, 1,
                   (select string_agg(md5(random()::text || g || s), '') from generate_series(1, 400) s)
            from generate_series($lo, $(( lo + 249 ))) g;" >/dev/null
done
ok "inserted $DOCS documents with ~12kB TOASTed bodies"

# A non-TOAST column changes: the body is carried as "unchanged" and must not
# be lost on the way through.
pri -c "update documents set version = version + 1, title = title || ' (rev)' where id % 2 = 0;" >/dev/null
for (( pass = 1; pass <= 3; pass++ )); do
  pri -c "update accounts set balance = balance + $pass * 10.5, updated_at = now(),
                 tags = tags || ('pass-$pass')::text where true;" >/dev/null
done
ok "updated every account 3 times, half of the documents once"

pri -c "delete from events where id % 4 = 0;" >/dev/null
pri -c "update accounts set active = false where id % 10 = 0;" >/dev/null
pri -c "begin; delete from accounts; rollback;" >/dev/null
ok "deleted 25% of events; a rolled-back delete of every account"

pri -c "select pg_switch_wal();" >/dev/null
WAL_AFTER=$(pri -c "select pg_current_wal_lsn();")
WAL_BYTES=$(pri -c "select pg_wal_lsn_diff('$WAL_AFTER', '$WAL_BEFORE')::bigint;")
ok "$(pretty "$WAL_BYTES") of WAL written in $(( SECONDS - STARTED ))s"

say "6. the slot is past max_slot_wal_keep_size"
pri -c "checkpoint;" >/dev/null; pri -c "checkpoint;" >/dev/null
retry 60 '[ "$(slot wal_status)" = "lost" ]' \
  || die "slot is $(slot wal_status), not lost; raise SCALE or lower max_slot_wal_keep_size"
ok "slot 'sub': wal_status=$(slot wal_status), invalidation_reason=$(slot invalidation_reason), restart_lsn=$(slot restart_lsn)"
OLDEST=$(pri -c "select min(name) from pg_ls_waldir() where name ~ '^[0-9A-F]{24}$';")
T0_SEG=$(pri -c "select pg_walfile_name('$T0');")
info "pg_wal now starts at $OLDEST; T0 was in $T0_SEG -- recycled"
ERR=$(pri -c "select pg_logical_slot_peek_binary_changes('sub', null, 1,
                'proto_version', '1', 'publication_names', 'p');" 2>&1 || true)
info "asking the primary for those changes: $(echo "$ERR" | head -1)"
case "$ERR" in *"can no longer"*|*"invalidated"*) ok "the primary cannot serve this subscriber any more" ;;
               *) die "expected the primary to refuse the invalidated slot, got: $ERR" ;; esac

say "7. ...but the wal-g archive has all of it"
LAST_SEG=$(pri -c "select pg_walfile_name('$WAL_AFTER'::pg_lsn - 1);")
retry 300 '[ "$(pri -c "select last_archived_wal >= '"'$LAST_SEG'"' from pg_stat_archiver;")" = "t" ]' \
  || die "archiving stalled at $(pri -c 'select last_archived_wal from pg_stat_archiver;')"
walg wal-fetch "$T0_SEG" /tmp/t0.seg || die "$T0_SEG is not in the archive"
ok "$(archived) segments in s3://pgs3/wal, from $T0_SEG through $LAST_SEG"
APPLIED=$(rep -c "select remote_lsn from pg_replication_origin_status;")
info "the replica's own origin says it applied up to $APPLIED"

say "8. repoint the replica at pgs3"
touch /state/ready
retry 60 '(exec 3<>/dev/tcp/pgs3/5439)' || die "pgs3 never started listening"
rep -c "alter subscription sub connection 'host=pgs3 port=5439 user=supabase_admin dbname=postgres';" >/dev/null
rep -c "alter subscription sub enable;" >/dev/null
CATCHUP=$SECONDS
EXPECT_EVENTS=$(pri -c "select count(*) from events;")
EXPECT_ACCOUNTS=$(digest pri accounts)
while :; do
  GOT=$(rep -c "select count(*) from events;")
  AT=$(rep -c "select coalesce(latest_end_lsn::text, '-') from pg_stat_subscription where subname = 'sub';")
  printf '\r        events %s / %s   replica confirmed %s of %s   ' "$GOT" "$EXPECT_EVENTS" "$AT" "$WAL_AFTER"
  if [ "$GOT" = "$EXPECT_EVENTS" ] && [ "$(digest rep accounts)" = "$EXPECT_ACCOUNTS" ]; then break; fi
  [ $(( SECONDS - CATCHUP )) -lt $(( 600 * SCALE )) ] || { echo; die "catch-up stalled"; }
  sleep 1
done
echo
ELAPSED=$(( SECONDS - CATCHUP )); [ "$ELAPSED" -gt 0 ] || ELAPSED=1
ok "caught up in ${ELAPSED}s, ~$(pretty "$(( WAL_BYTES / ELAPSED ))")/s of WAL decoded from S3"
[ "$(slot active)" = "f" ] && [ "$(pri -c 'select count(*) from pg_stat_replication;')" = 0 ] \
  || die "the primary served replication during catch-up"
ok "the primary served nothing: zero walsenders, and pgs3 has no route to it"

compare() {
  for t in accounts events documents; do
    P=$(digest pri "$t"); R=$(digest rep "$t")
    [ "$P" = "$R" ] || die "$t differs: primary $P, replica $R"
    ok "$t: $P"
  done
}

say "9. compare the two databases"
compare
Q="select count(*) from documents where version = 2 and length(body) = 12800;"
[ "$(rep -c "$Q")" = "$(( DOCS / 2 ))" ] || die "unchanged TOAST bodies were lost on update"
ok "updated documents kept their unchanged TOAST bodies"

say "10. it keeps following the archive"
pri -c "insert into events values (0, 1, 'after-catch-up', '{}', null, now());" >/dev/null
pri -c "select pg_switch_wal();" >/dev/null
retry 120 '[ "$(rep -c "select count(*) from events where id = 0;")" = 1 ]' \
  || die "pgs3 did not pick up the newly archived segment"
ok "a new write arrived via wal-push -> S3 -> wal-fetch -> pgs3"
pri -c "select pg_drop_replication_slot('sub');" >/dev/null
ok "dead slot dropped: the primary now retains nothing for this subscriber ($(pri -c 'select count(*) from pg_replication_slots;') slots)"

say "11. schema change on a table that is not published"
pri -c "create table audit_log (id bigserial primary key, note text);
        alter table audit_log add column at timestamptz default now();
        insert into audit_log (note) select 'n' || g from generate_series(1, 1000) g;" >/dev/null
pri -c "insert into events values (-1, 1, 'after-unpublished-ddl', '{}', null, now());" >/dev/null
pri -c "select pg_switch_wal();" >/dev/null
retry 120 '[ "$(rep -c "select count(*) from events where id = -1;")" = 1 ]' \
  || die "pgs3 stopped on DDL that does not touch a published table"
ok "CREATE/ALTER TABLE audit_log: pgs3 kept streaming; the guard only watches published relations"

say "12. schema changes on the primary only, with writes still running"
# A writer that does not stop for the DDL: new events, and balance updates on
# the very table being altered.
rm -f /tmp/stop-writer
( i=0
  while [ ! -f /tmp/stop-writer ]; do
    i=$(( i + 1 ))
    pri -c "insert into events values ($(( 200000000 + i )), $(( 1 + i % 100 )), 'during-ddl', '{}', null, now());
            update accounts set balance = balance + 1 where id = $(( 1 + i % 100 ));" >/dev/null
  done
  echo "$i" > /tmp/writer.count ) &
WRITER=$!
sleep 3
# Nothing is run on the replica: pgs3 has it as `target` in its config, and
# applies each change there itself, between the right two transactions.
# One transaction that changes the shape and immediately writes in it: the rows
# after the ALTER must decode with the new dictionary, the ones before with the
# old.
pri >/dev/null <<'SQL'
begin;
update accounts set balance = balance + 0.01 where id = 1;
alter table accounts add column region text not null default 'eu';
update accounts set region = (array['eu','us','ap'])[1 + id % 3] where id % 2 = 0;
commit;
SQL
ok "ADD COLUMN accounts.region and a backfill in the same transaction"
# A rewrite into a new relfilenode; binary=true sends int4 and int8 differently,
# so the replica must change between the last old row and the first new one.
pri -c "alter table documents alter column version type bigint;" >/dev/null
pri -c "update documents set version = version + 5000000000 where id % 3 = 0;" >/dev/null
ok "ALTER COLUMN documents.version TYPE bigint (a table rewrite), then versions > 2^32"
pri -c "alter table events rename column kind to event_kind;" >/dev/null
pri -c "alter table accounts drop column tags;" >/dev/null
ok "RENAME COLUMN events.kind, DROP COLUMN accounts.tags, under the writer"
sleep 3
touch /tmp/stop-writer; wait "$WRITER"
ok "$(cat /tmp/writer.count) writer transactions ran straight through all of it"
pri -c "select pg_switch_wal();" >/dev/null

EXPECT_EVENTS=$(pri -c "select count(*) from events;")
retry 300 '[ "$(rep -c "select count(*) from events;")" = "$EXPECT_EVENTS" ]' \
  || die "stalled at $(rep -c 'select count(*) from events;') of $EXPECT_EVENTS events; see /state/pgs3.log"
for t in accounts documents; do
  EXPECT=$(digest pri $t)
  retry 60 '[ "$(digest rep '$t')" = "$EXPECT" ]' || die "$t never converged"
done
grep -E 'new dictionary|applied to the target' /state/pgs3.log | sed -E 's/^pgs3: /        /' | cut -c1-150
! grep -q 'DDL at' /state/pgs3.log || die "pgs3 refused a schema change"
SHAPE="select table_name || '.' || column_name || ' ' || data_type from information_schema.columns
       where table_schema = 'public' and table_name in ('accounts', 'events', 'documents') order by 1;"
[ "$(pri -c "$SHAPE")" = "$(rep -c "$SHAPE")" ] || die "the replica's tables are not the primary's shape"
ok "the replica has the primary's shape, every change applied by pgs3 at its place in the stream"
info "recorded on the replica: $(rep -c "select count(*) from pgs3.schema_changes;") schema change phase(s) in pgs3.schema_changes"

say "13. compare again"
compare
ok "$(rep -c "select count(*) from documents where version > 4294967295;") documents with bigint versions, regions: $(rep -c "select string_agg(region || '=' || n, ' ') from (select region, count(*) n from accounts group by 1 order by 1) x;")"

printf '\n\033[1;32mAll steps passed.\033[0m The replica outlived its slot by %s of WAL,\n' "$(pretty "$WAL_BYTES")"
printf 'was brought back from the wal-g archive alone, and followed schema changes it applied itself.\n\n'
