# pgs3

Decode archived Postgres WAL into a logical replication stream, without the
primary retaining WAL and without a Postgres process in the middle.

## The problem

Logical replication with Postgres requires a slot on the primary and for the primary to retain the write-ahead log. Most primaries are already archiving the write-ahead log to S3 for backups, so maybe we can do it a different way.

A different way would allow us to read from almost any point in time in the write-ahead log. If a slot is lost, the replica can pick it back up again just by specifying that point in time, and we don't have to start from zero.


## Building and testing

```
cargo build --release      # binary at target/release/pgs3
cargo test                 # unit tests, no Postgres needed
```

End-to-end checks need a real Postgres in `PATH` (`initdb`, `pg_ctl`, `psql`):
`scripts/check.sh` builds a throwaway cluster and diffs `pgs3` against Postgres'
own logical decoding; `scripts/check-simdu.sh` runs the slotless init/serve/subscribe
flow against a real subscriber; `scripts/check-all.sh` runs the checks for every major in the nix
store. `devbox.json` provides the toolchain plus wal-g/minio for
`scripts/minio-demo.sh`.

## Using it

You need: `psql` wherever you run `init` and `subscribe`, a primary with
`wal_level = logical` and `archive_mode = on` shipping WAL somewhere, and one
command that can fetch a single segment from there (`%f` = segment name, `%p` =
destination), e.g. `aws s3 cp s3://bucket/wal/%f %p` or `wal-g wal-fetch %f %p`.
`export PGS3_WALG_CONFIG=/etc/wal-g/config.json` can be used in place of the WAL fetch command

### Quick start

```
scripts/quickstart.sh 'host=primary dbname=app' \
    --restore-command 'wal-g wal-fetch %f %p'
```

This builds pgs3, creates publication `p` (`FOR ALL TABLES`) if it is missing,
captures the dictionary, checks the archive, starts `serve` in the background,
and prints the `subscribe` command to run once the replica is ready. Everything
goes in `./pgs3-state/`, including a `config.yaml` that every `pgs3` command
accepts with `--config`. `scripts/quickstart.sh stop` stops it.

### Normal speed start

```
# 1. T0, on the primary: capture the dictionary and the start LSN. A temporary
#    slot is used and dropped -- nothing is left behind to retain WAL.
#    It also installs an event trigger that records schema changes (below).
#    From here on: no TRUNCATE on published tables.
pgs3 init --source 'host=primary dbname=app' --catalog catalog.json --publication p

# 2. Serve the archive. The --wal directory is a cache, created if missing,
#    and each segment is deleted once read. --source is optional: with it, pgs3
#    runs pg_switch_wal() (at most every 30s) when the segment it waits on is
#    the one still being written.
pgs3 serve --catalog catalog.json --wal /var/lib/pgs3/wal \
    --restore-command 'wal-g wal-fetch %f %p' \
    --listen 0.0.0.0:5439 --status-file /var/lib/pgs3/status.json \
    --source 'host=primary dbname=app'

# 3. On the replica, once its data is consistent at some LSN X >= the start
#    LSN init printed: subscribe it to pgs3 from X.
pgs3 subscribe --target 'host=replica dbname=app' \
    --source 'host=primary dbname=app' --publication p \
    --at X --connect 'host=pgs3-host port=5439' --catalog catalog.json
```

Options can also be in a YAML file: `pgs3 serve --config pgs3.yaml`


### Alternative: keep a slot and advance it

If the replica already has a subscription with a slot on the primary, keep it:
disable the subscription, start `serve` with `--start` set to the slot's
`restart_lsn` (not `confirmed_flush_lsn`) and with `--confirmed-lsn-file`, then
repoint the subscription at pgs3 and run `scripts/advance-slot.sh` against the
primary so the slot follows what the subscriber confirms:

```sql
ALTER SUBSCRIPTION sub DISABLE;
-- ... the upgrade ...
ALTER SUBSCRIPTION sub CONNECTION 'host=pgs3-host port=5439 dbname=app';
ALTER SUBSCRIPTION sub ENABLE;
```

The subscription must be `binary = true, streaming = off`. `docker/compose.yaml`
and `scripts/minio-demo.sh` walk through this shape end to end.

### Demo: a subscriber that outlived its slot (Supabase + wal-g)

`docker/supabase/` runs two `supabase/postgres` databases with wal-g archiving
to S3 (SeaweedFS). The replica subscribes the ordinary way, goes offline, and
the primary writes ~600MB of WAL against `max_slot_wal_keep_size = 128MB`:
1M-row bulk inserts, TOASTed documents, wide updates, deletes. Postgres
invalidates the slot (`wal_status = lost`) and recycles the WAL. The replica is
then repointed at pgs3, which has no network route to the primary, and catches
up from the wal-g archive alone. Both databases are compared row by row, and
the dead slot is dropped.

It then makes schema changes with a writer still running: DDL on an
unpublished table, `ADD COLUMN` plus a backfill in the same transaction, and a
rewriting `ALTER COLUMN ... TYPE bigint`. pgs3 picks up each new dictionary
from WAL and keeps streaming: no pause, no restart, no re-capture.

```
docker compose -f docker/supabase/compose.yaml up --build \
  --abort-on-container-exit --exit-code-from demo
SCALE=3 docker compose -f docker/supabase/compose.yaml up ...   # ~3x the WAL
docker compose -f docker/supabase/compose.yaml down -v
```

## The approach

Capture the catalog **once**, at T0, into a frozen dictionary
(`sql/dump_catalog.sql`). Decode against that snapshot instead of against the
primary's live catalog.


The consequence is the whole point: because we never consult the primary's
catalog, the primary never has to preserve it. It holds no `catalog_xmin` on our
behalf and no WAL beyond the slot-advance interval.

```
primary --archive--> S3 --wal-fetch--> pgs3 --pgoutput--> upgraded replica
   |                                     |
   +-- pg_replication_slot_advance() <---+   (advance to the subscriber's flush)
```

### Schema changes

A frozen dictionary goes stale at the first DDL on a published table, so the
source writes a new one into WAL. `pgs3 init` installs a `ddl_command_end`
event trigger (`sql/ddl_trigger.sql`) that, whenever a command touches a
published table, a type or the publication, runs the same query as
`sql/dump_catalog.sql` and emits the result with
`pg_logical_emit_message(true, 'pgs3.dictionary', ...)`. Being transactional,
the message sits in the DDL's own transaction, after that command's catalog
writes, and is discarded with it on rollback (savepoints included).

pgs3 switches at exactly that point. Changes later in the same transaction
decode with the new dictionary, every other transaction switches at its
commit (the `ACCESS EXCLUSIVE` lock behind rowtype-changing DDL means no other
transaction can hold rows in the old shape past it), and the subscriber is
sent a fresh Relation message before the first row in the new shape, as
Postgres does. The archive describes its own schema; nothing has to be
re-captured.

The guard stays as the safety net. pgs3 watches writes to `pg_class` and
`pg_attribute`, both of which lead with the Oid of the relation they
describe, and if one concerns a *published* relation without a dictionary
following it in the same transaction, it refuses at that commit: the
trigger was missing, disabled (`session_replication_role = replica`, PG17's
`event_triggers = off`), or failed (it warns and never blocks the user's
DDL). DDL on other tables is ignored. Two transactions changing schema
concurrently, where the later commit's dictionary predates the earlier
commit, are refused too, since that dictionary may not include both changes.

As with any logical replication, the subscriber needs the DDL too, in the
right order: added columns there first, dropped columns there last.
`binary = true` means a column type change must reach both sides with no
rows of that table in flight.

The trigger finds the publications to describe by a comment: `pgs3 init`
sets `COMMENT ON PUBLICATION p IS 'pgs3'` (and refuses to overwrite another
comment). `--no-ddl-trigger` skips all of it, leaving DDL on a published table
as a hard stop.

## Input and Output

Input half — done and verified:

| | |
|---|---|
| Segment reader | page headers, contrecords, CRC, block references |
| Records | heap INSERT/UPDATE/HOT_UPDATE/DELETE, heap2 MULTI_INSERT, xact COMMIT/ABORT/ASSIGNMENT |
| Tuples | null bitmaps, alignment, varlena short/long/compressed, pglz |
| Values | `numeric` and `jsonb` unpacking, arrays incl. multi-dimensional, empty and null-containing |
| TOAST | chunk collection and reassembly, including unchanged-value detection |
| Reorder | per-xid buffering, subtransactions merged at commit, commit ordering, rolled-back xacts and savepoints dropped |
| Schema changes | follows dictionaries the source writes into WAL with each DDL on a published table, mid-transaction and through savepoints; re-announces changed relations |
| Guard | refuses on DDL against a published relation not followed by a new dictionary, on an archive from another cluster, and on WAL written below `wal_level=logical` |

Output half — done and verified:

| | |
|---|---|
| pgoutput | Begin/Commit/Relation/Insert/Update/Delete, binary datums, byte-identical to Postgres' own plugin |
| Walsender | startup, `IDENTIFY_SYSTEM`, `START_REPLICATION`, CopyBoth, `w`/`k` out, `r` in |
| Resume | reads from `restart_lsn`, suppresses transactions the subscriber already applied |
| Advancer | `scripts/advance-slot.sh`, driven by the confirmed-LSN file |

Types: `bool bytea char name int2 int4 int8 oid float4 float8 text varchar
bpchar json jsonb numeric uuid date time timestamp timestamptz interval inet
cidr` and the `reg*` types, plus enums, domains, composites and arrays of any of
them. Anything else is refused when the stream starts, naming the column.

Not implemented: 

- streaming of in-progress transactions
- two-phase commit
- range/multirange/geometric types
- dictionaries on a schedule (a periodic `pg_logical_emit_message` of the same
  dictionary would let serve start from any LSN without `init`)
- schema changes on PG13 and older are only followed when the DDL and its
  dictionary are in the same (sub)transaction: subtransactions name their
  parent in WAL from PG14


## Deliberate limitations

- **Types are an allowlist**, enforced when the stream starts, never mid-
  transaction, with the offending column named. Binary mode makes most types a
  byteswap; `numeric` is a repacking; `jsonb` needs a full `jsonb_out` (its
  binary form *is* the text rendering), which is why `numeric_out` is in here
  too. Range, multirange and geometric types are still refused.
- **TRUNCATE of a published table stops the stream.** Truncate assigns a new
  relfilenode — to the table and its TOAST — which the frozen dictionary cannot
  follow; rows written afterwards would be silently dropped. The decoder
  refuses instead. Re-capture the dictionary at a new T0 afterwards.
- **`decode` mode's text output** covers a smaller allowlist and prints tagged
  hex otherwise. It exists to be diffed against `test_decoding`, not to be
  consumed.
- **pglz only.** `lz4`/`zstd` TOAST and `wal_compression` need their codecs.
- **Transactions buffer in memory.** Spill to disk before a bulk UPDATE of a wide
  table meets a small instance.
- **Segments come from a directory**, used as a cache. If a segment is missing
  from it, an archive fetcher runs before giving up: `--restore-command` (or
  `PGS3_RESTORE_COMMAND`) is a PostgreSQL-style template — any shell command
  where `%f` is the segment name and `%p` the local destination — so
  `wal-g wal-fetch %f %p`, `pgbackrest --stanza=main archive-get %f %p`,
  `barman-wal-restore`, or `cp /mnt/nfs/%f %p` all work; the tool itself owns
  creds, layout, compression and encryption. Without one, `SegmentDir` falls
  back to `wal-g wal-fetch` when `WALG_S3_PREFIX`/`PGS3_WALG_CONFIG` are set.
  Either way, a segment that isn't available yet is treated as "not archived yet" and retried, which is
  what lets `serve` follow a live archive. Each wait is logged once; a
  fetcher that cannot run at all (exit 126/127) is an error, not a wait.
  Repeat misses on the *same* segment
  back off from 1s to 30s, so waiting at the head of the archive costs a couple
  of fetcher invocations a minute rather than one per poll; any other segment is
  fetched immediately, so catch-up still reads at full speed.
- **One connection at a time.** A subscriber needs exactly one, and reconnects
  are handled, but a client that connects and stays silent is dropped after a
  10s handshake timeout rather than holding the listener shut.
- **lz4/zstd `wal_compression`** is rejected with a clear message rather than
  mis-parsed; only pglz is decoded.
- **32-bit sources are unreadable, by construction.** i386 and wasm32 builds have
  `MAXIMUM_ALIGNOF = 4` and flip `attbyval` for 8-byte types, so tuple layout
  differs. This rules out PGlite and postgres-wasm as decoders, not just as hosts.

**Never advance the primary's slot past the subscriber's confirmed flush.**
