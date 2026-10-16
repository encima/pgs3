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
#    From here on: no DDL or TRUNCATE on published tables.
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

## The approach

Capture the catalog **once**, at T0, into a frozen dictionary
(`sql/dump_catalog.sql`). Decode against that snapshot instead of against the
primary's live catalog.

You could also do this at regular points in time, like when the catalog changes, for example, so we can associate different WAL files with different catalogs. 

The consequence is the whole point: because we never consult the primary's
catalog, the primary never has to preserve it. It holds no `catalog_xmin` on our
behalf and no WAL beyond the slot-advance interval.

```
primary --archive--> S3 --wal-fetch--> pgs3 --pgoutput--> upgraded replica
   |                                     |
   +-- pg_replication_slot_advance() <---+   (advance to the subscriber's flush)
```

The cost of freezing is that DDL after T0 invalidates the dictionary. That is
already forbidden during a logical-replication upgrade, so `pgs3` enforces it —
but precisely. It watches writes to `pg_class` and `pg_attribute`, both of which
lead with the Oid of the relation they describe, and stops only when that Oid is
a *published* relation. DDL on other tables is fine.

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
| Guard | refuses on DDL against a published relation after T0, on an archive from another cluster, and on WAL written below `wal_level=logical` |

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
- scheduled/regular catalog caching


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
