# pgs3

Decode archived Postgres WAL into a logical replication stream, without the
primary retaining WAL and without a Postgres process in the middle.

## The problem

A major-version upgrade over logical replication needs a slot on the primary
from the moment the initial dump is taken until the new replica has caught up.
That slot pins `restart_lsn`, so the primary accumulates WAL for the entire
upgrade window. On a busy instance that is where you run out of disk.

The WAL is already in S3. Nothing reads it from there, because logical decoding
is not a pure function of WAL bytes — it needs a catalog to turn relfilenodes
and tuple bytes into named rows.

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

The cost of freezing is that DDL after T0 invalidates the dictionary. That is
already forbidden during a logical-replication upgrade, so `pgs3` enforces it —
but precisely. It watches writes to `pg_class` and `pg_attribute`, both of which
lead with the Oid of the relation they describe, and stops only when that Oid is
a *published* relation. DDL on other tables, the temp-table churn a busy system
produces constantly, and the statistics updates from (auto)vacuum and analyze
all pass through untouched.

## Postgres versions

Nothing about the WAL format is compiled in. The source server's version, WAL
segment size and system identifier all come from the catalog dictionary, and
`XLOG_PAGE_MAGIC` is not pinned at all -- it is learned from the first page and
then enforced across every later one.

Everything that genuinely differs between majors lives in `src/version.rs`.
Today that is one thing: PG15 split `BKPIMAGE_IS_COMPRESSED` into per-codec
flags and reused the vacated bit for `BKPIMAGE_APPLY`, which changes whether a
block-image header carries a trailing hole-length word. Get it wrong and the
record cursor shifts two bytes.

Provenance is checked directly instead of inferred from a version fingerprint:
the segment's `xlp_sysid` must match the dictionary's `system_identifier`, so an
archive from another cluster is refused rather than decoded against the wrong
schema.

`scripts/check-all.sh` runs the full check against every Postgres in the nix
store. Verified against **14.24, 16.15, 17.9 and 18.4** -- a range that straddles
the PG15 change, with `wal_compression` enabled so the block-image path is
actually exercised. PG10-13 are accepted but untested.

## Status

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

Not implemented: streaming of in-progress transactions, two-phase commit, and
range/multirange/geometric types. Each is refused explicitly rather than
mis-encoded.

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

### The three commands

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

`subscribe` creates the subscription against the primary with
`create_slot = false` (that is what fills `pg_subscription_rel`; see below),
sets its replication origin to X, points it at pgs3 and enables it. No slot is
created on the primary, ever. For a replica made by copying the primary's volume
and running `pg_upgrade`, X is `Latest checkpoint location` from `pg_controldata`
of the copy, taken before upgrading it.

Every option can live in a YAML file instead (`pgs3 serve --config pgs3.yaml`),
with keys spelled like the options: `wal: /var/lib/pgs3/wal`,
`restore-command: ...`, `exit-when-caught-up: true`. Command-line options win.
Misspelled options and keys are errors. `pgs3 check` runs the same validation as
`serve` without listening. `pgs3 help` lists everything.

Without `--restore-command` (or `PGS3_RESTORE_COMMAND`), pgs3 calls
`wal-g wal-fetch` itself when `PGS3_WALG_CONFIG` or `WALG_S3_PREFIX` is set.
Otherwise segments must already be in `--wal`.

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

### Why the subscription is created against the primary

Creating the subscription against `pgs3` directly with `connect = false` looks
shorter and is a trap. It leaves `pg_subscription_rel` empty, and the only way
to fill it afterwards is `ALTER SUBSCRIPTION ... REFRESH PUBLICATION`, which
`pgs3` cannot answer: it speaks `IDENTIFY_SYSTEM` and `START_REPLICATION`, not
SQL. The failure is silent — the apply worker starts, `received_lsn` advances,
the subscriber even confirms the LSN, and every change is discarded. The
signature is `pg_replication_origin_status.remote_lsn` stuck at `0/0` while
rows never arrive.

## Deliberate limits

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

## The invariant that matters at cutover

Three LSNs are easy to conflate:

| | |
|---|---|
| `restart_lsn` | oldest WAL the slot needs — drives retention |
| `confirmed_flush_lsn` | what the advancer moves |
| subscriber origin `remote_lsn` | where the subscriber actually is |

**Never advance the primary's slot past the subscriber's confirmed flush.**
Overshoot and cutover asks for WAL behind the slot; the primary silently resumes
from `confirmed_flush_lsn` instead. No error, no warning, missing rows. Assert it
on every advance call.
