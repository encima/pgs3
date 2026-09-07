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
| Records | heap INSERT/UPDATE/HOT_UPDATE/DELETE/TRUNCATE, heap2 MULTI_INSERT, xact COMMIT/ABORT/ASSIGNMENT |
| Tuples | null bitmaps, alignment, varlena short/long/compressed, pglz |
| Values | `numeric` and `jsonb` unpacking, arrays incl. multi-dimensional, empty and null-containing |
| TOAST | chunk collection and reassembly, including unchanged-value detection |
| Reorder | per-xid buffering, subtransaction folding, commit ordering, aborted xacts dropped |
| Guard | refuses on DDL against a published relation after T0, on an archive from another cluster, and on WAL written below `wal_level=logical` |

Output half — done and verified:

| | |
|---|---|
| pgoutput | Begin/Commit/Relation/Insert/Update/Delete/Truncate, binary datums, byte-identical to Postgres' own plugin |
| Walsender | startup, `IDENTIFY_SYSTEM`, `START_REPLICATION`, CopyBoth, `w`/`k` out, `r` in |
| Resume | reads from `restart_lsn`, suppresses transactions the subscriber already applied |
| Advancer | `scripts/advance-slot.sh`, driven by the confirmed-LSN file |

Types: `bool bytea char name int2 int4 int8 oid float4 float8 text varchar
bpchar json jsonb numeric uuid date time timestamp timestamptz interval`, and
arrays of any of them. Anything else is refused when the stream starts, naming
the column.

Not implemented: streaming of in-progress transactions, two-phase commit, and
composite/range/geometric types. Each is refused explicitly rather than
mis-encoded.

## Using it

```sh
# 1. Once, against the source primary:
psql -X -t -A -v pubname=p -f sql/dump_catalog.sql > catalog.json

# 2. On the replica box, serving archived WAL. --start must be the slot's
#    restart_lsn, which is how far back the archive has to reach.
pgs3 serve --wal /var/lib/wal-cache --catalog catalog.json \
     --start "$(psql -tAc "select restart_lsn from pg_replication_slots where slot_name='sub'")" \
     --end   "$(psql -tAc 'select pg_current_wal_lsn()')" \
     --listen 127.0.0.1:5439 --confirmed-lsn-file /run/pgs3.lsn

# 3. Keep the primary's slot trailing the subscriber, so it retains almost nothing:
scripts/advance-slot.sh "$PRIMARY_CONNINFO" sub /run/pgs3.lsn 10

# 4. Point the existing subscription at pgs3, let it catch up, then cut back:
psql -c "alter subscription sub connection 'host=127.0.0.1 port=5439'"
```

The subscription must have been created `WITH (binary = true, streaming = off)`;
`serve` refuses otherwise rather than sending datums the subscriber would
misread.

## Seeing it work

```sh
docker compose -f docker/compose.yaml up --build \
  --abort-on-container-exit --exit-code-from demo
```

Brings up a Postgres 18 primary, a Postgres 18 replica and pgs3, then walks the
whole upgrade shape and checks each step:

```
== 6. repoint the replica at pgs3
   ok  caught up: 6 rows
== 7. did the primary serve any of that?
   ok  slot still inactive, zero replication connections on the primary
== 8. compare the two databases
   ok  every row identical, including TOAST, jsonb, arrays and numeric
== 9. advance the primary's slot to what the subscriber confirmed
   ok  confirmed_flush_lsn = 0/2016198, exactly what the subscriber reported, never beyond
   ok  restart_lsn still 0/2012D68; it follows at the next checkpoint
== 10. new writes keep flowing, and the primary finally releases WAL
   ok  pgs3 followed the archive; the row arrived without the primary being read
   ok  restart_lsn 0/2012D68 -> 0/3000048, releasing 16 MB of WAL
```

Two details in `docker/compose.yaml` carry the argument:

- The primary's `archive_command` copies WAL to a volume that pgs3 mounts
  read-only. That archive is the *only* thing pgs3 reads.
- pgs3 sits on the `downstream` network; the primary is on `upstream`. There is
  no route between them, so "the catch-up did not come from the primary" is
  enforced by the topology rather than asserted. The demo also checks it from
  the primary's side: the slot stays inactive and `pg_stat_replication` stays
  empty throughout.

## Verifying

```sh
./scripts/check.sh
```

`scripts/check-all.sh` runs the same thing against every Postgres major in the
nix store.

Each run builds a throwaway cluster, runs a workload that exercises every decode
path, and checks against Postgres itself:

- **framing** — every record matches `pg_waldump` on lsn, rmgr, xid and `xl_tot_len`
- **decoding** — every change matches `test_decoding` consuming the same WAL,
  compared value-for-value
- **encoding** — the pgoutput byte stream is diffed message-for-message against
  `pg_logical_slot_peek_binary_changes` using Postgres' own pgoutput plugin
- **end to end** — a second cluster subscribes to pgs3 and catches up **with the
  primary stopped**, then its table contents are compared against the primary's
- **guards** — the DDL, wrong-archive and unencodable-type refusals all fire

Both TOAST paths are checked byte-exact: a 9600-byte external value stitched
from five chunks, and a 16000-byte pglz-compressed value. The workload
deliberately populates a table, checkpoints, then updates it, so that records
carrying *both* a compressed full-page image and tuple data are decoded — the
case where a mis-sized image header silently corrupts the row.

## Deliberate limits

- **Types are an allowlist**, enforced when the stream starts, never mid-
  transaction, with the offending column named. Binary mode makes most types a
  byteswap; `numeric` is a repacking; `jsonb` needs a full `jsonb_out` (its
  binary form *is* the text rendering), which is why `numeric_out` is in here
  too. Composite, range and geometric types are still refused.
- **`decode` mode's text output** covers a smaller allowlist and prints tagged
  hex otherwise. It exists to be diffed against `test_decoding`, not to be
  consumed.
- **pglz only.** `lz4`/`zstd` TOAST and `wal_compression` need their codecs.
- **Transactions buffer in memory.** Spill to disk before a bulk UPDATE of a wide
  table meets a small instance.
- **Segments come from a directory.** S3 lands behind `SegmentDir` by shelling
  out to `wal-g wal-fetch`. A segment that is not there yet is treated as "not
  archived yet" and retried, which is what lets `serve` follow a live archive.
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
