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

### Primary

- Get the catalog
  - `psql -X -t -A -v pubname=p -f sql/dump_catalog.sql > catalog.json`
- Get the LSN that you want to start from (maybe from catalog or `select pg_current_wal_lsn();`)

### PGS3 Host

- Set your WAL S3 dir
  - `export PGS3_WALG_CONFIG=/etc/wal-g/config.json`
- Run PGS3
  - `./pgs3 serve --wal /tml/walcache --catalog catalog.json --start "START_LSN" --end   "FFFFFFFF/FFFFFFFF" --listen 127.0.0.1:5439 --confirmed-lsn-file /run/pgs3.lsn`

### Subscriber

The subscription is created against the **primary**, not against `pgs3`, and
that is the same T0 the dictionary is captured at. Creating it there is what
fills `pg_subscription_rel`, and the apply worker only applies changes for
relations listed in it.

```sql
CREATE SUBSCRIPTION sub
  CONNECTION 'host=primary dbname=yourdb'
  PUBLICATION pub
  WITH (binary = true, streaming = off, copy_data = false, slot_name = 'sub');
```

Then the upgrade window opens. Disable it, take the slot's `restart_lsn` as
`--start`, and repoint it at `pgs3` once the replica is back.

```sql
ALTER SUBSCRIPTION sub DISABLE;
-- ... the upgrade ...
ALTER SUBSCRIPTION sub CONNECTION 'host=127.0.0.1 port=5439 dbname=yourdb';
ALTER SUBSCRIPTION sub ENABLE;
```

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
  too. Composite, range and geometric types are still refused.
- **`decode` mode's text output** covers a smaller allowlist and prints tagged
  hex otherwise. It exists to be diffed against `test_decoding`, not to be
  consumed.
- **pglz only.** `lz4`/`zstd` TOAST and `wal_compression` need their codecs.
- **Transactions buffer in memory.** Spill to disk before a bulk UPDATE of a wide
  table meets a small instance.
- **Segments come from a directory**, used as a cache. If a segment is missing
  from it and `WALG_S3_PREFIX` is set, `SegmentDir` shells out to
  `wal-g wal-fetch` before giving up; wal-g reads the rest of its S3 config
  (creds, region, etc.) from the environment. Either way, a segment that
  isn't available yet is treated as "not archived yet" and retried, which is
  what lets `serve` follow a live archive. Repeat misses on the *same* segment
  back off from 1s to 30s, so waiting at the head of the archive costs a couple
  of `wal-g` invocations a minute rather than one per poll; any other segment is
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
