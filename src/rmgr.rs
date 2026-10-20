//! Resource-manager record parsing: the heap and transaction records logical
//! decoding actually needs. Everything else (btree, freespace, visibility map)
//! is deliberately ignored.

use crate::xlog::{Record, RelFileLocator};
use anyhow::{bail, ensure, Result};

pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_HEAP_TRUNCATE: u8 = 0x30;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
pub const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;

pub const XLOG_XACT_COMMIT: u8 = 0x00;
pub const XLOG_XACT_ABORT: u8 = 0x20;
pub const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
pub const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
const XLOG_XACT_OPMASK: u8 = 0x70;
const XLOG_XACT_HAS_INFO: u8 = 0x80;

const XACT_XINFO_HAS_DBINFO: u32 = 1 << 0;
const XACT_XINFO_HAS_SUBXACTS: u32 = 1 << 1;

const XLH_INSERT_CONTAINS_NEW_TUPLE: u8 = 0x08;

const XLH_DELETE_CONTAINS_OLD_TUPLE: u8 = 0x02;
const XLH_DELETE_CONTAINS_OLD_KEY: u8 = 0x04;

const XLH_UPDATE_CONTAINS_OLD_TUPLE: u8 = 0x04;
const XLH_UPDATE_CONTAINS_OLD_KEY: u8 = 0x08;
const XLH_UPDATE_CONTAINS_NEW_TUPLE: u8 = 0x10;
const XLH_UPDATE_PREFIX_FROM_OLD: u8 = 0x20;
const XLH_UPDATE_SUFFIX_FROM_OLD: u8 = 0x40;

const SIZE_OF_HEAP_HEADER: usize = 5;

/// A tuple exactly as WAL carries it: the header fields that deform needs plus
/// everything from `SizeofHeapTupleHeader` onward.
#[derive(Clone, Debug)]
pub struct RawTuple {
    pub infomask2: u16,
    pub infomask: u16,
    pub t_hoff: u8,
    pub data: Vec<u8>,
}

impl RawTuple {
    fn parse(buf: &[u8]) -> Result<RawTuple> {
        ensure!(buf.len() >= SIZE_OF_HEAP_HEADER, "truncated xl_heap_header");
        Ok(RawTuple {
            infomask2: u16::from_le_bytes(buf[0..2].try_into()?),
            infomask: u16::from_le_bytes(buf[2..4].try_into()?),
            t_hoff: buf[4],
            data: buf[SIZE_OF_HEAP_HEADER..].to_vec(),
        })
    }
}

#[derive(Clone, Debug)]
pub enum HeapOp {
    /// The record changed a row but carries no tuple image. Normal for
    /// relations that are not logically logged (catalogs, unlogged tables); a
    /// fatal archive problem for a published one. Only the caller knows which,
    /// so the decision is deferred to it.
    NoTupleData,
    Insert { new: RawTuple },
    Update { old: Option<RawTuple>, new: RawTuple },
    Delete { old: Option<RawTuple> },
    MultiInsert { tuples: Vec<RawTuple> },
}

/// Decode a heap/heap2 record. `None` for records that carry no row change.
/// TRUNCATE registers no block reference, so it never reaches here; see
/// `parse_truncate`.
pub fn parse_heap(r: &Record) -> Result<Option<(RelFileLocator, HeapOp)>> {
    let md = &r.main_data;
    let op = r.heap_opcode();

    // Every row change registers the target page as block 0.
    let Some(blk0) = r.block(0) else {
        return Ok(None);
    };
    // Only the main fork holds tuples; visibility-map and fsm blocks never do.
    if blk0.forknum != crate::xlog::MAIN_FORKNUM {
        return Ok(None);
    }
    let rloc = blk0.rlocator;

    match (r.rmid, op) {
        (crate::xlog::RM_HEAP_ID, XLOG_HEAP_INSERT) => {
            ensure!(md.len() >= 3, "truncated xl_heap_insert");
            let flags = md[2];
            if flags & XLH_INSERT_CONTAINS_NEW_TUPLE == 0 {
                return Ok(Some((rloc, HeapOp::NoTupleData)));
            }
            Ok(Some((
                rloc,
                HeapOp::Insert { new: RawTuple::parse(&blk0.data)? },
            )))
        }

        (crate::xlog::RM_HEAP_ID, XLOG_HEAP_DELETE) => {
            ensure!(md.len() >= 8, "truncated xl_heap_delete");
            let flags = md[7];
            let old = if flags & (XLH_DELETE_CONTAINS_OLD_TUPLE | XLH_DELETE_CONTAINS_OLD_KEY) != 0
            {
                Some(RawTuple::parse(&md[8..])?)
            } else {
                None
            };
            Ok(Some((rloc, HeapOp::Delete { old })))
        }

        (crate::xlog::RM_HEAP_ID, XLOG_HEAP_UPDATE | XLOG_HEAP_HOT_UPDATE) => {
            ensure!(md.len() >= 14, "truncated xl_heap_update");
            let flags = md[7];
            if flags & (XLH_UPDATE_PREFIX_FROM_OLD | XLH_UPDATE_SUFFIX_FROM_OLD) != 0 {
                // Postgres only delta-encodes against the old tuple for
                // relations that are not logically logged.
                return Ok(Some((rloc, HeapOp::NoTupleData)));
            }
            if flags & XLH_UPDATE_CONTAINS_NEW_TUPLE == 0 {
                return Ok(Some((rloc, HeapOp::NoTupleData)));
            }
            let old = if flags & (XLH_UPDATE_CONTAINS_OLD_TUPLE | XLH_UPDATE_CONTAINS_OLD_KEY) != 0
            {
                Some(RawTuple::parse(&md[14..])?)
            } else {
                None
            };
            Ok(Some((rloc, HeapOp::Update { old, new: RawTuple::parse(&blk0.data)? })))
        }

        (crate::xlog::RM_HEAP2_ID, XLOG_HEAP2_MULTI_INSERT) => {
            ensure!(md.len() >= 4, "truncated xl_heap_multi_insert");
            let flags = md[0];
            let ntuples = u16::from_le_bytes(md[2..4].try_into()?) as usize;
            if flags & XLH_INSERT_CONTAINS_NEW_TUPLE == 0 {
                return Ok(Some((rloc, HeapOp::NoTupleData)));
            }
            // Block 0 data is a run of SHORTALIGN'd xl_multi_insert_tuple headers,
            // each followed by its tuple body.
            let mut tuples = Vec::with_capacity(ntuples);
            let d = &blk0.data;
            let mut p = 0usize;
            for i in 0..ntuples {
                p = (p + 1) & !1; // SHORTALIGN
                ensure!(p + 7 <= d.len(), "MULTI_INSERT at {} truncated at tuple {i}", r.lsn);
                let datalen = u16::from_le_bytes(d[p..p + 2].try_into()?) as usize;
                let infomask2 = u16::from_le_bytes(d[p + 2..p + 4].try_into()?);
                let infomask = u16::from_le_bytes(d[p + 4..p + 6].try_into()?);
                let t_hoff = d[p + 6];
                p += 7;
                ensure!(p + datalen <= d.len(), "MULTI_INSERT at {} truncated in tuple {i}", r.lsn);
                tuples.push(RawTuple {
                    infomask2,
                    infomask,
                    t_hoff,
                    data: d[p..p + datalen].to_vec(),
                });
                p += datalen;
            }
            Ok(Some((
                rloc,
                HeapOp::MultiInsert { tuples },
            )))
        }

        _ => Ok(None),
    }
}

/// `xl_heap_truncate`: the relations live in main data because truncate
/// registers no block reference. Returns (dbId, relIds).
pub fn parse_truncate(r: &Record) -> Result<(u32, Vec<u32>)> {
    let md = &r.main_data;
    // Oid dbId; uint32 nrelids; uint8 flags; (pad) Oid relids[]
    ensure!(md.len() >= 12, "truncated xl_heap_truncate at {}", r.lsn);
    let db = u32::from_le_bytes(md[0..4].try_into()?);
    let n = u32::from_le_bytes(md[4..8].try_into()?) as usize;
    // Bound the allocation by the bytes actually present before trusting n.
    ensure!(md.len() >= 12 + 4 * n, "truncated relid list at {}", r.lsn);
    let relids = md[12..12 + 4 * n].as_chunks::<4>().0.iter().map(|c| u32::from_le_bytes(*c)).collect();
    Ok((db, relids))
}

/// Tuple images carried by a record, read without consulting the logical-decoding
/// flags.
///
/// Catalog relations are not logically logged, so their records never set
/// `XLH_INSERT_CONTAINS_NEW_TUPLE` -- but the tuple bytes are registered as
/// block data all the same. `parse_heap` rightly refuses to treat them as a
/// logical change; the DDL guard reads them anyway, to find out which relation
/// a catalog write was for.
pub fn raw_tuples(r: &Record) -> Result<Vec<RawTuple>> {
    let Some(blk0) = r.block(0) else { return Ok(Vec::new()) };
    if blk0.data.is_empty() {
        // A full-page image was taken without keeping the tuple data.
        return Ok(Vec::new());
    }
    Ok(match (r.rmid, r.heap_opcode()) {
        (crate::xlog::RM_HEAP_ID, XLOG_HEAP_INSERT) => vec![RawTuple::parse(&blk0.data)?],
        (crate::xlog::RM_HEAP_ID, XLOG_HEAP_UPDATE)
        | (crate::xlog::RM_HEAP_ID, XLOG_HEAP_HOT_UPDATE) => {
            // Delta-compressed updates carry one or two uint16 lengths before
            // a tuple image stripped of its unchanged parts. The leading Oid
            // is exactly what gets stripped, so the bytes cannot be read as a
            // tuple header -- report nothing rather than misparse them.
            let md = &r.main_data;
            ensure!(md.len() >= 8, "truncated xl_heap_update at {}", r.lsn);
            if md[7] & (XLH_UPDATE_PREFIX_FROM_OLD | XLH_UPDATE_SUFFIX_FROM_OLD) != 0 {
                return Ok(Vec::new());
            }
            vec![RawTuple::parse(&blk0.data)?]
        }
        (crate::xlog::RM_HEAP2_ID, XLOG_HEAP2_MULTI_INSERT) => {
            let md = &r.main_data;
            ensure!(md.len() >= 4, "truncated xl_heap_multi_insert at {}", r.lsn);
            let ntuples = u16::from_le_bytes(md[2..4].try_into()?) as usize;
            let d = &blk0.data;
            let mut out = Vec::with_capacity(ntuples);
            let mut p = 0usize;
            for _ in 0..ntuples {
                p = (p + 1) & !1; // SHORTALIGN
                if p + 7 > d.len() {
                    break;
                }
                let datalen = u16::from_le_bytes(d[p..p + 2].try_into()?) as usize;
                let t = RawTuple {
                    infomask2: u16::from_le_bytes(d[p + 2..p + 4].try_into()?),
                    infomask: u16::from_le_bytes(d[p + 4..p + 6].try_into()?),
                    t_hoff: d[p + 6],
                    data: d.get(p + 7..p + 7 + datalen).unwrap_or_default().to_vec(),
                };
                p += 7 + datalen;
                out.push(t);
            }
            out
        }
        _ => Vec::new(),
    })
}

#[derive(Debug, Default)]
pub struct XactInfo {
    pub xinfo: u32,
    pub subxacts: Vec<u32>,
    /// `TimestampTz`: microseconds since 2000-01-01 UTC.
    pub xact_time: i64,
}

/// Parse the leading, fixed part of a commit/abort record: timestamp, optional
/// xinfo word, optional dbinfo, optional subtransaction list. Later sections
/// (relfilelocators, invalidations, two-phase, origin) are not needed here; the
/// `xinfo` flags tell us whether they exist.
pub fn parse_xact(r: &Record) -> Result<XactInfo> {
    let md = &r.main_data;
    let op = r.info & XLOG_XACT_OPMASK;
    let mut p = match op {
        XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => 8, // TimestampTz xact_time
        XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => 8,
        other => bail!("parse_xact called on opcode {other:#04x}"),
    };
    ensure!(md.len() >= 8, "truncated xact record at {}", r.lsn);
    let mut info = XactInfo { xact_time: i64::from_le_bytes(md[0..8].try_into()?), ..Default::default() };
    if r.info & XLOG_XACT_HAS_INFO != 0 {
        ensure!(md.len() >= p + 4, "truncated xl_xact_xinfo at {}", r.lsn);
        info.xinfo = u32::from_le_bytes(md[p..p + 4].try_into()?);
        p += 4;
    }
    if info.xinfo & XACT_XINFO_HAS_DBINFO != 0 {
        p += 8; // Oid dbId, Oid tsId
    }
    if info.xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        ensure!(md.len() >= p + 4, "truncated subxact count at {}", r.lsn);
        let n = i32::from_le_bytes(md[p..p + 4].try_into()?);
        ensure!(n >= 0, "negative subxact count at {}", r.lsn);
        let n = n as usize;
        p += 4;
        ensure!(md.len() >= p + 4 * n, "truncated subxact list at {}", r.lsn);
        for i in 0..n {
            info.subxacts.push(u32::from_le_bytes(md[p + 4 * i..p + 4 * i + 4].try_into()?));
        }
    }
    Ok(info)
}

/// `XLOG_LOGICAL_MESSAGE`, the only opcode of the logical message rmgr.
pub const XLOG_LOGICAL_MESSAGE: u8 = 0x00;

/// A `pg_logical_emit_message()` record.
pub struct LogicalMessage<'r> {
    pub db_oid: u32,
    pub transactional: bool,
    pub prefix: &'r str,
    pub payload: &'r [u8],
}

/// `xl_logical_message`: dbId, transactional, then two `Size`s at their
/// natural 8-byte alignment, then the NUL-terminated prefix and the payload.
pub fn parse_logical_message(r: &Record) -> Result<LogicalMessage<'_>> {
    const HEADER: usize = 24; // offsetof(xl_logical_message, message)
    let d = &r.main_data;
    ensure!(d.len() >= HEADER, "logical message at {} is truncated", r.lsn);
    let db_oid = u32::from_le_bytes(d[0..4].try_into()?);
    let transactional = d[4] != 0;
    let prefix_size = u64::from_le_bytes(d[8..16].try_into()?) as usize;
    let message_size = u64::from_le_bytes(d[16..24].try_into()?) as usize;
    ensure!(
        prefix_size >= 1 && d.len() - HEADER >= prefix_size && d.len() - HEADER - prefix_size >= message_size,
        "logical message at {} declares {prefix_size}+{message_size} bytes but carries {}",
        r.lsn,
        d.len() - HEADER
    );
    let prefix = std::str::from_utf8(&d[HEADER..HEADER + prefix_size - 1])
        .map_err(|_| anyhow::anyhow!("logical message at {} has a non-UTF-8 prefix", r.lsn))?;
    let start = HEADER + prefix_size;
    Ok(LogicalMessage { db_oid, transactional, prefix, payload: &d[start..start + message_size] })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsn::Lsn;
    use crate::xlog::{Record, RM_HEAP_ID};

    fn rec(rmid: u8, info: u8, main_data: Vec<u8>) -> Record {
        Record {
            lsn: Lsn(0), end_lsn: Lsn(0), tot_len: 0, xid: 1, rmid, info,
            main_data, blocks: Vec::new(), top_xid: None,
        }
    }

    #[test]
    fn logical_message_prefix_and_payload() {
        let mut md = Vec::new();
        md.extend_from_slice(&5u32.to_le_bytes()); // dbId
        md.extend_from_slice(&[1, 0xAA, 0xAA, 0xAA]); // transactional + padding
        md.extend_from_slice(&5u64.to_le_bytes()); // "pgs3\0"
        md.extend_from_slice(&2u64.to_le_bytes());
        md.extend_from_slice(b"pgs3\0{}");
        let r = rec(crate::xlog::RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE, md.clone());
        let m = parse_logical_message(&r).unwrap();
        assert_eq!((m.db_oid, m.transactional, m.prefix, m.payload), (5, true, "pgs3", &b"{}"[..]));
        // A declared size beyond the record is an error, not a panic.
        md[16..24].copy_from_slice(&99u64.to_le_bytes());
        let r = rec(crate::xlog::RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE, md);
        assert!(parse_logical_message(&r).is_err());
    }

    #[test]
    fn truncate_relids_come_from_main_data() {
        let mut md = Vec::new();
        md.extend_from_slice(&999u32.to_le_bytes()); // dbId
        md.extend_from_slice(&2u32.to_le_bytes()); // nrelids
        md.extend_from_slice(&[1, 0xAA, 0xAA, 0xAA]); // flags + uninitialised padding
        md.extend_from_slice(&16384u32.to_le_bytes());
        md.extend_from_slice(&16385u32.to_le_bytes());
        let (db, relids) = parse_truncate(&rec(RM_HEAP_ID, XLOG_HEAP_TRUNCATE, md)).unwrap();
        assert_eq!((db, relids), (999, vec![16384, 16385]));
        // A count the bytes cannot back is an error, not a huge allocation.
        let mut bad = vec![0u8; 12];
        bad[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(parse_truncate(&rec(RM_HEAP_ID, XLOG_HEAP_TRUNCATE, bad)).is_err());
    }
}
