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
pub const XLOG_XACT_ASSIGNMENT: u8 = 0x50;
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
    Truncate,
}

/// Decode a heap/heap2 record. `None` for records that carry no row change.
pub fn parse_heap(r: &Record) -> Result<Option<(RelFileLocator, HeapOp)>> {
    let md = &r.main_data;
    let op = r.heap_opcode();

    // Every row change registers the target page as block 0.
    let blk0 = match r.block(0) {
        Some(b) => b,
        None if op == XLOG_HEAP_TRUNCATE => {
            return Ok(None); // truncate carries relids in main data; handled by caller
        }
        None => return Ok(None),
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

        (crate::xlog::RM_HEAP_ID, XLOG_HEAP_TRUNCATE) => Ok(Some((rloc, HeapOp::Truncate))),

        _ => Ok(None),
    }
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
        (crate::xlog::RM_HEAP_ID, XLOG_HEAP_INSERT)
        | (crate::xlog::RM_HEAP_ID, XLOG_HEAP_UPDATE)
        | (crate::xlog::RM_HEAP_ID, XLOG_HEAP_HOT_UPDATE) => {
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
        let n = i32::from_le_bytes(md[p..p + 4].try_into()?) as usize;
        p += 4;
        ensure!(md.len() >= p + 4 * n, "truncated subxact list at {}", r.lsn);
        for i in 0..n {
            info.subxacts.push(u32::from_le_bytes(md[p + 4 * i..p + 4 * i + 4].try_into()?));
        }
    }
    Ok(info)
}

/// `XLOG_XACT_ASSIGNMENT` binds subtransactions to their top-level xid before
/// commit, which matters when a subxact's changes must be attributed early.
pub fn parse_assignment(r: &Record) -> Result<(u32, Vec<u32>)> {
    let md = &r.main_data;
    ensure!(md.len() >= 8, "truncated xl_xact_assignment at {}", r.lsn);
    let top = u32::from_le_bytes(md[0..4].try_into()?);
    let n = i32::from_le_bytes(md[4..8].try_into()?) as usize;
    let mut subs = Vec::with_capacity(n);
    for i in 0..n {
        let o = 8 + 4 * i;
        ensure!(md.len() >= o + 4, "truncated assignment list at {}", r.lsn);
        subs.push(u32::from_le_bytes(md[o..o + 4].try_into()?));
    }
    Ok((top, subs))
}
