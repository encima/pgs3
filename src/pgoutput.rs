//! pgoutput encoding: turn decoded events into the bytes a real Postgres
//! subscriber expects on a logical replication stream.
//!
//! Values go out in **binary** format, so the subscription must be created
//! `WITH (binary = true)`. Binary is what makes this tractable: for most types
//! `send()` is the on-disk representation byte-swapped to network order, so
//! there is no need to reimplement `numeric_out`, `timestamptz_out` and the
//! rest of the output-function tail. It is not free, though -- `numeric` is
//! packed differently on disk than on the wire, and `jsonb`/arrays/composites
//! need real structural conversion, so those are refused up front rather than
//! guessed at.

use crate::catalog::{Catalog, Column, ElemType, Relation};
use crate::reorder::{Event, Kind};
use crate::tuple::Datum;
use anyhow::{bail, Context, Result};
use std::collections::HashSet;

/// Types whose `send()` output we can produce from the on-disk bytes.
/// Everything else is rejected when the stream starts, not mid-transaction.
const SUPPORTED: &[&str] = &[
    "bool", "bytea", "char", "name", "int8", "int2", "int4", "text", "oid", "json", "float4",
    "float8", "bpchar", "varchar", "date", "time", "timestamp", "timestamptz", "interval",
    "numeric", "uuid", "jsonb",
];

/// Reject a publication we cannot encode faithfully, before a subscriber has
/// applied anything. Silently sending a wrong datum is far worse than refusing.
pub fn check_publication_encodable(cat: &Catalog) -> Result<()> {
    let mut bad: Vec<String> = Vec::new();
    let mut generated: Vec<String> = Vec::new();
    for rel in &cat.relations {
        for c in &rel.columns {
            if c.attisdropped {
                continue;
            }
            if !c.attgenerated.is_empty() {
                generated.push(format!("{}.{}", rel.qualified_name(), c.attname));
            }
            // For an array it is the element type that has to be encodable.
            let effective = c.elem.as_ref().map_or(c.typname.as_str(), |e| e.typname.as_str());
            if !SUPPORTED.contains(&effective) {
                bad.push(format!("{}.{} is {}", rel.qualified_name(), c.attname, c.typname));
            }
        }
    }
    if !generated.is_empty() {
        bail!(
            "publication contains generated columns, whose replication behaviour differs by \
             server version and publication setting: {}",
            generated.join(", ")
        );
    }
    if !bad.is_empty() {
        bail!(
            "publication contains types this encoder cannot produce binary output for: {}\n\
             Supported: {}",
            bad.join(", "),
            SUPPORTED.join(", ")
        );
    }
    Ok(())
}

fn columns(rel: &Relation) -> impl Iterator<Item = &Column> {
    rel.columns.iter().filter(|c| !c.attisdropped)
}

#[derive(Default)]
struct Buf(Vec<u8>);

impl Buf {
    fn u8(&mut self, v: u8) -> &mut Self {
        self.0.push(v);
        self
    }
    fn i16(&mut self, v: i16) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn i32(&mut self, v: i32) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn i64(&mut self, v: i64) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn cstr(&mut self, s: &str) -> &mut Self {
        self.0.extend_from_slice(s.as_bytes());
        self.0.push(0);
        self
    }
    fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.0.extend_from_slice(b);
        self
    }
}

pub struct Encoder {
    /// Relations already described to this subscriber. The catalog is frozen,
    /// so a relation never needs re-sending once announced.
    announced: HashSet<u32>,
}

impl Encoder {
    pub fn new() -> Encoder {
        Encoder { announced: HashSet::new() }
    }

    /// Encode one event, possibly preceded by a Relation message.
    pub fn encode(&mut self, e: &Event) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        match e {
            Event::Begin { xid, commit_lsn, commit_time, .. } => {
                let mut b = Buf::default();
                b.u8(b'B').i64(commit_lsn.0 as i64).i64(*commit_time).i32(*xid as i32);
                out.push(b.0);
            }
            Event::Commit { commit_lsn, end_lsn, commit_time, .. } => {
                let mut b = Buf::default();
                b.u8(b'C')
                    .u8(0)
                    .i64(commit_lsn.0 as i64)
                    .i64(end_lsn.0 as i64)
                    .i64(*commit_time);
                out.push(b.0);
            }
            Event::Change { rel, kind, old, new, .. } => {
                if self.announced.insert(rel.oid) {
                    out.push(relation_message(rel));
                }
                let mut b = Buf::default();
                match kind {
                    Kind::Insert => {
                        let new = new.as_ref().expect("INSERT always carries a new tuple");
                        b.u8(b'I').i32(rel.oid as i32).u8(b'N');
                        b.bytes(&tuple_data(rel, new)?);
                    }
                    Kind::Update => {
                        let new = new.as_ref().expect("UPDATE always carries a new tuple");
                        b.u8(b'U').i32(rel.oid as i32);
                        if let Some(old) = old {
                            // 'O' is a whole old tuple (REPLICA IDENTITY FULL),
                            // 'K' is just the identity key.
                            b.u8(if rel.relreplident == 'f' { b'O' } else { b'K' });
                            b.bytes(&tuple_data(rel, old)?);
                        }
                        b.u8(b'N');
                        b.bytes(&tuple_data(rel, new)?);
                    }
                    Kind::Delete => {
                        let old = match old {
                            Some(o) => o,
                            None => bail!(
                                "DELETE on {} carries no old tuple: the table has no REPLICA \
                                 IDENTITY, so the row cannot be identified downstream",
                                rel.qualified_name()
                            ),
                        };
                        b.u8(b'D').i32(rel.oid as i32);
                        b.u8(if rel.relreplident == 'f' { b'O' } else { b'K' });
                        b.bytes(&tuple_data(rel, old)?);
                    }
                    Kind::Truncate => {
                        b.u8(b'T').i32(1).u8(0).i32(rel.oid as i32);
                    }
                }
                out.push(b.0);
            }
        }
        Ok(out)
    }
}

fn relation_message(rel: &Relation) -> Vec<u8> {
    let mut b = Buf::default();
    b.u8(b'R')
        .i32(rel.oid as i32)
        .cstr(&rel.nspname)
        .cstr(&rel.relname)
        .u8(rel.relreplident as u8)
        .i16(columns(rel).count() as i16);
    for c in columns(rel) {
        // Under REPLICA IDENTITY FULL the whole tuple is the identity, so
        // Postgres flags every column. The subscriber checks its own identity
        // columns against these flags and refuses to apply updates if any is
        // missing ("publisher did not send replica identity column").
        b.u8(u8::from(rel.relreplident == 'f' || c.attispkey))
            .cstr(&c.attname)
            .i32(c.atttypid as i32)
            .i32(c.atttypmod);
    }
    b.0
}

fn tuple_data(rel: &Relation, vals: &[Datum]) -> Result<Vec<u8>> {
    let mut b = Buf::default();
    b.i16(columns(rel).count() as i16);
    for (c, v) in rel.columns.iter().zip(vals) {
        if c.attisdropped {
            continue;
        }
        match v {
            Datum::Null => {
                b.u8(b'n');
            }
            Datum::UnchangedToast => {
                b.u8(b'u');
            }
            Datum::Toast(_) => bail!(
                "unresolved TOAST pointer for {}.{}",
                rel.qualified_name(),
                c.attname
            ),
            Datum::Fixed(bytes) | Datum::Var(bytes) => {
                let wire = encode_datum(c, bytes)
                    .with_context(|| format!("{}.{}", rel.qualified_name(), c.attname))?;
                b.u8(b'b').i32(wire.len() as i32).bytes(&wire);
            }
        }
    }
    Ok(b.0)
}

/// Encode one column value: an array walk, or a plain scalar conversion.
pub fn encode_datum(col: &Column, bytes: &[u8]) -> Result<Vec<u8>> {
    match &col.elem {
        Some(e) => array_to_send(e, bytes),
        None => to_send_format(&col.typname, bytes),
    }
}

/// `array_send`: dimensions, then every element length-prefixed in its own
/// send format, row-major, with -1 for nulls.
fn array_to_send(elem: &ElemType, payload: &[u8]) -> Result<Vec<u8>> {
    // Alignment inside an array is computed from the start of the ArrayType,
    // which includes the 4-byte varlena header our payload has already lost.
    // Put it back so 8-byte-aligned element types land where Postgres put them.
    // ponytail: one copy per array datum. Thread an alignment bias through
    // read_attr instead if wide arrays ever show up in a profile.
    let mut buf = vec![0u8; 4];
    buf.extend_from_slice(payload);
    let b = &buf[..];

    let rd = |at: usize| -> Result<i32> {
        if at + 4 > b.len() {
            bail!("array truncated: wanted 4 bytes at {at}");
        }
        Ok(i32::from_le_bytes(b[at..at + 4].try_into().unwrap()))
    };
    let ndim = rd(4)?;
    let dataoffset = rd(8)?;
    let elemtype = rd(12)? as u32;
    if !(0..=6).contains(&ndim) {
        bail!("array claims {ndim} dimensions");
    }
    if elemtype != elem.oid {
        bail!("array holds element type {elemtype}, catalog says {}", elem.oid);
    }

    let ndim = ndim as usize;
    // On disk the dimensions come first as a block, then the lower bounds:
    // `dim[ndim], lbound[ndim]`. array_send writes them interleaved instead,
    // so the two layouts only coincide when ndim is 1.
    let mut dims = Vec::with_capacity(ndim);
    let mut lbounds = Vec::with_capacity(ndim);
    for i in 0..ndim {
        dims.push(rd(16 + 4 * i)?);
        lbounds.push(rd(16 + 4 * ndim + 4 * i)?);
    }
    // An empty array has zero dimensions, and the product of no dimensions is
    // 1, not 0 -- so it has to be special-cased or we read an element that
    // isn't there.
    let nitems: usize = if ndim == 0 {
        0
    } else {
        dims.iter().try_fold(1usize, |acc, d| {
            if *d < 0 {
                bail!("array dimension {d} is negative");
            }
            acc.checked_mul(*d as usize)
                .ok_or_else(|| anyhow::anyhow!("array element count overflows"))
        })?
    };

    // A null bitmap exists exactly when dataoffset is set.
    let has_nulls = dataoffset != 0;
    let bitmap_at = 16 + 8 * ndim;
    let data_at = if has_nulls {
        dataoffset as usize
    } else {
        // ARR_OVERHEAD_NONULLS: MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * ndim)
        crate::lsn::maxalign(16 + 8 * ndim as u64) as usize
    };

    let mut out = Buf::default();
    out.i32(ndim as i32).i32(i32::from(has_nulls)).i32(elem.oid as i32);
    for i in 0..ndim {
        out.i32(dims[i]).i32(lbounds[i]);
    }

    let att = crate::tuple::AttrDesc { attlen: elem.typlen, attalign: elem.typalign };
    let mut off = data_at;
    for i in 0..nitems {
        let is_null = has_nulls && {
            let byte = bitmap_at + (i >> 3);
            if byte >= b.len() {
                bail!("array null bitmap truncated at element {i}");
            }
            b[byte] & (1 << (i & 7)) == 0
        };
        if is_null {
            out.i32(-1);
            continue;
        }
        let (d, next) = crate::tuple::read_attr(b, off, &att)
            .with_context(|| format!("array element {i}"))?;
        off = next;
        let bytes = match &d {
            crate::tuple::Datum::Fixed(v) | crate::tuple::Datum::Var(v) => v,
            other => bail!("array element {i} decoded as {other:?}, which cannot appear in an array"),
        };
        let wire = to_send_format(&elem.typname, bytes)?;
        out.i32(wire.len() as i32).bytes(&wire);
    }
    Ok(out.0)
}

/// Convert an on-disk datum to what the type's `send()` function would emit.
///
/// Fixed-width types are native-endian on disk and big-endian on the wire.
/// Variable-length text-ish types pass through untouched. `numeric` is the one
/// type here whose on-disk packing genuinely differs from its wire form.
pub fn to_send_format(typname: &str, b: &[u8]) -> Result<Vec<u8>> {
    let swap = |n: usize| -> Result<Vec<u8>> {
        if b.len() != n {
            bail!("{typname} datum is {} bytes on disk, expected {n}", b.len());
        }
        Ok(b.iter().rev().copied().collect())
    };
    Ok(match typname {
        // Single bytes and opaque byte strings go as-is.
        "bool" | "char" | "bytea" | "text" | "varchar" | "bpchar" | "name" | "json" | "uuid" => {
            b.to_vec()
        }
        "int2" => swap(2)?,
        "int4" | "oid" | "float4" | "date" => swap(4)?,
        "int8" | "float8" | "time" | "timestamp" | "timestamptz" => swap(8)?,
        "interval" => {
            // struct Interval { TimeOffset time; int32 day; int32 month }
            if b.len() != 16 {
                bail!("interval datum is {} bytes, expected 16", b.len());
            }
            let mut o = Vec::with_capacity(16);
            o.extend(b[0..8].iter().rev());
            o.extend(b[8..12].iter().rev());
            o.extend(b[12..16].iter().rev());
            o
        }
        "numeric" => crate::numeric::to_send(&crate::numeric::parse(b)?),
        "jsonb" => {
            // jsonb_send is a version byte followed by the text rendering.
            let mut v = vec![1u8];
            v.extend_from_slice(crate::jsonb::to_text(b)?.as_bytes());
            v
        }
        other => bail!("no binary encoder for type {other}"),
    })
}


/// Postgres timestamps count microseconds from 2000-01-01 UTC.
pub fn now_pg_timestamp() -> i64 {
    const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs() as i64 - PG_EPOCH_UNIX_SECS) * 1_000_000 + d.subsec_micros() as i64
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_width_types_are_byteswapped_to_network_order() {
        assert_eq!(to_send_format("int4", &42i32.to_le_bytes()).unwrap(), 42i32.to_be_bytes());
        assert_eq!(to_send_format("int8", &(-9i64).to_le_bytes()).unwrap(), (-9i64).to_be_bytes());
        // Opaque byte strings must NOT be swapped.
        assert_eq!(to_send_format("text", b"hello").unwrap(), b"hello");
        assert_eq!(to_send_format("uuid", &[1u8; 16]).unwrap(), vec![1u8; 16]);
    }

    #[test]
    fn wrong_width_is_refused_rather_than_padded() {
        assert!(to_send_format("int4", &[1, 2]).is_err());
        assert!(to_send_format("interval", &[0; 12]).is_err());
    }

    #[test]
    fn numeric_is_repacked_not_byteswapped() {
        // 10.50 short-form on disk -> ndigits/weight/sign/dscale + digits.
        let disk = [0x00u8, 0x81, 0x0a, 0x00, 0x88, 0x13];
        let w = to_send_format("numeric", &disk).unwrap();
        assert_eq!(&w[0..2], &2i16.to_be_bytes());
        assert_eq!(&w[8..10], &10i16.to_be_bytes());
        assert_eq!(&w[10..12], &5000i16.to_be_bytes());
    }
}
