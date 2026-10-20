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

use crate::catalog::{Catalog, Column, Relation};
use crate::reorder::{Event, Kind};
use crate::tuple::Datum;
use anyhow::{bail, ensure, Context, Result};
use std::collections::HashMap;

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
    for rel in &cat.relations {
        for c in columns(rel) {
            if let Err(why) = encodable(cat, c.atttypid) {
                bad.push(format!("{}.{} is {} ({why})", rel.qualified_name(), c.attname, c.typname));
            }
        }
    }
    if !bad.is_empty() {
        bail!(
            "publication contains types this encoder cannot produce binary output for: {}\n\
             Supported: {}, inet, cidr, reg*, and enums, domains, composites and arrays of them",
            bad.join(", "),
            SUPPORTED.join(", ")
        );
    }
    Ok(())
}

const REG: &[&str] = &[
    "regclass", "regtype", "regproc", "regprocedure", "regoper", "regoperator", "regconfig",
    "regdictionary", "regnamespace", "regrole", "regcollation",
];

/// Whether `send` can handle every value of type `oid`, and if not, why.
fn encodable(cat: &Catalog, oid: u32) -> std::result::Result<(), String> {
    let t = cat.type_info(oid).ok_or_else(|| format!("type {oid} missing from the dictionary"))?;
    match t.typtype {
        'd' => encodable(cat, t.basetype.ok_or("domain without a base type")?),
        'e' => t.labels.as_ref().map(|_| ()).ok_or_else(|| "enum with no labels".into()),
        'c' => t.attrs.iter().flatten().filter(|a| !a.attisdropped).try_for_each(|a| encodable(cat, a.atttypid)),
        _ if t.elem.is_some() => encodable(cat, t.elem.unwrap()),
        _ if SUPPORTED.contains(&t.typname.as_str())
            || REG.contains(&t.typname.as_str())
            || t.typname == "inet"
            || t.typname == "cidr" =>
        {
            Ok(())
        }
        _ => Err(format!("no encoder for {}", t.typname)),
    }
}

/// Columns pgoutput publishes: not dropped, and not generated. Generated
/// columns are never sent before PG18, nor by PG18's default
/// publish_generated_columns = none; the subscriber computes its own.
// ponytail: a PG18 publication WITH (publish_generated_columns = stored) would
// need them sent; capture pubgencols in the dictionary if that ever matters.
fn published(c: &Column) -> bool {
    !c.attisdropped && c.attgenerated.is_empty()
}

fn columns(rel: &Relation) -> impl Iterator<Item = &Column> {
    rel.columns.iter().filter(|c| published(c))
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

#[derive(Default)]
pub struct Encoder {
    /// The definition each relation was last described to this subscriber
    /// with. A schema change brings a new dictionary and so a new definition,
    /// which is announced again before its first row, as Postgres does.
    announced: HashMap<u32, *const Relation>,
}

impl Encoder {
    pub fn new() -> Encoder {
        Encoder::default()
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
            Event::Change { rel, cat, kind, old, new, .. } => {
                let rel: &Relation = rel;
                if self.announced.insert(rel.oid, rel) != Some(rel as *const _) {
                    // A changed definition can bring a column type we cannot
                    // send; refuse before the subscriber sees any of it.
                    for c in columns(rel) {
                        if let Err(why) = encodable(cat, c.atttypid) {
                            self.announced.remove(&rel.oid);
                            bail!("{}.{} is {} ({why})", rel.qualified_name(), c.attname, c.typname);
                        }
                    }
                    out.push(relation_message(rel));
                }
                let mut b = Buf::default();
                match kind {
                    Kind::Insert => {
                        let new = new.as_ref().expect("INSERT always carries a new tuple");
                        b.u8(b'I').i32(rel.oid as i32).u8(b'N');
                        b.bytes(&tuple_data(cat, rel, new)?);
                    }
                    Kind::Update => {
                        let new = new.as_ref().expect("UPDATE always carries a new tuple");
                        b.u8(b'U').i32(rel.oid as i32);
                        if let Some(old) = old {
                            // 'O' is a whole old tuple (REPLICA IDENTITY FULL),
                            // 'K' is just the identity key.
                            b.u8(if rel.relreplident == 'f' { b'O' } else { b'K' });
                            b.bytes(&tuple_data(cat, rel, old)?);
                        }
                        b.u8(b'N');
                        b.bytes(&tuple_data(cat, rel, new)?);
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
                        b.bytes(&tuple_data(cat, rel, old)?);
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

fn tuple_data(cat: &Catalog, rel: &Relation, vals: &[Datum]) -> Result<Vec<u8>> {
    let mut b = Buf::default();
    b.i16(columns(rel).count() as i16);
    for (c, v) in rel.columns.iter().zip(vals) {
        if !published(c) {
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
                let wire = send(cat, c.atttypid, bytes)
                    .with_context(|| format!("{}.{}", rel.qualified_name(), c.attname))?;
                b.u8(b'b').i32(wire.len() as i32).bytes(&wire);
            }
        }
    }
    Ok(b.0)
}

/// What type `oid`'s `send()` would emit for these on-disk bytes.
pub fn send(cat: &Catalog, oid: u32, b: &[u8]) -> Result<Vec<u8>> {
    let t = cat.type_info(oid).with_context(|| format!("type {oid} missing from the dictionary"))?;
    match t.typtype {
        'd' => send(cat, t.basetype.context("domain without a base type")?, b),
        'e' => {
            // enum_send is the label; on disk it is the pg_enum row's Oid.
            let o = u32::from_le_bytes(b.try_into().context("enum datum is not 4 bytes")?);
            let (_, label) = t.labels.iter().flatten().find(|(e, _)| *e == o).with_context(|| {
                format!("enum {} has no value {o}; was a label added after T0?", t.typname)
            })?;
            Ok(label.as_bytes().to_vec())
        }
        'c' => record_send(cat, t, b),
        _ => match t.elem {
            Some(e) => array_to_send(cat, e, b),
            None => to_send_format(&t.typname, b),
        },
    }
}

/// `record_send`: the count of live attributes, then per attribute its type
/// Oid and its length-prefixed send() form, -1 for null. On disk a composite is
/// a heap tuple header without its first 4 bytes (the varlena header we have
/// already stripped), so the header fields sit 4 bytes early.
fn record_send(cat: &Catalog, t: &crate::catalog::TypeInfo, b: &[u8]) -> Result<Vec<u8>> {
    let attrs = t.attrs.as_ref().with_context(|| format!("composite {} has no attributes", t.typname))?;
    ensure!(b.len() >= 19, "{} datum truncated inside its tuple header", t.typname);
    let infomask2 = u16::from_le_bytes([b[14], b[15]]);
    let infomask = u16::from_le_bytes([b[16], b[17]]);
    let descs: Vec<_> = attrs
        .iter()
        .map(|a| crate::tuple::AttrDesc { attlen: a.attlen, attalign: a.attalign })
        .collect();
    let vals = crate::tuple::deform(&b[19..], infomask, infomask2, b[18], &descs)
        .with_context(|| format!("composite {}", t.typname))?;
    let mut out = Buf::default();
    out.i32(attrs.iter().filter(|a| !a.attisdropped).count() as i32);
    for (a, v) in attrs.iter().zip(&vals) {
        if a.attisdropped {
            continue;
        }
        out.i32(a.atttypid as i32);
        match v {
            Datum::Null => {
                out.i32(-1);
            }
            Datum::Fixed(v) | Datum::Var(v) => {
                let wire = send(cat, a.atttypid, v)?;
                out.i32(wire.len() as i32).bytes(&wire);
            }
            other => bail!("{} attribute decoded as {other:?}", t.typname),
        }
    }
    Ok(out.0)
}

/// `array_send`: dimensions, then every element length-prefixed in its own
/// send format, row-major, with -1 for nulls.
fn array_to_send(cat: &Catalog, elem_oid: u32, payload: &[u8]) -> Result<Vec<u8>> {
    let elem = cat.type_info(elem_oid).with_context(|| format!("array element type {elem_oid} missing"))?;
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
        let wire = send(cat, elem.oid, bytes)?;
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
        "bool" | "char" | "bytea" | "text" | "varchar" | "bpchar" | "json" | "uuid" => {
            b.to_vec()
        }
        "name" => {
            // namesend emits strlen bytes; the datum is NUL-padded to 64 and
            // namerecv rejects anything at or above NAMEDATALEN.
            let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            b[..end].to_vec()
        }
        "int2" => swap(2)?,
        "int4" | "oid" | "float4" | "date" => swap(4)?,
        t if REG.contains(&t) => swap(4)?,
        "inet" | "cidr" => {
            // On disk: family, bits, address. inet_send adds is_cidr and the
            // address length between them and the address.
            ensure!(
                b.len() == 6 || b.len() == 18,
                "{typname} datum is {} bytes, expected 6 or 18",
                b.len()
            );
            let mut o = vec![b[0], b[1], u8::from(typname == "cidr"), (b.len() - 2) as u8];
            o.extend_from_slice(&b[2..]);
            o
        }
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

    #[test]
    fn name_is_sent_as_strlen_bytes() {
        let mut disk = b"tbl".to_vec();
        disk.resize(64, 0); // NUL-padded to NAMEDATALEN on disk
        assert_eq!(to_send_format("name", &disk).unwrap(), b"tbl");
    }
}
