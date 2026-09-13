//! Heap tuple deform against the frozen catalog: null bitmap, alignment,
//! varlena headers, TOAST pointers, pglz.

use anyhow::{bail, ensure, Context, Result};

pub const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;

pub const HEAP_HASNULL: u16 = 0x0001;
pub const HEAP_NATTS_MASK: u16 = 0x07FF;

const VARTAG_ONDISK: u8 = 18;
const VARLENA_EXTSIZE_BITS: u32 = 30;
const VARLENA_EXTSIZE_MASK: u32 = (1 << VARLENA_EXTSIZE_BITS) - 1;

pub const TOAST_PGLZ: u8 = 0;
pub const TOAST_LZ4: u8 = 1;

/// The subset of `pg_attribute` deform actually needs.
#[derive(Clone, Copy, Debug)]
pub struct AttrDesc {
    pub attlen: i16,
    pub attalign: char,
}

impl From<&crate::catalog::Column> for AttrDesc {
    fn from(c: &crate::catalog::Column) -> Self {
        AttrDesc { attlen: c.attlen, attalign: c.attalign }
    }
}

/// The three columns of every TOAST heap. Fixed by Postgres, so no catalog
/// lookup is needed to deform a chunk.
pub const TOAST_CHUNK_ATTRS: [AttrDesc; 3] = [
    AttrDesc { attlen: 4, attalign: 'i' },  // chunk_id  oid
    AttrDesc { attlen: 4, attalign: 'i' },  // chunk_seq int4
    AttrDesc { attlen: -1, attalign: 'i' }, // chunk_data bytea
];

#[derive(Clone, Debug, PartialEq)]
pub enum Datum {
    Null,
    /// Fixed-length type, raw on-disk bytes.
    Fixed(Vec<u8>),
    /// Variable-length type, fully detoasted and decompressed, header stripped.
    Var(Vec<u8>),
    /// External TOAST pointer, chunks not yet stitched in.
    Toast(ToastPointer),
    /// Present in the tuple but the WAL does not carry the value: an UPDATE
    /// left an external TOAST value untouched, so no chunks were rewritten.
    /// pgoutput sends these as the `u` (unchanged) marker.
    UnchangedToast,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ToastPointer {
    pub raw_size: i32,
    pub ext_size: u32,
    pub method: u8,
    pub value_id: u32,
    pub toast_relid: u32,
}

impl ToastPointer {
    pub fn is_compressed(&self) -> bool {
        // Uncompressed when the external size accounts for the whole raw value
        // minus its 4-byte varlena header.
        self.ext_size != (self.raw_size as u32).wrapping_sub(4)
    }
}

fn align_to(off: usize, a: char) -> usize {
    let n = match a {
        'c' => 1,
        's' => 2,
        'i' => 4,
        'd' => 8,
        _ => 1,
    };
    (off + n - 1) & !(n - 1)
}

/// Total on-disk size of the varlena at `p[0..]`, header included.
fn varsize_any(p: &[u8]) -> Result<usize> {
    ensure!(!p.is_empty(), "varlena read past end of tuple");
    let h = p[0];
    if h == 0x01 {
        // External: 1 byte header, 1 byte tag, then the tag-specific struct.
        ensure!(p.len() >= 2, "truncated external TOAST pointer");
        let size = match p[1] {
            VARTAG_ONDISK => 18,
            1 => 2 + std::mem::size_of::<usize>() + 4, // INDIRECT, never in WAL
            other => bail!("unsupported vartag {other} in WAL tuple"),
        };
        Ok(size)
    } else if h & 0x01 == 0x01 {
        Ok((h >> 1) as usize & 0x7F)
    } else {
        ensure!(p.len() >= 4, "truncated 4-byte varlena header");
        let w = u32::from_le_bytes(p[0..4].try_into()?);
        Ok(((w >> 2) & 0x3FFF_FFFF) as usize)
    }
}

/// Decode one varlena to its payload, decompressing inline-compressed values
/// and surfacing external pointers for the reorder buffer to stitch. Called
/// before `varsize_any`, so every slice needs its own bound: the tuple bytes
/// come from WAL, where zeros are the most common corruption pattern.
fn decode_varlena(p: &[u8]) -> Result<Datum> {
    ensure!(!p.is_empty(), "varlena read past end of tuple");
    let h = p[0];
    if h == 0x01 {
        ensure!(p.len() >= 18, "truncated external TOAST pointer");
        ensure!(p[1] == VARTAG_ONDISK, "unexpected vartag {} in WAL", p[1]);
        let b = &p[2..18];
        let raw_size = i32::from_le_bytes(b[0..4].try_into()?);
        // PG14 carved the top 2 bits of va_extinfo out for the compression
        // method. Before that the field was a plain va_extsize and no real
        // value reaches 2^30, so those bits read as 0 == pglz either way; the
        // unpacking needs no version branch.
        let extinfo = u32::from_le_bytes(b[4..8].try_into()?);
        return Ok(Datum::Toast(ToastPointer {
            raw_size,
            ext_size: extinfo & VARLENA_EXTSIZE_MASK,
            method: (extinfo >> VARLENA_EXTSIZE_BITS) as u8,
            value_id: u32::from_le_bytes(b[8..12].try_into()?),
            toast_relid: u32::from_le_bytes(b[12..16].try_into()?),
        }));
    }
    if h & 0x01 == 0x01 {
        // Short header: never compressed, never external.
        let total = (h >> 1) as usize & 0x7F;
        ensure!(p.len() >= total, "short-header varlena claims {total} bytes, {} present", p.len());
        return Ok(Datum::Var(p[1..total].to_vec()));
    }
    ensure!(p.len() >= 4, "truncated 4-byte varlena header");
    let w = u32::from_le_bytes(p[0..4].try_into()?);
    let total = ((w >> 2) & 0x3FFF_FFFF) as usize;
    ensure!(total >= 4 && p.len() >= total, "varlena claims {total} bytes, {} present", p.len());
    if w & 0x03 == 0x02 {
        // Compressed inline: 4-byte header, then va_tcinfo, then the stream.
        ensure!(total >= 8, "compressed varlena too short for its header");
        let tcinfo = u32::from_le_bytes(p[4..8].try_into()?);
        let raw = (tcinfo & VARLENA_EXTSIZE_MASK) as usize;
        let method = (tcinfo >> VARLENA_EXTSIZE_BITS) as u8;
        return Ok(Datum::Var(decompress(&p[8..total], raw, method)?));
    }
    Ok(Datum::Var(p[4..total].to_vec()))
}

pub fn decompress(src: &[u8], raw_len: usize, method: u8) -> Result<Vec<u8>> {
    match method {
        TOAST_PGLZ => crate::pglz::decompress(src, raw_len),
        TOAST_LZ4 => bail!(
            "value uses lz4 TOAST compression; rebuild with the lz4 codec or set \
             default_toast_compression=pglz on the source"
        ),
        other => bail!("unknown TOAST compression method {other}"),
    }
}

/// The first attribute of a WAL-form tuple, read as an Oid.
///
/// `pg_class` and `pg_attribute` both lead with a 4-byte Oid identifying the
/// relation (`oid` and `attrelid` respectively), and neither can be null, so
/// the value sits at the start of the user data with no walking required.
pub fn leading_oid(data: &[u8], t_hoff: u8) -> Option<u32> {
    let at = (t_hoff as usize).checked_sub(SIZEOF_HEAP_TUPLE_HEADER)?;
    data.get(at..at + 4).map(|b| u32::from_le_bytes(b.try_into().unwrap()))
}

/// Deform a tuple as stored in WAL, i.e. everything from `SizeofHeapTupleHeader`
/// onward: the null bitmap, alignment padding, then user data at `t_hoff`.
pub fn deform(
    data: &[u8],
    infomask: u16,
    infomask2: u16,
    t_hoff: u8,
    attrs: &[AttrDesc],
) -> Result<Vec<Datum>> {
    let natts = (infomask2 & HEAP_NATTS_MASK) as usize;
    ensure!(
        natts <= attrs.len(),
        "tuple has {natts} attributes but the frozen catalog knows {}; the source \
         schema changed after T0",
        attrs.len()
    );
    let has_nulls = infomask & HEAP_HASNULL != 0;
    let bitmap = if has_nulls {
        let len = natts.div_ceil(8);
        ensure!(data.len() >= len, "tuple truncated inside null bitmap");
        &data[..len]
    } else {
        &[][..]
    };

    let data_start = (t_hoff as usize)
        .checked_sub(SIZEOF_HEAP_TUPLE_HEADER)
        .ok_or_else(|| anyhow::anyhow!("t_hoff {t_hoff} below the tuple header size"))?;
    ensure!(data_start <= data.len(), "t_hoff {t_hoff} points past the tuple");
    let tp = &data[data_start..];

    let mut out = Vec::with_capacity(attrs.len());
    let mut off = 0usize;
    for (i, att) in attrs.iter().enumerate() {
        if i >= natts {
            // Column added after this row was written.
            out.push(Datum::Null);
            continue;
        }
        if has_nulls && bitmap[i >> 3] & (1 << (i & 7)) == 0 {
            out.push(Datum::Null);
            continue;
        }
        let (d, next) = read_attr(tp, off, att)
            .with_context(|| format!("attribute {}", i + 1))?;
        out.push(d);
        off = next;
    }
    Ok(out)
}

/// Read one attribute at `off`, returning it and the offset just past it.
///
/// Shared by tuple deform and the array walker: array elements obey exactly the
/// same alignment and varlena rules as tuple columns.
pub fn read_attr(tp: &[u8], mut off: usize, att: &AttrDesc) -> Result<(Datum, usize)> {
    if att.attlen == -1 {
        // A varlena is only aligned when the byte at the unaligned offset is a
        // pad byte; short headers are packed tight.
        if off < tp.len() && tp[off] == 0 {
            off = align_to(off, att.attalign);
        }
        ensure!(off < tp.len(), "truncated before a varlena");
        let d = decode_varlena(&tp[off..])?;
        Ok((d, off + varsize_any(&tp[off..])?))
    } else if att.attlen == -2 {
        let end = tp[off..]
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| anyhow::anyhow!("unterminated cstring"))?;
        Ok((Datum::Var(tp[off..off + end].to_vec()), off + end + 1))
    } else {
        off = align_to(off, att.attalign);
        let n = att.attlen as usize;
        ensure!(off + n <= tp.len(), "truncated inside a fixed-width attribute");
        Ok((Datum::Fixed(tp[off..off + n].to_vec()), off + n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INT4: AttrDesc = AttrDesc { attlen: 4, attalign: 'i' };
    const TEXT: AttrDesc = AttrDesc { attlen: -1, attalign: 'i' };
    const BOOL: AttrDesc = AttrDesc { attlen: 1, attalign: 'c' };

    /// Build the WAL-form tuple body: null bitmap + padding to t_hoff + data.
    fn tuple(natts: u16, nulls: Option<&[bool]>, data: &[u8]) -> (Vec<u8>, u16, u16, u8) {
        let mut buf = Vec::new();
        let infomask = if nulls.is_some() { HEAP_HASNULL } else { 0 };
        if let Some(n) = nulls {
            let mut bm = vec![0u8; (natts as usize).div_ceil(8)];
            for (i, notnull) in n.iter().enumerate() {
                if *notnull {
                    bm[i >> 3] |= 1 << (i & 7);
                }
            }
            buf.extend_from_slice(&bm);
        }
        // t_hoff is MAXALIGN(23 + bitmaplen); our body starts at offset 23.
        let t_hoff = crate::lsn::maxalign(SIZEOF_HEAP_TUPLE_HEADER as u64 + buf.len() as u64) as u8;
        buf.resize(t_hoff as usize - SIZEOF_HEAP_TUPLE_HEADER, 0);
        buf.extend_from_slice(data);
        (buf, infomask, natts, t_hoff)
    }

    #[test]
    fn fixed_width_with_alignment() {
        // bool at 0, then int4 must skip 3 pad bytes to offset 4.
        let mut d = vec![1u8, 0, 0, 0];
        d.extend_from_slice(&42i32.to_le_bytes());
        let (b, im, im2, hoff) = tuple(2, None, &d);
        let vals = deform(&b, im, im2, hoff, &[BOOL, INT4]).unwrap();
        assert_eq!(vals[0], Datum::Fixed(vec![1]));
        assert_eq!(vals[1], Datum::Fixed(42i32.to_le_bytes().to_vec()));
    }

    #[test]
    fn short_varlena_is_packed_without_alignment() {
        let mut d = 7i32.to_le_bytes().to_vec();
        // Short header: (len+1) << 1 | 1, payload "hi".
        d.push((3 << 1) | 1);
        d.extend_from_slice(b"hi");
        let (b, im, im2, hoff) = tuple(2, None, &d);
        let vals = deform(&b, im, im2, hoff, &[INT4, TEXT]).unwrap();
        assert_eq!(vals[1], Datum::Var(b"hi".to_vec()));
    }

    #[test]
    fn nulls_are_skipped_and_consume_no_space() {
        let d = 9i32.to_le_bytes().to_vec();
        let (b, im, im2, hoff) = tuple(2, Some(&[false, true]), &d);
        let vals = deform(&b, im, im2, hoff, &[TEXT, INT4]).unwrap();
        assert_eq!(vals[0], Datum::Null);
        assert_eq!(vals[1], Datum::Fixed(9i32.to_le_bytes().to_vec()));
    }

    #[test]
    fn external_toast_pointer_is_surfaced_not_inlined() {
        let mut d = Vec::new();
        d.push(0x01);
        d.push(VARTAG_ONDISK);
        d.extend_from_slice(&9604i32.to_le_bytes()); // raw_size
        d.extend_from_slice(&9600u32.to_le_bytes()); // extinfo: uncompressed
        d.extend_from_slice(&16391u32.to_le_bytes()); // value_id
        d.extend_from_slice(&16388u32.to_le_bytes()); // toast relid
        let (b, im, im2, hoff) = tuple(1, None, &d);
        let vals = deform(&b, im, im2, hoff, &[TEXT]).unwrap();
        match &vals[0] {
            Datum::Toast(t) => {
                assert_eq!(t.value_id, 16391);
                assert_eq!(t.ext_size, 9600);
                assert!(!t.is_compressed(), "extsize == rawsize-4 means uncompressed");
            }
            other => panic!("expected a TOAST pointer, got {other:?}"),
        }
    }

    #[test]
    fn tuple_with_more_attrs_than_catalog_is_refused() {
        let d = 1i32.to_le_bytes().to_vec();
        let (b, im, _, hoff) = tuple(1, None, &d);
        // Claim 5 attributes against a 1-column catalog: schema drifted.
        let err = deform(&b, im, 5, hoff, &[INT4]).unwrap_err().to_string();
        assert!(err.contains("schema changed after T0"), "got: {err}");
    }

    #[test]
    fn zeroed_varlena_is_an_error_not_a_panic() {
        // Four zero bytes: 4-byte header claiming a total size of 0.
        let (b, im, im2, hoff) = tuple(1, None, &[0, 0, 0, 0]);
        assert!(deform(&b, im, im2, hoff, &[TEXT]).is_err());
        // Short header claiming 15 bytes when one is present.
        let (b, im, im2, hoff) = tuple(1, None, &[0x1F]);
        assert!(deform(&b, im, im2, hoff, &[TEXT]).is_err());
        // Truncated external pointer.
        let (b, im, im2, hoff) = tuple(1, None, &[0x01, VARTAG_ONDISK, 0, 0]);
        assert!(deform(&b, im, im2, hoff, &[TEXT]).is_err());
    }
}
