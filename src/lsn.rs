//! Log sequence numbers and WAL segment naming.

use std::fmt;

pub const XLOG_BLCKSZ: u64 = 8192;
pub const DEFAULT_WAL_SEG_SIZE: u64 = 16 * 1024 * 1024;

/// MAXALIGN for a 64-bit build. A 32-bit source (i386, wasm32) aligns to 4 and
/// lays tuples out differently, so its WAL is not readable here at all.
pub const MAXIMUM_ALIGNOF: u64 = 8;

pub const fn maxalign(n: u64) -> u64 {
    (n + MAXIMUM_ALIGNOF - 1) & !(MAXIMUM_ALIGNOF - 1)
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    pub fn parse(s: &str) -> anyhow::Result<Lsn> {
        let (hi, lo) = s
            .split_once('/')
            .ok_or_else(|| anyhow::anyhow!("bad LSN {s:?}, want form 0/1A2B3C4"))?;
        let hi = u32::from_str_radix(hi.trim(), 16)?;
        let lo = u32::from_str_radix(lo.trim(), 16)?;
        Ok(Lsn(((hi as u64) << 32) | lo as u64))
    }

    pub fn segment_no(&self, seg_size: u64) -> u64 {
        self.0 / seg_size
    }

    /// Byte offset of this LSN within its segment.
    pub fn segment_offset(&self, seg_size: u64) -> u64 {
        self.0 % seg_size
    }

    /// Offset within the 8kB page.
    pub fn page_offset(&self) -> u64 {
        self.0 % XLOG_BLCKSZ
    }
}

/// `000000010000000000000002` — tli, then the segment number split into the
/// high 32 bits of the LSN and the segment index within it.
pub fn segment_name(tli: u32, segno: u64, seg_size: u64) -> String {
    let segs_per_id = 0x1_0000_0000u64 / seg_size;
    format!(
        "{:08X}{:08X}{:08X}",
        tli,
        segno / segs_per_id,
        segno % segs_per_id
    )
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 as u32)
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_roundtrip_and_segment_naming() {
        let l = Lsn::parse("0/202FA68").unwrap();
        assert_eq!(l.0, 0x202FA68);
        assert_eq!(l.to_string(), "0/202FA68");
        // 0x202FA68 / 16MB == segment 2
        assert_eq!(l.segment_no(DEFAULT_WAL_SEG_SIZE), 2);
        assert_eq!(
            segment_name(1, l.segment_no(DEFAULT_WAL_SEG_SIZE), DEFAULT_WAL_SEG_SIZE),
            "000000010000000000000002"
        );
        // segment 256 rolls into the high word: 256 * 16MB == 0x1_00000000
        assert_eq!(
            segment_name(1, 256, DEFAULT_WAL_SEG_SIZE),
            "000000010000000100000000"
        );
        assert_eq!(maxalign(20), 24);
        assert_eq!(maxalign(24), 24);
    }
}
