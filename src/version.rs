//! Everything that differs between Postgres major versions, in one file.
//!
//! The WAL record format has been stable since the 9.5 rewrite: `XLogRecord`,
//! the page headers, the block-reference headers, `RelFileLocator` (renamed
//! from `RelFileNode` in 16, same layout), the heap record structs and the
//! varlena/TOAST encodings are all unchanged across the range we care about.
//!
//! Two things are not stable, and both are handled here rather than by
//! hardcoding a build-time constant:
//!
//!   * `XLOG_PAGE_MAGIC` changes almost every major. It is not pinned at all --
//!     `xlog.rs` learns it from the first page and then requires every
//!     subsequent page to agree. Provenance is instead proven by matching the
//!     segment's `xlp_sysid` against the catalog dictionary's
//!     `system_identifier`, which is a far stronger check than a version
//!     fingerprint.
//!
//!   * PG15 split `BKPIMAGE_IS_COMPRESSED` into per-codec flags and reused the
//!     vacated bit for `BKPIMAGE_APPLY`. That changes whether a block-image
//!     header carries a trailing hole-length word, i.e. it moves the read
//!     cursor. See `image_is_compressed`.

use anyhow::{bail, Result};

/// `server_version_num`, e.g. 160004 for 16.4. Taken from the catalog
/// dictionary, which was captured from the source server itself.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct PgVersion(pub u32);

/// Logical replication arrived in PG10. Below that there is no subscriber to
/// feed, so there is nothing for this tool to do.
const MIN_MAJOR: u32 = 10;
/// Highest major whose WAL format has actually been exercised by the test
/// suite. `scripts/check-all.sh` currently runs 14, 16, 17 and 18 against real
/// clusters. 10-13 are accepted on the strength of the format being unchanged,
/// not on a passing test.
const VERIFIED_MAX_MAJOR: u32 = 18;

impl PgVersion {
    pub fn major(&self) -> u32 {
        self.0 / 10000
    }

    pub fn check_supported(&self) -> Result<()> {
        if self.major() < MIN_MAJOR {
            bail!(
                "source server is PG {}, but logical replication requires {MIN_MAJOR} or later",
                self.major()
            );
        }
        if self.major() > VERIFIED_MAX_MAJOR {
            eprintln!(
                "warning: source server is PG {}, newer than the most recent version this \
                 decoder has been verified against (PG {VERIFIED_MAX_MAJOR}). The WAL record \
                 format has been stable since 9.5, but check the release notes for changes to \
                 xlogrecord.h and heapam_xlog.h.",
                self.major()
            );
        }
        Ok(())
    }

    /// Does this block-image header describe a compressed image?
    ///
    /// PG <= 14:  HAS_HOLE 0x01 | IS_COMPRESSED 0x02 | APPLY 0x04
    /// PG >= 15:  HAS_HOLE 0x01 | APPLY 0x02 | PGLZ 0x04 | LZ4 0x08 | ZSTD 0x10
    ///
    /// Getting this wrong on a compressed image desynchronises the record
    /// cursor by two bytes, which then fails CRC rather than corrupting
    /// silently -- but only if `wal_compression` was ever enabled.
    pub fn image_is_compressed(&self, bimg_info: u8) -> bool {
        if self.major() >= 15 {
            bimg_info & (BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD)
                != 0
        } else {
            bimg_info & BKPIMAGE_IS_COMPRESSED_PRE15 != 0
        }
    }

}

const BKPIMAGE_IS_COMPRESSED_PRE15: u8 = 0x02;
pub const BKPIMAGE_COMPRESS_PGLZ: u8 = 0x04;
pub const BKPIMAGE_COMPRESS_LZ4: u8 = 0x08;
pub const BKPIMAGE_COMPRESS_ZSTD: u8 = 0x10;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_flag_moved_in_pg15() {
        let pg14 = PgVersion(140012);
        let pg15 = PgVersion(150007);
        // 0x02 is IS_COMPRESSED before 15 and APPLY from 15 on.
        assert!(pg14.image_is_compressed(0x02));
        assert!(!pg15.image_is_compressed(0x02));
        // 0x04 is APPLY before 15 and COMPRESS_PGLZ from 15 on.
        assert!(!pg14.image_is_compressed(0x04));
        assert!(pg15.image_is_compressed(0x04));
        // A plain uncompressed image with a hole reads the same either way.
        assert!(!pg14.image_is_compressed(0x01));
        assert!(!pg15.image_is_compressed(0x01));
    }

    #[test]
    fn version_floor_is_enforced() {
        assert!(PgVersion(90604).check_supported().is_err());
        assert!(PgVersion(100023).check_supported().is_ok());
        assert!(PgVersion(180004).check_supported().is_ok());
    }

    #[test]
    fn major_is_extracted_from_server_version_num() {
        assert_eq!(PgVersion(180004).major(), 18);
        assert_eq!(PgVersion(140012).major(), 14);
    }
}
