//! Physical WAL segment reader: page framing, record assembly, CRC, block refs.
//!
//! This is the part `pg_walstream` and friends do not have. They parse pgoutput,
//! which is the *output* of logical decoding. This parses what is actually in S3.

use crate::lsn::{maxalign, Lsn, XLOG_BLCKSZ};
use crate::version::PgVersion;
use anyhow::{bail, ensure, Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub const SIZE_OF_XLOG_RECORD: u64 = 24;
const SIZE_OF_XLOG_SHORT_PHD: u64 = 24;
const SIZE_OF_XLOG_LONG_PHD: u64 = 40;

const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
const XLP_LONG_HEADER: u16 = 0x0002;
const XLP_FIRST_IS_OVERWRITE_CONTRECORD: u16 = 0x0008;

const XLR_MAX_BLOCK_ID: u8 = 32;
const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

const BKPBLOCK_FORK_MASK: u8 = 0x0F;
const BKPBLOCK_HAS_IMAGE: u8 = 0x10;
const BKPBLOCK_HAS_DATA: u8 = 0x20;
const BKPBLOCK_SAME_REL: u8 = 0x80;

const BKPIMAGE_HAS_HOLE: u8 = 0x01;

// Order is fixed by rmgrlist.h; getting these off by one silently decodes
// btree records as heap ones.
pub const RM_XLOG_ID: u8 = 0;
pub const RM_XACT_ID: u8 = 1;
pub const RM_HEAP2_ID: u8 = 9;
pub const RM_HEAP_ID: u8 = 10;

pub const MAIN_FORKNUM: u8 = 0;

/// `XLOG_SWITCH` abandons the rest of the segment; everything after it is
/// zero padding, not records.
const XLOG_SWITCH: u8 = 0x40;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct RelFileLocator {
    pub spc_oid: u32,
    pub db_oid: u32,
    pub rel_number: u32,
}

#[derive(Clone, Debug)]
pub struct BlockRef {
    pub id: u8,
    pub rlocator: RelFileLocator,
    pub forknum: u8,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct Record {
    pub lsn: Lsn,
    /// Where the next record starts: the end of this one rounded up to
    /// MAXALIGN, matching xlogreader's `EndRecPtr`. pgoutput's Commit message
    /// carries it as the transaction's end position, and the subscriber's flush
    /// feedback is reported against it, so the rounding is not cosmetic.
    pub end_lsn: Lsn,
    /// `xl_tot_len`: the record's own length, excluding any page headers it
    /// straddles. Distinct from `end_lsn - lsn`.
    pub tot_len: u32,
    pub xid: u32,
    pub rmid: u8,
    /// Top 4 bits are the rmgr-specific opcode; low 4 are XLR_* flags.
    pub info: u8,
    pub main_data: Vec<u8>,
    pub blocks: Vec<BlockRef>,
    pub toplevel_xid: Option<u32>,
}

impl Record {
    /// Heap and Heap2 opcode. The mask is 0x70, not 0xF0: bit 0x80 is
    /// XLOG_HEAP_INIT_PAGE, a flag rather than part of the opcode.
    pub fn heap_opcode(&self) -> u8 {
        self.info & 0x70
    }

    pub fn block(&self, id: u8) -> Option<&BlockRef> {
        self.blocks.iter().find(|b| b.id == id)
    }
}

/// The next segment has not been archived yet. Distinct from a read error:
/// callers stop cleanly and retry rather than treating it as corruption.
#[derive(Debug)]
pub struct SegmentUnavailable(pub String);

impl std::fmt::Display for SegmentUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WAL segment {} is not in the archive yet", self.0)
    }
}

impl std::error::Error for SegmentUnavailable {}

/// Where segments come from. `dir` is a local cache; if a segment is missing
/// from it and wal-g is configured (`WALG_S3_PREFIX` and/or `PGS3_WALG_CONFIG`
/// set), it is fetched with `wal-g wal-fetch` before falling back to "not
/// archived yet". Everything about the bucket itself — creds, prefix, region
/// — comes from wal-g's own config, whether that's env vars or a config file.
pub struct SegmentDir {
    dir: PathBuf,
    cache: HashMap<u64, Vec<u8>>,
    /// The segment wal-g last failed to fetch, when that was, and how long to
    /// wait before asking for it again.
    last_miss: Option<(String, Instant, Duration)>,
}

const FETCH_RETRY_MIN: Duration = Duration::from_secs(1);
const FETCH_RETRY_MAX: Duration = Duration::from_secs(30);

/// Whether to shell out to wal-g again for `name`. Waiting for the next
/// segment to be archived means being asked for it at the caller's polling
/// rate — several times a second — and every ask is a wal-g process and a set
/// of S3 requests. So the wait after a miss doubles up to `FETCH_RETRY_MAX`,
/// and only for the segment that missed: any other name is a fresh miss and is
/// fetched immediately, which is what keeps catch-up reading at full speed.
fn fetch_is_due(last_miss: Option<(&str, Duration, Duration)>, name: &str) -> bool {
    match last_miss {
        Some((missed, ago, wait)) => missed != name || ago >= wait,
        None => true,
    }
}

fn next_wait(prev: Option<Duration>) -> Duration {
    match prev {
        None => FETCH_RETRY_MIN,
        Some(w) => (w * 2).min(FETCH_RETRY_MAX),
    }
}

impl SegmentDir {
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self { dir: dir.as_ref().to_path_buf(), cache: HashMap::new(), last_miss: None }
    }

    /// `wal-g [--config <path>] wal-fetch <segment> <dest>`. Returns Ok(false)
    /// rather than an error when wal-g exits nonzero: that means "not archived
    /// yet", the same as a missing file, and the caller retries instead of
    /// aborting. Repeat asks for a segment that keeps missing are spaced out by
    /// `fetch_is_due`.
    fn walg_fetch(&mut self, name: &str, dest: &Path) -> Result<bool> {
        let config = std::env::var("PGS3_WALG_CONFIG").ok();
        if config.is_none() && std::env::var_os("WALG_S3_PREFIX").is_none() {
            return Ok(false);
        }
        let due = {
            let last =
                self.last_miss.as_ref().map(|(n, at, wait)| (n.as_str(), at.elapsed(), *wait));
            fetch_is_due(last, name)
        };
        if !due {
            return Ok(false);
        }
        let walg = std::env::var("PGS3_WALG_BIN").unwrap_or_else(|_| "wal-g".into());
        let mut cmd = std::process::Command::new(&walg);
        if let Some(path) = &config {
            cmd.args(["--config", path]);
        }
        let status = cmd
            .args(["wal-fetch", name, dest.to_str().context("--wal path is not valid UTF-8")?])
            .status()
            .with_context(|| format!("running `{walg} wal-fetch {name}`"))?;
        if status.success() {
            self.last_miss = None;
            return Ok(true);
        }
        let prev =
            self.last_miss.as_ref().filter(|(n, _, _)| n == name).map(|(_, _, wait)| *wait);
        self.last_miss = Some((name.to_string(), Instant::now(), next_wait(prev)));
        Ok(false)
    }

    fn get(&mut self, tli: u32, segno: u64, seg_size: u64) -> Result<&[u8]> {
        if !self.cache.contains_key(&segno) {
            let name = crate::lsn::segment_name(tli, segno, seg_size);
            let path = self.dir.join(&name);
            if !path.exists() && !self.walg_fetch(&name, &path)? {
                // Not on disk and wal-g couldn't get it either: treat exactly
                // like a missing file below, since we only ever read forward.
                return Err(anyhow::Error::new(SegmentUnavailable(name)));
            }
            let bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(anyhow::Error::new(SegmentUnavailable(name)));
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e))
                        .with_context(|| format!("reading WAL segment {name} from {:?}", self.dir))
                }
            };
            ensure!(
                bytes.len() as u64 == seg_size,
                "segment {name} is {} bytes, expected {seg_size}",
                bytes.len()
            );
            // Keep only one segment resident; records never span more than two.
            if self.cache.len() > 1 {
                self.cache.retain(|k, _| *k + 1 >= segno);
            }
            self.cache.insert(segno, bytes);
        }
        Ok(self.cache.get(&segno).unwrap())
    }
}

pub struct WalReader {
    src: SegmentDir,
    tli: u32,
    seg_size: u64,
    pos: Lsn,
    end: Lsn,
    version: PgVersion,
    /// XLOG_PAGE_MAGIC is not pinned to a build. It is learned from the first
    /// page and then enforced across every later one, so a segment from a
    /// different major version cannot be spliced into the stream.
    page_magic: Option<u16>,
    /// `system_identifier` from the catalog dictionary, when available. Proves
    /// the archive and the dictionary came from the same cluster.
    expect_sysid: Option<u64>,
    sysid: Option<u64>,
}

impl WalReader {
    pub fn new(
        src: SegmentDir,
        tli: u32,
        seg_size: u64,
        start: Lsn,
        end: Lsn,
        version: PgVersion,
        expect_sysid: Option<u64>,
    ) -> Self {
        Self {
            src,
            tli,
            seg_size,
            pos: start,
            end,
            version,
            page_magic: None,
            expect_sysid,
            sysid: None,
        }
    }

    /// Validate the page header at `self.pos` (which must be page-aligned) and
    /// step past it. `continuing` is true when a record is mid-flight, in which
    /// case the page must claim to continue one.
    fn consume_page_header(&mut self, continuing: bool) -> Result<()> {
        debug_assert_eq!(self.pos.page_offset(), 0);
        let segno = self.pos.segment_no(self.seg_size);
        let off = self.pos.segment_offset(self.seg_size) as usize;
        let seg = self.src.get(self.tli, segno, self.seg_size)?;
        let hdr = &seg[off..off + SIZE_OF_XLOG_LONG_PHD as usize];

        let magic = u16::from_le_bytes([hdr[0], hdr[1]]);
        let info = u16::from_le_bytes([hdr[2], hdr[3]]);
        let tli = u32::from_le_bytes(hdr[4..8].try_into()?);
        let pageaddr = u64::from_le_bytes(hdr[8..16].try_into()?);
        let rem_len = u32::from_le_bytes(hdr[16..20].try_into()?);

        match self.page_magic {
            // First page seen: adopt its magic as the expected one. A page that
            // is not WAL at all still fails the pageaddr and CRC checks below.
            None => self.page_magic = Some(magic),
            Some(expected) => ensure!(
                magic == expected,
                "page at {} has magic {magic:#06x}, earlier pages had {expected:#06x}; \
                 segments from two different major versions in one stream",
                self.pos
            ),
        }
        ensure!(
            pageaddr == self.pos.0,
            "page at {} claims to be {}; torn or misaligned segment",
            self.pos,
            Lsn(pageaddr)
        );
        ensure!(tli == self.tli, "page at {} has tli {tli}, expected {}", self.pos, self.tli);

        let is_cont = info & XLP_FIRST_IS_CONTRECORD != 0;
        if continuing {
            ensure!(
                is_cont,
                "record continues past {} but next page is not marked CONTRECORD",
                self.pos
            );
            ensure!(rem_len > 0, "CONTRECORD page at {} has zero xlp_rem_len", self.pos);
        }
        if is_cont && info & XLP_FIRST_IS_OVERWRITE_CONTRECORD != 0 && !continuing {
            bail!("overwrite-contrecord at {}, WAL was rewound here", self.pos);
        }

        let hdr_len = if info & XLP_LONG_HEADER != 0 {
            let sysid = u64::from_le_bytes(hdr[24..32].try_into()?);
            let seg_size = u32::from_le_bytes(hdr[32..36].try_into()?) as u64;
            let blcksz = u32::from_le_bytes(hdr[36..40].try_into()?) as u64;
            ensure!(
                seg_size == self.seg_size,
                "segment declares wal_segment_size {seg_size}, reader configured for {}",
                self.seg_size
            );
            ensure!(blcksz == XLOG_BLCKSZ, "segment built with XLOG_BLCKSZ {blcksz}, need 8192");
            match self.sysid {
                None => self.sysid = Some(sysid),
                Some(prev) => ensure!(
                    prev == sysid,
                    "segment at {} belongs to system {sysid}, previous was {prev}",
                    self.pos
                ),
            }
            SIZE_OF_XLOG_LONG_PHD
        } else {
            SIZE_OF_XLOG_SHORT_PHD
        };
        self.pos = Lsn(self.pos.0 + hdr_len);
        Ok(())
    }

    /// Copy `n` bytes of record payload, transparently stepping over page
    /// headers. `continuing` marks that we are already inside a record.
    fn read_bytes(&mut self, n: usize, mut continuing: bool) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            if self.pos.page_offset() == 0 {
                self.consume_page_header(continuing)?;
            }
            let want = n - out.len();
            let left_on_page = (XLOG_BLCKSZ - self.pos.page_offset()) as usize;
            let take = want.min(left_on_page);
            let segno = self.pos.segment_no(self.seg_size);
            let off = self.pos.segment_offset(self.seg_size) as usize;
            let seg = self.src.get(self.tli, segno, self.seg_size)?;
            out.extend_from_slice(&seg[off..off + take]);
            self.pos = Lsn(self.pos.0 + take as u64);
            continuing = true;
        }
        Ok(out)
    }

    /// Check the archive really belongs to the cluster the catalog dictionary
    /// was captured from, before decoding a single byte.
    ///
    /// The system identifier only appears in *long* page headers, which sit at
    /// the start of every segment. A decode window starting mid-segment would
    /// otherwise never encounter one, so read the segment's first page directly
    /// rather than waiting to stumble across it.
    pub fn verify_provenance(&mut self) -> Result<()> {
        let Some(want) = self.expect_sysid else { return Ok(()) };
        let segno = self.pos.segment_no(self.seg_size);
        let seg = self.src.get(self.tli, segno, self.seg_size)?;
        let info = u16::from_le_bytes([seg[2], seg[3]]);
        ensure!(
            info & XLP_LONG_HEADER != 0,
            "first page of segment {segno} has no long header, so the archive cannot be \
             matched against the catalog dictionary"
        );
        let sysid = u64::from_le_bytes(seg[24..32].try_into()?);
        ensure!(
            sysid == want,
            "segment {segno} belongs to cluster {sysid}, but the catalog dictionary was \
             captured from cluster {want}. Wrong archive for this dictionary."
        );
        let declared = u32::from_le_bytes(seg[32..36].try_into()?) as u64;
        ensure!(
            declared == self.seg_size,
            "archive was written with wal_segment_size {declared}, dictionary says {}",
            self.seg_size
        );
        Ok(())
    }

    /// Peek the magic of the page at `self.pos` without consuming it. A zero
    /// magic means we have reached WAL that was never written.
    fn page_is_written(&mut self) -> Result<bool> {
        let segno = self.pos.segment_no(self.seg_size);
        let off = self.pos.segment_offset(self.seg_size) as usize;
        let seg = self.src.get(self.tli, segno, self.seg_size)?;
        Ok(u16::from_le_bytes([seg[off], seg[off + 1]]) != 0)
    }

    /// Read the next record, or `None` at `end`, at the end of written WAL, or
    /// when the next segment has not been archived yet. Position is left
    /// untouched in that last case so a later call can pick up where it stopped.
    pub fn next_record(&mut self) -> Result<Option<Record>> {
        let resume_at = self.pos;
        match self.read_next_record() {
            Err(e) if e.downcast_ref::<SegmentUnavailable>().is_some() => {
                self.pos = resume_at;
                Ok(None)
            }
            other => other,
        }
    }

    fn read_next_record(&mut self) -> Result<Option<Record>> {
        loop {
            self.pos = Lsn(maxalign(self.pos.0));
            if self.pos >= self.end {
                return Ok(None);
            }
            if self.pos.page_offset() == 0 {
                if !self.page_is_written()? {
                    return Ok(None);
                }
                self.consume_page_header(false)?;
                continue;
            }
            // Note: a record header CAN straddle a page boundary. Postgres does
            // not pad the tail of a page to fit one, so `read_bytes` has to
            // assemble the 24-byte header across pages like any other payload.
            break;
        }

        let start = self.pos;
        let hdr = self.read_bytes(SIZE_OF_XLOG_RECORD as usize, false)?;
        let tot_len = u32::from_le_bytes(hdr[0..4].try_into()?) as u64;
        let xid = u32::from_le_bytes(hdr[4..8].try_into()?);
        let _prev = u64::from_le_bytes(hdr[8..16].try_into()?);
        let info = hdr[16];
        let rmid = hdr[17];
        let crc = u32::from_le_bytes(hdr[20..24].try_into()?);

        // All-zero header means we ran off the end of written WAL.
        if tot_len == 0 {
            return Ok(None);
        }
        ensure!(
            tot_len >= SIZE_OF_XLOG_RECORD && tot_len < 1 << 30,
            "implausible xl_tot_len {tot_len} at {start}"
        );

        let body = self.read_bytes((tot_len - SIZE_OF_XLOG_RECORD) as usize, true)?;

        // PG CRCs the body first, then the header up to (not including) xl_crc.
        let computed = crc32c::crc32c_append(crc32c::crc32c(&body), &hdr[..20]);
        ensure!(computed == crc, "CRC mismatch at {start}: got {computed:#x}, want {crc:#x}");

        let end_lsn = Lsn(maxalign(self.pos.0));
        let (main_data, blocks, toplevel_xid) = parse_record_body(&body, start, self.version)?;

        // After a switch the remainder of the segment is zero padding. Jump to
        // the next segment rather than trying to parse it.
        if rmid == RM_XLOG_ID && info & 0xF0 == XLOG_SWITCH {
            let segno = start.segment_no(self.seg_size);
            self.pos = Lsn((segno + 1) * self.seg_size);
        }

        Ok(Some(Record { lsn: start, end_lsn, tot_len: tot_len as u32, xid, rmid, info, main_data, blocks, toplevel_xid }))
    }
}

/// Split a record body into block references and main data.
// `take!` bumps the cursor on its last use too; that write is genuinely unread.
#[allow(unused_assignments)]
fn parse_record_body(
    body: &[u8],
    lsn: Lsn,
    version: PgVersion,
) -> Result<(Vec<u8>, Vec<BlockRef>, Option<u32>)> {
    struct Pending {
        id: u8,
        fork_flags: u8,
        data_len: u16,
        img_len: u16,
        rlocator: RelFileLocator,
    }

    // Records such as XLOG_SWITCH have no block references and no main data.
    if body.is_empty() {
        return Ok((Vec::new(), Vec::new(), None));
    }

    let mut p = 0usize;
    let mut pending: Vec<Pending> = Vec::new();
    let mut main_len: usize = 0;
    let mut toplevel_xid = None;
    let mut last_rlocator = RelFileLocator::default();
    // Running total of payload bytes the headers have promised. The header
    // section ends when the bytes left equal that total -- NOT when a
    // data-length header appears. A record with no main data (XLOG's
    // FPI_FOR_HINT, for one) omits that header entirely, and looking for it
    // reads straight into the block image.
    let mut datatotal: usize = 0;

    macro_rules! take {
        ($n:expr) => {{
            let n = $n;
            ensure!(p + n <= body.len(), "record at {lsn} truncated in header section");
            let s = &body[p..p + n];
            p += n;
            s
        }};
    }

    while body.len() - p > datatotal {
        let id = take!(1)[0];
        match id {
            XLR_BLOCK_ID_DATA_SHORT => {
                main_len = take!(1)[0] as usize;
                datatotal += main_len;
            }
            XLR_BLOCK_ID_DATA_LONG => {
                main_len = u32::from_le_bytes(take!(4).try_into()?) as usize;
                datatotal += main_len;
            }
            XLR_BLOCK_ID_ORIGIN => {
                let _origin = u16::from_le_bytes(take!(2).try_into()?);
            }
            XLR_BLOCK_ID_TOPLEVEL_XID => {
                toplevel_xid = Some(u32::from_le_bytes(take!(4).try_into()?));
            }
            id if id <= XLR_MAX_BLOCK_ID => {
                let h = take!(3);
                let fork_flags = h[0];
                let data_len = u16::from_le_bytes([h[1], h[2]]);
                let mut img_len = 0u16;
                if fork_flags & BKPBLOCK_HAS_IMAGE != 0 {
                    let ih = take!(5);
                    img_len = u16::from_le_bytes([ih[0], ih[1]]);
                    let bimg_info = ih[4];
                    // A compressed image with a hole carries an extra length
                    // word. Which bits mean "compressed" moved in PG15.
                    if version.image_is_compressed(bimg_info)
                        && bimg_info & BKPIMAGE_HAS_HOLE != 0
                    {
                        take!(2);
                    }
                    datatotal += img_len as usize;
                }
                if fork_flags & BKPBLOCK_HAS_DATA != 0 {
                    datatotal += data_len as usize;
                }
                if fork_flags & BKPBLOCK_SAME_REL == 0 {
                    let r = take!(12);
                    last_rlocator = RelFileLocator {
                        spc_oid: u32::from_le_bytes(r[0..4].try_into()?),
                        db_oid: u32::from_le_bytes(r[4..8].try_into()?),
                        rel_number: u32::from_le_bytes(r[8..12].try_into()?),
                    };
                }
                let _blkno = u32::from_le_bytes(take!(4).try_into()?);
                pending.push(Pending {
                    id,
                    fork_flags,
                    data_len,
                    img_len,
                    rlocator: last_rlocator,
                });
            }
            other => bail!("unknown block id {other} in record at {lsn}"),
        }
    }

    // Payload section: per block, the image then the data, then main data last.
    let mut blocks = Vec::with_capacity(pending.len());
    for b in pending {
        if b.fork_flags & BKPBLOCK_HAS_IMAGE != 0 {
            // Skipped, not decoded. A full-page image is a copy of the heap page
            // for crash recovery; the row change we want is always in the
            // record's own block data.
            take!(b.img_len as usize);
        }
        let data = if b.fork_flags & BKPBLOCK_HAS_DATA != 0 {
            take!(b.data_len as usize).to_vec()
        } else {
            Vec::new()
        };
        blocks.push(BlockRef {
            id: b.id,
            rlocator: b.rlocator,
            forknum: b.fork_flags & BKPBLOCK_FORK_MASK,
            data,
        });
    }
    let main_data = take!(main_len).to_vec();
    Ok((main_data, blocks, toplevel_xid))
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_backoff_holds_off_only_the_segment_that_missed() {
        let seg = "000000010000000000000002";
        // Never asked for: ask now.
        assert!(fetch_is_due(None, seg));
        // Asked 100ms ago, told to wait a second: don't spawn wal-g again.
        assert!(!fetch_is_due(Some((seg, Duration::from_millis(100), FETCH_RETRY_MIN)), seg));
        // Wait elapsed: ask again.
        assert!(fetch_is_due(Some((seg, Duration::from_secs(2), FETCH_RETRY_MIN)), seg));
        // Another segment is a fresh miss however recent the last one was, so
        // reading forward through an archive that is already complete never
        // waits.
        assert!(fetch_is_due(
            Some((seg, Duration::from_millis(1), FETCH_RETRY_MAX)),
            "000000010000000000000003"
        ));
        // Doubling, capped.
        assert_eq!(next_wait(None), FETCH_RETRY_MIN);
        assert_eq!(next_wait(Some(Duration::from_secs(4))), Duration::from_secs(8));
        assert_eq!(next_wait(Some(FETCH_RETRY_MAX)), FETCH_RETRY_MAX);
    }
}
