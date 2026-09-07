mod catalog;
mod jsonb;
mod lsn;
mod numeric;
mod pglz;
mod pgoutput;
mod reorder;
mod rmgr;
mod tuple;
mod version;
mod walsender;
mod xlog;

use anyhow::{Context, Result};
use lsn::{Lsn, DEFAULT_WAL_SEG_SIZE};
use version::PgVersion;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let usage = "usage: pgs3 <frame|decode|pgoutput|serve> --wal <dir> --start <lsn> --end <lsn> \
                 [--catalog <file>] [--server-version <num>] [--tli <n>] \
                 [--wal-segment-size <bytes>] [--allow-catalog-changes]

The catalog dictionary supplies the source server version, WAL segment size and
system identifier. `frame` can run without one if --server-version is given.

serve options:
  --listen <addr>              default 127.0.0.1:5439
  --confirmed-lsn-file <path>  where to publish the subscriber's flush LSN
  --exit-when-caught-up        stop once the subscriber has confirmed everything
";
    let mode = args.get(1).cloned().unwrap_or_default();
    let get = |k: &str| -> Option<String> {
        args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned()
    };
    let wal = get("--wal").context(usage)?;
    let start = Lsn::parse(&get("--start").context(usage)?)?;
    let end = Lsn::parse(&get("--end").context(usage)?)?;
    let tli: u32 = get("--tli").unwrap_or_else(|| "1".into()).parse()?;

    // Nothing about the WAL format is compiled in: version, segment size and
    // cluster identity all come from the dictionary captured at T0.
    let cat = match get("--catalog") {
        Some(p) => Some(catalog::Catalog::load(&p)?),
        None => None,
    };
    let version = match (&cat, get("--server-version")) {
        (_, Some(v)) => PgVersion(v.parse().context("--server-version wants a server_version_num, e.g. 160004")?),
        (Some(c), None) => PgVersion(c.server_version_num as u32),
        (None, None) => anyhow::bail!("need --catalog or --server-version\n\n{usage}"),
    };
    version.check_supported()?;
    let seg_size = match (get("--wal-segment-size"), &cat) {
        (Some(v), _) => v.parse()?,
        (None, Some(c)) => c.wal_segment_size,
        (None, None) => DEFAULT_WAL_SEG_SIZE,
    };
    let sysid = cat.as_ref().and_then(|c| c.system_identifier.parse::<u64>().ok());

    let src = xlog::SegmentDir::new(&wal);
    let mut reader = xlog::WalReader::new(src, tli, seg_size, start, end, version, sysid);
    reader.verify_provenance()?;

    match mode.as_str() {
        // Framing only: prove the reader agrees with pg_waldump record for record.
        "frame" => {
            while let Some(r) = reader.next_record()? {
                println!(
                    "{} rmid:{:<3} info:{:#04x} xid:{:<6} tot_len:{:<6} blocks:{} main:{}",
                    r.lsn,
                    r.rmid,
                    r.info,
                    r.xid,
                    r.tot_len,
                    r.blocks.len(),
                    r.main_data.len()
                );
            }
        }
        "decode" => {
            let cat = cat.as_ref().context("decode needs --catalog")?;
            let mut rb = reorder::ReorderBuffer::new(cat);
            rb.strict_ddl = !args.iter().any(|a| a == "--allow-catalog-changes");
            while let Some(r) = reader.next_record()? {
                for e in rb.feed(&r)? {
                    if let Some(line) = reorder::render(&e) {
                        println!("{line}");
                    }
                }
            }
        }
        // Emit the pgoutput byte stream as hex, one message per line, so it can
        // be diffed against pg_logical_slot_peek_binary_changes.
        "pgoutput" => {
            let cat = cat.as_ref().context("pgoutput needs --catalog")?;
            pgoutput::check_publication_encodable(cat)?;
            let mut rb = reorder::ReorderBuffer::new(cat);
            rb.strict_ddl = !args.iter().any(|a| a == "--allow-catalog-changes");
            let mut enc = pgoutput::Encoder::new();
            while let Some(r) = reader.next_record()? {
                for e in rb.feed(&r)? {
                    for msg in enc.encode(&e)? {
                        println!("{}", msg.iter().map(|b| format!("{b:02x}")).collect::<String>());
                    }
                }
            }
        }
        // Serve a logical replication stream to a real subscriber.
        "serve" => {
            let cat = cat.as_ref().context("serve needs --catalog")?;
            pgoutput::check_publication_encodable(cat)?;
            let opts = walsender::Options {
                wal_dir: wal.clone().into(),
                tli,
                seg_size,
                version,
                sysid,
                start,
                end,
                strict_ddl: !args.iter().any(|a| a == "--allow-catalog-changes"),
                confirmed_file: get("--confirmed-lsn-file").map(Into::into),
                exit_when_caught_up: args.iter().any(|a| a == "--exit-when-caught-up"),
            };
            let addr = get("--listen").unwrap_or_else(|| "127.0.0.1:5439".into());
            walsender::serve(&addr, cat, opts)?;
        }
        _ => anyhow::bail!(usage),
    }
    Ok(())
}
