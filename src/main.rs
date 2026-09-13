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
    let usage = "usage: pgs3 init --source <conninfo> --target <conninfo> --catalog <file> \
                 [--publication <name>]
       pgs3 <frame|decode|pgoutput|serve> --wal <dir> --start <lsn> --end <lsn> \
                 [--catalog <file>] [--server-version <num>] [--tli <n>] \
                 [--wal-segment-size <bytes>] [--allow-catalog-changes] \
                 [--restore-command <cmd>]

init captures the catalog dictionary from the source at T0 and copies the
schema to the target (needs psql and pg_dump in PATH).

The catalog dictionary supplies the source server version, WAL segment size and
system identifier. `frame` can run without one if --server-version is given.

--restore-command fetches missing segments from the archive, PostgreSQL-style:
any shell command where %f is the segment name and %p the local destination,
e.g. 'pgbackrest --stanza=main archive-get %f %p'. Env: PGS3_RESTORE_COMMAND.
Without it, falls back to wal-g when WALG_S3_PREFIX/PGS3_WALG_CONFIG are set.

serve options:
  --listen <addr>              default 127.0.0.1:5439
  --confirmed-lsn-file <path>  where to publish the subscriber's flush LSN
  --exit-when-caught-up        stop once the subscriber has confirmed everything
";
    let mode = args.get(1).cloned().unwrap_or_default();
    let get = |k: &str| -> Option<String> {
        args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned()
    };
    if mode == "init" {
        return init(&get, usage);
    }
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
    let seg_size: u64 = match (get("--wal-segment-size"), &cat) {
        (Some(v), _) => v.parse()?,
        (None, Some(c)) => c.wal_segment_size,
        (None, None) => DEFAULT_WAL_SEG_SIZE,
    };
    // Segment size divides every LSN and sizes page-header offsets; a bad one
    // is a divide-by-zero or a slice panic deep inside the reader.
    anyhow::ensure!(
        seg_size.is_power_of_two() && (1 << 20..=1 << 30).contains(&seg_size),
        "WAL segment size {seg_size} is not a power of two between 1MB and 1GB"
    );
    // A dictionary without a usable identifier must not silently disable the
    // archive-provenance check.
    let sysid = cat.as_ref().and_then(|c| match c.system_identifier.parse::<u64>() {
        Ok(v) => Some(v),
        Err(_) => {
            eprintln!(
                "pgs3: warning: dictionary system_identifier {:?} is not a number; \
                 the archive-provenance check is disabled",
                c.system_identifier
            );
            None
        }
    });

    // --restore-command wins over the env var; both are optional and fall back
    // to wal-g inside SegmentDir when neither is set.
    let restore_command =
        get("--restore-command").or_else(|| std::env::var("PGS3_RESTORE_COMMAND").ok());

    let src = xlog::SegmentDir::new(&wal).with_restore_command(restore_command.clone());
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
                restore_command,
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

/// One-shot bootstrap at T0: capture the frozen dictionary from the source and
/// copy the schema to the target. Shells out to psql/pg_dump -- the workflow
/// already requires them, and a replication protocol client in the binary
/// would be a second implementation of the same thing.
fn init(get: &dyn Fn(&str) -> Option<String>, usage: &str) -> Result<()> {
    let source = get("--source").context("--source <conninfo> for the source primary\n\n".to_owned() + usage)?;
    let target = get("--target").context("--target <conninfo> for the destination\n\n".to_owned() + usage)?;
    let catalog_path = get("--catalog").context("--catalog <file> to write the dictionary to\n\n".to_owned() + usage)?;
    let publication = get("--publication").unwrap_or_else(|| "p".into());

    use std::io::Write;
    use std::process::{Command, Stdio};

    // Dictionary first: its captured_at_lsn is T0, and from here on the
    // source's schema must not change.
    let sql = include_str!("../sql/dump_catalog.sql");
    let mut child = Command::new("psql")
        .args([
            "-X", "-t", "-A", "-q",
            "-f", "-",
            "-v", &format!("pubname={publication}"),
            &source,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .context("spawning psql (is it in PATH?)")?;
    child
        .stdin
        .as_mut()
        .context("piped stdin")?
        .write_all(sql.as_bytes())?;
    drop(child.stdin.take()); // close stdin so psql runs the script
    let out = child.wait_with_output().context("running psql")?;
    anyhow::ensure!(
        out.status.success(),
        "psql failed capturing the dictionary from the source:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    std::fs::write(&catalog_path, &out.stdout)
        .with_context(|| format!("writing {catalog_path}"))?;
    // Validate what we captured before touching the target.
    let cat = catalog::Catalog::load(&catalog_path)?;
    eprintln!(
        "pgs3: dictionary for {} published relation(s) written to {catalog_path} (T0 = {})",
        cat.relations.len(),
        cat.captured_at_lsn
    );

    // Schema copy: pg_dump --schema-only | psql.
    let dump = Command::new("pg_dump")
        .args(["--schema-only", "--no-owner", "--no-privileges", &source])
        .output()
        .context("spawning pg_dump (is it in PATH?)")?;
    anyhow::ensure!(
        dump.status.success(),
        "pg_dump failed:\n{}",
        String::from_utf8_lossy(&dump.stderr)
    );
    let mut child = Command::new("psql")
        .args(["-X", "-q", "-v", "ON_ERROR_STOP=1", &target])
        .stdin(Stdio::piped())
        .spawn()
        .context("spawning psql for the target")?;
    child
        .stdin
        .as_mut()
        .context("piped stdin")?
        .write_all(&dump.stdout)?;
    drop(child.stdin.take());
    let status = child.wait().context("applying the schema to the target")?;
    anyhow::ensure!(status.success(), "applying the schema to the target failed");

    eprintln!("pgs3: schema applied to the target");
    eprintln!(
        "pgs3: next: create the logical replication slot on the source and the \
         subscription on the primary (see README); do not run DDL on the source \
         from here on"
    );
    Ok(())
}
