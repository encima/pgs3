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

use anyhow::{bail, Context, Result};
use lsn::{Lsn, DEFAULT_WAL_SEG_SIZE};
use std::collections::HashMap;
use version::PgVersion;

const USAGE: &str = "\
pgs3: serve a logical replication stream decoded from archived WAL, so the
primary does not have to retain WAL for its subscribers.

usage: pgs3 <command> [options]          (pgs3 help for this text)

The usual run is three commands:

  pgs3 init      --source <primary> --catalog catalog.json [--publication p]
  pgs3 serve     --catalog catalog.json --wal <cache-dir> --restore-command '<cmd>'
  pgs3 subscribe --target <replica> --source <primary> --at <lsn> [--catalog catalog.json]

commands:
  init       capture the catalog dictionary (and the start LSN) from the primary
             at T0. With --target, also copies the schema there. Needs psql
             (and pg_dump for --target). No DDL on published tables after this.
  check      validate a dictionary and the archive without serving: types are
             encodable, the start segment can be fetched and belongs to this
             cluster.
  serve      speak the walsender protocol to a subscriber, fetching segments
             from the archive as needed. Follows the archive until stopped.
  subscribe  create the subscription on the replica without a slot on the
             primary: reads the table list from --source, sets the origin to
             --at, then points it at pgs3 and enables it. Needs psql.
  frame | decode | pgoutput
             debugging: print records, test_decoding-style rows or pgoutput hex.

options (any of these may also be a key in --config; the command line wins):
  --config <file.yaml>         defaults for any option below, e.g. `wal: /data/wal`
  --catalog <file>             the dictionary written by init
  --source <conninfo>          the primary. init: captured from. serve: runs
                               pg_switch_wal() there when the segment still being
                               written is the one we are waiting on (optional).
                               subscribe: where the table list is read from.
  --publication <name>         init, subscribe: default p

  --wal <dir>                  serve/check: local segment cache, created if
                               missing. Fetched segments are deleted once read.
  --restore-command <cmd>      fetch a missing segment, PostgreSQL-style: %f is
                               the segment name, %p the destination path, e.g.
                               'wal-g wal-fetch %f %p' or
                               'aws s3 cp s3://bucket/wal/%f %p'.
                               Env: PGS3_RESTORE_COMMAND. Without it, wal-g is
                               used when PGS3_WALG_CONFIG or WALG_S3_PREFIX is set.
  --listen <addr>              serve: default 127.0.0.1:5439
  --confirmed-lsn-file <path>  serve: publish the subscriber's flush LSN here
                               (for scripts/advance-slot.sh, if you keep a slot)
  --status-file <path>         serve: JSON {state, lsn, confirmed} for monitoring
  --exit-when-caught-up        serve: stop once the subscriber confirmed everything
  --allow-catalog-changes      do not stop on DDL against a published table

  --target <conninfo>          init: copy the schema here. subscribe: the replica.
  --at <lsn>                   subscribe: the LSN the replica's data is consistent
                               at; everything committed after it is replayed.
                               After pg_upgrade of a copied volume, that is
                               `Latest checkpoint location` from pg_controldata
                               of the source copy, before upgrading it.
  --connect <conninfo>         subscribe: how the replica reaches pgs3;
                               default derived from --listen
  --subscription <name>        subscribe: default pgs3

  --start <lsn>  --tli <n>     default to what init recorded
  --end <lsn>                  default: never, follow the archive
  --server-version <num>       only without --catalog (frame), e.g. 170004
  --wal-segment-size <bytes>   only without --catalog
";

/// Every option, as `(name, takes_value)`. One list for the command line and
/// the config file, so a typo is an error rather than a silently ignored
/// setting -- a misspelled --restore-command otherwise looks exactly like an
/// archive that has not caught up yet.
const OPTIONS: &[(&str, bool)] = &[
    ("config", true),
    ("catalog", true),
    ("source", true),
    ("target", true),
    ("publication", true),
    ("wal", true),
    ("restore-command", true),
    ("listen", true),
    ("confirmed-lsn-file", true),
    ("status-file", true),
    ("exit-when-caught-up", false),
    ("allow-catalog-changes", false),
    ("at", true),
    ("connect", true),
    ("subscription", true),
    ("start", true),
    ("tli", true),
    ("end", true),
    ("server-version", true),
    ("wal-segment-size", true),
];

const COMMANDS: &[&str] = &["init", "check", "serve", "subscribe", "frame", "decode", "pgoutput"];

/// Resolved options: the command line over the config file.
struct Opts(HashMap<String, String>);

impl Opts {
    fn get(&self, k: &str) -> Option<String> {
        self.0.get(k).cloned()
    }

    fn flag(&self, k: &str) -> bool {
        self.0.get(k).is_some_and(|v| matches!(v.as_str(), "true" | "on" | "yes" | "1"))
    }
}

fn unknown_option(name: &str) -> anyhow::Error {
    let near: Vec<&str> = OPTIONS
        .iter()
        .map(|(o, _)| *o)
        .filter(|o| o.starts_with(name.get(..3).unwrap_or(name)) || name.starts_with(o))
        .collect();
    match near.as_slice() {
        [] => anyhow::anyhow!("unknown option --{name}; see `pgs3 help`"),
        _ => anyhow::anyhow!("unknown option --{name}; did you mean --{}?", near.join(" or --")),
    }
}

fn takes_value(name: &str) -> Result<bool> {
    OPTIONS.iter().find(|(o, _)| *o == name).map(|(_, v)| *v).ok_or_else(|| unknown_option(name))
}

fn parse_args(args: &[String]) -> Result<Opts> {
    let mut vals = HashMap::new();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        let Some(opt) = arg.strip_prefix("--") else {
            bail!("unexpected argument {arg:?}; options look like --name value");
        };
        let (name, inline) = match opt.split_once('=') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (opt, None),
        };
        let value = match (takes_value(name)?, inline) {
            (_, Some(v)) => v,
            (true, None) => it.next().with_context(|| format!("--{name} needs a value"))?.clone(),
            (false, None) => "true".into(),
        };
        vals.insert(name.to_string(), value);
    }

    if let Some(path) = vals.get("config").cloned() {
        let text = std::fs::read_to_string(&path).with_context(|| format!("reading {path}"))?;
        let cfg: HashMap<String, serde_yaml::Value> =
            serde_yaml::from_str(&text).with_context(|| format!("parsing {path}"))?;
        for (k, v) in cfg {
            let name = k.trim_start_matches("--");
            takes_value(name).with_context(|| format!("in {path}"))?;
            let v = match v {
                serde_yaml::Value::Null => continue,
                serde_yaml::Value::String(s) => s,
                serde_yaml::Value::Bool(b) => b.to_string(),
                serde_yaml::Value::Number(n) => n.to_string(),
                _ => bail!("{path}: {k} must be a single value"),
            };
            vals.entry(name.to_string()).or_insert(v);
        }
    }
    Ok(Opts(vals))
}

fn main() {
    if let Err(e) = run() {
        eprintln!("pgs3: error: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = args.first().map(String::as_str).unwrap_or("help");
    if matches!(mode, "help" | "-h" | "--help") {
        print!("{USAGE}");
        return Ok(());
    }
    if !COMMANDS.contains(&mode) {
        bail!("unknown command {mode:?}; expected one of {}. See `pgs3 help`.", COMMANDS.join(", "));
    }
    let o = parse_args(&args[1..])?;
    match mode {
        "init" => return init(&o),
        "subscribe" => return subscribe(&o),
        _ => {}
    }
    let wal = o.get("wal").context("--wal <dir> is required: the local segment cache")?;

    // Nothing about the WAL format is compiled in: version, segment size and
    // cluster identity all come from the dictionary captured at T0.
    let cat = match o.get("catalog") {
        Some(p) => Some(catalog::Catalog::load(&p)?),
        None => None,
    };
    let start = o
        .get("start")
        .or_else(|| cat.as_ref().and_then(|c| c.start_lsn.clone()))
        .context("need --start, or a dictionary written by `pgs3 init`")?;
    let start = Lsn::parse(&start)?;
    let end = Lsn::parse(&o.get("end").unwrap_or_else(|| "FFFFFFFF/FFFFFFFF".into()))?;
    let tli: u32 = match o.get("tli") {
        Some(t) => t.parse().context("--tli wants a number")?,
        None => cat.as_ref().and_then(|c| c.timeline_id).unwrap_or(1),
    };
    let version = match (&cat, o.get("server-version")) {
        (_, Some(v)) => PgVersion(v.parse().context("--server-version wants a server_version_num, e.g. 160004")?),
        (Some(c), None) => PgVersion(c.server_version_num as u32),
        (None, None) => bail!("need --catalog (or --server-version for `frame`)"),
    };
    version.check_supported()?;
    let seg_size: u64 = match (o.get("wal-segment-size"), &cat) {
        (Some(v), _) => v.parse().context("--wal-segment-size wants a number of bytes")?,
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

    let restore_command =
        o.get("restore-command").or_else(|| std::env::var("PGS3_RESTORE_COMMAND").ok());
    let strict_ddl = !o.flag("allow-catalog-changes");

    if mode == "check" || mode == "serve" {
        let cat = cat.as_ref().with_context(|| format!("{mode} needs --catalog"))?;
        pgoutput::check_publication_encodable(cat)?;
        // A fetch into a directory that does not exist fails just like a
        // segment that is not archived yet, and would be retried forever.
        std::fs::create_dir_all(&wal).with_context(|| format!("creating --wal {wal}"))?;
        eprintln!(
            "pgs3: {} published relation(s), PG {}, timeline {tli}, from {start}; segments via {}",
            cat.relations.len(),
            version.0,
            xlog::describe_fetcher(restore_command.as_deref(), &wal)
        );
    }

    // Pre-flight check: validate everything without starting a stream.
    if mode == "check" {
        let cat = cat.as_ref().expect("checked above");
        let src = xlog::SegmentDir::new(&wal)
            .with_restore_command(restore_command)
            .with_switch_wal(o.get("source"));
        let mut reader = xlog::WalReader::new(src, tli, seg_size, start, end, version, sysid);
        // Right after init the start segment is usually the one the primary is
        // still writing; serve waits for it, so that is not a failure.
        if let Err(e) = reader.verify_provenance() {
            let Some(missing) = e.downcast_ref::<xlog::SegmentUnavailable>() else { return Err(e) };
            eprintln!(
                "pgs3: check OK, except: {missing}. That is expected until the primary \
                 archives it (archive_timeout, or pg_switch_wal()); serve will wait for it."
            );
            return Ok(());
        }
        eprintln!(
            "pgs3: check OK — {} published relation(s), all types encodable, start segment \
             fetched and from the right cluster. Ready to serve.",
            cat.relations.len()
        );
        return Ok(());
    }

    // Serve a logical replication stream to a real subscriber. Each connection
    // opens its own reader and waits for the start segment to be archived.
    if mode == "serve" {
        let cat = cat.as_ref().expect("checked above");
        let opts = walsender::Options {
            wal_dir: wal.into(),
            restore_command,
            switch_wal_source: o.get("source"),
            tli,
            seg_size,
            version,
            sysid,
            start,
            end,
            strict_ddl,
            confirmed_file: o.get("confirmed-lsn-file").map(Into::into),
            exit_when_caught_up: o.flag("exit-when-caught-up"),
            status_file: o.get("status-file").map(Into::into),
        };
        let addr = o.get("listen").unwrap_or_else(|| DEFAULT_LISTEN.into());
        return walsender::serve(&addr, cat, opts);
    }

    let src = xlog::SegmentDir::new(&wal).with_restore_command(restore_command);
    let mut reader = xlog::WalReader::new(src, tli, seg_size, start, end, version, sysid);
    reader.verify_provenance()?;

    match mode {
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
            rb.strict_ddl = strict_ddl;
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
            rb.strict_ddl = strict_ddl;
            let mut enc = pgoutput::Encoder::new(cat);
            while let Some(r) = reader.next_record()? {
                for e in rb.feed(&r)? {
                    for msg in enc.encode(&e)? {
                        println!("{}", msg.iter().map(|b| format!("{b:02x}")).collect::<String>());
                    }
                }
            }
        }
        _ => unreachable!("validated against COMMANDS"),
    }
    Ok(())
}

const DEFAULT_LISTEN: &str = "127.0.0.1:5439";

/// Run psql with `sql` on stdin, `vars` as psql variables, stopping at the
/// first error. Returns stdout.
fn psql(conninfo: &str, vars: &[(&str, &str)], sql: &str, what: &str) -> Result<Vec<u8>> {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut cmd = Command::new("psql");
    cmd.args(["-X", "-t", "-A", "-q", "-v", "ON_ERROR_STOP=1", "-f", "-"]);
    for (k, v) in vars {
        cmd.args(["-v", &format!("{k}={v}")]);
    }
    let mut child = cmd
        .args(["-d", conninfo])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning psql (is it in PATH?)")?;
    child.stdin.take().context("piped stdin")?.write_all(sql.as_bytes())?;
    let out = child.wait_with_output().context("running psql")?;
    anyhow::ensure!(
        out.status.success(),
        "psql failed {what}:\n{}",
        String::from_utf8_lossy(&out.stderr).trim_end()
    );
    Ok(out.stdout)
}

/// One-shot bootstrap at T0: capture the frozen dictionary from the source and
/// copy the schema to the target. Shells out to psql/pg_dump -- the workflow
/// already requires them, and a replication protocol client in the binary
/// would be a second implementation of the same thing.
fn init(o: &Opts) -> Result<()> {
    let source = o.get("source").context("init needs --source <conninfo> for the primary")?;
    let target = o.get("target");
    let catalog_path = o.get("catalog").context("init needs --catalog <file> to write the dictionary to")?;
    let publication = o.get("publication").unwrap_or_else(|| "p".into());

    use std::io::Write;
    use std::process::{Command, Stdio};

    // A missing publication would otherwise dump an empty dictionary that
    // serves nothing and says so nowhere.
    let found = psql(
        &source,
        &[("pubname", &publication)],
        "select count(*) from pg_publication where pubname = :'pubname';",
        "looking up the publication on the source",
    )?;
    anyhow::ensure!(
        String::from_utf8_lossy(&found).trim() == "1",
        "publication {publication:?} does not exist on the source; create it first, e.g.\n  \
         CREATE PUBLICATION {publication} FOR ALL TABLES;"
    );

    // Dictionary first: its captured_at_lsn is T0, and from here on the
    // source's schema must not change.
    // A temporary slot waits until every transaction running now has ended, so
    // its restart_lsn is where decoding sees each later commit whole. The
    // dictionary query reads it by name and the slot goes when psql exits:
    // nothing is left on the source to retain WAL.
    let sql = concat!(
        "\\o /dev/null\n",
        "select pg_create_logical_replication_slot('pgs3_init_' || pg_backend_pid(), 'pgoutput', true);\n",
        "\\o\n",
        include_str!("../sql/dump_catalog.sql"),
    );
    let out = psql(&source, &[("pubname", &publication)], sql, "capturing the dictionary from the source")?;
    std::fs::write(&catalog_path, &out).with_context(|| format!("writing {catalog_path}"))?;
    // Validate what we captured before touching the target.
    let cat = catalog::Catalog::load(&catalog_path)?;
    anyhow::ensure!(cat.start_lsn.is_some(), "the source returned no start LSN from the temporary slot");
    anyhow::ensure!(
        !cat.relations.is_empty(),
        "publication {publication:?} has no tables; add some before capturing"
    );
    pgoutput::check_publication_encodable(&cat)?;
    eprintln!(
        "pgs3: dictionary for {} published relation(s) written to {catalog_path}; \
         start {} (T0 = {})",
        cat.relations.len(),
        cat.start_lsn.as_deref().unwrap_or("?"),
        cat.captured_at_lsn
    );

    if let Some(target) = target {
        let dump = Command::new("pg_dump")
            .args(["--schema-only", "--no-owner", "--no-privileges", &source])
            .output()
            .context("spawning pg_dump (is it in PATH?)")?;
        anyhow::ensure!(dump.status.success(), "pg_dump failed:\n{}", String::from_utf8_lossy(&dump.stderr));
        let mut child = Command::new("psql")
            .args(["-X", "-q", "-v", "ON_ERROR_STOP=1", &target])
            .stdin(Stdio::piped())
            .spawn()
            .context("spawning psql for the target")?;
        child.stdin.take().context("piped stdin")?.write_all(&dump.stdout)?;
        let status = child.wait().context("applying the schema to the target")?;
        anyhow::ensure!(status.success(), "applying the schema to the target failed");
        eprintln!("pgs3: schema applied to the target");
    }

    eprintln!(
        "pgs3: from now on, no DDL or TRUNCATE on published tables. Next:\n  \
         pgs3 serve --catalog {catalog_path} --wal <cache-dir> --restore-command '<fetch %f to %p>'"
    );
    Ok(())
}

/// Turn a listen address into a conninfo for reaching it from the same host.
fn conninfo_for_listen(listen: &str) -> String {
    let (host, port) = listen.rsplit_once(':').unwrap_or((listen, "5439"));
    let host = match host.trim_matches(['[', ']']) {
        "0.0.0.0" | "" => "127.0.0.1",
        "::" => "::1",
        h => h,
    };
    format!("host={host} port={port}")
}

/// Create the subscription on the replica without a slot on the primary.
///
/// CREATE SUBSCRIPTION has to connect to the real primary once: that is what
/// fills pg_subscription_rel, and pgs3 cannot answer the catalog query it
/// uses. With create_slot = false nothing is left behind there. The origin is
/// then set to the LSN the replica's data is consistent at, so pgs3 replays
/// exactly the transactions committed after it.
fn subscribe(o: &Opts) -> Result<()> {
    let target = o.get("target").context("subscribe needs --target <conninfo> for the replica")?;
    let source = o.get("source").context(
        "subscribe needs --source <conninfo> for the primary: CREATE SUBSCRIPTION reads the \
         publication's table list from it (no slot is created there)",
    )?;
    let at = o.get("at").context(
        "subscribe needs --at <lsn>: where the replica's data is consistent. After pg_upgrade of \
         a copied volume, that is `Latest checkpoint location` from pg_controldata of the copy, \
         taken before upgrading it",
    )?;
    let at_lsn = Lsn::parse(&at)?;
    let publication = o.get("publication").unwrap_or_else(|| "p".into());
    let name = o.get("subscription").unwrap_or_else(|| "pgs3".into());
    let connect = o
        .get("connect")
        .unwrap_or_else(|| conninfo_for_listen(&o.get("listen").unwrap_or_else(|| DEFAULT_LISTEN.into())));

    // pgs3 reads from the dictionary's start; a replica consistent before it
    // would be missing the transactions in between.
    if let Some(path) = o.get("catalog") {
        let cat = catalog::Catalog::load(&path)?;
        if let Some(start) = cat.start_lsn.as_deref().map(Lsn::parse).transpose()? {
            anyhow::ensure!(
                at_lsn >= start,
                "--at {at_lsn} is before the dictionary's start {start}; the transactions in \
                 between would never be replayed. Capture the dictionary before the replica's \
                 snapshot is taken."
            );
        }
    }

    let sql = r#"
create subscription :"sub" connection :'source' publication :"pub"
  with (enabled = false, create_slot = false, slot_name = :'sub',
        copy_data = false, binary = true, streaming = off);
select pg_replication_origin_advance('pg_' || oid, :'at') from pg_subscription
 where subname = :'sub' and subdbid = (select oid from pg_database where datname = current_database());
alter subscription :"sub" connection :'connect';
alter subscription :"sub" enable;
"#;
    psql(
        &target,
        &[
            ("sub", &name),
            ("pub", &publication),
            ("source", &source),
            ("at", &at),
            ("connect", &connect),
        ],
        sql,
        "creating the subscription on the replica",
    )?;
    eprintln!(
        "pgs3: subscription {name:?} created at {at_lsn}, enabled, and pointed at pgs3 ({connect}).\n\
         pgs3: no slot was created on the primary. Watch progress with:\n  \
         select received_lsn, latest_end_lsn from pg_stat_subscription where subname = '{name}';"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &[&str]) -> Vec<String> {
        s.iter().map(|a| a.to_string()).collect()
    }

    #[test]
    fn options_accept_both_spellings_and_flags() {
        let o = parse_args(&args(&["--wal", "/w", "--listen=0.0.0.0:1", "--exit-when-caught-up"])).unwrap();
        assert_eq!(o.get("wal").as_deref(), Some("/w"));
        assert_eq!(o.get("listen").as_deref(), Some("0.0.0.0:1"));
        assert!(o.flag("exit-when-caught-up"));
        assert!(!o.flag("allow-catalog-changes"));
    }

    #[test]
    fn typos_are_errors_not_ignored() {
        let e = parse_args(&args(&["--restore-cmd", "x"])).err().unwrap().to_string();
        assert!(e.contains("--restore-command"), "{e}");
        assert!(parse_args(&args(&["stray"])).is_err());
        assert!(parse_args(&args(&["--wal"])).is_err());
    }

    #[test]
    fn config_file_fills_gaps_and_cli_wins() {
        let dir = std::env::temp_dir().join(format!("pgs3-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.yaml");
        std::fs::write(&path, "wal: /from-config\n--listen: x:1\nexit-when-caught-up: true\ntli: 2\n").unwrap();
        let p = path.to_str().unwrap();
        let o = parse_args(&args(&["--config", p, "--wal", "/from-cli"])).unwrap();
        assert_eq!(o.get("wal").as_deref(), Some("/from-cli"));
        assert_eq!(o.get("listen").as_deref(), Some("x:1"));
        assert_eq!(o.get("tli").as_deref(), Some("2"));
        assert!(o.flag("exit-when-caught-up"));

        std::fs::write(&path, "restore_command: x\n").unwrap();
        assert!(parse_args(&args(&["--config", p])).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn listen_to_conninfo() {
        assert_eq!(conninfo_for_listen("0.0.0.0:5439"), "host=127.0.0.1 port=5439");
        assert_eq!(conninfo_for_listen("10.0.0.5:6000"), "host=10.0.0.5 port=6000");
        assert_eq!(conninfo_for_listen("[::]:5439"), "host=::1 port=5439");
    }
}
