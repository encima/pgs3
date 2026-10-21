//! The walsender half: enough of the Postgres wire protocol for a real
//! subscriber's apply worker to connect and stream from us.
//!
//! Deliberately small. This listens on loopback for one connection from the
//! replica on the same box, so there is no TLS, no SASL and no concurrency.

use crate::catalog::Catalog;
use crate::lsn::Lsn;
use crate::pgoutput::{now_pg_timestamp, Encoder};
use crate::reorder::{Event, ReorderBuffer};
use crate::version::PgVersion;
use crate::xlog::{SegmentDir, SegmentUnavailable, WalReader};
use anyhow::{bail, Context, Result};
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub struct Options {
    pub wal_dir: PathBuf,
    /// PostgreSQL-style restore_command template for archive fetches; None
    /// keeps SegmentDir's wal-g fallback.
    pub restore_command: Option<String>,
    /// Source conninfo for `pg_switch_wal()` when waiting on the live segment.
    pub switch_wal_source: Option<String>,
    pub tli: u32,
    pub seg_size: u64,
    pub version: PgVersion,
    pub sysid: Option<u64>,
    /// Where to begin reading WAL. This must be the slot's **restart_lsn**, not
    /// its confirmed_flush_lsn: a transaction can begin before the confirmed
    /// point and commit after it, and its early changes live in that gap. That
    /// is exactly why Postgres keeps restart_lsn behind confirmed_flush_lsn,
    /// and it sets how far back the archive must reach.
    pub start: Lsn,
    pub end: Lsn,
    pub strict_ddl: bool,
    /// Where to publish the subscriber's confirmed flush LSN. The slot
    /// advancer reads this file; because it is written only from the
    /// subscriber's own feedback, the advancer can never overtake it.
    pub confirmed_file: Option<PathBuf>,
    /// Exit once the subscriber has confirmed everything we decoded.
    pub exit_when_caught_up: bool,
    /// Where to periodically write a JSON status snapshot for external
    /// monitoring (healthchecks, systemd, k8s probes).
    pub status_file: Option<PathBuf>,
    /// The replica's conninfo. When set, schema changes are applied to it at
    /// the right point in the stream; otherwise they are only logged.
    pub target: Option<String>,
}

struct Conn {
    stream: TcpStream,
    inbuf: Vec<u8>,
}

impl Conn {
    fn send(&mut self, tag: u8, body: &[u8]) -> Result<()> {
        let mut msg = Vec::with_capacity(body.len() + 5);
        msg.push(tag);
        msg.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        msg.extend_from_slice(body);
        self.stream.write_all(&msg)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.stream.flush()?;
        Ok(())
    }

    /// Pull whatever is available into `inbuf` without blocking.
    fn fill(&mut self) -> Result<()> {
        let mut tmp = [0u8; 8192];
        loop {
            match self.stream.read(&mut tmp) {
                Ok(0) => bail!("client closed the connection"),
                Ok(n) => self.inbuf.extend_from_slice(&tmp[..n]),
                Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                    return Ok(())
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
            if self.inbuf.len() > 1 << 20 {
                return Ok(());
            }
        }
    }

    /// `try_message` without waiting out the read timeout. The streaming loop
    /// polls between WAL records, and 20ms per record caps catch-up at ~50
    /// records a second.
    fn poll_message(&mut self) -> Result<Option<(u8, Vec<u8>)>> {
        self.stream.set_nonblocking(true)?;
        let r = self.try_message();
        self.stream.set_nonblocking(false)?;
        r
    }

    /// Next complete tagged message, if one has arrived.
    fn try_message(&mut self) -> Result<Option<(u8, Vec<u8>)>> {
        self.fill()?;
        if self.inbuf.len() < 5 {
            return Ok(None);
        }
        let len = u32::from_be_bytes(self.inbuf[1..5].try_into()?) as usize;
        // The length counts itself, so 4 is the floor; below that the frame is
        // malformed, not partial, and the slice below would panic.
        if len < 4 {
            bail!("malformed message: declared length {len} below the 4-byte minimum");
        }
        if self.inbuf.len() < len + 1 {
            return Ok(None);
        }
        let tag = self.inbuf[0];
        let body = self.inbuf[5..len + 1].to_vec();
        self.inbuf.drain(..len + 1);
        Ok(Some((tag, body)))
    }

    fn row_description(&mut self, cols: &[(&str, u32)]) -> Result<()> {
        let mut b = Vec::new();
        b.extend_from_slice(&(cols.len() as u16).to_be_bytes());
        for (name, typoid) in cols {
            b.extend_from_slice(name.as_bytes());
            b.push(0);
            b.extend_from_slice(&0u32.to_be_bytes()); // table oid
            b.extend_from_slice(&0u16.to_be_bytes()); // attnum
            b.extend_from_slice(&typoid.to_be_bytes());
            b.extend_from_slice(&(-1i16).to_be_bytes()); // typlen
            b.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
            b.extend_from_slice(&0u16.to_be_bytes()); // text format
        }
        self.send(b'T', &b)
    }

    fn data_row(&mut self, vals: &[Option<&str>]) -> Result<()> {
        let mut b = Vec::new();
        b.extend_from_slice(&(vals.len() as u16).to_be_bytes());
        for v in vals {
            match v {
                None => b.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(s) => {
                    b.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    b.extend_from_slice(s.as_bytes());
                }
            }
        }
        self.send(b'D', &b)
    }

    fn command_complete(&mut self, tag: &str) -> Result<()> {
        let mut b = tag.as_bytes().to_vec();
        b.push(0);
        self.send(b'C', &b)
    }

    fn ready(&mut self) -> Result<()> {
        self.send(b'Z', b"I")?;
        self.flush()
    }

    fn error(&mut self, msg: &str) -> Result<()> {
        let mut b = Vec::new();
        for (code, val) in [(b'S', "ERROR"), (b'C', "XX000"), (b'M', msg)] {
            b.push(code);
            b.extend_from_slice(val.as_bytes());
            b.push(0);
        }
        b.push(0);
        self.send(b'E', &b)?;
        self.flush()
    }
}

fn write_status(opts: &Options, state: &str, lsn: Lsn, confirmed: Lsn) {
    let Some(path) = &opts.status_file else { return };
    let json = format!(
        r#"{{"state":"{state}","lsn":"{lsn}","confirmed":"{confirmed}"}}"#
    );
    let tmp = path.with_extension("tmp");
    let _ = std::fs::write(&tmp, json);
    let _ = std::fs::rename(&tmp, path);
}

pub fn serve(addr: &str, cat: &Catalog, opts: Options) -> Result<()> {
    let listener = TcpListener::bind(addr).with_context(|| format!("binding {addr}"))?;
    eprintln!("pgs3: listening on {}", listener.local_addr()?);
    write_status(&opts, "waiting", opts.start, Lsn(0));

    // An apply worker restarts and reconnects after any error, so one
    // connection is never enough. In catch-up mode we stop as soon as the
    // subscriber has confirmed everything; as a daemon we keep serving.
    loop {
        let (stream, peer) = listener.accept()?;
        eprintln!("pgs3: subscriber connected from {peer}");
        match session(stream, cat, &opts) {
            Ok(caught_up) => {
                if caught_up && opts.exit_when_caught_up {
                    return Ok(());
                }
                eprintln!("pgs3: subscriber disconnected; waiting for it to come back");
                write_status(&opts, "waiting", opts.start, Lsn(0));
            }
            Err(e) if opts.exit_when_caught_up => return Err(e),
            Err(e) => {
                eprintln!("pgs3: connection failed: {e:#}");
                write_status(&opts, "error", opts.start, Lsn(0));
            }
        }
    }
}

/// Handle one subscriber connection. Returns true once the subscriber has
/// confirmed everything we decoded.
fn session(stream: TcpStream, cat: &Catalog, opts: &Options) -> Result<bool> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_millis(20)))?;
    // A subscriber that stops reading must not park write_all forever and
    // freeze the single-connection listener with it.
    stream.set_write_timeout(Some(Duration::from_secs(60)))?;
    let mut conn = Conn { stream, inbuf: Vec::new() };

    // libpq turns this into PQserverVersion, which the apply worker uses to pick
    // its protocol version. Report the source server, not something invented.
    let v = cat.server_version_num;
    handshake(&mut conn, &format!("{}.{}", v / 10000, v % 10000))?;

    // Same reason as the handshake deadline: a client that completes startup
    // and then goes quiet must not hold the listener shut.
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    loop {
        match conn.try_message()? {
            Some((tag, body)) => match tag {
                b'X' => return Ok(false),
                b'Q' => {
                    let sql = String::from_utf8_lossy(&body)
                        .trim_end_matches('\0')
                        .trim()
                        .to_string();
                    if let Some(caught_up) = simple_query(&mut conn, &sql, cat, opts)? {
                        return Ok(caught_up);
                    }
                }
                other => bail!("unexpected message type {:?} before streaming", other as char),
            },
            None => {
                if Instant::now() > deadline {
                    bail!("subscriber sent no command within {HANDSHAKE_TIMEOUT:?} of connecting");
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }
}

/// How long a new connection has to send its startup packet.
///
/// pgs3 serves one connection at a time, so a client that connects and then
/// says nothing -- a health probe, a port scan -- would otherwise hold the
/// listener shut against the real subscriber.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// SSL/GSS rejection, startup packet, then the parameters libpq expects.
fn handshake(conn: &mut Conn, server_version: &str) -> Result<()> {
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    loop {
        // The startup packet has no type byte: length first, then a version code.
        while conn.inbuf.len() < 8 {
            if Instant::now() > deadline {
                bail!("client connected but sent no startup packet within {HANDSHAKE_TIMEOUT:?}");
            }
            conn.fill()?;
            std::thread::sleep(Duration::from_millis(2));
        }
        let len = u32::from_be_bytes(conn.inbuf[0..4].try_into()?) as usize;
        let code = u32::from_be_bytes(conn.inbuf[4..8].try_into()?);
        match code {
            // SSLRequest / GSSENCRequest: decline, the client retries in plaintext.
            80877103 | 80877104 => {
                conn.inbuf.drain(..8);
                conn.stream.write_all(b"N")?;
                conn.flush()?;
            }
            196608 => {
                // Postgres caps the startup packet at 10 KB; an
                // attacker-declared length must not size our buffer.
                const MAX_STARTUP_PACKET_LENGTH: usize = 10000;
                if !(8..=MAX_STARTUP_PACKET_LENGTH).contains(&len) {
                    bail!("startup packet declares an impossible length of {len} bytes");
                }
                while conn.inbuf.len() < len {
                    if Instant::now() > deadline {
                        bail!("startup packet never completed");
                    }
                    conn.fill()?;
                }
                conn.inbuf.drain(..len);
                break;
            }
            other => bail!("unsupported startup code {other}"),
        }
    }

    conn.send(b'R', &0u32.to_be_bytes())?; // AuthenticationOk
    for (k, v) in [
        ("server_version", server_version),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", "UTC"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
    ] {
        let mut b = k.as_bytes().to_vec();
        b.push(0);
        b.extend_from_slice(v.as_bytes());
        b.push(0);
        conn.send(b'S', &b)?;
    }
    let mut key = Vec::new();
    key.extend_from_slice(&1u32.to_be_bytes());
    key.extend_from_slice(&1u32.to_be_bytes());
    conn.send(b'K', &key)?;
    conn.ready()
}

/// Handle one simple query. `Some(caught_up)` means the session is over.
fn simple_query(conn: &mut Conn, sql: &str, cat: &Catalog, opts: &Options) -> Result<Option<bool>> {
    let upper = sql.to_uppercase();

    if upper.starts_with("IDENTIFY_SYSTEM") {
        let sysid = cat.system_identifier.clone();
        let tli = opts.tli.to_string();
        let pos = opts.end.to_string();
        conn.row_description(&[
            ("systemid", 25),
            ("timeline", 23),
            ("xlogpos", 25),
            ("dbname", 25),
        ])?;
        conn.data_row(&[Some(&sysid), Some(&tli), Some(&pos), Some("pgs3")])?;
        conn.command_complete("IDENTIFY_SYSTEM")?;
        return conn.ready().map(|_| None);
    }

    // libpqwalreceiver runs this on every connection before anything else.
    if upper.contains("SET_CONFIG") && upper.contains("SEARCH_PATH") {
        conn.row_description(&[("set_config", 25)])?;
        conn.data_row(&[Some("")])?;
        conn.command_complete("SELECT 1")?;
        return conn.ready().map(|_| None);
    }

    if upper.starts_with("START_REPLICATION") {
        return stream(conn, sql, cat, opts).map(Some);
    }

    conn.error(&format!(
        "pgs3 speaks only IDENTIFY_SYSTEM and START_REPLICATION; got: {sql}"
    ))?;
    bail!("subscriber issued an unsupported command: {sql}")
}

/// Parse `key 'value'` pairs out of the option list of START_REPLICATION.
fn option<'a>(sql: &'a str, key: &str) -> Option<&'a str> {
    // ASCII folding only: to_lowercase() can change byte lengths (İ), which
    // would shift the index against the original string.
    let at = sql.to_ascii_lowercase().find(&format!("{key} '"))?;
    let rest = &sql[at + key.len() + 2..];
    rest.find('\'').map(|e| &rest[..e])
}

fn stream(conn: &mut Conn, sql: &str, cat: &Catalog, opts: &Options) -> Result<bool> {
    // We only produce binary datums, so a text-mode subscriber would silently
    // receive garbage. Refuse instead.
    match option(sql, "binary") {
        Some(v) if v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on") => {}
        _ => {
            let m = "pgs3 emits binary datums only; create the subscription \
                     WITH (binary = true)";
            conn.error(m)?;
            bail!(m);
        }
    }
    if let Some(v) = option(sql, "streaming") {
        if !v.eq_ignore_ascii_case("false") && !v.eq_ignore_ascii_case("off") {
            let m = "pgs3 does not emit in-progress transaction streams; create the \
                     subscription WITH (streaming = off)";
            conn.error(m)?;
            bail!(m);
        }
    }

    // The subscriber asks to resume from its origin's remote_lsn: everything
    // committed at or before that point is already applied. We still have to
    // *read* from restart_lsn to pick up transactions that straddle it, and
    // suppress the ones it has already seen.
    let Some(resume) = sql
        .split_whitespace()
        .find(|w| w.contains('/'))
        .and_then(|w| Lsn::parse(w).ok())
    else {
        // Resuming at zero would silently re-deliver everything instead of
        // failing on a command we cannot honor.
        let m = format!("START_REPLICATION without a resume LSN: {sql}");
        conn.error(&m)?;
        bail!(m);
    };
    if resume.0 != 0 && resume < opts.start {
        let m = format!(
            "subscriber asked to resume at {resume}, but the archive only reaches back to \
             {}. Point --start at the slot's restart_lsn, not its confirmed_flush_lsn.",
            opts.start
        );
        conn.error(&m)?;
        bail!(m);
    }
    let from = opts.start;
    eprintln!("pgs3: reading from {from}, resuming the subscriber past {resume}, to {}", opts.end);

    // CopyBothResponse: format 0, no columns.
    conn.send(b'W', &[0, 0, 0])?;
    conn.flush()?;

    let src = SegmentDir::new(&opts.wal_dir)
        .with_restore_command(opts.restore_command.clone())
        .with_switch_wal(opts.switch_wal_source.clone());
    let mut reader =
        WalReader::new(src, opts.tli, opts.seg_size, from, opts.end, opts.version, opts.sysid);
    // The start segment may still be the one the source is writing.
    loop {
        match reader.verify_provenance() {
            Err(e) if e.downcast_ref::<SegmentUnavailable>().is_some() => {
                keepalive(conn, resume.max(from), false)?;
                std::thread::sleep(Duration::from_secs(1));
            }
            r => break r?,
        }
    }
    let mut rb = ReorderBuffer::new(cat);
    rb.strict_ddl = opts.strict_ddl;
    let mut enc = Encoder::new();

    // Only commit boundaries count as progress: the subscriber's flush LSN
    // advances to a transaction's end_lsn once it has applied it, never to a
    // mid-transaction position. Comparing against anything else would wait
    // forever for a confirmation that cannot arrive.
    let mut last_commit_end = resume.max(from);
    let mut confirmed = Lsn(0);
    let mut idle_since: Option<Instant> = None;
    let mut sent_msgs = 0u64;
    let mut sent_commits = 0u64;
    let mut announced_drain = false;

    let mut last_status = Instant::now();
    loop {
        // Client feedback first: it drives both the confirmed-LSN file and the
        // decision to stop.
        if !read_feedback(conn, opts, &mut confirmed, last_commit_end)? {
            return Ok(false);
        }

        {
            match reader.next_record()? {
                Some(rec) => {
                    // feed() returns a whole transaction at once, at its commit,
                    // so the already-applied ones can be dropped as a unit.
                    let batch = rb.feed(&rec)?;
                    let batch_end = batch.iter().find_map(|e| match e {
                        Event::Commit { end_lsn, .. } | Event::SchemaChange { end_lsn, .. } => {
                            Some(*end_lsn)
                        }
                        _ => None,
                    });
                    // Its schema change included: the replica has that too.
                    if batch_end.is_some_and(|end| end <= resume) {
                        last_commit_end = last_commit_end.max(batch_end.unwrap());
                        continue;
                    }
                    let mut after: Option<(Lsn, Vec<String>)> = None;
                    for ev in batch {
                        if let Event::SchemaChange { commit_lsn, end_lsn, old, new } = &ev {
                            let plan = crate::ddl::plan(old, new);
                            for n in &plan.notes {
                                eprintln!("pgs3: schema change at {commit_lsn}: {n}");
                            }
                            let Some(target) = &opts.target else {
                                for stmt in plan.before.iter().chain(&plan.after) {
                                    eprintln!(
                                        "pgs3: schema change at {commit_lsn}; apply on the replica: {stmt}"
                                    );
                                }
                                continue;
                            };
                            // Rows in the new shape must not reach the replica
                            // before it can take them, nor the change before
                            // the rows in the old shape ahead of it.
                            if !plan.before.is_empty() {
                                if sent_commits > 0
                                    && !wait_confirmed(conn, opts, &mut confirmed, last_commit_end)?
                                {
                                    return Ok(false);
                                }
                                apply_on_target(target, &plan.before, *commit_lsn, "before")?;
                            }
                            after = Some((*commit_lsn, plan.after));
                            last_commit_end = last_commit_end.max(*end_lsn);
                            continue;
                        }
                        let at = event_lsn(&ev);
                        if let Event::Commit { end_lsn, .. } = &ev {
                            last_commit_end = *end_lsn;
                            sent_commits += 1;
                        }
                        for msg in enc.encode(&ev)? {
                            xlog_data(conn, at, last_commit_end.max(at), &msg)?;
                            sent_msgs += 1;
                        }
                    }
                    // What rows in the old shape would trip over waits until
                    // the transaction that made the change is applied.
                    if let Some((at, after)) = after.filter(|(_, a)| !a.is_empty()) {
                        if sent_commits > 0
                            && !wait_confirmed(conn, opts, &mut confirmed, last_commit_end)?
                        {
                            return Ok(false);
                        }
                        let target = opts.target.as_deref().expect("plans are only kept with a target");
                        apply_on_target(target, &after, at, "after")?;
                    }
                    continue;
                }
                None => {
                    if !announced_drain {
                        announced_drain = true;
                        conn.flush()?;
                        eprintln!(
                            "pgs3: decoded to {last_commit_end}, {sent_msgs} messages sent; \
                             waiting for the subscriber to confirm"
                        );
                    }
                }
            }
        }

        // Throttled status update during streaming (at most once per second).
        if last_status.elapsed() >= Duration::from_secs(1) {
            last_status = Instant::now();
            write_status(opts, "streaming", last_commit_end, confirmed);
        }

        // Nothing left to decode: nudge the subscriber for feedback.
        keepalive(conn, last_commit_end, true)?;
        if confirmed >= last_commit_end {
            write_status(opts, "caught_up", last_commit_end, confirmed);
            if opts.exit_when_caught_up {
                eprintln!("pgs3: subscriber confirmed {confirmed}; caught up");
                return Ok(true);
            }
            // Daemon mode: stay connected and keep looking for newly archived
            // segments. next_record() resumes wherever it stopped. Leave
            // announced_drain set so an idle follow does not spam the log.
            idle_since = None;
            std::thread::sleep(Duration::from_millis(200));
            continue;
        }
        match idle_since {
            None => idle_since = Some(Instant::now()),
            Some(t) if opts.exit_when_caught_up && t.elapsed() > Duration::from_secs(60) => {
                bail!("subscriber went 60s without confirming past {confirmed}")
            }
            _ => {}
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Read whatever feedback the subscriber has sent. False once it has closed
/// the stream.
fn read_feedback(conn: &mut Conn, opts: &Options, confirmed: &mut Lsn, wal_end: Lsn) -> Result<bool> {
    while let Some((tag, body)) = conn.poll_message()? {
        match tag {
            b'd' if body.first() == Some(&b'r') && body.len() >= 34 => {
                // StandbyStatusUpdate: write, flush, apply, clock, replyRequested
                let flush = Lsn(u64::from_be_bytes(body[9..17].try_into()?));
                if flush > *confirmed {
                    *confirmed = flush;
                    publish_confirmed(opts, *confirmed)?;
                }
                if body[33] == 1 {
                    keepalive(conn, wal_end, false)?;
                }
            }
            b'd' => {} // hot-standby feedback and friends: nothing to do
            b'c' | b'X' => return Ok(false),
            _ => {}
        }
    }
    Ok(true)
}

/// Hold the stream until the subscriber has applied everything up to `upto`.
/// False if it closed the stream meanwhile.
fn wait_confirmed(conn: &mut Conn, opts: &Options, confirmed: &mut Lsn, upto: Lsn) -> Result<bool> {
    conn.flush()?;
    let started = Instant::now();
    let mut asked: Option<Instant> = None;
    let mut announced = false;
    while *confirmed < upto {
        if !read_feedback(conn, opts, confirmed, upto)? {
            return Ok(false);
        }
        if asked.is_none_or(|t| t.elapsed() >= Duration::from_secs(1)) {
            keepalive(conn, upto, true)?;
            asked = Some(Instant::now());
        }
        if !announced && started.elapsed() > Duration::from_secs(10) {
            announced = true;
            eprintln!(
                "pgs3: waiting for the subscriber to confirm {upto} (at {confirmed}) before \
                 changing its schema"
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(true)
}

/// Apply one phase of a schema change to the replica, all or nothing, and
/// at most once: `pgs3.schema_changes` on the replica records it in the same
/// transaction, because a reconnect replays the stream from the subscriber's
/// last commit and would otherwise apply it again -- possibly against a table
/// a later change has since renamed.
fn apply_on_target(target: &str, stmts: &[String], at: Lsn, phase: &str) -> Result<()> {
    let vars = [("lsn", at.to_string()), ("phase", phase.to_string())];
    let vars: Vec<(&str, &str)> = vars.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let done = crate::psql(
        target,
        &vars,
        "create schema if not exists pgs3;\n\
         create table if not exists pgs3.schema_changes (\n\
           lsn pg_lsn, phase text, applied_at timestamptz not null default now(),\n\
           statements text[] not null, primary key (lsn, phase));\n\
         select count(*) from pgs3.schema_changes where lsn = :'lsn' and phase = :'phase';",
        "checking the target's applied schema changes",
    )?;
    if String::from_utf8_lossy(&done).trim() != "0" {
        return Ok(());
    }
    let sql = format!(
        "begin;\n{};\n\
         insert into pgs3.schema_changes (lsn, phase, statements) values (:'lsn', :'phase', array[{}]);\n\
         commit;\n",
        stmts.join(";\n"),
        stmts.iter().map(|s| format!("'{}'", s.replace('\'', "''"))).collect::<Vec<_>>().join(", ")
    );
    crate::psql(target, &vars, &sql, "applying a schema change to the target")
        .with_context(|| format!("schema change at {at}"))?;
    for s in stmts {
        eprintln!("pgs3: schema change at {at}, applied to the target ({phase}): {s}");
    }
    Ok(())
}

/// Where in WAL this message's change lives. Used as the XLogData start LSN.
fn event_lsn(e: &Event) -> Lsn {
    match e {
        Event::SchemaChange { commit_lsn, .. } => *commit_lsn,
        Event::Begin { commit_lsn, .. } => *commit_lsn,
        Event::Commit { end_lsn, .. } => *end_lsn,
        Event::Change { lsn, .. } => *lsn,
    }
}

fn xlog_data(conn: &mut Conn, start: Lsn, wal_end: Lsn, payload: &[u8]) -> Result<()> {
    let mut b = Vec::with_capacity(payload.len() + 25);
    b.push(b'w');
    b.extend_from_slice(&start.0.to_be_bytes());
    b.extend_from_slice(&wal_end.0.to_be_bytes());
    b.extend_from_slice(&now_pg_timestamp().to_be_bytes());
    b.extend_from_slice(payload);
    conn.send(b'd', &b)
}

fn keepalive(conn: &mut Conn, wal_end: Lsn, request_reply: bool) -> Result<()> {
    let mut b = Vec::with_capacity(18);
    b.push(b'k');
    b.extend_from_slice(&wal_end.0.to_be_bytes());
    b.extend_from_slice(&now_pg_timestamp().to_be_bytes());
    b.push(u8::from(request_reply));
    conn.send(b'd', &b)?;
    conn.flush()
}

/// Publish the subscriber's confirmed flush LSN for the slot advancer.
/// Written via rename so a reader never sees a half-written value.
fn publish_confirmed(opts: &Options, lsn: Lsn) -> Result<()> {
    let Some(path) = &opts.confirmed_file else { return Ok(()) };
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, format!("{lsn}\n"))?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_survives_non_ascii_input() {
        let sql = "START_REPLICATION SLOT \"İx\" LOGICAL 0/1 (binary 'true')";
        assert_eq!(option(sql, "binary"), Some("true"));
        assert_eq!(option(sql, "streaming"), None);
    }
}
