//! Reorder buffer: WAL interleaves transactions, logical replication does not.
//!
//! Buffer changes per writing xid, collect TOAST chunks alongside them, merge
//! the subtransactions the commit record lists, and emit in commit order once
//! that record arrives. Aborted transactions and rolled-back savepoints are
//! dropped at their own ABORT record without ever being emitted.

use crate::catalog::{Catalog, Relation};
use crate::lsn::Lsn;
use crate::rmgr::{self, HeapOp, RawTuple};
use crate::tuple::{self, Datum};
use crate::xlog::{Record, RM_HEAP2_ID, RM_HEAP_ID, RM_XACT_ID};
use anyhow::{bail, ensure, Result};
use std::collections::{BTreeMap, HashMap};
use std::fmt;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Kind {
    Insert,
    Update,
    Delete,
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Kind::Insert => "INSERT",
            Kind::Update => "UPDATE",
            Kind::Delete => "DELETE",
        })
    }
}

struct Change {
    lsn: Lsn,
    relfilenode: u32,
    kind: Kind,
    old: Option<Vec<Datum>>,
    new: Option<Vec<Datum>>,
}

/// A decoded logical event, in commit order. Consumers render it as text
/// (`decode` mode) or encode it as pgoutput (`serve` mode).
pub enum Event<'a> {
    Begin { xid: u32, commit_lsn: Lsn, commit_time: i64 },
    Change {
        lsn: Lsn,
        rel: &'a Relation,
        kind: Kind,
        old: Option<Vec<Datum>>,
        new: Option<Vec<Datum>>,
    },
    Commit { xid: u32, commit_lsn: Lsn, end_lsn: Lsn, commit_time: i64 },
}

#[derive(Default)]
struct Xact {
    changes: Vec<Change>,
    /// TOAST chunks seen so far in this transaction: value id -> seq -> bytes.
    // ponytail: in memory. Spill to disk once a xact exceeds a size cap;
    // a bulk UPDATE of a wide table will otherwise blow up resident size.
    toast: HashMap<u32, BTreeMap<u32, Vec<u8>>>,
}

pub struct ReorderBuffer<'a> {
    cat: &'a Catalog,
    xacts: HashMap<u32, Xact>,
    /// Refuse when DDL touches a published relation after T0.
    pub strict_ddl: bool,
    /// Catalog deletes cannot be attributed to a relation; warn once, not once
    /// per record.
    warned_catalog_delete: bool,
}

impl<'a> ReorderBuffer<'a> {
    pub fn new(cat: &'a Catalog) -> ReorderBuffer<'a> {
        ReorderBuffer {
            cat,
            xacts: HashMap::new(),
            strict_ddl: true,
            warned_catalog_delete: false,
        }
    }

    pub fn feed(&mut self, r: &Record) -> Result<Vec<Event<'a>>> {
        match r.rmid {
            RM_HEAP_ID | RM_HEAP2_ID => {
                // Catalog writes are checked from the raw record, before
                // parse_heap discards them for lacking logical-decoding flags.
                if let Some(b) = r.block(0) {
                    if let Some(name) =
                        self.cat.rowtype_catalog_name(b.rlocator.db_oid, b.rlocator.rel_number)
                    {
                        self.check_rowtype_catalog_write(r, name)?;
                        return Ok(Vec::new());
                    }
                }
                self.feed_heap(r)?;
                Ok(Vec::new())
            }
            RM_XACT_ID => self.feed_xact(r),
            _ => Ok(Vec::new()),
        }
    }

    fn feed_heap(&mut self, r: &Record) -> Result<()> {
        // TRUNCATE carries its relations in main data (no block reference).
        // It assigns a new relfilenode to the table and its toast, which the
        // frozen dictionary cannot follow -- refuse rather than silently drop
        // every row written afterwards.
        if r.rmid == RM_HEAP_ID && r.heap_opcode() == rmgr::XLOG_HEAP_TRUNCATE {
            let (db_oid, relids) = rmgr::parse_truncate(r)?;
            if db_oid == self.cat.database_oid {
                if let Some(rel) = relids.iter().find_map(|o| self.cat.by_oid(*o)) {
                    bail!(
                        "TRUNCATE of published relation {} at {}: truncate assigns a new \
                         relfilenode, which the frozen catalog dictionary cannot follow. \
                         Re-capture the dictionary at a new T0.",
                        rel.qualified_name(),
                        r.lsn
                    );
                }
            }
            return Ok(());
        }

        let Some((rloc, op)) = rmgr::parse_heap(r)? else {
            return Ok(());
        };
        if rloc.db_oid != self.cat.database_oid {
            return Ok(());
        }

        // Route TOAST chunk inserts to the owning relation's chunk store.
        let is_toast_heap = self.cat.owner_of_toast(rloc.db_oid, rloc.rel_number).is_some();
        if is_toast_heap {
            let tuples: Vec<&RawTuple> = match &op {
                HeapOp::Insert { new } => vec![new],
                HeapOp::MultiInsert { tuples } => tuples.iter().collect(),
                // Chunks of a value being replaced get deleted; the new value's
                // chunks arrive as inserts, so deletes here are not interesting.
                _ => return Ok(()),
            };
            for t in tuples {
                let vals =
                    tuple::deform(&t.data, t.infomask, t.infomask2, t.t_hoff, &tuple::TOAST_CHUNK_ATTRS)?;
                let (Datum::Fixed(id), Datum::Fixed(seq), Datum::Var(bytes)) =
                    (&vals[0], &vals[1], &vals[2])
                else {
                    bail!("TOAST chunk at {} did not deform to (oid, int4, bytea)", r.lsn);
                };
                let value_id = u32::from_le_bytes(id[..].try_into()?);
                let chunk_seq = u32::from_le_bytes(seq[..].try_into()?);
                self.xacts
                    .entry(r.xid)
                    .or_default()
                    .toast
                    .entry(value_id)
                    .or_default()
                    .insert(chunk_seq, bytes.clone());
            }
            return Ok(());
        }

        let Some(rel) = self.cat.by_relfilenode(rloc.db_oid, rloc.rel_number) else {
            return Ok(()); // not published, or a catalog/index relation
        };
        let attrs: Vec<tuple::AttrDesc> = rel.columns.iter().map(Into::into).collect();
        let de = |t: &RawTuple| tuple::deform(&t.data, t.infomask, t.infomask2, t.t_hoff, &attrs);

        let mut push = |kind, old, new| {
            self.xacts.entry(r.xid)
                .or_default()
                .changes
                .push(Change { lsn: r.lsn, relfilenode: rloc.rel_number, kind, old, new });
        };

        match op {
            HeapOp::NoTupleData => bail!(
                "record at {} touches published relation {} but carries no tuple image. \
                 The source was not running wal_level=logical when this WAL was written, \
                 so the row data does not exist in the archive.",
                r.lsn,
                rel.qualified_name()
            ),
            HeapOp::Insert { new } => push(Kind::Insert, None, Some(de(&new)?)),
            HeapOp::MultiInsert { tuples } => {
                for t in &tuples {
                    push(Kind::Insert, None, Some(de(t)?));
                }
            }
            HeapOp::Update { old, new } => {
                let o = old.as_ref().map(de).transpose()?;
                push(Kind::Update, o, Some(de(&new)?))
            }
            HeapOp::Delete { old } => {
                let o = old.as_ref().map(de).transpose()?;
                push(Kind::Delete, o, None)
            }
        }
        Ok(())
    }

    /// Decide whether a write to pg_class or pg_attribute concerns us.
    ///
    /// Both catalogs lead with the Oid of the relation they describe, so an
    /// INSERT or UPDATE can be attributed exactly: DDL on an unpublished table,
    /// and the temp-table churn that a busy system produces constantly, are
    /// ignored. Only opcodes that carry tuple images are examined below --
    /// XLOG_HEAP_INPLACE (vacuum/analyze statistics), lock records, and HEAP2
    /// prune/visible traffic touch catalog pages harmlessly all day, and must
    /// not burn the one warning the unattributable path gets.
    ///
    /// Deletes are the gap: catalogs are not logically logged, so a catalog
    /// DELETE carries no old tuple and cannot be attributed. DROP COLUMN is an
    /// UPDATE (it sets attisdropped) so it is caught when the update keeps its
    /// tuple image intact; a delta-compressed one (see `raw_tuples`) and a DROP
    /// TABLE fall to the warning. That leaves a warning rather than a refusal.
    fn check_rowtype_catalog_write(&mut self, r: &Record, catname: &str) -> Result<()> {
        if !self.strict_ddl {
            return Ok(());
        }
        let op = r.heap_opcode();
        let carries_tuples = (r.rmid == RM_HEAP_ID
            && matches!(
                op,
                rmgr::XLOG_HEAP_INSERT
                    | rmgr::XLOG_HEAP_DELETE
                    | rmgr::XLOG_HEAP_UPDATE
                    | rmgr::XLOG_HEAP_HOT_UPDATE
            ))
            || (r.rmid == RM_HEAP2_ID && op == rmgr::XLOG_HEAP2_MULTI_INSERT);
        if !carries_tuples {
            return Ok(());
        }
        let tuples = rmgr::raw_tuples(r)?;
        if tuples.is_empty() {
            // A DELETE, a delta-compressed UPDATE whose leading Oid was
            // stripped, or a record whose tuple data was folded into a
            // full-page image. Either way there is nothing to attribute.
            if !self.warned_catalog_delete {
                self.warned_catalog_delete = true;
                eprintln!(
                    "pgs3: warning: {catname} written at {} with no readable tuple; this one \
                     cannot be attributed to a relation. If a published table was dropped or \
                     rewritten, re-capture the dictionary.",
                    r.lsn
                );
            }
            return Ok(());
        }
        let hit = tuples.iter().find_map(|t| {
            tuple::leading_oid(&t.data, t.t_hoff).filter(|o| self.cat.is_published_oid(*o))
        });
        match hit {
            None => Ok(()),
            Some(oid) => {
                let rel = self
                    .cat
                    .relations
                    .iter()
                    .find(|x| x.oid == oid)
                    .map(|x| x.qualified_name())
                    .unwrap_or_else(|| format!("oid {oid}"));
                bail!(
                    "DDL at {}: {catname} was written for the published relation {rel} after \
                     the catalog dictionary was captured. Decoding its rows against the frozen \
                     dictionary would misread them. Re-capture at a new T0, or pass \
                     --allow-catalog-changes if you are certain the rowtype did not change.",
                    r.lsn
                )
            }
        }
    }

    fn feed_xact(&mut self, r: &Record) -> Result<Vec<Event<'a>>> {
        let op = r.info & 0x70;
        match op {
            rmgr::XLOG_XACT_ABORT | rmgr::XLOG_XACT_ABORT_PREPARED => {
                let info = rmgr::parse_xact(r)?;
                for x in info.subxacts.iter().chain(std::iter::once(&r.xid)) {
                    self.xacts.remove(x);
                }
                Ok(Vec::new())
            }

            rmgr::XLOG_XACT_COMMIT | rmgr::XLOG_XACT_COMMIT_PREPARED => {
                let info = rmgr::parse_xact(r)?;
                let mut xact = self.xacts.remove(&r.xid).unwrap_or_default();
                for s in &info.subxacts {
                    if let Some(sub) = self.xacts.remove(s) {
                        xact.changes.extend(sub.changes);
                        for (k, v) in sub.toast {
                            xact.toast.entry(k).or_default().extend(v);
                        }
                    }
                }
                if xact.changes.is_empty() {
                    return Ok(Vec::new());
                }

                // WAL order within a transaction is apply order.
                xact.changes.sort_by_key(|c| c.lsn);

                // Copy the catalog reference out so the events borrow it for
                // 'a rather than for the duration of this &mut self borrow.
                let cat: &'a Catalog = self.cat;
                let (xid, commit_lsn, end_lsn, commit_time) =
                    (r.xid, r.lsn, r.end_lsn, info.xact_time);
                let mut out = vec![Event::Begin { xid, commit_lsn, commit_time }];
                for c in xact.changes {
                    let rel = cat
                        .by_relfilenode(cat.database_oid, c.relfilenode)
                        .expect("relation was resolved when the change was buffered");
                    out.push(Event::Change {
                        lsn: c.lsn,
                        rel,
                        kind: c.kind,
                        old: c.old.map(|v| detoast(&v, &xact.toast)).transpose()?,
                        new: c.new.map(|v| detoast(&v, &xact.toast)).transpose()?,
                    });
                }
                out.push(Event::Commit { xid, commit_lsn, end_lsn, commit_time });
                Ok(out)
            }

            _ => Ok(Vec::new()),
        }
    }
}

/// Replace external TOAST pointers with their reassembled value. A pointer with
/// no chunks in this transaction is an UPDATE that left the value alone.
fn detoast(
    vals: &[Datum],
    chunks: &HashMap<u32, BTreeMap<u32, Vec<u8>>>,
) -> Result<Vec<Datum>> {
    let mut out = Vec::with_capacity(vals.len());
    for v in vals {
        out.push(match v {
            Datum::Toast(p) => match chunks.get(&p.value_id) {
                None => Datum::UnchangedToast,
                Some(map) => {
                    // The pointer comes from untrusted WAL: a raw_size below 4
                    // would wrap the subtraction below, and ext_size sizes an
                    // allocation before anything is verified.
                    ensure!(
                        p.raw_size >= 4 && (p.raw_size as u64) <= (1 << 30),
                        "TOAST pointer for value {} claims raw size {}",
                        p.value_id,
                        p.raw_size
                    );
                    let mut buf = Vec::with_capacity((p.ext_size as usize).min(1 << 20));
                    for (i, (seq, bytes)) in map.iter().enumerate() {
                        if *seq as usize != i {
                            bail!(
                                "TOAST value {} is missing chunk {i}; the archive has a gap",
                                p.value_id
                            );
                        }
                        buf.extend_from_slice(bytes);
                    }
                    if buf.len() != p.ext_size as usize {
                        bail!(
                            "TOAST value {} reassembled to {} bytes, pointer says {}",
                            p.value_id,
                            buf.len(),
                            p.ext_size
                        );
                    }
                    if p.is_compressed() {
                        Datum::Var(tuple::decompress(&buf, p.raw_size as usize - 4, p.method)?)
                    } else {
                        Datum::Var(buf)
                    }
                }
            },
            other => other.clone(),
        });
    }
    Ok(out)
}

/// Render an event the way `test_decoding` does, so the two can be diffed.
pub fn render(e: &Event) -> Option<String> {
    match e {
        Event::Begin { xid, .. } => Some(format!("BEGIN {xid}")),
        Event::Commit { xid, .. } => Some(format!("COMMIT {xid}")),
        Event::Change { rel, kind, old, new, .. } => {
            let mut s = format!("table {}: {}:", rel.qualified_name(), kind);
            if let Some(old) = old {
                s.push_str(" old-key:");
                s.push_str(&render_row(rel, old, *kind == Kind::Update));
                if new.is_some() {
                    s.push_str(" new-tuple:");
                }
            }
            if let Some(new) = new {
                s.push_str(&render_row(rel, new, false));
            }
            Some(s)
        }
    }
}

fn render_row(rel: &Relation, vals: &[Datum], key_only: bool) -> String {
    let mut s = String::new();
    for (col, v) in rel.columns.iter().zip(vals) {
        if col.attisdropped {
            continue;
        }
        // An UPDATE's old image under REPLICA IDENTITY DEFAULT holds only key
        // columns; non-key positions come through as NULL and are not meaningful.
        if key_only && rel.relreplident != 'f' && !col.attispkey {
            continue;
        }
        s.push_str(&format!(" {}[{}]:{}", col.attname, col.typname, render_value(&col.typname, v)));
    }
    s
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Text rendering for the type allowlist.
///
/// Deliberately small. The production output is pgoutput in **binary** mode
/// (`CREATE SUBSCRIPTION ... WITH (binary = true)`), where the on-disk bytes go
/// on the wire nearly as-is and none of this is needed. Reimplementing
/// `numeric_out`/`jsonb_out`/`timestamptz_out` is a bottomless tail, so
/// anything outside the allowlist is rendered as tagged hex and the schema is
/// checked against this list at startup.
fn render_value(typname: &str, d: &Datum) -> String {
    let bytes = match d {
        Datum::Null => return "null".into(),
        Datum::UnchangedToast => return "unchanged-toast-datum".into(),
        Datum::Toast(_) => return "toast-pointer-unresolved".into(),
        Datum::Fixed(b) | Datum::Var(b) => b,
    };
    let fixed = |n: usize| -> Option<[u8; 8]> {
        if bytes.len() != n {
            return None;
        }
        let mut a = [0u8; 8];
        a[..n].copy_from_slice(bytes);
        Some(a)
    };
    match typname {
        "bool" => if bytes.first() == Some(&0) { "false" } else { "true" }.into(),
        "int2" => fixed(2).map_or_else(hexdump(bytes), |a| {
            i16::from_le_bytes(a[..2].try_into().unwrap()).to_string()
        }),
        "int4" => fixed(4).map_or_else(hexdump(bytes), |a| {
            i32::from_le_bytes(a[..4].try_into().unwrap()).to_string()
        }),
        "oid" | "xid" => fixed(4).map_or_else(hexdump(bytes), |a| {
            u32::from_le_bytes(a[..4].try_into().unwrap()).to_string()
        }),
        "int8" => fixed(8).map_or_else(hexdump(bytes), |a| i64::from_le_bytes(a).to_string()),
        "float4" => fixed(4).map_or_else(hexdump(bytes), |a| {
            f32::from_le_bytes(a[..4].try_into().unwrap()).to_string()
        }),
        "float8" => fixed(8).map_or_else(hexdump(bytes), |a| f64::from_le_bytes(a).to_string()),
        "text" | "varchar" | "bpchar" | "json" | "xml" => {
            quote(&String::from_utf8_lossy(bytes))
        }
        "name" => {
            // nameout stops at the terminator; the datum is NUL-padded to 64.
            let end = bytes.iter().position(|&c| c == 0).unwrap_or(bytes.len());
            quote(&String::from_utf8_lossy(&bytes[..end]))
        }
        "bytea" => quote(&format!("\\x{}", hex(bytes))),
        "numeric" => crate::numeric::parse(bytes)
            .map(|n| crate::numeric::to_text(&n))
            .unwrap_or_else(|_| format!("<numeric:\\x{}>", hex(bytes))),
        "jsonb" => crate::jsonb::to_text(bytes)
            .map(|t| quote(&t))
            .unwrap_or_else(|_| format!("<jsonb:\\x{}>", hex(bytes))),
        "uuid" if bytes.len() == 16 => {
            let h = hex(bytes);
            quote(&format!(
                "{}-{}-{}-{}-{}",
                &h[0..8],
                &h[8..12],
                &h[12..16],
                &h[16..20],
                &h[20..32]
            ))
        }
        // Not in the allowlist: surface the raw datum rather than guess.
        other => format!("<{other}:\\x{}>", hex(bytes)),
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn hexdump(b: &[u8]) -> impl FnOnce() -> String + '_ {
    move || format!("<badlen:\\x{}>", hex(b))
}
