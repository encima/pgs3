//! The frozen catalog dictionary.
//!
//! Captured once from the source primary at T0 (see `sql/dump_catalog.sql`) and
//! never consulted again. That is the whole point: because we never need the
//! primary's live catalog, the primary is free to vacuum it, so it holds no
//! `catalog_xmin` on our behalf and no WAL beyond the slot advance interval.
//!
//! The cost of freezing is that DDL after T0 invalidates us. `reorder.rs`
//! detects that and refuses rather than decoding tuples with a stale rowtype.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// Mirrors the columns of `pg_attribute` the decoder and the pgoutput encoder
/// need. Fields are populated by serde from the dictionary even where only the
/// encoder reads them.
/// Element type of an array column: what the payload walk and the per-element
/// encoder need.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ElemType {
    pub oid: u32,
    pub typname: String,
    pub typlen: i16,
    pub typalign: char,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Column {
    pub attnum: i16,
    pub attname: String,
    pub atttypid: u32,
    pub atttypmod: i32,
    pub attlen: i16,
    pub attbyval: bool,
    pub attalign: char,
    pub attstorage: char,
    pub attnotnull: bool,
    pub attisdropped: bool,
    pub attispkey: bool,
    /// Empty for an ordinary column; 's' for a stored generated column.
    pub attgenerated: String,
    pub typname: String,
    pub typnsp: String,
    /// Present only for array columns.
    pub elem: Option<ElemType>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Relation {
    pub oid: u32,
    pub relfilenode: u32,
    pub tablespace_oid: u32,
    pub nspname: String,
    pub relname: String,
    pub relreplident: char,
    pub toast_relfilenode: Option<u32>,
    pub columns: Vec<Column>,
}

impl Relation {
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.nspname, self.relname)
    }

}

/// A type reachable from a published column: the column's own type, array
/// elements, domain base types and composite attributes, recursively. Enough
/// to reproduce each type's `send()` from its on-disk bytes.
#[derive(Debug, Deserialize)]
pub struct TypeInfo {
    pub oid: u32,
    pub typname: String,
    /// b(ase) c(omposite) d(omain) e(num) r(ange) m(ultirange) p(seudo)
    pub typtype: char,
    pub typlen: i16,
    pub typalign: char,
    /// Element type, for arrays.
    pub elem: Option<u32>,
    /// Base type, for domains.
    pub basetype: Option<u32>,
    /// `pg_enum` Oid and label, for enums.
    pub labels: Option<Vec<(u32, String)>>,
    /// Every attribute including dropped ones, for composites: dropped
    /// attributes still occupy a slot in the stored tuple.
    pub attrs: Option<Vec<TypeAttr>>,
}

#[derive(Debug, Deserialize)]
pub struct TypeAttr {
    pub atttypid: u32,
    pub attlen: i16,
    pub attalign: char,
    pub attisdropped: bool,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct RowtypeCatalogs {
    pub pg_class: u32,
    pub pg_attribute: u32,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Catalog {
    pub database_oid: u32,
    pub wal_segment_size: u64,
    pub server_version_num: i32,
    pub system_identifier: String,
    pub captured_at_lsn: String,
    /// Consistent decoding start and its timeline, set by `pgs3 init`; absent
    /// when the dictionary was dumped by hand.
    pub start_lsn: Option<String>,
    pub timeline_id: Option<u32>,
    pub relations: Vec<Relation>,
    pub rowtype_catalogs: RowtypeCatalogs,
    #[serde(default)]
    pub types: Vec<TypeInfo>,
    #[serde(skip)]
    types_by_oid: HashMap<u32, usize>,
    #[serde(skip)]
    by_relfilenode: HashMap<u32, usize>,
    /// toast relfilenode -> index of the relation that owns it
    #[serde(skip)]
    by_toast_relfilenode: HashMap<u32, usize>,
    #[serde(skip)]
    published_oids: std::collections::HashSet<u32>,
}

impl Catalog {
    pub fn load(path: impl AsRef<Path>) -> Result<Catalog> {
        let text = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("reading catalog dictionary {:?}", path.as_ref()))?;
        let mut cat: Catalog = serde_json::from_str(&text).context("parsing catalog dictionary")?;
        // The dictionary is trusted input, but a malformed one would otherwise
        // surface much later as corrupt pgoutput frames or a decoding loop
        // that never advances.
        for r in &cat.relations {
            anyhow::ensure!(
                !r.nspname.contains('\0') && !r.relname.contains('\0'),
                "{}: NUL byte in relation name would corrupt pgoutput framing",
                r.qualified_name()
            );
            for c in &r.columns {
                anyhow::ensure!(!c.attname.contains('\0'), "{}.{}: NUL byte in column name", r.qualified_name(), c.attname);
                anyhow::ensure!(c.attlen != 0, "{}.{}: attlen 0 would stall the tuple walk", r.qualified_name(), c.attname);
                if let Some(e) = &c.elem {
                    anyhow::ensure!(e.typlen != 0, "{}.{}: array element typlen 0 would stall the payload walk", r.qualified_name(), c.attname);
                }
            }
        }
        let mut seen = std::collections::HashSet::new();
        for (i, r) in cat.relations.iter().enumerate() {
            anyhow::ensure!(seen.insert(r.relfilenode), "duplicate relfilenode {} in dictionary", r.relfilenode);
            cat.by_relfilenode.insert(r.relfilenode, i);
            if let Some(t) = r.toast_relfilenode {
                cat.by_toast_relfilenode.insert(t, i);
            }
        }
        cat.published_oids = cat.relations.iter().map(|r| r.oid).collect();
        anyhow::ensure!(
            cat.relations.is_empty() || !cat.types.is_empty(),
            "dictionary has no type map (it predates it); capture it again with `pgs3 init`"
        );
        for (i, t) in cat.types.iter().enumerate() {
            anyhow::ensure!(t.typlen != 0, "type {}: typlen 0 would stall the tuple walk", t.typname);
            cat.types_by_oid.insert(t.oid, i);
        }
        Ok(cat)
    }

    pub fn type_info(&self, oid: u32) -> Option<&TypeInfo> {
        self.types_by_oid.get(&oid).map(|&i| &self.types[i])
    }

    /// A published relation by Oid (what TRUNCATE records carry).
    pub fn by_oid(&self, oid: u32) -> Option<&Relation> {
        // ponytail: linear scan; dictionaries hold published tables, not millions.
        self.relations.iter().find(|r| r.oid == oid)
    }

    /// A published relation whose heap this WAL record touches.
    pub fn by_relfilenode(&self, db_oid: u32, relfilenode: u32) -> Option<&Relation> {
        if db_oid != self.database_oid {
            return None;
        }
        self.by_relfilenode.get(&relfilenode).map(|i| &self.relations[*i])
    }

    /// Is this the heap of a catalog that defines a relation's rowtype?
    /// Writes to these are what can invalidate the frozen dictionary.
    pub fn rowtype_catalog_name(&self, db_oid: u32, relfilenode: u32) -> Option<&'static str> {
        if db_oid != self.database_oid {
            return None;
        }
        if relfilenode == self.rowtype_catalogs.pg_attribute {
            Some("pg_attribute")
        } else if relfilenode == self.rowtype_catalogs.pg_class {
            Some("pg_class")
        } else {
            None
        }
    }

    pub fn is_published_oid(&self, oid: u32) -> bool {
        self.published_oids.contains(&oid)
    }

    /// The owner of a TOAST heap, so chunk inserts can be routed to it.
    pub fn owner_of_toast(&self, db_oid: u32, relfilenode: u32) -> Option<&Relation> {
        if db_oid != self.database_oid {
            return None;
        }
        self.by_toast_relfilenode.get(&relfilenode).map(|i| &self.relations[*i])
    }

}
