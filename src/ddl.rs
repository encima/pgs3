//! The DDL a subscriber needs to follow a schema change, from the difference
//! between the dictionary before it and the one the source wrote with it.
//!
//! Generated from the dictionaries rather than replayed from the source's SQL:
//! the statement that changed the source may carry DML or anything else that
//! must not run twice, and only the shape replication cares about is needed
//! downstream. Everything is idempotent (IF [NOT] EXISTS, or guarded on the
//! subscriber's own catalog), because a reconnect replays the stream from the
//! last confirmed commit and may apply a change a second time.
//!
//! Split in two by when it can run:
//!   * `before` the transaction that made the change is applied: anything rows
//!     in the new shape need (added columns, renames, new types, identity).
//!   * `after` it is applied: anything rows in the old shape would trip over
//!     (dropped columns, NOT NULL tightened).

use crate::catalog::{Catalog, Column, Relation};

#[derive(Default, Debug)]
pub struct Plan {
    pub before: Vec<String>,
    pub after: Vec<String>,
    /// Changes that cannot be applied automatically, to be logged.
    pub notes: Vec<String>,
}

impl Plan {
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.before.is_empty() && self.after.is_empty() && self.notes.is_empty()
    }
}

pub fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn qualified(nsp: &str, rel: &str) -> String {
    format!("{}.{}", ident(nsp), ident(rel))
}

/// The column's type as `format_type()` renders it, typmod included.
/// Dictionaries from before it was captured fall back to the bare type name.
fn type_of(c: &Column) -> String {
    c.typdef.clone().unwrap_or_else(|| qualified(&c.typnsp, &c.typname))
}

fn column_def(c: &Column) -> String {
    let mut s = format!("{} {}", ident(&c.attname), type_of(c));
    match (&c.default_expr, c.attgenerated.as_str()) {
        (Some(e), "s") => s.push_str(&format!(" GENERATED ALWAYS AS ({e}) STORED")),
        (Some(e), _) => s.push_str(&format!(" DEFAULT {e}")),
        _ => {}
    }
    if c.attnotnull {
        s.push_str(" NOT NULL");
    }
    s
}

/// Run `stmt` only while the subscriber's own catalog says `cond`.
fn guarded(cond: &str, stmt: &str) -> String {
    format!("DO $pgs3$ BEGIN IF {cond} THEN {stmt}; END IF; END $pgs3$")
}

/// Whether the subscriber has the column, optionally only if `extra` (an
/// expression over pg_attribute) also holds for it.
fn column_where(table: &str, col: &str, extra: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass({}) \
         AND attname = {} AND NOT attisdropped{extra})",
        literal(table),
        literal(col)
    )
}

fn column_exists(table: &str, col: &str) -> String {
    column_where(table, col, "")
}

pub fn plan(old: &Catalog, new: &Catalog) -> Plan {
    let mut p = Plan::default();
    for n in &new.relations {
        match old.relations.iter().find(|o| o.oid == n.oid) {
            Some(o) => relation(&mut p, o, n),
            None => p.notes.push(format!(
                "{} joined the publication: create it on the replica and run \
                 ALTER SUBSCRIPTION ... REFRESH PUBLICATION against the source",
                n.qualified_name()
            )),
        }
    }
    for o in &old.relations {
        if !new.relations.iter().any(|n| n.oid == o.oid) {
            // Never drop data downstream on the source's say-so.
            p.notes.push(format!(
                "{} left the publication (or was dropped); left as it is on the replica",
                o.qualified_name()
            ));
        }
    }
    p
}

fn relation(p: &mut Plan, o: &Relation, n: &Relation) {
    let old_name = qualified(&o.nspname, &o.relname);
    // Renames first: everything below addresses the table by its new name.
    if o.nspname != n.nspname {
        p.before.push(guarded(
            &format!("to_regclass({}) IS NOT NULL", literal(&old_name)),
            &format!("ALTER TABLE {old_name} SET SCHEMA {}", ident(&n.nspname)),
        ));
    }
    if o.relname != n.relname {
        let moved = qualified(&n.nspname, &o.relname);
        p.before.push(guarded(
            &format!("to_regclass({}) IS NOT NULL", literal(&moved)),
            &format!("ALTER TABLE {moved} RENAME TO {}", ident(&n.relname)),
        ));
    }
    let table = qualified(&n.nspname, &n.relname);

    for nc in n.columns.iter().filter(|c| !c.attisdropped) {
        let Some(oc) = o.columns.iter().find(|c| c.attnum == nc.attnum && !c.attisdropped) else {
            p.before.push(format!("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {}", column_def(nc)));
            continue;
        };
        let col = ident(&nc.attname);
        if oc.attname != nc.attname {
            p.before.push(guarded(
                &column_exists(&table, &oc.attname),
                &format!("ALTER TABLE {table} RENAME COLUMN {} TO {col}", ident(&oc.attname)),
            ));
        }
        if type_of(oc) != type_of(nc) {
            // Guarded: replayed, it would rewrite the table a second time.
            let ty = type_of(nc);
            p.before.push(guarded(
                &column_where(
                    &table,
                    &nc.attname,
                    &format!(" AND format_type(atttypid, atttypmod) <> {}", literal(&ty)),
                ),
                &format!("ALTER TABLE {table} ALTER COLUMN {col} TYPE {ty} USING {col}::{ty}"),
            ));
        }
        if oc.default_expr != nc.default_expr && nc.attgenerated.is_empty() {
            p.before.push(match &nc.default_expr {
                Some(e) => format!("ALTER TABLE {table} ALTER COLUMN {col} SET DEFAULT {e}"),
                None => format!("ALTER TABLE {table} ALTER COLUMN {col} DROP DEFAULT"),
            });
        }
        match (oc.attnotnull, nc.attnotnull) {
            (true, false) => p.before.push(format!("ALTER TABLE {table} ALTER COLUMN {col} DROP NOT NULL")),
            // Guarded: replayed, it would scan the table a second time.
            (false, true) => p.after.push(guarded(
                &column_where(&table, &nc.attname, " AND NOT attnotnull"),
                &format!("ALTER TABLE {table} ALTER COLUMN {col} SET NOT NULL"),
            )),
            _ => {}
        }
    }
    for oc in o.columns.iter().filter(|c| !c.attisdropped) {
        let gone = n.columns.iter().find(|c| c.attnum == oc.attnum).is_none_or(|c| c.attisdropped);
        if gone {
            p.after.push(format!("ALTER TABLE {table} DROP COLUMN IF EXISTS {}", ident(&oc.attname)));
        }
    }

    if o.relreplident != n.relreplident {
        match n.relreplident {
            'f' => p.before.push(format!("ALTER TABLE {table} REPLICA IDENTITY FULL")),
            'd' => p.before.push(format!("ALTER TABLE {table} REPLICA IDENTITY DEFAULT")),
            'n' => p.before.push(format!("ALTER TABLE {table} REPLICA IDENTITY NOTHING")),
            _ => p.notes.push(format!(
                "{table}: REPLICA IDENTITY USING INDEX is not applied automatically; set it on the replica"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cat(cols: &str, relname: &str, replident: char) -> Catalog {
        Catalog::from_json(&format!(
            r#"{{"database_oid": 5, "wal_segment_size": 16777216, "server_version_num": 170006,
                "system_identifier": "1", "captured_at_lsn": "0/1",
                "rowtype_catalogs": {{"pg_class": 1259, "pg_attribute": 1249}},
                "types": [{{"oid": 23, "typname": "int4", "typtype": "b", "typlen": 4, "typalign": "i"}}],
                "relations": [{{"oid": 100, "relfilenode": 200, "tablespace_oid": 0,
                  "nspname": "public", "relname": "{relname}", "relreplident": "{replident}",
                  "toast_relfilenode": null, "columns": [{cols}]}}]}}"#
        ))
        .unwrap()
    }

    fn col(attnum: i16, name: &str, typdef: &str, notnull: bool, default: Option<&str>, dropped: bool) -> String {
        format!(
            r#"{{"attnum": {attnum}, "attname": "{name}", "atttypid": 23, "atttypmod": -1,
                "attlen": 4, "attbyval": true, "attalign": "i", "attstorage": "p",
                "attnotnull": {notnull}, "attisdropped": {dropped}, "attispkey": {pk},
                "attgenerated": "", "typname": "int4", "typnsp": "pg_catalog",
                "typdef": "{typdef}", "default": {default}}}"#,
            pk = attnum == 1,
            default = default.map_or("null".into(), |d| format!("\"{d}\"")),
        )
    }

    #[test]
    fn add_rename_retype_and_drop_land_in_the_right_phase() {
        let old = cat(
            &[
                col(1, "id", "integer", true, None, false),
                col(2, "a", "integer", false, None, false),
                col(3, "b", "integer", false, None, false),
                col(4, "c", "integer", false, None, false),
            ]
            .join(","),
            "t",
            'd',
        );
        let new = cat(
            &[
                col(1, "id", "integer", true, None, false),
                col(2, "a2", "integer", false, None, false),
                col(3, "b", "bigint", true, Some("0"), false),
                col(4, "........pg.dropped.4........", "-", false, None, true),
                col(5, "d", "text", true, Some("'x'::text"), false),
            ]
            .join(","),
            "t2",
            'f',
        );
        let p = plan(&old, &new);
        assert_eq!(
            p.before,
            vec![
                r#"DO $pgs3$ BEGIN IF to_regclass('"public"."t"') IS NOT NULL THEN ALTER TABLE "public"."t" RENAME TO "t2"; END IF; END $pgs3$"#,
                r#"DO $pgs3$ BEGIN IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('"public"."t2"') AND attname = 'a' AND NOT attisdropped) THEN ALTER TABLE "public"."t2" RENAME COLUMN "a" TO "a2"; END IF; END $pgs3$"#,
                r#"DO $pgs3$ BEGIN IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('"public"."t2"') AND attname = 'b' AND NOT attisdropped AND format_type(atttypid, atttypmod) <> 'bigint') THEN ALTER TABLE "public"."t2" ALTER COLUMN "b" TYPE bigint USING "b"::bigint; END IF; END $pgs3$"#,
                r#"ALTER TABLE "public"."t2" ALTER COLUMN "b" SET DEFAULT 0"#,
                r#"ALTER TABLE "public"."t2" ADD COLUMN IF NOT EXISTS "d" text DEFAULT 'x'::text NOT NULL"#,
                r#"ALTER TABLE "public"."t2" REPLICA IDENTITY FULL"#,
            ]
        );
        assert_eq!(
            p.after,
            vec![
                r#"DO $pgs3$ BEGIN IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('"public"."t2"') AND attname = 'b' AND NOT attisdropped AND NOT attnotnull) THEN ALTER TABLE "public"."t2" ALTER COLUMN "b" SET NOT NULL; END IF; END $pgs3$"#,
                r#"ALTER TABLE "public"."t2" DROP COLUMN IF EXISTS "c""#,
            ]
        );
        assert!(p.notes.is_empty());
    }

    #[test]
    fn no_change_no_plan() {
        let c = || cat(&col(1, "id", "integer", true, None, false), "t", 'd');
        assert!(plan(&c(), &c()).is_empty());
    }
}
