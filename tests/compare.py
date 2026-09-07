#!/usr/bin/env python3
"""Compare pgs3 decode output against Postgres' own test_decoding output.

The oracle is the real thing: both sides consumed the same WAL, one through
Postgres' logical decoding and one through ours. Any disagreement on an
allowlisted type is a decoder bug.
"""
import re, sys

# test_decoding prints SQL type names; the catalog dictionary carries pg_type.typname.
SQL_TO_TYPNAME = {
    "integer": "int4", "smallint": "int2", "bigint": "int8", "oid": "oid",
    "text": "text", "boolean": "bool", "uuid": "uuid", "bytea": "bytea",
    "character varying": "varchar", "character": "bpchar", "name": "name",
    "real": "float4", "double precision": "float8", "json": "json",
    "numeric": "numeric", "jsonb": "jsonb", "timestamp with time zone": "timestamptz",
    "timestamp without time zone": "timestamp", "date": "date",
}
# Types pgs3 renders as text today. Everything else is compared for
# presence/NULL-ness only, because pgs3 emits tagged hex for it by design --
# production output is binary pgoutput, where none of this rendering exists.
ALLOWLIST = {"int2", "int4", "int8", "oid", "text", "varchar", "bpchar", "name",
             "bool", "uuid", "bytea", "float4", "float8", "json", "numeric", "jsonb"}

# The type name can itself contain brackets: `tags[text[]]:`.
FIELD = re.compile(r"(\w+)\[([^\]]*(?:\[\][^\]]*)*)\]:")

def to_typname(sql_type):
    """test_decoding prints SQL type names; the dictionary carries pg_type.typname.
    Array types print as `text[]` but are named `_text`."""
    if sql_type.endswith("[]"):
        return "_" + SQL_TO_TYPNAME.get(sql_type[:-2], sql_type[:-2])
    return SQL_TO_TYPNAME.get(sql_type, sql_type)


def parse_fields(s):
    """Yield (col, typname, value). Values may be bare or '..'-quoted with '' escapes."""
    out, pos = [], 0
    for m in FIELD.finditer(s):
        col, typ = m.group(1), m.group(2)
        p = m.end()
        if p < len(s) and s[p] == "'":
            p += 1
            buf = []
            while p < len(s):
                if s[p] == "'":
                    if p + 1 < len(s) and s[p + 1] == "'":
                        buf.append("'"); p += 2; continue
                    p += 1; break
                buf.append(s[p]); p += 1
            val = "".join(buf)
        else:
            end = s.find(" ", p)
            val = s[p:] if end < 0 else s[p:end]
        out.append((col, to_typname(typ), val))
    return out

def parse_stream(lines):
    """Canonicalise to a list of (op, table, section, [(col, typ, val)])."""
    changes = []
    for raw in lines:
        line = raw.rstrip("\n")
        if "|" in line and line.split("|", 2)[0].count("/") == 1:
            line = line.split("|", 2)[2]        # strip lsn|xid| from psql output
        if line.startswith(("BEGIN", "COMMIT")):
            changes.append((line.split()[0], None, None, []))
            continue
        m = re.match(r"table ([^:]+): (\w+): (.*)", line)
        if not m:
            continue
        tbl, op, rest = m.groups()
        if op == "UPDATE" and "new-tuple:" in rest:
            old, new = rest.split("new-tuple:", 1)
            old = old.replace("old-key:", "").replace("old-tuple:", "")
            changes.append((op, tbl, "old", parse_fields(old)))
            changes.append((op, tbl, "new", parse_fields(new)))
        else:
            rest = rest.replace("old-key:", "").replace("old-tuple:", "")
            changes.append((op, tbl, "row", parse_fields(rest)))
    return changes

def main(expected_path, actual_path):
    exp = parse_stream(open(expected_path))
    act = parse_stream(open(actual_path))
    # test_decoding's slot starts earlier than our decode window; align on the
    # first change both streams share.
    if len(exp) > len(act):
        exp = exp[len(exp) - len(act):]

    errs, compared = [], 0
    if len(exp) != len(act):
        errs.append(f"change count: test_decoding {len(exp)}, pgs3 {len(act)}")
    for i, (e, a) in enumerate(zip(exp, act)):
        if e[0] != a[0] or e[1] != a[1] or e[2] != a[2]:
            errs.append(f"[{i}] shape: expected {e[:3]}, got {a[:3]}"); continue
        ecols = {c: (t, v) for c, t, v in e[3]}
        acols = {c: (t, v) for c, t, v in a[3]}
        # test_decoding calls tuple_to_stringinfo(skip_nulls=true) for old-tuple
        # and DELETE sections, so it omits NULL columns there. pgs3 emits them.
        # Drop them on our side rather than teach the decoder an output quirk.
        if a[2] == "old" or a[0] == "DELETE":
            acols = {c: tv for c, tv in acols.items() if tv[1] != "null"}
            ecols = {c: tv for c, tv in ecols.items() if tv[1] != "null"}
        if set(ecols) != set(acols):
            errs.append(f"[{i}] columns: only-pg={set(ecols)-set(acols)} only-pgs3={set(acols)-set(ecols)}")
            continue
        for col, (etyp, eval_) in ecols.items():
            atyp, aval = acols[col]
            if etyp != atyp:
                errs.append(f"[{i}] {col}: type {etyp} vs {atyp}"); continue
            if (eval_ == "null") != (aval == "null"):
                errs.append(f"[{i}] {col}: nullness {eval_!r} vs {aval!r}"); continue
            if etyp in ALLOWLIST and eval_ != aval:
                errs.append(f"[{i}] {col}[{etyp}]: {eval_!r} != {aval!r}")
            elif etyp in ALLOWLIST:
                compared += 1

    print(f"{len(act)} records, {compared} values compared byte-for-byte "
          f"({len(ALLOWLIST)} types in allowlist)")
    if errs:
        print("\nMISMATCH:")
        for e in errs[:25]:
            print("  " + (e[:200] + "..." if len(e) > 200 else e))
        return 1
    print("PASS: pgs3 agrees with Postgres logical decoding")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
