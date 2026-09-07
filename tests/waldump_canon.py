#!/usr/bin/env python3
"""Canonicalise pg_waldump output to `lsn|rmgr_id|xid|xl_tot_len` for diffing."""
import sys, re
RMGR = {n: i for i, n in enumerate([
    "XLOG", "Transaction", "Storage", "CLOG", "Database", "Tablespace",
    "MultiXact", "RelMap", "Standby", "Heap2", "Heap", "Btree", "Hash", "Gin",
    "Gist", "Sequence", "SPGist", "BRIN", "CommitTs", "ReplicationOrigin",
    "Generic", "LogicalMessage"])}
PAT = re.compile(
    r"rmgr: (\S+)\s+len \(rec/tot\):\s+\d+/\s*(\d+), tx:\s+(\d+), lsn: ([0-9A-F]+)/([0-9A-F]+)")
for line in sys.stdin:
    m = PAT.match(line)
    if m:
        rm, tot, tx, hi, lo = m.groups()
        print(f"{int(hi,16):X}/{int(lo,16):X}|{RMGR[rm]}|{tx}|{tot}")
