#!/usr/bin/env python3
"""Compare two files of hex-encoded pgoutput messages, one per line.

Reports the first differing message and the byte offset within it, with a
window of context, rather than dumping two 400-character lines side by side.
"""
import sys

TYPES = {
    "B": "Begin", "C": "Commit", "R": "Relation", "Y": "Type",
    "I": "Insert", "U": "Update", "D": "Delete", "T": "Truncate",
    "O": "Origin", "M": "Message",
}


def main(expected_path, actual_path):
    exp = [l.strip() for l in open(expected_path) if l.strip()]
    act = [l.strip() for l in open(actual_path) if l.strip()]

    for i, (e, a) in enumerate(zip(exp, act)):
        if e == a:
            continue
        eb, ab = bytes.fromhex(e), bytes.fromhex(a)
        kind = TYPES.get(chr(eb[0]), f"0x{eb[0]:02x}")
        at = next((j for j in range(min(len(eb), len(ab))) if eb[j] != ab[j]),
                  min(len(eb), len(ab)))
        lo, hi = max(0, at - 12), at + 12
        print(f"message {i} ({kind}): differs at byte {at} "
              f"(expected {len(eb)} bytes, got {len(ab)})")
        print(f"  postgres: ...{eb[lo:hi].hex()}...")
        print(f"  pgs3:     ...{ab[lo:hi].hex()}...")
        print(f"            {' ' * (3 + 2 * (at - lo))}^^")
        return 1

    if len(exp) != len(act):
        print(f"message count: postgres {len(exp)}, pgs3 {len(act)}")
        longer, name = (exp, "postgres") if len(exp) > len(act) else (act, "pgs3")
        extra = longer[min(len(exp), len(act))]
        print(f"  first extra message from {name}: "
              f"{TYPES.get(chr(bytes.fromhex(extra)[0]), '?')} {extra[:80]}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
