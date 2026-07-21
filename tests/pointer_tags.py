#!/usr/bin/env python3
"""Every tag that carries a $-prefixed value, vs the dictionary's sfpointerflg.

Answers "could the $-stripping in loader/entryload.py:_value() be driven off
the dictionary instead of lexically?" -- see PORT_NOTES.md, "Risks this takes
on".  Applies exactly the rule _value() uses (value starts with `$` and
contains no whitespace) to every tag of every entry in both archives, then
joins the result against dict.adit_item_tbl.

Needs the `dict` schema loaded.  Takes ~10 minutes over the full archives.

    venv/bin/python tests/pointer_tags.py

Last run: 121 tags carry $-values (536,427 values); 120 are sfpointerflg='Y';
the one exception is Entity_assembly.Entity_assembly_name, 203 values, all in
metabolomics bmse entries and all duplicating the row's own Entity_label.
"""

import glob
import os
import re
import sys
from collections import Counter

import psycopg2
import pynmrstar

ENCODING = "iso8859-15"
SPACE = re.compile(r"\s")

_HERE = os.path.dirname(os.path.realpath(__file__))
_GIT = os.path.realpath(os.path.join(_HERE, "..", "..", ".."))

ARCHIVES = {
    "macromolecules": os.path.join(_GIT, "query-bmrb/bmrb_entries/*/*.str"),
    "metabolomics": os.path.join(_GIT, "query-bmrb/bmrb_metabolomics/*/*.str"),
}


def is_pointer(val):
    if val is None:
        return False
    val = str(val).strip()
    return val.startswith("$") and not SPACE.search(val)


def category(prefix):
    return prefix.lstrip("_").split(".")[0]


hits = Counter()          # (table, tag) -> how many values
seen = set()              # (table, tag) -> every tag we saw at all
failed = 0

for (archive, pattern) in ARCHIVES.items():
    files = sorted(glob.glob(pattern))
    sys.stderr.write("%s: %d files\n" % (archive, len(files)))
    for (n, f) in enumerate(files):
        if n % 2000 == 0:
            sys.stderr.write("  %d...\n" % (n,))
        try:
            with open(f, encoding=ENCODING) as fh:
                entry = pynmrstar.Entry.from_string(fh.read())
        except Exception:
            failed += 1
            continue

        for frame in entry.frame_list:
            table = category(frame.tag_prefix)
            for (tag, val) in frame.tags:
                seen.add((table, tag))
                if is_pointer(val):
                    hits[(table, tag)] += 1
            for loop in frame.loops:
                ltable = category(loop.category)
                tags = list(loop.tags)
                for t in tags:
                    seen.add((ltable, t))
                for row in loop.data:
                    for (t, v) in zip(tags, row):
                        if is_pointer(v):
                            hits[(ltable, t)] += 1

sys.stderr.write("unparseable files: %d\n" % (failed,))

conn = psycopg2.connect(dbname="bmrb", host="/tmp", user="bmrb")
with conn.cursor() as curs:
    curs.execute("select tagcategory, tagfield, sfpointerflg from dict.adit_item_tbl")
    flag = {(t, f): p for (t, f, p) in curs.fetchall()}

unflagged = []
flagged = []
notindict = []
for (key, count) in hits.items():
    p = flag.get(key)
    if p is None:
        notindict.append((key, count))
    elif p == "Y":
        flagged.append((key, count))
    else:
        unflagged.append((key, count, p))

print("tags carrying $-values: %d   (%d flagged sfpointerflg=Y, %d not, %d not in dictionary)"
      % (len(hits), len(flagged), len(unflagged), len(notindict)))
print("total $-values: %d" % (sum(hits.values()),))
print()
print("=== carry $-values but sfpointerflg is NOT 'Y' ===")
for ((t, f), c, p) in sorted(unflagged, key=lambda x: -x[1]):
    print("  %-40s %-34s flag=%-6r %8d values" % (t, f, p, c))
print()
print("=== carry $-values and are not in adit_item_tbl at all ===")
for ((t, f), c) in sorted(notindict, key=lambda x: -x[1]):
    print("  %-40s %-34s %8d values" % (t, f, c))
print()
print("=== flagged sfpointerflg=Y but never carry a $-value ===")
never = [k for (k, v) in flag.items() if v == "Y" and k not in hits]
print("  %d of %d flagged tags" % (len(never), sum(1 for v in flag.values() if v == "Y")))
for k in sorted(never)[:15]:
    print("  %-40s %s" % k)
