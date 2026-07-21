#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Drive dbloader's entry load against a local PostgreSQL, with the one external
# system the entry path touches stubbed out.
#
# Runs unchanged under Python 2 (the golden stack) and Python 3 (the rewrite):
# it imports whatever `loader` package is on sys.path, so the same driver
# produces the golden and the run being compared to it.  Point it at the py2
# worktree in ../.golden-stack/dbloader for the former.
#
# The stub: entries.py cross-checks the macromolecule file list against the
# released-ID list from the ETS tracking database (ets.bmrb.wisc.edu), which is
# not reachable from here and is deferred by design (MODERNIZATION_PLAN.md §6).
# We replace `loader.released_ids_itr` with one that yields exactly the IDs
# present in entrydir, which is what the real ETS would report for a corpus of
# released entries -- so the file list is unchanged and no entry is skipped.
#
#     python tests/load_entries.py -c <config> -s macromolecules|metabolomics|all
#
# Exit status: 0 on success.

import argparse
import glob
import os
import re
import sys

try:
    from configparser import ConfigParser              # Python 3
except ImportError:
    from ConfigParser import SafeConfigParser as ConfigParser

_HERE = os.path.dirname(os.path.realpath(__file__))


def released_ids_stub(config):
    """Yield the BMRB IDs of every entry directory in [macromolecules] entrydir.

    Same shape as loader.ets.released_ids_itr: an iterator of ID strings.
    """
    entrydir = os.path.realpath(config.get("macromolecules", "entrydir"))
    pat = re.compile(r"bmr(\d+)$")
    ids = []
    for d in glob.glob(os.path.join(entrydir, "*")):
        m = pat.search(d)
        if m and os.path.isdir(d):
            ids.append(m.group(1))
    for i in sorted(ids, key=int):
        yield i


def main():
    ap = argparse.ArgumentParser(description="load entries with ETS stubbed out")
    ap.add_argument("-c", "--config", dest="conffile", required=True)
    ap.add_argument("-s", "--schema", dest="db", default="all",
                    choices=("macromolecules", "metabolomics", "all"))
    ap.add_argument("-r", "--repo", dest="repo", default=os.path.join(_HERE, ".."),
                    help="dbloader checkout to import `loader` from")
    ap.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False)
    ap.add_argument("--truncate", dest="drop", action="store_false", default=True,
                    help="reload into the existing tables instead of dropping the schema")
    args = ap.parse_args()

    sys.path.insert(0, os.path.realpath(args.repo))
    import loader
    loader.released_ids_itr = released_ids_stub
    sys.stderr.write("loader from %s\n" % (os.path.dirname(loader.__file__),))

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    # drop_tables=True is the only path the legacy code implements, so it is
    # what the golden was built with; --truncate exercises the one the rewrite
    # added, which has to end up with the same database.
    if args.db in ("macromolecules", "all"):
        with loader.timer(label="Load macromolecules"):
            loader.load_macromolecules(config=cp, drop_tables=args.drop, verbose=args.verbose)
    if args.db in ("metabolomics", "all"):
        with loader.timer(label="Load metabolomics"):
            loader.load_metabolomics(config=cp, drop_tables=args.drop, verbose=args.verbose)


if __name__ == "__main__":
    main()
