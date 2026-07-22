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


def _argnames(fn):
    """Parameter names of `fn`, under both Python 2 and 3."""

    import inspect
    try:
        return list(inspect.signature(fn).parameters)          # Python 3
    except AttributeError:
        return list(inspect.getargspec(fn).args)               # Python 2


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
    ap.add_argument("--no-swap", dest="swap", action="store_false", default=True,
                    help="py3 only: leave the data in <schema>_new")
    args = ap.parse_args()

    sys.path.insert(0, os.path.realpath(args.repo))
    import loader
    loader.released_ids_itr = released_ids_stub
    sys.stderr.write("loader from %s\n" % (os.path.dirname(loader.__file__),))

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    # The legacy loader takes drop_tables (and only implements drop_tables=True);
    # the rewrite dropped the argument along with the in-place paths, and always
    # builds a shadow schema.  Both are driven from here, so ask which one this
    # is rather than assuming.
    legacy = "drop_tables" in _argnames(loader.load_macromolecules)

    loaded = []
    if args.db in ("macromolecules", "all"):
        with loader.timer(label="Load macromolecules"):
            if legacy:
                loader.load_macromolecules(config=cp, drop_tables=True, verbose=args.verbose)
            else:
                loader.load_macromolecules(config=cp, verbose=args.verbose)
        loaded.append(cp.get("macromolecules", "schema"))
    if args.db in ("metabolomics", "all"):
        with loader.timer(label="Load metabolomics"):
            if legacy:
                loader.load_metabolomics(config=cp, drop_tables=True, verbose=args.verbose)
            else:
                loader.load_metabolomics(config=cp, verbose=args.verbose)
        loaded.append(cp.get("metabolomics", "schema"))

    # the legacy loader has no shadow to swap: it loaded in place
    if args.swap and loaded and not legacy:
        loader.shadow.swap(loader.db.dsn(cp, "macromolecules"),
                           [(s, loader.shadow.shadow_of(s)) for s in loaded],
                           config=cp, verbose=args.verbose)


if __name__ == "__main__":
    main()
