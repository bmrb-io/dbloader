#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Rebuild the BMRB database: load every schema, then dump it back out to CSV.
#
#     python -m dbloader -c loader.properties --dictdir <dir> -d <outdir>
#
# Stages run in dependency order and are individually switchable with --no-*.
# The dictionary must be loaded before the entries -- it is what defines their
# tables -- and the web schema's statistics are computed from the loaded
# macromolecules, so it comes last.
#

import argparse
import os
import sys
from configparser import ConfigParser

import loader

# load stages, in order: flag name -> (description, what it needs)
STAGES = ("dict", "chemcomps", "metabolomics", "macromolecules", "web")

def parse_args(argv=None):

    ap = argparse.ArgumentParser(description="Reload the BMRB database")
    ap.add_argument("-v", "--verbose", default=False, action="store_true",
                    help="print lots of messages to stdout", dest="verbose")
    ap.add_argument("-t", "--time", help="time the operatons", dest="time",
                    action="store_false", default=True)
    ap.add_argument("-c", "--config", help="config file", dest="conffile", required=True)

    # Where this release lands, stated at the point of use rather than by
    # editing the deployed properties file.  See loader/db.py:repoint().
    loader.db.add_target_args(ap)

    ap.add_argument("-d", "--outdir", help="directory for output CSV files", dest="outdir")
    ap.add_argument("--dictdir", help="directory with dictionary files", dest="dictdir")

    ap.add_argument("--no-dump", help="don't dump the database", dest="dump",
                    action="store_false", default=True)
    ap.add_argument("--no-load", help="don't load the database", dest="load",
                    action="store_false", default=True)

    dumps = ap.add_mutually_exclusive_group()
    dumps.add_argument("--dump-macromolecule-db", help="dump the macromolecule database",
                       dest="dump_macro", action="store_true", default=False)
    dumps.add_argument("--dump-metabolomics-db", help="dump the metabolomics database",
                       dest="dump_metab", action="store_true", default=False)

    for stage in STAGES:
        ap.add_argument("--no-" + stage, help="don't load the %s schema" % (stage,),
                        dest="load_" + stage, action="store_false", default=True)

    ap.add_argument("--no-swap", dest="swap", action="store_false", default=True,
                    help="leave every stage in its <schema>_new and do not swap")

    # Accepted and ignored.  Every load now builds a fresh shadow schema and
    # swaps it in, so there is no longer a choice between dropping and
    # truncating the live one -- but a deployed condor job that still passes it
    # should not fail on an unrecognized argument.  updater_dag stopped sending
    # it on the single-server branch (jobs 110, 131 and 231); this can go once
    # that is what is deployed.
    ap.add_argument("--drop-tables", dest="drop_tables", action="store_true", default=False,
                    help=argparse.SUPPRESS)

    args = ap.parse_args(argv)

    if args.drop_tables:
        sys.stderr.write("--drop-tables is obsolete and ignored: every load now builds"
                         " <schema>_new and swaps it in.\n")

    # --no-load turns off every load stage
    if not args.load:
        for stage in STAGES:
            setattr(args, "load_" + stage, False)

    if args.load_dict and args.dictdir is None:
        ap.error("--dictdir is required to load the dictionary (or pass --no-dict)")
    if args.dump and args.outdir is None:
        ap.error("-d/--outdir is required to dump (or pass --no-dump)")

    return args


def main(argv=None):

    args = parse_args(argv)

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))
    loader.db.repoint(cp, host=args.host, port=args.port,
                      database=args.database, verbose=args.verbose)

    failed = []
    built = []          # live schema names, in load order, to swap in at the end

    with loader.timer(label="total", silent=args.time):

        if args.load_dict:
            dictdir = os.path.realpath(args.dictdir)
            if not os.path.isdir(dictdir):
                sys.stderr.write("Not a directory: %s\n" % (dictdir,))
                return 1
            with loader.timer(label="load dictionary", silent=args.time):
                loader.load_dict(config=cp, path=dictdir, verbose=args.verbose)
            # dictionary.sql builds validict alongside dict, as views over it,
            # so the two have to move together
            built += loader.shadow.declared_schemas(
                os.path.join(dictdir, loader.dictionary_ddlfile(cp)))

        if args.load_chemcomps:
            with loader.timer(label="load chem. comps", silent=args.time):
                loader.load_chem_comps(config=cp, verbose=args.verbose)
            built.append(cp.get("chemcomps", "schema"))

        if args.load_metabolomics:
            with loader.timer(label="load metabolomics", silent=args.time):
                failed += loader.load_metabolomics(config=cp, verbose=args.verbose)
                loader.load_meta_schema(config=cp, verbose=args.verbose)
            built.append(cp.get("metabolomics", "schema"))
            built.append(cp.get("meta", "schema"))

        if args.load_macromolecules:
            with loader.timer(label="load macromolecules", silent=args.time):
                failed += loader.load_macromolecules(config=cp, verbose=args.verbose)
                loader.fix_macromolecules(config=cp, verbose=args.verbose)
            built.append(cp.get("macromolecules", "schema"))

            # CS statistics are computed from the macromolecules just loaded
            if args.load_web:
                with loader.timer(label="load web extras", silent=args.time):
                    loader.load_web_schema(config=cp, verbose=args.verbose)
                built.append(cp.get("web", "schema"))

        # One transaction renaming every schema built above into place.  Until
        # this runs the load is invisible: readers are still on the previous
        # contents, and a failure anywhere above leaves them there for good.
        if args.swap and built:
            with loader.timer(label="swap schemas in", silent=args.time):
                loader.shadow.swap(loader.db.dsn(cp, "dictionary"),
                                   [(x, loader.shadow.shadow_of(x)) for x in built],
                                   config=cp, verbose=args.verbose)

        if args.dump:
            if args.dump_macro:
                label, dump = "dump macromolecule database", loader.dump_macromolecules
            elif args.dump_metab:
                label, dump = "dump metabolomics database", loader.dump_metabolomics
            else:
                label, dump = "dump bmrbeverything database", loader.dump_new
            with loader.timer(label=label, silent=args.time):
                dump(config=cp, path=args.outdir, verbose=args.verbose)

    if len(failed) > 0:
        sys.stderr.write("** %d entries failed to load:\n" % (len(failed),))
        for f in failed:
            sys.stderr.write("%s\n" % (f,))
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())

# EOF
#
