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

    ap.add_argument("--drop-tables", dest="drop_tables", action="store_true", default=False,
                    help="drop and re-create entry tables instead of truncating them")

    args = ap.parse_args(argv)

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

    failed = []

    with loader.timer(label="total", silent=args.time):

        if args.load_dict:
            dictdir = os.path.realpath(args.dictdir)
            if not os.path.isdir(dictdir):
                sys.stderr.write("Not a directory: %s\n" % (dictdir,))
                return 1
            with loader.timer(label="load dictionary", silent=args.time):
                loader.load_dict(config=cp, path=dictdir, verbose=args.verbose)

        if args.load_chemcomps:
            with loader.timer(label="load chem. comps", silent=args.time):
                loader.load_chem_comps(config=cp, verbose=args.verbose)

        if args.load_metabolomics:
            with loader.timer(label="load metabolomics", silent=args.time):
                failed += loader.load_metabolomics(config=cp, drop_tables=args.drop_tables,
                                                   verbose=args.verbose)
                loader.load_meta_schema(config=cp, verbose=args.verbose)

        if args.load_macromolecules:
            with loader.timer(label="load macromolecules", silent=args.time):
                failed += loader.load_macromolecules(config=cp, drop_tables=args.drop_tables,
                                                     verbose=args.verbose)
                loader.fix_macromolecules(config=cp, verbose=args.verbose)

            # CS statistics are computed from the macromolecules just loaded
            if args.load_web:
                with loader.timer(label="load web extras", silent=args.time):
                    loader.load_web_schema(config=cp, verbose=args.verbose)

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
