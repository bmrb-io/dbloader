#!/usr/bin/env python3
#
# The metabolomics "meta" schema -- hand-maintained CSVs that predate the
# dictionary-driven tables and have no other home.
#  - until we replace that old setup with something better.
#

import argparse
import glob
import os
import re
import sys
from configparser import ConfigParser

_UP = os.path.abspath(os.path.join(os.path.split(__file__)[0], ".."))
sys.path.append(_UP)
from loader import db
from loader import datafiles

DB = "meta"


# main
#
#
def load(config, verbose=False):
    create_schema(config, verbose)
    add_grants(config, verbose)
    load_files(config, verbose)


#######################################
# run DDL script
#
#
def create_schema(config, verbose=False):
    if verbose:
        sys.stdout.write("create_schema()\n")

    script = datafiles.path(config, DB, "ddlfile")

    return db.run_sql_file(db.dsn(config, DB), script, config=config, verbose=verbose)


# these files are named meta.tablename.csv
#
#
def load_files(config, verbose=False):
    if verbose:
        sys.stdout.write("load_files()\n")

    datadir = datafiles.path(config, DB, "csvdir")
    if not os.path.isdir(datadir):
        raise IOError("Not a directory: %s" % (datadir,))

    pat = re.compile(r"([^.]+)\.([^.]+)\.csv$")
    dsn = db.dsn(config, DB)
    for name in sorted(glob.glob(os.path.join(datadir, "meta.*.csv"))):
        m = pat.search(os.path.split(name)[1])
        if not m:
            sys.stderr.write("%s does not match pattern, skipping\n" % (name,))
            continue
        db.copy_from_csv(dsn, filename=name, schema=m.group(1), table=m.group(2),
                         config=config, verbose=verbose)


# add grants
#
#
def add_grants(config, verbose=False):
    if verbose:
        sys.stdout.write("add_grants()\n")

    if config.has_option(DB, "rouser"):
        db.add_ro_grants(db.dsn(config, DB), schema=config.get(DB, "schema"),
                         user=config.get(DB, "rouser"), config=config, verbose=verbose)


#
#
#
if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="load the metabolomics meta schema")
    ap.add_argument("-v", "--verbose", help="print lots of messages to stdout", dest="verbose",
                    action="store_true", default=False)
    ap.add_argument("-t", "--time", help="time the operatons", dest="time",
                    action="store_true", default=False)
    ap.add_argument("-c", "--config", help="config file", dest="conffile", required=True)
    args = ap.parse_args()

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    import loader
    with loader.timer(label="load additional metabolomics CSVs", silent=not args.time):
        load(config=cp, verbose=args.verbose)

#
# eof
