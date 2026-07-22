#!/usr/bin/env python3
#
# Load the NMR-STAR dictionary into the `dict` schema.
#
# Inputs are produced by ../nmr-star-dictionary-scripts: dictionary.sql (the
# DDL) plus one dict.<table>.csv per table.  This schema is not just reference
# data -- the entry tables are generated from it, so it has to be loaded
# before any entries are.
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
from loader import shadow

SCHEMA = "dict"


#
# main
#
def load(config, path, verbose=False):

    if not config.has_section("dictionary"):
        raise Exception("No [dictionary] section in config file\n")
    if not config.has_option("dictionary", "ddlfile"):
        raise Exception("No ddlfile in [dictionary] section in config file\n")

    wd = os.path.realpath(path)
    if not os.path.isdir(wd):
        raise Exception("Not a directory: %s\n" % (wd,))

    dsn = db.dsn(config, "dictionary")
    ddl = os.path.realpath(os.path.join(wd, config.get("dictionary", "ddlfile")))

    # dictionary.sql names its own schemas -- it opens with `drop schema if
    # exists dict cascade` and builds `validict` alongside -- so building the
    # shadow means rewriting the script, not just passing a different schema
    # name.  Both schemas move together: validict is views over dict, and a
    # view binds to the table OID it was created against (see shadow.py).
    schema = shadow.target(config, "dictionary")
    mapping = dict((s, shadow.shadow_of(s)) for s in shadow.declared_schemas(ddl))
    ddl = shadow.rewritten_copy(ddl, mapping, os.path.join(wd, "shadow"), verbose=verbose)

    if not db.run_sql_file(dsn, ddl, config=config, verbose=verbose):
        return False

    if not fromcsv(dsn, path=wd, schema=schema, config=config, verbose=verbose):
        return False

    if config.has_option("dictionary", "rouser"):
        db.add_ro_grants(dsn, schema=schema,
                         user=config.get("dictionary", "rouser"),
                         config=config, verbose=verbose)
    return True


# files are named dict.<table>.csv, first row is column headers (in whatever
# order); the schema is the one dictionary.sql creates, not the file prefix.
#
def fromcsv(dsn, path, schema=SCHEMA, config=None, verbose=False):

    d = os.path.realpath(path)
    if not os.path.isdir(d):
        raise IOError("Not a directory: %s" % (d,))

    pat = re.compile(r"([^.]+)\.([^.]+)\.csv$")
    errs = 0
    for i in sorted(glob.glob(os.path.join(d, "dict.*.csv"))):
        m = pat.search(os.path.split(i)[1])
        if not m:
            sys.stderr.write("%s does not match pattern, skipping\n" % (i,))
            errs += 1
            continue

        if not db.copy_from_csv(dsn, filename=i, schema=schema, table=m.group(2),
                                config=config, verbose=verbose):
            errs += 1

    return (errs == 0)


#
#
#
if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="load NMR-STAR dictionary into PostgreSQL database")
    ap.add_argument("-v", "--verbose", help="print lots of messages to stdout", dest="verbose",
                    action="store_true", default=False)
    ap.add_argument("-c", "--config", help="config file", dest="conffile", required=True)
    ap.add_argument("-d", "--dir", help="directory with input files", dest="indir", required=True)
    ap.add_argument("--no-swap", dest="swap", action="store_false", default=True,
                    help="leave the dictionary in dict_new/validict_new instead of"
                         " swapping it in (for a caller that swaps several together)")
    args = ap.parse_args()

    wd = os.path.realpath(args.indir)
    if not os.path.isdir(wd):
        sys.stderr.write("Not a directory: %s\n" % (wd,))
        sys.exit(1)

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    if not load(config=cp, path=wd, verbose=args.verbose):
        sys.exit(1)

    if args.swap:
        ddlfile = os.path.join(wd, cp.get("dictionary", "ddlfile"))
        schemas = shadow.declared_schemas(ddlfile)
        shadow.swap(db.dsn(cp, "dictionary"),
                    [(x, shadow.shadow_of(x)) for x in schemas],
                    config=cp, verbose=args.verbose)
