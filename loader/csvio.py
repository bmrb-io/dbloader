#!/usr/bin/env python3
#
# CSV in and out.
#
# Replaces csvload.py + csvdump.py + csvdump_better.py.  csvdump_better was
# never wired up -- __main__ imported its `Dumper` and then called csvdump's
# `dump_new` instead -- and its own command line referenced functions it did
# not define, so it could not run at all.  Its one idea worth keeping, a
# generated reload script, is `write_reload_script()` below.
#
# There are two output layouts:
#
#  "new"  everything in one database, one file per table, <schema>.<table>.csv.
#         This is what the `bmrbeverything` dump uses.
#  "old"  separate website databases for macromolecules+web and
#         metabolomics+meta, each with its own copy of `dict`; the entry tables
#         are unqualified, so their CSVs have no schema prefix and the DDL has
#         the schema stripped out of it.
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

# config sections
#
ALLSECTIONS = ("dictionary", "macromolecules", "metabolomics", "web", "meta", "chemcomps")
MACROSECTIONS = ("dictionary", "macromolecules", "web")
METASECTIONS = ("dictionary", "metabolomics", "meta")

_CSVNAME = re.compile(r"([^.]+)\.([^.]+)\.csv$")


#######################################
# in


def fromcsv(dsn, filename, schema, table, config=None, verbose=False):
    """Load one CSV file into one table."""

    return db.copy_from_csv(dsn, filename=filename, schema=schema, table=table,
                            config=config, verbose=verbose)


def all_fromcsv(path, config, section="bmrbeverything", verbose=False):
    """Load a whole directory of <schema>.<table>.csv files."""

    d = os.path.realpath(path)
    if not os.path.isdir(d):
        raise IOError("Not a directory: %s" % (d,))

    dsn = db.dsn(config, section)
    errs = 0
    for i in sorted(glob.glob(os.path.join(d, "*.csv"))):
        m = _CSVNAME.search(os.path.split(i)[1])
        if not m:
            sys.stderr.write("%s does not match pattern, skipping\n" % (i,))
            errs += 1
            continue

        if not fromcsv(dsn, filename=i, schema=m.group(1), table=m.group(2),
                       config=config, verbose=verbose):
            errs += 1

    return (errs == 0)


#######################################
# out


def tocsv(dsn, table, outfile, config=None, verbose=False):
    """Dump one table (or a parenthesized query) to CSV.  Empty tables are skipped."""

    if table.lower().startswith("select") or table.lower().startswith("(select"):
        sql = "select count(*) from %s as foo" % (table,)
    else:
        sql = "select count(*) from %s" % (table,)

    with db.connection(dsn) as conn:
        with conn.cursor() as curs:
            if verbose:
                sys.stdout.write(">%s\n" % (sql,))
            curs.execute(sql)
            numrows = curs.fetchone()[0]

    if numrows < 1:
        if verbose:
            sys.stdout.write("No rows to dump: %s\n" % (table,))
        return True

    return db.copy_to_csv(dsn, table, outfile, config=config, verbose=verbose)


def dump_new(config, path, verbose=False):
    """Dump every schema of the `bmrbeverything` database."""

    return dump(config, path, sections=ALLSECTIONS, verbose=verbose)


def dump_macromolecules(config, path, verbose=False):
    """"old-style" dump of the macromolecule website database."""

    return dump(config, path, sections=MACROSECTIONS, verbose=verbose)


def dump_metabolomics(config, path, verbose=False):
    """"old-style" dump of the metabolomics website database."""

    return dump(config, path, sections=METASECTIONS, verbose=verbose)


#
# this only works if all schemas are in the same database as "dictionary"
#
def dump(config, path, sections, verbose=False):

    assert sections in (ALLSECTIONS, MACROSECTIONS, METASECTIONS)

    if verbose:
        sys.stdout.write("dump( %s )\n" % (path,))

    outdir = os.path.realpath(path)
    os.umask(0o002)
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    elif not os.path.isdir(outdir):
        raise IOError("Not a directory: %s" % (outdir,))
    else:
        for i in glob.glob(os.path.join(outdir, "*")):
            os.unlink(i)

    # the "old" layouts leave one schema unqualified; "new" keeps them all
    old = None
    if sections is MACROSECTIONS:
        old = "macromolecules"
    elif sections is METASECTIONS:
        old = "metabolomics"

    scams = [config.get(key, "schema") for key in sections]
    dsn = db.dsn(config, "dictionary")

    if not dump_ddl(dsn, scams, os.path.join(outdir, "schema.sql"), old=old,
                    config=config, verbose=verbose):
        sys.stderr.write("Error dumping DDL\n")
        return False

    ok = True
    for scam in scams:
        for table in db.list_tables(dsn, scam, nonempty_only=True, verbose=verbose):
            if scam == old:
                outfile = os.path.join(outdir, "%s.csv" % (table,))
            else:
                outfile = os.path.join(outdir, "%s.%s.csv" % (scam, table,))
            if not tocsv(dsn, db.qualified(scam, table), outfile,
                         config=config, verbose=verbose):
                ok = False

    return ok


# pg_dump only really works between same postgres versions
#
def dump_ddl(dsn, schemata, outfile, old=None, config=None, verbose=False):

    # "clean", no-owner, no ACLs, plain SQL output, schema only
    cmd = [db.binary(config, "pg_dump"), "-c", "-O", "-x", "-F", "p", "-s"]
    for s in schemata:
        cmd.extend(["-n", s])
    # it'll fail if there is a password
    for opt, flag in (("user", "-U"), ("host", "-h"), ("port", "-p")):
        if opt in dsn:
            cmd.extend([flag, str(dsn[opt])])
    cmd.append(dsn["dbname"])

    (ok, out) = db.run_command(cmd, verbose)
    if not ok:
        return False
    if len(out) < 1:
        sys.stderr.write("Empty output from pg_dump\n")
        return False

    lines = out.splitlines()

    # "new": one database, many schemas -- dump it as it is.
    if old is None:
        with open(outfile, "w") as fout:
            for line in lines:
                fout.write("%s\n" % (line,))
        return True

    with open(outfile, "w") as fout:
        for line in _rewrite_ddl(lines, old):
            fout.write("%s\n" % (line,))
    return True


# "old": strip off postgres-10-isms and the entry schema's name, for the
# website databases -- separate ones for metabolomics+meta and
# macromolecules+web, each with its own dict.  (Hopefully this will go away
# soon. -ish...)
#
def _rewrite_ddl(lines, old):

    assert old in ("macromolecules", "metabolomics")

    drop_pat = re.compile(r"^drop\s+(?:\S+\s+)?(\S+)\s+(\S+);$", re.IGNORECASE)
    #                         DROP MATERIALIZED VIEW web.hupo_psi_id;
    create_pat = re.compile(r"^create\s+(\S+)\s+(\S+)", re.IGNORECASE)

    firstline = True
    for line in lines:

        # actual DDL commands start with "ALTER TABLE"
        if firstline and line.upper().startswith("ALTER"):
            yield "SET search_path = public, pg_catalog;\n"
            firstline = False

        # these don't exist in 9.x
        if line.startswith(("SET lock_timeout", "SET idle_in_transaction_session_timeout",
                            "SET row_security")):
            continue

        # ALTER: there is only one to alter
        if line.upper().startswith("ALTER"):
            yield line.replace(old + ".entry_saveframes", "entry_saveframes")
            continue

        if line.upper().startswith("DROP"):
            m = drop_pat.search(line)
            if not m:
                raise Exception("DROP no match: %s" % (line,))
            (what, name) = (m.group(1).upper(), m.group(2))

            # strip the "main" schema
            if what == "TABLE":
                yield "DROP TABLE IF EXISTS %s CASCADE;" \
                    % (name[len(old) + 1:] if name.startswith(old + ".") else name,)
            # omit this: no such schema
            elif what == "SCHEMA":
                if name != old:
                    yield "DROP SCHEMA IF EXISTS %s CASCADE;" % (name,)
            # else add "if exists" and "cascade"
            else:
                yield "DROP %s IF EXISTS %s CASCADE;" % (m.group(1), name,)
            continue

        if line.upper().startswith("CREATE"):
            m = create_pat.search(line)
            if not m:
                raise Exception("CREATE no match: %s" % (line,))
            (what, name) = (m.group(1).upper(), m.group(2))

            # strip the "main" schema
            if what == "TABLE":
                yield line.replace(old + ".", "") if name.startswith(old + ".") else line
                continue
            # omit this: no such schema
            if what == "SCHEMA":
                if not line.startswith("CREATE SCHEMA " + old):
                    yield line
                continue

        yield line


# csvdump_better's contribution: a script that reloads a dump made by dump().
# Nothing calls it yet -- it is here so the idea is not lost with the module.
#
def write_reload_script(config, outdir, sections=ALLSECTIONS, verbose=False):

    dsn = db.dsn(config, "dictionary")
    out = [ "--", "-- drop and reload script", "--", "", "BEGIN;", "" ]

    for scam in [config.get(key, "schema") for key in sections]:
        for table in db.list_tables(dsn, scam, nonempty_only=True, verbose=verbose):
            tbl = db.qualified(scam, table)
            out.append("truncate %s;" % (tbl,))
            out.append("\\copy %s from '%s.%s.csv' csv header;" % (tbl, scam, table,))

    out.extend(["", "COMMIT;", "", "--"])

    path = os.path.join(os.path.realpath(outdir), "_load_all.sql")
    with open(path, "w") as f:
        f.write("\n".join(out))
        f.write("\n")
    return path


#######################################################################################
#
#
if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="dump/load the NMR-STAR PostgreSQL database")
    ap.add_argument("-v", "--verbose", help="print lots of messages to stdout", dest="verbose",
                    action="store_true", default=False)
    ap.add_argument("-c", "--config", help="config file", dest="conffile", required=True)
    ap.add_argument("-d", "--dir", help="directory for output (or input) files",
                    dest="dir", required=True)
    ap.add_argument("--load", help="load the directory instead of dumping to it",
                    dest="load", action="store_true", default=False)
    ap.add_argument("--macromol", help="dump macromolecule database", dest="macromol",
                    action="store_true", default=False)
    ap.add_argument("--metabol", help="dump metabolomics database", dest="metabol",
                    action="store_true", default=False)
    args = ap.parse_args()

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    if args.load:
        ok = all_fromcsv(path=args.dir, config=cp, verbose=args.verbose)
    elif args.macromol:
        ok = dump_macromolecules(config=cp, path=args.dir, verbose=args.verbose)
    elif args.metabol:
        ok = dump_metabolomics(config=cp, path=args.dir, verbose=args.verbose)
    else:
        ok = dump_new(config=cp, path=args.dir, verbose=args.verbose)

    if not ok:
        sys.exit(1)

#
# eof
