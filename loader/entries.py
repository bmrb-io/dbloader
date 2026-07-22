#!/usr/bin/env python3
#
# Parse NMR-STAR entry files into the `macromolecules` and `metabolomics`
# schemas.
#
# The tables are not defined here: they are generated from the `dict` schema
# (loaded by dictionary.py) -- see loader/starschema.py -- and the entries are
# parsed and inserted by loader/entryload.py.  This module finds the files,
# decides which ones to load, prepares the schema, and keeps score.
#
# The two archives differ in one respect: macromolecule tables are all `text`
# (use_types = False) while metabolomics tables get real column types from the
# dictionary (use_types = True).
#

import argparse
import glob
import os
import pprint
import re
import sys
import traceback
from configparser import ConfigParser

_UP = os.path.abspath(os.path.join(os.path.split(__file__)[0], ".."))
sys.path.append(_UP)
import loader
from loader import db
from loader.entryload import EntryLoader
from loader import shadow
from loader import indexes

DATABASES = ("macromolecules", "metabolomics")


# wrappers
#
#
def load_metabolomics(config, verbose=False):
    return load_db("metabolomics", config, verbose)


#
#
def load_macromolecules(config, verbose=False):
    return load_db("macromolecules", config, verbose)


#
#
def load_db(dbname, config, verbose=False):
    """Load one archive into its shadow schema and grant the read-only user
    access to it.  The caller swaps it in -- see loader/shadow.py.

    Returns the list of entry files that failed to load (empty on success).
    """

    failed = load_entries(dbname, config, verbose)

    # Index and analyze the shadow schema before anyone sees it: cs_stats.sql
    # runs minutes later and is nine correlated scans of Atom_chem_shift, and
    # BMRB-API would otherwise build the same indexes against the live schema
    # after the swap.  See loader/indexes.py.
    schema = shadow.target(config, dbname)
    (made, analyzed) = indexes.prepare(db.dsn(config, dbname), schema, verbose)
    if verbose:
        sys.stdout.write("%s: %d indexes, %d tables analyzed\n"
                         % (schema, made, analyzed,))

    if config.has_option(dbname, "rouser"):
        db.add_ro_grants(db.dsn(config, dbname), schema=shadow.target(config, dbname),
                         user=config.get(dbname, "rouser"), config=config, verbose=verbose)
    return failed


#
#
#
def load_entries(dbname, config, verbose=False):

    if verbose:
        sys.stdout.write("load_entries( %s )\n" % (dbname,))

    assert dbname in DATABASES

    # the dictionary section is needed too: the `dict` schema is what says
    # which tables to create
    for (section, what) in ((dbname, "database"), ("dictionary", "database"),
                            (dbname, "entrydir")):
        if not config.has_section(section):
            raise Exception("No [%s] section in config file" % (section,))
        if not config.has_option(section, what):
            raise Exception("No %s in [%s] section in config file" % (what, section,))

    files = _gen_file_list(dbname, directory=config.get(dbname, "entrydir"), verbose=verbose)
    if len(files) < 1:
        raise Exception("Nothing to load: no entry files under %s"
                        % (config.get(dbname, "entrydir"),))

    if dbname == "macromolecules":
        files = _released_only(files, config)

    if verbose:
        sys.stdout.write("*********\nFiles to load:\n")
        pprint.pprint(files)

    return _load_entries(config, dbname, files, verbose=verbose)


# cross-check the files on the website against ETS: everything released should
# be on disk, and nothing on disk should be unreleased.
#
def _released_only(files, config):

    released = set(loader.released_ids_itr(config))

    pat = re.compile(r"bmr(\d+)_3\.str$")
    toload = []
    for f in files:
        m = pat.search(f)
        if not m:
            raise Exception("this should never happen: %s in file list" % (f,))
        bmrbid = m.group(1)
        if bmrbid in released:
            released.remove(bmrbid)
            toload.append(f)
        else:
            sys.stderr.write("*************** ERROR ******************\n")
            sys.stderr.write("BMRB ID of %s is not in released IDs!\n" % (f,))
            sys.stderr.write("Delete from public website!\n")
            sys.stderr.write("****************************************\n")

    if len(released) > 0:
        sys.stderr.write("Following BMRB IDs are released but not in file list:\n")
        for i in sorted(released, key=int):
            sys.stderr.write("%s\n" % (i,))

    return toload


# list input files for the metabolomics or macromolecule database.
# this reads files actually on the website, without checking ETS status
#
def _gen_file_list(dbname, directory, verbose=False):

    assert dbname in DATABASES
    entrydir = os.path.realpath(directory)
    if not os.path.isdir(entrydir):
        sys.stderr.write("Not a directory: %s\n" % (entrydir,))
        return []

    if dbname == "macromolecules":
        dirpat = re.compile(r"bmr(\d+)$")
        filename = "bmr%s_3.str"
    else:
        dirpat = re.compile(r"(bms[et]\d+)$")
        filename = "%s.str"

    filelist = []
    for i in glob.glob(os.path.join(entrydir, "*")):
        m = dirpat.search(i)
        if not m:
            sys.stderr.write("%s does not match pattern\n" % (i,))
            continue
        if not os.path.isdir(i):
            sys.stderr.write("%s: not a directory\n" % (i,))
            continue
        infile = os.path.join(i, filename % (m.group(1),))
        if not os.path.exists(infile):
            sys.stderr.write("Not found: %s\n" % (infile,))
            continue

        filelist.append(infile)

    # sorted: keeps the load -- and with it Sf_ID assignment -- reproducible
    return sorted(filelist)


# Create the shadow schema the entries are loaded into.
#
# It is always built from scratch: the table set comes from the dictionary, so
# a dictionary update adds and removes tables and only a re-create picks that
# up.  The two in-place paths this used to have -- drop-and-refill and
# truncate-and-refill of the *live* schema -- are gone; they were what made a
# reload visible to readers.  Any leftover shadow from a run that died before
# the swap is dropped here.
#
# Every table ends up empty, entry_saveframes included: the next Sf_ID comes
# from max(sfid) there, so numbering starts at 1.
#
def _prepare_schema(conn, schema, verbose=False):

    with conn.cursor() as curs:
        curs.execute("set client_min_messages=WARNING")
        if verbose:
            sys.stdout.write("building shadow schema %s\n" % (schema,))
        curs.execute("drop schema if exists %s cascade" % (schema,))
        curs.execute("create schema %s" % (schema,))


#
#
#
def _load_entries(config, dbname, filelist, verbose=False):

    # macromolecules load as all-text, metabolomics with types from the dictionary
    use_types = (dbname != "macromolecules")
    schema = shadow.target(config, dbname)

    conn = db.connect(db.dsn(config, dbname))
    try:
        # DDL in its own transaction, then one transaction per entry.  This
        # comes first: psycopg2 will not change the session's autocommit once
        # a transaction is open, and looking the dictionary up opens one.
        conn.autocommit = True

        dict_schema = shadow.dict_schema(conn, config)
        if verbose:
            sys.stdout.write("generating %s from %s\n" % (schema, dict_schema,))

        # One commit per entry means one fsync per entry -- ~14,800 of them for
        # the macromolecule archive -- to protect a load that is thrown away
        # and re-run if it does not finish.  The schema is dropped and rebuilt
        # from scratch here, so there is nothing in it worth waiting on the
        # disk for: a crash costs the run, not the database.  (Session-local;
        # it does not outlive this connection.)
        with conn.cursor() as curs:
            curs.execute("set synchronous_commit = off")

        _prepare_schema(conn, schema, verbose)
        loader_ = EntryLoader(conn, schema, dict_schema, verbose=verbose)
        n = loader_.create_tables(dict_schema, use_types=use_types)
        if verbose:
            sys.stdout.write("created %d tables in %s\n" % (n, schema,))
        conn.autocommit = False

        return _parse_all(conn, loader_, filelist, verbose)
    finally:
        conn.close()


def _parse_all(conn, entryloader, filelist, verbose=False):

    failed = []
    for f in filelist:
        if verbose:
            sys.stdout.write("> %s\n" % (f,))
        try:
            rows = entryloader.load_file(f)
            conn.commit()
            if verbose:
                sys.stdout.write("  %d rows\n" % (rows,))

        # One bad entry must not abort the archive load -- but it must be
        # counted.  The legacy bare `except:` also caught KeyboardInterrupt and
        # left the run looking successful.
        except Exception:
            conn.rollback()
            entryloader.resync()
            sys.stderr.write("Exception on %s\n" % (f,))
            traceback.print_exc()
            failed.append(f)

    if len(failed) > 0:
        sys.stderr.write("** %d of %d entries failed to load\n" % (len(failed), len(filelist),))

    return failed


#
#
#
if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="load NMR-STAR files into PostgreSQL database")
    ap.add_argument("--time", help="print out timings", dest="time", action="store_false",
                    default=True)
    ap.add_argument("-v", "--verbose", help="print lots of messages to stdout", dest="verbose",
                    action="store_true", default=False)
    ap.add_argument("-c", "--config", help="config file", dest="conffile", required=True)
    ap.add_argument("-s", "--schema", help="database to load: macromolecules or metabolomics",
                    dest="db", default="all", choices=DATABASES + ("all",))
    ap.add_argument("--no-swap", dest="swap", action="store_false", default=True,
                    help="leave the data in <schema>_new instead of swapping it in"
                         " (for a caller that swaps several schemas together)")
    args = ap.parse_args()

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    failed = []
    loaded = []
    for dbname in DATABASES:
        if args.db in (dbname, "all"):
            with loader.timer(label="Load " + dbname, silent=args.time):
                failed.extend(load_db(dbname, config=cp, verbose=args.verbose))
            loaded.append((dbname, cp.get(dbname, "schema")))

    # every section names the same database, so any of them will do for the DSN
    if args.swap and loaded:
        shadow.swap(db.dsn(cp, loaded[0][0]),
                    [(s, shadow.shadow_of(s)) for (_, s) in loaded],
                    config=cp, verbose=args.verbose)

    if len(failed) > 0:
        sys.exit(1)

#
# eof
