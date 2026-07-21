#!/usr/bin/env python3
#
# Parse NMR-STAR entry files into the `macromolecules` and `metabolomics`
# schemas.
#
# The tables are not defined here: `starobj` generates them from the `dict`
# schema (loaded by dictionary.py), then SAX-parses each entry and inserts its
# rows.  This module finds the files, decides which ones to load, prepares the
# schema, and keeps score.
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

DATABASES = ("macromolecules", "metabolomics")


# wrappers
#
#
def load_metabolomics(config, drop_tables, verbose=False):
    return load_db("metabolomics", config, drop_tables, verbose)


#
#
def load_macromolecules(config, drop_tables, verbose=False):
    return load_db("macromolecules", config, drop_tables, verbose)


#
#
def load_db(dbname, config, drop_tables, verbose=False):
    """Load one archive and grant the read-only user access to it.

    Returns the list of entry files that failed to load (empty on success).
    """

    failed = load_entries(dbname, config, drop_tables, verbose)
    if config.has_option(dbname, "rouser"):
        db.add_ro_grants(db.dsn(config, dbname), schema=config.get(dbname, "schema"),
                         user=config.get(dbname, "rouser"), config=config, verbose=verbose)
    return failed


#
#
#
def load_entries(dbname, config, drop_tables=False, verbose=False):

    if verbose:
        sys.stdout.write("load_entries( %s, %s )\n"
                         % (dbname, drop_tables and "drop_tables" or "truncate_tables",))

    assert dbname in DATABASES

    # the dictionary section is needed too: starobj reads the `dict` schema
    # through it to find out what tables to create
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

    # starobj wants its own config section for the connection it inserts through
    #
    if not config.has_section("entry"):
        config.add_section("entry")
    config.set("entry", "engine", "psycopg2")
    config.set("entry", "database", config.get(dbname, "database"))
    config.set("entry", "schema", dbname)
    for opt in ("user", "host", "password"):
        if config.has_option(dbname, opt):
            config.set("entry", opt, config.get(dbname, opt))

    return _load_entries(config, files, drop_tables, verbose=verbose)


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


# Empty the entry schema, one way or the other.
#
# Dropping and re-creating is the safe choice: the table set comes from the
# dictionary, so a dictionary update adds and removes tables and only a
# re-create picks that up.  Truncating is for reloading against an unchanged
# dictionary; it used to be `raise Exception( "FIXME!!!! Not implemented" )`,
# which meant the *documented default* aborted every time.
#
# Either way every table ends up empty, entry_saveframes included -- starobj
# takes the next Sf_ID from max(sfid) there, so both paths number from 1.
#
def _prepare_schema(config, dbname, se, sd, dbwrapper, use_types, drop_tables, verbose=False):

    schema = dbwrapper.schema(se.CONNECTION)
    conn = dbwrapper._connections[se.CONNECTION]["conn"]

    # DDL gets its own transaction; the parse loop below runs with autocommit off.
    # (Under pgdb this assignment did nothing at all -- see STAROBJ_PY3_REVIEW.md
    # finding 7 -- so the legacy DDL rode along in the first entry's transaction.)
    conn.autocommit = True
    try:
        se.execute("set client_min_messages=WARNING")

        if not drop_tables:
            tables = db.list_tables(db.dsn(config, dbname), schema)
            if len(tables) > 0:
                if verbose:
                    sys.stdout.write("truncating %d tables in %s\n" % (len(tables), schema,))
                se.execute("truncate %s"
                           % (",".join(db.qualified(schema, t) for t in tables),))
                return
            sys.stderr.write("%s: nothing to truncate, creating the tables\n" % (schema,))

        se.execute("drop schema if exists %s cascade" % (schema,))
        se.execute("create schema %s" % (schema,))
        se.create_tables(dictionary=sd, db=dbwrapper, use_types=use_types, verbose=verbose)
    finally:
        conn.autocommit = False


#
#
#
def _load_entries(config, filelist, drop_tables=False, verbose=False):

    dbname = config.get("entry", "schema")
    use_types = (dbname != "macromolecules")

    dbwrapper = loader.starobj.DbWrapper(config, verbose=False)
    dbwrapper.connect()

    sd = loader.starobj.StarDictionary(dbwrapper, verbose=False)
    se = loader.starobj.NMRSTAREntry(dbwrapper, verbose=False)

    try:
        return _parse_all(config, dbname, filelist, use_types, drop_tables,
                          dbwrapper, sd, se, verbose)
    finally:
        dbwrapper.close()


def _parse_all(config, dbname, filelist, use_types, drop_tables,
               dbwrapper, sd, se, verbose=False):

    _prepare_schema(config, dbname, se, sd, dbwrapper, use_types, drop_tables, verbose)

    conn = dbwrapper._connections[se.CONNECTION]["conn"]
    failed = []
    errs = []
    for f in filelist:
        del errs[:]
        if verbose:
            sys.stdout.write("> %s\n" % (f,))
        try:
            if loader.starobj.StarParser.parse_file(db=dbwrapper, dictionary=sd, filename=f,
                                                    errlist=errs, types=use_types,
                                                    create_tables=False, verbose=False):
                conn.commit()
            else:
                sys.stderr.write("** Errors parsing %s\n" % (f,))
                conn.rollback()
                failed.append(f)
            if len(errs) > 0:
                sys.stderr.write("** Parser output for %s\n" % (f,))
                for e in errs:
                    sys.stderr.write("%s\n" % (e,))

        # One bad entry must not abort the archive load -- but it must be
        # counted.  The legacy bare `except:` also caught KeyboardInterrupt and
        # left the run looking successful.
        except Exception:
            conn.rollback()
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
    ap.add_argument("-d", "--drop-tables", dest="droptables", action="store_true", default=False,
                    help="drop and re-create tables (default: truncate existing)")
    args = ap.parse_args()

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    failed = []
    for dbname in DATABASES:
        if args.db in (dbname, "all"):
            with loader.timer(label="Load " + dbname, silent=args.time):
                failed.extend(load_db(dbname, config=cp, drop_tables=args.droptables,
                                      verbose=args.verbose))

    if len(failed) > 0:
        sys.exit(1)

#
# eof
