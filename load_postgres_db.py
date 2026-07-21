#!/usr/bin/env python3
#
# Load a CSV dump made by `__main__.py --no-load -d <dir>` into a *different*
# PostgreSQL server: the one the website reads.
#
# This is the second half of the update pipeline (see updater_dag):
#
#   __main__.py  builds the schemas in the build database, then dumps them
#                to /projects/BMRB/staging/dbdump/{bmrb,metabolomics,bmrbeverything}
#   this script  truncates and re-fills the corresponding tables on the
#                staging/serving host from those CSVs, then re-grants select
#                to the read-only web user
#
# Files are `[<schema>.]<table>.csv`; the column order comes from the first
# line, since it need not match the database.  Mixed-case names are quoted.
#
# NOTE this deliberately imports nothing from `loader`: the DAG runs it with
# the system /usr/bin/python3, not the dbloader venv, so it must work with the
# standard library and `psql` alone.  That is also why it shells out rather
# than using psycopg2.
#

import argparse
import glob
import os
import re
import subprocess
import sys
import time
from contextlib import contextmanager


@contextmanager
def timer(label, verbose=True):
    start = time.time()
    try:
        yield
    finally:
        end = time.time()
        if verbose:
            sys.stdout.write("%s: %0.3f\n" % (label, (end - start)))


#
#
#
class PgLoader(object):

    CONF = {
        "psql": "/usr/bin/psql",
        "rwuser": "bmrb",
        "rouser": "web",
        "host": "bmrb-staging.cam.uchc.edu",
        "ddlfile": "schema.sql",
        "mailfrom": "web@bmrb.wisc.edu",
        "databases": {
            "bmrb": {
                "dir": "/projects/BMRB/staging/dbdump/bmrb",
            },
            "metabolomics": {
                "dir": "/projects/BMRB/staging/dbdump/metabolomics",
            },
            "bmrbeverything": {
                "dir": "/projects/BMRB/staging/dbdump/bmrbeverything",
            }
        }
    }

    # wrapper for subprocess call
    #
    @staticmethod
    def psql(database, command, verbose=False):

        cmd = [PgLoader.CONF["psql"]]
        cmd.extend(["-U", PgLoader.CONF["rwuser"]])
        cmd.extend(["-d", database])
        if PgLoader.CONF.get("host"):
            cmd.extend(["-h", PgLoader.CONF["host"]])
        if PgLoader.CONF.get("port"):
            cmd.extend(["-p", str(PgLoader.CONF["port"])])

        if not verbose:
            cmd.append("-q")

        cmd.extend(command)

        if verbose:
            sys.stdout.write("%s\n" % (" ".join(cmd),))

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True)
        (out, err) = p.communicate()
        if p.returncode != 0:
            sys.stderr.write("ERR: psql returned %d\n" % (p.returncode,))
            sys.stderr.write("%s\n" % (" ".join(cmd),))

        if (p.returncode != 0) or verbose:
            sys.stderr.write("** STDERR **\n%s\n" % (err,))
            sys.stderr.write("** STDOUT **\n%s\n" % (out,))

        return p.returncode

    # load one table from csv
    # read column order from 1st line: it may not match the db
    # truncate table before load: it's 0-cost if it's empty
    #
    @staticmethod
    def fromcsv(filename, database, schema, table, verbose=False):

        infile = os.path.realpath(filename)
        if not os.path.exists(infile):
            raise IOError("Not found: %s" % (infile,))

        # column names
        #
        with open(infile) as f:
            cols = [c.strip().strip("'\"") for c in f.readline().split(",")]

        if len(cols) < 1 or cols == [""]:
            sys.stderr.write("no columns in %s\n" % (infile,))
            return -1

        colstr = ",".join(c if c.islower() else '"%s"' % (c,) for c in cols)

        if (schema is None) or (str(schema).strip() == ""):
            scam = ""
        else:
            scam = "%s." % (str(schema).strip(),)
        if table.islower():
            tbl = table
        else:
            tbl = '"%s"' % (table,)

        # `only`: never cascade to inherited tables
        trunc = "truncate table only %s%s" % (scam, tbl,)
        stmt = "\\copy %s%s (%s) from '%s' csv header" % (scam, tbl, colstr, infile,)

        if verbose:
            cmd = ["-c", "\\timing on", "-c", trunc, "-c", stmt]
        else:
            cmd = ["-c", trunc, "-c", stmt]

        return PgLoader.psql(database=database, command=cmd, verbose=verbose)

    # add read-only grants for RO user
    #
    @staticmethod
    def add_ro_grants(db="bmrb", verbose=False):

        sqls = ("grant usage on schema %s to %s",
                "grant select on all tables in schema %s to %s",
                "alter default privileges in schema %s grant select on tables to %s",
                "grant usage on all sequences in schema %s to %s",
                "alter default privileges in schema %s grant usage on sequences to %s",)

        rc = ""

        # want stdout from psql
        #
        psql = [PgLoader.CONF["psql"]]
        psql.extend(["-U", PgLoader.CONF["rwuser"]])
        if not verbose:
            psql.append("-q")
        psql.extend(["-d", db])
        if PgLoader.CONF.get("host"):
            psql.extend(["-h", PgLoader.CONF["host"]])
        if PgLoader.CONF.get("port"):
            psql.extend(["-p", str(PgLoader.CONF["port"])])

        cmd = psql[:]
        cmd.extend(["-A", "-t", "-F,"])  # CSV output, tuples only
        cmd.extend(["-c", r"\dn"])
        if verbose:
            sys.stdout.write("%s\n" % (" ".join(cmd),))

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True)
        (out, err) = p.communicate()
        if p.returncode != 0:
            rc += "%s: psql -c %s returned %d\n" % (db, r"\dn", p.returncode,)
            sys.stderr.write("%s%s\n" % (rc, err,))
            return rc

        for line in out.splitlines():
            if line.strip() == "":
                continue
            schema = line.split(",")[0]
            for sql in sqls:
                cmd = psql[:]
                cmd.extend(["-c", sql % (schema, PgLoader.CONF["rouser"],)])
                if verbose:
                    sys.stdout.write("%s\n" % (" ".join(cmd),))
                p = subprocess.Popen(cmd)
                p.wait()
                if p.returncode != 0:
                    rc += "%s: psql -c 'grant r/o privs' returned %d\n" % (db, p.returncode,)

        return rc

    # run schema.sql to drop and recreate tables
    #
    @staticmethod
    def runscript(scriptfile, db="bmrb", verbose=False):

        script = os.path.realpath(scriptfile)
        if not os.path.exists(script):
            raise IOError("Not found: %s" % (script,))

        return PgLoader.psql(database=db, command=["-f", script], verbose=verbose)

    # glob files and decide which to load where
    #
    @staticmethod
    def update_db(db="bmrb", create=False, schema="any", path=None, verbose=False):

        if path is not None:
            inputdir = os.path.realpath(path)
        else:
            if db not in PgLoader.CONF["databases"]:
                raise KeyError("No default input directory for %s -- pass -i" % (db,))
            inputdir = os.path.realpath(PgLoader.CONF["databases"][db]["dir"])
        if not os.path.isdir(inputdir):
            raise IOError("Not a directory: %s" % (inputdir,))

        rc = ""

        # -c: drop and re-create everything from the dump's own DDL first.
        # The updater does NOT do this -- it truncates and re-fills, so the
        # target keeps its schema (and anything else living in it).
        #
        if create:
            script = os.path.join(inputdir, PgLoader.CONF["ddlfile"])
            if not os.path.exists(script):
                raise IOError("Not found: %s" % (script,))
            x = PgLoader.runscript(scriptfile=script, db=db, verbose=verbose)
            if x != 0:
                sys.stderr.write("runscript %s returned %s\n" % (script, x,))

        # could be table.csv or schema.table.csv
        #
        pat = re.compile(r"(?:([^.]+)\.)?([^.]+)\.csv$")

        files = sorted(f for f in glob.glob(os.path.join(inputdir, "*.csv"))
                       if pat.search(os.path.split(f)[1]))

        if len(files) < 1:
            rc += "No input files for %s - %s\n" % (db, schema,)
            return rc

        for f in files:
            m = pat.search(os.path.split(f)[1])

            # skip schema?
            #
            if (schema is not None) and (schema != "any"):
                if schema != m.group(1):
                    continue

            x = PgLoader.fromcsv(filename=f, database=db, schema=m.group(1),
                                 table=m.group(2), verbose=verbose)
            if x != 0:
                rc += "\npsql load of %s returned %s\n" % (f, x,)

        return rc


########################################################################################
#
#
if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="Load a dbloader CSV dump into the serving database")
    ap.add_argument("-v", "--verbose", default=False, action="store_true",
                    help="print lots of messages to stdout", dest="verbose")
    ap.add_argument("-d", "--database", dest="db", default="all", required=True,
                    help="DB to load: bmrb, metabolomics, bmrbeverything, or all")
    ap.add_argument("-s", "--schema", dest="schema", default="any",
                    help="load only given schema (e.g. dict)")
    ap.add_argument("-c", "--create", default=False, action="store_true",
                    help="run schema.sql first to drop and re-create all objects",
                    dest="create")
    ap.add_argument("-i", "--input", dest="filedir",
                    help="directory with input files")
    ap.add_argument("-g", "--grants", default=False, action="store_true",
                    help="add read-only grants for web user", dest="grant")
    # the server was hard-coded; it still defaults to the same one
    ap.add_argument("-H", "--host", dest="host", default=PgLoader.CONF["host"],
                    help="database server (default: %(default)s)")
    ap.add_argument("-p", "--port", dest="port", default=None, help="database port")
    ap.add_argument("-U", "--user", dest="user", default=PgLoader.CONF["rwuser"],
                    help="database user (default: %(default)s)")
    ap.add_argument("--ro-user", dest="rouser", default=PgLoader.CONF["rouser"],
                    help="user the --grants are given to (default: %(default)s)")
    ap.add_argument("--psql", dest="psql", default=PgLoader.CONF["psql"],
                    help="psql binary (default: %(default)s)")

    args = ap.parse_args()

    PgLoader.CONF["host"] = args.host
    PgLoader.CONF["port"] = args.port
    PgLoader.CONF["rwuser"] = args.user
    PgLoader.CONF["rouser"] = args.rouser
    PgLoader.CONF["psql"] = args.psql

    wanted = args.db.lower()
    if wanted not in list(PgLoader.CONF["databases"].keys()) + ["all"]:
        ap.error("don't know how to load %s" % (args.db,))

    messages = ""
    for db in ("bmrb", "bmrbeverything", "metabolomics"):
        if wanted not in (db, "all"):
            continue
        with timer("Load " + db, verbose=True):
            messages += PgLoader.update_db(db=db, create=args.create, schema=args.schema,
                                           path=args.filedir, verbose=args.verbose) or ""
            if args.grant:
                messages += PgLoader.add_ro_grants(db=db, verbose=args.verbose) or ""

    if messages.strip() != "":
        sys.stderr.write("%s\n" % (messages,))
        sys.exit(1)

#
# eof
#
