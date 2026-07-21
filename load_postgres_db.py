#!/usr/bin/env python3
#
# Load a CSV dump made by `__main__.py --no-load -d <dir>` into a *different*
# PostgreSQL server: the one the website reads.
#
# This is the second half of the update pipeline (see updater_dag):
#
#   __main__.py  builds the schemas in the build database, then dumps them
#                to /projects/BMRB/staging/dbdump/{bmrb,metabolomics,bmrbeverything}
#   this script  loads those CSVs into the staging/serving host and grants
#                select to the read-only web user
#
# By default it does that as a *swap*: build <schema>_new from the dump's own
# schema.sql, load into that, then rename it into place.  The rename is one
# transaction that touches no rows, so readers are blocked for about a
# millisecond rather than for the length of the reload, and a load that fails
# part way leaves the live database untouched.  And because schema.sql comes
# from the build database, whose schema dbloader generates from the dictionary,
# the swap rebuilds the serving schema from the dictionary -- a dictionary
# change no longer has to be applied to the serving database by hand.
#
# A dump that cannot be swapped falls back to truncating and refilling the live
# tables in place, with a line in the log saying why.  That is the old-style
# layouts, whose entry tables are unqualified (`-d bmrb`), and any load of a
# single schema (`-s`), where swapping would take the other schemas of the dump
# live *empty*.  `--shadow` demands a swap and fails instead of falling back;
# `--no-shadow` forces the in-place load.
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

    # The separate `metabolomics` serving database was retired in October 2024
    # (commit 0d68fd1 on the `python3` branch, "No more metabolomics db"); that
    # data is served from `bmrbeverything` now.  updater_dag job 160 still runs
    # `-d metabolomics`, so this has to stay a no-op rather than an error --
    # but a *loud* one.  It was previously done by commenting the branch out,
    # which left a job that looked like it was loading a database and was not.
    #
    # Job 151 still dumps to /projects/BMRB/staging/dbdump/metabolomics and
    # nothing reads it; 160 and 151 can both go when updater_dag is next
    # touched.
    RETIRED = ("metabolomics",)

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
    def psql(database, command, verbose=False, stop_on_error=True):

        cmd = [PgLoader.CONF["psql"]]
        # Without this psql carries on after a failed statement and still exits
        # 0 -- which is why the `-c begin ... -c commit` that used to be in
        # fromcsv() was useless: the \copy failed, psql went on to the commit,
        # and the TRUNCATE was committed anyway.
        if stop_on_error:
            cmd.extend(["-v", "ON_ERROR_STOP=1"])
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
    def fromcsv(filename, database, schema, table, verbose=False, truncate=True):

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

        stmt = "\\copy %s%s (%s) from '%s' csv header" % (scam, tbl, colstr, infile,)

        cmd = ["-c", "\\timing on"] if verbose else []
        if truncate:
            # In one transaction, so a failed \copy leaves the old rows in
            # place instead of an empty table.  Multiple -c options share a
            # session, and ON_ERROR_STOP (see psql()) makes psql exit before
            # the commit, which rolls the truncate back.
            #
            # `only`: never cascade to inherited tables.
            cmd.extend(["-c", "begin",
                        "-c", "truncate table only %s%s" % (scam, tbl,),
                        "-c", stmt,
                        "-c", "commit"])
        else:
            # loading a freshly created shadow table: nothing to truncate
            cmd.extend(["-c", stmt])

        return PgLoader.psql(database=database, command=cmd, verbose=verbose)

    # add read-only grants for RO user
    #
    @staticmethod
    def add_ro_grants(db="bmrb", verbose=False, schemas=None):

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

        # `schemas`: grant on exactly these (the shadow schemas, before they
        # are swapped in).  Otherwise ask the server what is there.
        if schemas is None:
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
            schemas = [line.split(",")[0] for line in out.splitlines() if line.strip() != ""]

        for schema in schemas:
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

    ####################################################################
    # shadow schemas
    #
    # Instead of truncating the live tables and refilling them -- which leaves
    # readers looking at a half-loaded database, and an empty table if a copy
    # fails -- build the whole thing alongside as <schema>_new and swap it in
    # with renames, which are atomic and take a lock for the length of one
    # transaction that does no work.
    #
    # This also answers "rebuild the serving schema from the dictionary on
    # reload": the DDL comes from the dump's own schema.sql, which pg_dump took
    # from the build database, whose schema dbloader generated from the
    # dictionary (see loader/starschema.py).  Creating the shadow schema from
    # it *is* the dictionary-driven rebuild -- so a dictionary change no longer
    # has to be applied to the serving database by hand.

    SHADOW = "_new"
    RETIRING = "_old"

    @staticmethod
    def can_shadow(script, files, pat, schema_filter):
        """Can this load be done as a shadow swap? Returns (yes, why not).

        The swap replaces every schema in the dump's DDL at once, so it is only
        correct when the load fills every one of them.
        """

        if not os.path.exists(script):
            return (False, "the dump has no %s to build the schema from"
                    % (PgLoader.CONF["ddlfile"],))

        unqualified = sorted(set(os.path.split(f)[1] for f in files
                                 if pat.search(os.path.split(f)[1]).group(1) is None))
        if unqualified:
            return (False, "%d of the CSVs have no schema prefix (e.g. %s) -- the"
                           " old-style layouts put entry tables in the search_path"
                           % (len(unqualified), unqualified[0],))

        if schema_filter is not None and schema_filter != "any":
            # loading one schema would swap the rest in empty
            return (False, "only the %s schema is being loaded, and a swap replaces"
                           " every schema in the dump at once" % (schema_filter,))

        if len(PgLoader.schemas_in_ddl(script)) < 1:
            return (False, "no CREATE SCHEMA in %s -- nothing to swap" % (script,))

        return (True, "")

    @staticmethod
    def schemas_in_ddl(script):
        """Schema names the dump's DDL creates, in the order it creates them."""

        pat = re.compile(r"^CREATE SCHEMA (?:IF NOT EXISTS )?([A-Za-z_][A-Za-z_0-9]*)\s*;",
                         re.IGNORECASE)
        out = []
        with open(script) as f:
            for line in f:
                m = pat.match(line.strip())
                if m and m.group(1) not in out:
                    out.append(m.group(1))
        return out

    @staticmethod
    def shadow_ddl(script, schemas, outfile):
        """Rewrite schema.sql to build <schema>_new instead of <schema>.

        pg_dump was run with `-c`, so the script starts by dropping what it is
        about to create; none of that applies to a schema we just created
        empty, and with ON_ERROR_STOP the first one aborts the run.  That
        clean section is everything between the `SET` header and the first
        `CREATE` -- DROP statements, but also `ALTER TABLE ... DROP CONSTRAINT`
        and `ALTER TABLE ... ALTER COLUMN ... DROP DEFAULT` -- so it is skipped
        by position rather than by trying to enumerate the forms.  The ALTERs
        *after* the first CREATE define constraints and defaults and are kept.

        Every reference is schema-qualified (checked: the views are too, and
        none of them crosses a schema), so renaming is a matter of rewriting
        `<schema>.` -- which has to happen inside view bodies as well, or a
        view in dict_new would read from the live dict.
        """

        subs = [(re.compile(r"\b%s\." % (re.escape(s),)), "%s%s." % (s, PgLoader.SHADOW,))
                for s in schemas]
        create = re.compile(r"^(CREATE SCHEMA (?:IF NOT EXISTS )?)(%s)\b"
                            % ("|".join(re.escape(s) for s in schemas),), re.IGNORECASE)
        clean = re.compile(r"^\s*(DROP|ALTER)\s", re.IGNORECASE)

        kept = 0
        seen_create = False
        with open(script) as f, open(outfile, "w") as out:
            for line in f:
                if not seen_create:
                    if line.upper().startswith("CREATE "):
                        seen_create = True
                    elif clean.match(line):
                        continue
                line = create.sub(lambda m: m.group(1) + m.group(2) + PgLoader.SHADOW, line)
                for (pat, rep) in subs:
                    line = pat.sub(rep, line)
                out.write(line)
                kept += 1
        return kept

    @staticmethod
    def create_shadow(db, script, schemas, verbose=False):
        """Drop any leftover shadow schemas and build them from the dump's DDL."""

        cmd = []
        for s in schemas:
            cmd.extend(["-c", "drop schema if exists %s%s cascade" % (s, PgLoader.SHADOW,)])
        rc = PgLoader.psql(database=db, command=cmd, verbose=verbose)
        if rc != 0:
            return rc

        ddl = script + PgLoader.SHADOW
        PgLoader.shadow_ddl(script, schemas, ddl)
        if verbose:
            sys.stdout.write("shadow DDL: %s\n" % (ddl,))
        return PgLoader.psql(database=db, command=["-f", ddl], verbose=verbose)

    @staticmethod
    def swap_shadow(db, schemas, verbose=False):
        """Swap every shadow schema in, in one transaction, then drop the old.

        The renames are what the readers see: one transaction that touches no
        rows, so the exclusive locks are held for microseconds rather than for
        the length of a reload.  Dropping the old schemas afterwards is done
        outside that transaction so it cannot hold them.
        """

        stmts = ["begin"]
        for s in schemas:
            # first run: there may be nothing to rename out of the way
            stmts.append(
                "do $swap$ begin"
                " if exists (select 1 from pg_namespace where nspname = '%s') then"
                "   execute 'alter schema %s rename to %s%s';"
                " end if;"
                " execute 'alter schema %s%s rename to %s';"
                " end $swap$" % (s, s, s, PgLoader.RETIRING, s, PgLoader.SHADOW, s,))
        stmts.append("commit")

        cmd = []
        for s in stmts:
            cmd.extend(["-c", s])
        rc = PgLoader.psql(database=db, command=cmd, verbose=verbose)
        if rc != 0:
            return rc

        cmd = []
        for s in schemas:
            cmd.extend(["-c", "drop schema if exists %s%s cascade" % (s, PgLoader.RETIRING,)])
        return PgLoader.psql(database=db, command=cmd, verbose=verbose)

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
    def update_db(db="bmrb", create=False, schema="any", path=None, verbose=False,
                  shadow=None, rouser=None):
        """Load a dump into `db`.

        `shadow`: None decides per dump -- swap where that is possible, which
        is the good path and wants no thinking about; True demands it and fails
        if the dump cannot be swapped; False forces truncate-in-place.
        """

        if path is not None:
            inputdir = os.path.realpath(path)
        else:
            if db not in PgLoader.CONF["databases"]:
                raise KeyError("No default input directory for %s -- pass -i" % (db,))
            inputdir = os.path.realpath(PgLoader.CONF["databases"][db]["dir"])
        if not os.path.isdir(inputdir):
            raise IOError("Not a directory: %s" % (inputdir,))

        rc = ""
        script = os.path.join(inputdir, PgLoader.CONF["ddlfile"])

        # could be table.csv or schema.table.csv
        #
        pat = re.compile(r"(?:([^.]+)\.)?([^.]+)\.csv$")

        files = sorted(f for f in glob.glob(os.path.join(inputdir, "*.csv"))
                       if pat.search(os.path.split(f)[1]))

        if len(files) < 1:
            rc += "No input files for %s - %s\n" % (db, schema,)
            return rc

        (possible, why) = PgLoader.can_shadow(script, files, pat, schema)
        if shadow is None:
            shadow = possible
            if not possible:
                sys.stdout.write("%s: loading in place, not swapping -- %s\n" % (db, why,))
        elif shadow and not possible:
            raise ValueError("--shadow was asked for but %s" % (why,))

        shadow_schemas = []
        if shadow:
            shadow_schemas = PgLoader.schemas_in_ddl(script)
            sys.stdout.write("%s: building %s and swapping it in\n"
                             % (db, ", ".join(s + PgLoader.SHADOW for s in shadow_schemas),))

            x = PgLoader.create_shadow(db=db, script=script, schemas=shadow_schemas,
                                       verbose=verbose)
            if x != 0:
                return "building the shadow schemas from %s returned %s\n" % (script, x,)

        # -c: drop and re-create everything from the dump's own DDL first.
        # Without --shadow the updater does NOT do this -- it truncates and
        # re-fills, so the target keeps its schema (and anything else in it).
        #
        elif create:
            if not os.path.exists(script):
                raise IOError("Not found: %s" % (script,))
            x = PgLoader.runscript(scriptfile=script, db=db, verbose=verbose)
            if x != 0:
                sys.stderr.write("runscript %s returned %s\n" % (script, x,))

        for f in files:
            m = pat.search(os.path.split(f)[1])

            # skip schema?
            #
            if (schema is not None) and (schema != "any"):
                if schema != m.group(1):
                    continue

            target = m.group(1)
            if shadow:
                target += PgLoader.SHADOW

            x = PgLoader.fromcsv(filename=f, database=db, schema=target,
                                 table=m.group(2), verbose=verbose,
                                 truncate=not shadow)
            if x != 0:
                rc += "\npsql load of %s returned %s\n" % (f, x,)

        if not shadow:
            if rouser is not None:
                rc += PgLoader.add_ro_grants(db=db, verbose=verbose) or ""
            return rc

        if rc != "":
            # nothing has been swapped in, so the live database is untouched
            return rc + ("\nNOT swapping %s in: the shadow load failed. The live"
                         " database is unchanged.\n" % (db,))

        # grant before the swap, not after: privileges follow the objects
        # through a rename, so the read-only user never sees a gap
        if rouser is not None:
            rc += PgLoader.add_ro_grants(db=db, verbose=verbose,
                                         schemas=[s + PgLoader.SHADOW
                                                  for s in shadow_schemas]) or ""

        x = PgLoader.swap_shadow(db=db, schemas=shadow_schemas, verbose=verbose)
        if x != 0:
            rc += "swapping the shadow schemas in returned %s\n" % (x,)

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
    # the swap is the default wherever it is possible; these are the overrides
    ap.add_argument("--shadow", default=None, action="store_true", dest="shadow",
                    help="require the shadow swap, and fail if this dump cannot be"
                         " swapped (rather than quietly loading in place)")
    ap.add_argument("--no-shadow", action="store_false", dest="shadow",
                    help="load in place even when the dump could be swapped")
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

    # the three known databases have a default input directory; anything else
    # is fine too as long as -i says where to read it from
    known = ("bmrb", "bmrbeverything", "metabolomics")
    wanted = args.db.lower()
    if wanted == "all":
        targets = known
    elif wanted in known:
        targets = (wanted,)
    elif args.filedir:
        targets = (args.db,)
    else:
        ap.error("don't know where to load %s from -- pass -i" % (args.db,))

    messages = ""
    for db in targets:
        if db in PgLoader.RETIRED:
            sys.stdout.write("Skipping %s: that serving database was retired,"
                             " its data is in bmrbeverything\n" % (db,))
            continue
        with timer("Load " + db, verbose=True):
            # update_db does the grants: a swap has to grant *before* it swaps,
            # so that the read-only user never sees a window without them
            messages += PgLoader.update_db(db=db, create=args.create, schema=args.schema,
                                           path=args.filedir, verbose=args.verbose,
                                           shadow=args.shadow,
                                           rouser=args.rouser if args.grant else None) or ""

    if messages.strip() != "":
        sys.stderr.write("%s\n" % (messages,))
        sys.exit(1)

#
# eof
#
