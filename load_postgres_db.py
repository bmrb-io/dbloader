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
# It does that as a *swap*, and only as a swap: build <schema>_new from the
# dump's own schema.sql, load into that, then rename it into place.  The rename
# is one transaction that touches no rows, so readers are blocked for about a
# millisecond rather than for the length of the reload, and a load that fails
# part way leaves the live database untouched.  And because schema.sql comes
# from the build database, whose schema dbloader generates from the dictionary,
# the swap rebuilds the serving schema from the dictionary -- a dictionary
# change no longer has to be applied to the serving database by hand.
#
# There used to be a second path that truncated the live tables and refilled
# them in place.  It existed for the old-style serving databases, whose entry
# tables were unqualified and so could not be swapped without renaming
# `public`.  Those are retired (see RETIRED), so it is gone: one way to load,
# and it is the safe one.
#
# Files are `<schema>.<table>.csv`; the column order comes from the first line,
# since it need not match the database.  Mixed-case names are quoted.
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

    # The separate website serving databases are retired; everything is served
    # from `bmrbeverything`.  `metabolomics` went in October 2024 (commit
    # 0d68fd1, "No more metabolomics db"), `bmrb` since.
    #
    # updater_dag still has a job for each -- 160 runs `-d metabolomics` and
    # 260 runs `-d bmrb` -- so these stay no-ops rather than errors, and *loud*
    # ones: the metabolomics retirement was done by commenting the branch out,
    # which left a job that looked like it was loading a database and was not.
    #
    # Their dumps are still produced. 151 writes dbdump/metabolomics, which
    # nothing reads at all; 251 writes dbdump/bmrb, which job 602 rsyncs to
    # ftp/pub/bmrb/relational_tables -- so *that* dump is still wanted even
    # though nothing loads it any more.  Jobs 160, 260 and 151 can go when
    # updater_dag is next touched; 251 must stay.
    RETIRED = ("bmrb", "metabolomics")

    CONF = {
        "psql": "/usr/bin/psql",
        "rwuser": "bmrb",
        "rouser": "web",
        "host": "bmrb-staging.cam.uchc.edu",
        "ddlfile": "schema.sql",
        "mailfrom": "web@bmrb.wisc.edu",
        "databases": {
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
    #
    # The target is always a shadow table that was just created, so there is
    # nothing to truncate and no transaction to hold: if this fails, the
    # shadow schema is thrown away and the live one was never touched.
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
        tbl = table if table.islower() else '"%s"' % (table,)

        stmt = "\\copy %s.%s (%s) from '%s' csv header" \
            % (str(schema).strip(), tbl, colstr, infile,)

        cmd = ["-c", "\\timing on"] if verbose else []
        cmd.append("-c")
        cmd.append(stmt)

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
    def check_loadable(script, files, pat, schema_filter):
        """Raise unless this dump can be swapped in. Returns its schemas."""

        if not os.path.exists(script):
            raise IOError("the dump has no %s to build the schema from"
                          % (PgLoader.CONF["ddlfile"],))

        unqualified = sorted(set(os.path.split(f)[1] for f in files
                                 if pat.search(os.path.split(f)[1]).group(1) is None))
        if unqualified:
            raise ValueError(
                "%d of the CSVs have no schema prefix (e.g. %s). That is an old-style"
                " dump, whose entry tables go in the search_path; swapping one in would"
                " mean renaming `public`. The serving databases that used that layout"
                " are retired -- see PgLoader.RETIRED."
                % (len(unqualified), unqualified[0],))

        in_ddl = PgLoader.schemas_in_ddl(script)
        if len(in_ddl) < 1:
            raise ValueError("no CREATE SCHEMA in %s -- nothing to swap" % (script,))

        if schema_filter is not None and schema_filter != "any":
            if schema_filter not in in_ddl:
                raise ValueError("%s is not one of the schemas in %s"
                                 % (schema_filter, script,))

        return in_ddl

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
    def swap_shadow(db, schemas, discard=None, verbose=False):
        """Swap the shadow schemas in, in one transaction, then drop the old.

        `discard`: shadow schemas that were built but not filled -- a load of
        one schema still runs the whole DDL, since it is one script -- and so
        must be thrown away rather than swapped in empty.

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
        for s in (discard or []):
            cmd.extend(["-c", "drop schema if exists %s%s cascade" % (s, PgLoader.SHADOW,)])
        return PgLoader.psql(database=db, command=cmd, verbose=verbose)

    # glob files and decide which to load where
    #
    @staticmethod
    def update_db(db="bmrbeverything", schema="any", path=None, verbose=False,
                  rouser=None):
        """Build the dump's schemas alongside the live ones, fill them, swap."""

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

        # everything the DDL builds, and the subset this load fills -- only the
        # ones it fills may be swapped in, or the rest go live empty
        shadow_schemas = PgLoader.check_loadable(script, files, pat, schema)
        if schema is not None and schema != "any":
            swap_schemas = [schema]
        else:
            swap_schemas = shadow_schemas[:]

        sys.stdout.write("%s: building %s and swapping it in\n"
                         % (db, ", ".join(s + PgLoader.SHADOW for s in swap_schemas),))

        x = PgLoader.create_shadow(db=db, script=script, schemas=shadow_schemas,
                                   verbose=verbose)
        if x != 0:
            return "building the shadow schemas from %s returned %s\n" % (script, x,)

        for f in files:
            m = pat.search(os.path.split(f)[1])

            # skip schema?
            #
            if (schema is not None) and (schema != "any"):
                if schema != m.group(1):
                    continue

            x = PgLoader.fromcsv(filename=f, database=db,
                                 schema=m.group(1) + PgLoader.SHADOW,
                                 table=m.group(2), verbose=verbose)
            if x != 0:
                rc += "\npsql load of %s returned %s\n" % (f, x,)

        if rc != "":
            # nothing has been swapped in, so the live database is untouched
            return rc + ("\nNOT swapping %s in: the shadow load failed. The live"
                         " database is unchanged.\n" % (db,))

        # grant before the swap, not after: privileges follow the objects
        # through a rename, so the read-only user never sees a gap
        if rouser is not None:
            rc += PgLoader.add_ro_grants(db=db, verbose=verbose,
                                         schemas=[s + PgLoader.SHADOW
                                                  for s in swap_schemas]) or ""

        x = PgLoader.swap_shadow(db=db, schemas=swap_schemas,
                                 discard=[s for s in shadow_schemas
                                          if s not in swap_schemas],
                                 verbose=verbose)
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
                    help="DB to load: bmrbeverything, or all. bmrb and metabolomics"
                         " are accepted and do nothing -- those serving databases"
                         " are retired")
    ap.add_argument("-s", "--schema", dest="schema", default="any",
                    help="load only given schema (e.g. dict), and swap only that one")
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

    # `bmrbeverything` has a default input directory; the retired names are
    # still accepted so the updater_dag jobs that pass them stay green; any
    # other name is fine as long as -i says where to read it from
    known = tuple(PgLoader.CONF["databases"].keys())
    wanted = args.db.lower()
    if wanted == "all":
        targets = known + PgLoader.RETIRED
    elif wanted in known + PgLoader.RETIRED:
        targets = (wanted,)
    elif args.filedir:
        targets = (args.db,)
    else:
        ap.error("don't know where to load %s from -- pass -i" % (args.db,))

    messages = ""
    for db in targets:
        if db in PgLoader.RETIRED:
            sys.stdout.write("Skipping %s: that serving database is retired,"
                             " its data is in bmrbeverything\n" % (db,))
            continue
        with timer("Load " + db, verbose=True):
            # update_db does the grants: the swap has to grant *before* it
            # swaps, so the read-only user never sees a window without them
            messages += PgLoader.update_db(db=db, schema=args.schema,
                                           path=args.filedir, verbose=args.verbose,
                                           rouser=args.rouser if args.grant else None) or ""

    if messages.strip() != "":
        sys.stderr.write("%s\n" % (messages,))
        sys.exit(1)

#
# eof
#
