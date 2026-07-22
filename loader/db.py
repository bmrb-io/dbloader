#!/usr/bin/env python3
#
# The one place that talks to PostgreSQL.
#
# Two ways in, for two different reasons:
#
#  * psycopg2, for ordinary queries and inserts.  This replaces `pgdb`
#    (PyGreSQL), which is Python 2 only.
#
#  * `psql`, for running DDL scripts and for COPY.  Server-side COPY needs
#    superuser (or pg_read_server_files) and reads files on the *server*;
#    psql's \copy is client-side and needs neither.  That is why these shell
#    out instead of using psycopg2's copy_expert: the loader is expected to run
#    as an unprivileged user against a remote server.
#

import os
import subprocess
import sys
from contextlib import contextmanager

import psycopg2

# psql/pg_dump: overridable via the config's [tools] section (see `binary()`),
# then $PSQL/$PG_DUMP, then whatever is on PATH.  The legacy code hard-coded
# /bin/psql and /bin/pg_dump, which is wrong anywhere PostgreSQL is installed
# under /usr/pgsql-NN/bin -- including the production hosts, where the
# hard-coded paths were commented out and back in by hand.
PSQL = os.environ.get("PSQL", "psql")
PGDUMP = os.environ.get("PG_DUMP", "pg_dump")


def binary(config, which):
    """Path to the `psql` or `pg_dump` executable to use."""

    assert which in ("psql", "pg_dump")
    if config is not None and config.has_option("tools", which):
        return config.get("tools", which)
    return PSQL if which == "psql" else PGDUMP


def dsn(config, section):
    """Connection parameters from a config section, as psycopg2 keyword arguments.

    Unlike the pgdb version this does not pack host and port into one
    "host:port" string that every caller then had to split apart again (and
    two of them got wrong): psycopg2 takes them separately, and so do psql and
    pg_dump.
    """

    if not config.has_section(section):
        raise Exception("No [%s] section in config file" % (section,))
    if not config.has_option(section, "database"):
        raise Exception("No database in [%s] section in config file" % (section,))

    rc = {"dbname": config.get(section, "database")}
    for opt, key in (("user", "user"), ("password", "password"), ("host", "host"),
                     ("port", "port")):
        if config.has_option(section, opt):
            val = config.get(section, opt)
            if val is not None and str(val).strip() != "":
                rc[key] = val
    return rc


# Sections whose connection points at the database being loaded, and so follow
# a --host/--port/--database override.  [ets] is absent on purpose: it is the
# tracking database on its own server, is only ever read, and repointing it at
# the target would either fail or -- worse -- silently read the wrong
# released-ID list.
TARGET_SECTIONS = ("dictionary", "macromolecules", "metabolomics",
                   "chemcomps", "web", "meta")


def repoint(config, host=None, port=None, database=None, verbose=False):
    """Override the server and database for the sections naming the target.

    Which database a release lands in is the thing an operator wants to state
    at the point of use -- a condor job, a test run against a scratch host --
    rather than by editing a deployed properties file.

    The chem-comp source (`ccdb`) is pinned first.  It shares the [chemcomps]
    section with the target and falls back to the unprefixed options when its
    own `src*` ones are unset, so moving the target would drag the source along
    with it, to a server that has no ccdb.  Writing the current value into
    `srchost` before the override keeps it where it was.  `srcdatabase` is
    always set explicitly, so it needs no such protection.
    """

    if host is None and port is None and database is None:
        return

    if config.has_section("chemcomps"):
        for (src, opt, val) in (("srchost", "host", host), ("srcport", "port", port)):
            if val is not None and not config.has_option("chemcomps", src) \
                    and config.has_option("chemcomps", opt):
                config.set("chemcomps", src, config.get("chemcomps", opt))
                if verbose:
                    sys.stdout.write("chemcomps: pinning source %s to %s\n"
                                     % (src, config.get("chemcomps", src),))

    for section in TARGET_SECTIONS:
        if not config.has_section(section):
            continue
        for (opt, val) in (("host", host), ("port", port), ("database", database)):
            if val is not None:
                config.set(section, opt, str(val))

    if verbose:
        sys.stdout.write("target: %s%s%s\n"
                         % (database or "(config)", " on " + host if host else "",
                            ":" + str(port) if port else "",))


def add_target_args(ap):
    """Add --host/--port/--database to an argument parser."""

    ap.add_argument("--host", dest="host", default=None,
                    help="PostgreSQL host, overriding the config")
    ap.add_argument("--port", dest="port", default=None,
                    help="PostgreSQL port, overriding the config")
    ap.add_argument("--database", dest="database", default=None,
                    help="database to load, overriding the config -- applies to every"
                         " section naming the target, never to [ets] or the chem-comp"
                         " source")
    return ap


def connect(dsn, autocommit=False):
    """Open a psycopg2 connection from a `dsn()` dict."""

    conn = psycopg2.connect(**dsn)
    conn.autocommit = bool(autocommit)
    return conn


@contextmanager
def connection(dsn, autocommit=False):
    """`connect()` as a context manager that actually closes the connection.

    psycopg2's own `with conn:` only ends the transaction -- it leaves the
    connection open -- which matters here because the dump path opens one per
    table, a couple of hundred per run.
    """

    conn = connect(dsn, autocommit)
    try:
        yield conn
    finally:
        conn.close()


def quote(name):
    """Quote an identifier if PostgreSQL would otherwise fold its case.

    NMR-STAR table and column names are mixed-case (`Entry`, `Sf_ID`), so they
    have to be double-quoted; all-lowercase names are left bare, as the legacy
    code did, to keep generated SQL comparable.
    """

    return name if name.islower() else '"%s"' % (name,)


def qualified(schema, table):
    """`schema.table`, each quoted as needed. Empty/None schema means unqualified."""

    if schema is None or str(schema).strip() == "":
        return quote(table)
    return "%s.%s" % (quote(str(schema).strip()), quote(table))


#######################################
# psql/pg_dump plumbing


def _cmd(binary, dsn):
    """Base command line for psql/pg_dump against `dsn`."""

    cmd = [binary, "-d", dsn["dbname"]]
    for opt, flag in (("user", "-U"), ("host", "-h"), ("port", "-p")):
        if opt in dsn:
            cmd.extend([flag, str(dsn[opt])])
    return cmd


def run_command(cmd, verbose=False):
    """Run a psql/pg_dump command; return (ok, stdout).

    On failure the command and both streams go to stderr -- the legacy code
    repeated that block eight times, once per call site.
    """

    if verbose:
        sys.stderr.write("%s\n" % (" ".join(cmd),))

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         universal_newlines=True)
    (out, err) = p.communicate()

    if p.returncode != 0:
        sys.stderr.write("ERR: %s returned %d\n" % (os.path.basename(cmd[0]), p.returncode))
        sys.stderr.write("%s\n" % (" ".join(cmd),))
        sys.stderr.write("** STDERR **\n%s\n" % (err,))
        sys.stderr.write("** STDOUT **\n%s\n" % (out,))
        return (False, out)

    if verbose:
        sys.stderr.write("** STDERR **\n%s\n" % (err,))
        sys.stderr.write("** STDOUT **\n%s\n" % (out,))
    else:
        # psql -f keeps going after an error and still exits 0, so the only
        # sign of a half-applied DDL script is what it wrote to stderr.  The
        # DDL scripts all start by dropping what they are about to create, so
        # the NOTICEs about that (and their DETAIL continuations) are noise.
        # Server-side messages are upper case ("psql:f.sql:9: ERROR: ..."),
        # psql's own -- a \copy that cannot open its file, say -- are lower.
        problems = [l for l in err.splitlines()
                    if any(s in l.lower()
                           for s in (": error:", ": fatal:", ": panic:", ": warning:"))]
        if len(problems) > 0:
            sys.stderr.write("%s\n" % ("\n".join(problems),))

    return (True, out)


def run_sql_file(dsn, script, config=None, verbose=False, stop_on_error=True):
    """`psql -f script`. Returns True on success.

    ON_ERROR_STOP matters more than it looks.  Without it psql runs every
    remaining statement after a failure and still exits 0, so a script that
    half worked reports success -- which is how a run of webapi.sql that could
    not build `query_grid` (a missing dependency, eight statements in) came
    back "ok" with the rest of the schema built around the hole.  Under the
    shadow-and-swap loader that partial schema is then renamed into place over
    the good one.

    The scripts this runs are all `drop ... if exists` / `create` DDL, so there
    is nothing here that is expected to fail; `stop_on_error=False` is for a
    caller that knows otherwise.
    """

    f = os.path.realpath(script)
    if not os.path.exists(f):
        raise IOError("File not found: %s" % (f,))

    cmd = _cmd(binary(config, "psql"), dsn)
    if stop_on_error:
        cmd.extend(["-v", "ON_ERROR_STOP=1"])
    if not verbose:
        cmd.extend(["--quiet", "--echo-errors"])
    cmd.extend(["-f", f])

    return run_command(cmd, verbose)[0]


def psql_command(dsn, stmt, config=None, verbose=False):
    """`psql -c stmt`. Returns (ok, stdout)."""

    cmd = _cmd(binary(config, "psql"), dsn)
    if not verbose:
        cmd.append("-q")
    cmd.extend(["-c", stmt])
    return run_command(cmd, verbose)


def copy_from_csv(dsn, filename, schema, table, config=None, verbose=False):
    """Load a CSV into a table with `\\copy ... from`.

    The file's first row is column headers; column order need not match the
    table's, so the column list is taken from the header.  No foreign keys are
    defined on these tables, so load order does not matter.
    """

    infile = os.path.realpath(filename)
    if not os.path.exists(infile):
        raise IOError("Not found: %s" % (infile,))

    with open(infile) as f:
        cols = [c.strip().strip("'\"") for c in f.readline().split(",")]
    if len(cols) < 1 or cols == [""]:
        sys.stderr.write("no columns in %s\n" % (infile,))
        return False

    stmt = "\\copy %s (%s) from '%s' csv header" \
        % (qualified(schema, table), ",".join(quote(c) for c in cols), infile)

    return psql_command(dsn, stmt, config=config, verbose=verbose)[0]


def copy_to_csv(dsn, what, outfile, config=None, verbose=False):
    """Dump a table (or a parenthesized query) to CSV with `\\copy ... to`."""

    stmt = "\\copy %s to '%s' csv header" % (what, outfile)
    return psql_command(dsn, stmt, config=config, verbose=verbose)[0]


#######################################


def add_ro_grants(dsn, schema, user, config=None, verbose=False):
    """Grant the read-only web user select on everything in `schema`."""

    sqls = ("grant usage on schema %s to %s",
            "grant select on all tables in schema %s to %s",
            "alter default privileges in schema %s grant select on tables to %s",
            "grant usage on all sequences in schema %s to %s",
            "alter default privileges in schema %s grant usage on sequences to %s",)

    for sql in sqls:
        if not psql_command(dsn, sql % (schema, user,), config=config, verbose=verbose)[0]:
            return False
    return True


def list_tables(dsn, schema, nonempty_only=False, verbose=False):
    """Base table names in `schema`, sorted; optionally only those with rows."""

    sql = "select table_name from information_schema.tables" \
          " where table_schema = %s and table_type = 'BASE TABLE' order by table_name"

    with connection(dsn) as conn:
        with conn.cursor() as curs:
            curs.execute(sql, (schema,))
            tables = [row[0] for row in curs.fetchall()]

            if not nonempty_only:
                return tables

            rc = []
            for table in tables:
                count = "select count(*) from %s" % (qualified(schema, table),)
                curs.execute(count)
                row = curs.fetchone()
                if verbose:
                    sys.stdout.write("%s : %s\n" % (count, row is None and "NULL" or row[0],))
                if row is not None and row[0] > 0:
                    rc.append(table)
            return rc

#
# eof
