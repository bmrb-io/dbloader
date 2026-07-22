#!/usr/bin/env python3
#
# The `web` schema: everything the website needs that isn't in an entry.
#
# NOTE that this only works if "web" is a schema in the same database as
# "macromolecules": the chemical-shift statistics are computed from the
# macromolecule tables (by cs_stats.sql, which hardcodes both schema names and
# the CSV output paths) and stored here for the API to query.
#
# The processing queue, the deposition-ID map and the BMRB-PDB map all come
# from the ETS tracking database -- a different server, see loader/ets.py.
#

import argparse
import os
import re
import sys
from configparser import ConfigParser

_UP = os.path.abspath(os.path.join(os.path.split(__file__)[0], ".."))
sys.path.append(_UP)
import loader
from loader import db
from loader import shadow
from loader import datafiles

DB = "web"


def _schema(config):
    """The web schema to write to -- the shadow one during a shadow load."""

    return shadow.target(config, DB)


# wrapper
#
#
def load(config, verbose=False):

    ensure_extensions(config, verbose)
    create_schema(config, verbose)
    load_procq(config, verbose)
    load_depids(config, verbose)
    load_extras(config, verbose)
    load_bmrb_pdb_map(config, start=1, verbose=verbose)
    generate_stats(config, verbose)
    # before the API tables: query_grid and instant_extra_search_terms read
    # web.timedomain_data
    load_timedomain(config, verbose)
    # last: it builds on everything above (pdb_link and timedomain_data in
    # particular) as well as on the loaded entry schemas
    generate_api_tables(config, verbose)
    if config.has_option(DB, "rouser"):
        db.add_ro_grants(db.dsn(config, DB), schema=_schema(config),
                         user=config.get(DB, "rouser"), config=config, verbose=verbose)


def _script(config, section, option):
    """One of the SQL scripts that ship with dbloader -- the config only has
    to name it to point somewhere else (loader/datafiles.py)."""

    return datafiles.path(config, section, option)


#
#
#
def _workdir(config):
    """Where rewritten copies of the SQL scripts go."""

    return shadow.workdir(config, DB)


# Extensions the schemas depend on, and the schema they must live in.
#
# `public`, specifically: an extension belongs to a schema, and the swap drops
# the schema it replaces (`drop schema web_old cascade`).  pg_trgm installed
# into `web` would therefore be dropped by the first reload that swapped web --
# taking every index that uses gin_trgm_ops with it, which is the whole of the
# API's instant search.  Nothing would report it; the indexes would simply stop
# existing and the queries would go to sequential scans, if they worked at all.
#
# webapi.sql used to say `CREATE extension IF NOT EXISTS pg_trgm` with no
# schema, which lands it wherever search_path points -- usually public, but
# that is a default, not a guarantee, and it says nothing about an extension
# already installed somewhere else years ago.  Owning it here makes it explicit
# and, where it is already in the wrong place, fixes it.
EXTENSIONS = ("pg_trgm",)
EXTENSION_SCHEMA = "public"


def _swapped_schemas(config):
    """Live schema names the swap replaces -- an extension must not be in one."""

    out = set()
    for section in ("dictionary", "macromolecules", "metabolomics", "chemcomps",
                    "web", "meta"):
        if config.has_section(section) and config.has_option(section, "schema"):
            out.add(config.get(section, "schema"))
    return out


def ensure_extensions(config, verbose=False):
    """Install the required extensions, outside any schema the swap replaces.

    Idempotent: creates what is missing, moves what is in the way, and leaves
    alone what is already right.
    """

    doomed = _swapped_schemas(config)

    with db.connection(db.dsn(config, DB), autocommit=True) as conn:
        with conn.cursor() as curs:
            for ext in EXTENSIONS:
                curs.execute("select n.nspname from pg_extension e"
                             " join pg_namespace n on n.oid = e.extnamespace"
                             " where e.extname = %s", (ext,))
                row = curs.fetchone()

                if row is None:
                    curs.execute("create extension if not exists %s with schema %s"
                                 % (db.quote(ext), db.quote(EXTENSION_SCHEMA),))
                    if verbose:
                        sys.stdout.write("created extension %s in %s\n"
                                         % (ext, EXTENSION_SCHEMA,))
                    continue

                where = row[0]
                if where not in doomed:
                    if verbose:
                        sys.stdout.write("extension %s already in %s\n" % (ext, where,))
                    continue

                # In a schema this reload is about to replace.  Move it rather
                # than let the swap drop it.
                sys.stderr.write("extension %s is in schema %s, which this reload"
                                 " replaces -- moving it to %s\n"
                                 % (ext, where, EXTENSION_SCHEMA,))
                try:
                    curs.execute("alter extension %s set schema %s"
                                 % (db.quote(ext), db.quote(EXTENSION_SCHEMA),))
                except Exception as e:
                    raise Exception(
                        "extension %s is in schema %s, which the swap drops, and it"
                        " could not be moved to %s: %s\nRun this as a superuser"
                        " before reloading:\n    alter extension %s set schema %s;"
                        % (ext, where, EXTENSION_SCHEMA, str(e).strip(),
                           ext, EXTENSION_SCHEMA,))


def create_schema(config, verbose=False):
    if verbose:
        sys.stdout.write("create_schema()\n")

    # webschema.sql names its own schema (`drop schema if exists web cascade`),
    # so a shadow load has to rewrite it -- same as dictionary.sql.
    script = _script(config, DB, "ddlfile")
    sfx = shadow.suffix(config, DB)
    if sfx:
        mapping = dict((x, x + sfx) for x in shadow.declared_schemas(script))
        script = shadow.rewritten_copy(script, mapping, _workdir(config), verbose=verbose)

    if not db.run_sql_file(db.dsn(config, DB), script, config=config,
                           verbose=verbose):
        raise Exception("failed to build the web schema: %s" % (script,))
    return True


# The SQL script creates the statistics tables in the web schema from the
# tables in the macromolecules schema.  Every reference in it is schema-
# qualified -- 36 `macromolecules.` and 92 `web.` -- so `search_path` cannot
# redirect it and a shadow load rewrites it instead.  That is also what lets
# the statistics be computed from the shadow macromolecules *before* the swap,
# rather than from a schema that is already live.
#
# The trailing `\copy ... to '/projects/BMRB/public/ftp/...'` lines write the
# published statistics CSVs to hardcoded absolute paths; `csstats_outdir`
# redirects them at a directory that exists on this machine.
#
def generate_stats(config, verbose=False):
    if verbose:
        sys.stdout.write("generate_stats()\n")

    script = _script(config, "macromolecules", "csstats")

    mapping = {}
    for section in (DB, "macromolecules"):
        sfx = shadow.suffix(config, section)
        if sfx:
            mapping[config.get(section, "schema")] = config.get(section, "schema") + sfx

    if mapping or config.has_option(DB, "csstats_outdir"):
        outdir = (os.path.realpath(config.get(DB, "csstats_outdir"))
                  if config.has_option(DB, "csstats_outdir") else None)
        script = shadow.rewritten_copy(script, mapping, _workdir(config),
                                       copy_outdir=outdir, verbose=verbose)

    if not db.run_sql_file(db.dsn(config, DB), script, config=config,
                           verbose=verbose):
        raise Exception("failed to build the chemical-shift statistics: %s" % (script,))
    return True


# Time domain (raw FID) data: how many data sets each entry has and how much
# disk they take.  Ported from BMRB-API's reloaders/timedomain.py, which ran as
# part of the same post-reload job webapi.sql used to, and for the same reason:
# webapi.sql reads web.timedomain_data while building query_grid and
# instant_extra_search_terms, so it has to be filled before them and inside the
# same swap.
#
# The port drops a dependency rather than adding one.  The API took its entry
# list from Redis (`macromolecules:entry_list`); here the archive that was just
# loaded is the authority, so it comes from the shadow schema's own Entry
# table -- no Redis, and no chance of scanning a list that disagrees with what
# is in the database.
#
# The directory layout is the one thing that cannot be worked out from here:
# the API's config points at `<entry dir>/bmr<id>/clean`, dbloader's `entrydir`
# at `<entry dir>/bmr<id>`, and which is right depends on the host.  Hence
# `timedomain_dir`, and hence the count reported at the end -- a pattern that
# matches nothing is the failure mode to expect, and it would otherwise look
# exactly like an archive with no time domain data.
#
def _timedomain_pattern(config):
    """Directory holding one entry's time domain data, with %s for the ID."""

    if config.has_option(DB, "timedomain_dir"):
        pattern = config.get(DB, "timedomain_dir").strip()
    else:
        pattern = os.path.join(config.get("macromolecules", "entrydir"),
                               "bmr%s", "timedomain_data")
    if "%s" not in pattern:
        raise Exception("[%s] timedomain_dir must contain %%s for the entry ID: %s"
                        % (DB, pattern,))
    return pattern


def _timedomain_path(pattern, entry_id):
    """`pattern` with the entry ID substituted, however many times it appears.

    BMRB's paths repeat the ID -- `.../bmr%s/clean/bmr%s_3.str` -- so the count
    is taken from the pattern rather than assumed to be one, the same way
    BMRB-API does it.
    """

    return pattern % ((entry_id,) * pattern.count("%s"))


def _dir_size(path):
    """Total bytes under `path`."""

    total = 0
    for (dirpath, _, filenames) in os.walk(path):
        for name in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, name))
            except OSError:          # vanished or unreadable mid-walk
                pass
    return total


def _data_sets(path):
    """How many time domain data sets are in `path`.

    A faithful port, quirks included: an archive counts as a set unless a
    directory of the same name is also there (so `foo.tar.gz` beside `foo/` is
    not counted twice), and a lone subdirectory is descended into, on the
    assumption that it is a wrapper around the real sets rather than one set.
    Changing any of that would change published numbers.
    """

    sets = 0
    last_set = ""
    for name in os.listdir(path):
        entry = os.path.join(path, name)
        if os.path.isdir(entry):
            sets += 1
            last_set = entry
        elif os.path.isfile(entry):
            matching = name.replace(".zip", "").replace(".gz", "") \
                           .replace(".bz2", "").replace(".tar", "")
            if not os.path.isdir(os.path.join(path, matching)):
                sets += 1

    if sets == 1 and last_set:
        child = _data_sets(last_set)
        if child > 1:
            return child
    return sets


def _loaded_entry_ids(config):
    """Entry IDs in the macromolecule archive that was just loaded."""

    schema = shadow.target(config, "macromolecules")
    with db.connection(db.dsn(config, "macromolecules")) as conn:
        with conn.cursor() as curs:
            curs.execute('select distinct "ID" from %s order by "ID"'
                         % (db.qualified(schema, "Entry"),))
            return [row[0] for row in curs.fetchall()]


def load_timedomain(config, verbose=False):
    if verbose:
        sys.stdout.write("load_timedomain()\n")

    pattern = _timedomain_pattern(config)
    sql = "insert into %s.timedomain_data (bmrbid, size, sets)" \
          " values (%%(id)s,%%(size)s,%%(sets)s)" % (_schema(config),)

    found = [0]

    def rows():
        for entry_id in _loaded_entry_ids(config):
            path = _timedomain_path(pattern, entry_id)
            # isdir, not exists: BMRB-API pointed this at a path inside the
            # entry *file* rather than its directory, where `exists` is False
            # for every entry and the scan silently found nothing at all
            if not os.path.isdir(path):
                continue
            found[0] += 1
            yield ((entry_id,), {"id": entry_id, "size": _dir_size(path),
                                 "sets": _data_sets(path)})

    _insert_all(config, sql, rows(), truncate=_schema(config) + ".timedomain_data",
                verbose=verbose)

    if found[0] == 0:
        sys.stderr.write(
            "*** no time domain data found for any entry, under %s\n"
            "    If the archive really has none this is fine; if not, the\n"
            "    directory layout is wrong -- set [%s] timedomain_dir.\n"
            % (pattern, DB,))
    elif verbose:
        sys.stdout.write("timedomain: %d entries with data\n" % (found[0],))
    return found[0]


# The API's derived tables -- query_grid, chem_shifts, instant_cache,
# instant_extra_search_terms, metabolomics_summary -- see webapi.sql.
#
# This was a separate condor job (BMRB-API `reloaders --sql`) that ran after the
# reload, against the live schemas.  That could not survive the move to loading
# the served database directly: the swap replaces `web` wholesale, so every
# object this builds would be destroyed and then rebuilt over the following
# minutes, in public.  Built here it is inside the swap, and the whole release
# -- archive, statistics and API tables -- appears at once.
#
# It reads `metabolomics` as well as `macromolecules` and `web`, so all three
# have to be redirected at their shadows; that is the only reason this is not
# just another run_sql_file.
#
def generate_api_tables(config, verbose=False):
    if verbose:
        sys.stdout.write("generate_api_tables()\n")

    script = _script(config, DB, "apiddl")

    mapping = {}
    for section in (DB, "macromolecules", "metabolomics"):
        if not config.has_section(section):
            continue
        sfx = shadow.suffix(config, section)
        if sfx:
            mapping[config.get(section, "schema")] = config.get(section, "schema") + sfx

    if mapping:
        script = shadow.rewritten_copy(script, mapping, _workdir(config), verbose=verbose)

    if not db.run_sql_file(db.dsn(config, DB), script, config=config,
                           verbose=verbose):
        raise Exception("failed to build the API tables: %s" % (script,))
    return True


def _insert_all(config, sql, rows, truncate=None, verbose=False):
    """Insert an iterator's worth of rows in one transaction."""

    conn = db.connect(db.dsn(config, DB))
    try:
        with conn.cursor() as curs:
            if truncate is not None:
                curs.execute("truncate %s" % (truncate,))
            for (row, vals) in rows:
                if verbose:
                    sys.stdout.write("%s\n" % (",".join(str(i) for i in row),))
                curs.execute(sql, vals)
                if verbose:
                    sys.stdout.write(": inserted %d\n" % (curs.rowcount,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# processing queue from ETS
#
def load_procq(config, verbose=False):
    if verbose:
        sys.stdout.write("load_procq()\n")

    sql = "insert into %s.procque (accno,received,onhold,status,released)" \
          " values (%%(id)s,%%(recv)s,%%(hld)s,%%(st)s,%%(rel)s)" % (_schema(config),)

    def rows():
        # tuples: bmrb id, date received, on hold, release status, date returned to author
        for row in loader.processing_queue_itr(config):
            yield (row, {"id": row[0], "recv": row[1], "hld": row[2],
                         "st": row[3], "rel": row[4]})
        # tuples: bmrb id, date received
        for row in loader.removed_ids_itr(config):
            yield (row, {"id": row[0], "recv": row[1], "hld": "N",
                         "st": "Withdrawn", "rel": None})

    _insert_all(config, sql, rows(), verbose=verbose)


# deposition id to accession id map
#
def load_depids(config, verbose=False):
    if verbose:
        sys.stdout.write("load_depids()\n")

    sql = "insert into %s.dep2accno (depno,accno) values (%%(dep)s,%%(id)s)" % (_schema(config),)

    # tuples: deposition id, bmrb id
    rows = ((row, {"dep": row[0], "id": row[1]}) for row in loader.depids_itr(config))

    _insert_all(config, sql, rows, truncate=_schema(config) + ".dep2accno", verbose=verbose)


# BMRB-PDB ID map
#
def load_bmrb_pdb_map(config, start, verbose=False):
    if verbose:
        sys.stdout.write("load_bmrb_pdb_map()\n")

    sql = "insert into %s.pdb_link (bmrb_id, pdb_id) values (%%(bmrbid)s,%%(pdbid)s)" % (_schema(config),)

    # tuples: bmrb id, pdb id
    rows = ((row, {"bmrbid": row[0], "pdbid": row[1]})
            for row in loader.bmrb_pdb_ids_itr(config, start))

    _insert_all(config, sql, rows, truncate=_schema(config) + ".pdb_link", verbose=verbose)


# couple of extra files
#
def load_extras(config, verbose=False):
    if verbose:
        sys.stdout.write("load_extras()\n")

    pat = re.compile(r"([^.]+)\.([^.]+)\.csv$")
    dsn = db.dsn(config, DB)
    for f in datafiles.paths(config, DB, "csvfiles"):
        m = pat.search(os.path.split(f)[1])
        if not m:
            sys.stderr.write("%s does not match pattern\n" % (f,))
            continue
        # the file prefix names the live schema; during a shadow load the rows
        # have to go into the shadow one
        db.copy_from_csv(dsn, filename=f, schema=m.group(1) + shadow.suffix(config, DB),
                         table=m.group(2), config=config, verbose=verbose)


#
#
#
if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="load the web schema into the PostgreSQL database")
    ap.add_argument("-v", "--verbose", help="print lots of messages to stdout", dest="verbose",
                    action="store_true", default=False)
    ap.add_argument("-t", "--time", help="time the operatons", dest="time",
                    action="store_true", default=False)
    ap.add_argument("-c", "--config", help="config file", dest="conffile", required=True)
    ap.add_argument("--no-cs-stats", help="don't generate chemical shift statistics",
                    dest="genstats", action="store_false", default=True)
    ap.add_argument("--no-proc-queue", help="don't load processing queue", dest="procq",
                    action="store_false", default=True)
    ap.add_argument("--no-extras", help="don't load extra tables", dest="extras",
                    action="store_false", default=True)
    args = ap.parse_args()

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    with loader.timer(label="make web schema", silent=not args.time):
        create_schema(config=cp, verbose=args.verbose)
        if cp.has_option(DB, "rouser"):
            db.add_ro_grants(db.dsn(cp, DB), schema=_schema(cp),
                             user=cp.get(DB, "rouser"), config=cp, verbose=args.verbose)

    if args.genstats:
        with loader.timer(label="generate CS stats", silent=not args.time):
            generate_stats(config=cp, verbose=args.verbose)

    if args.procq:
        with loader.timer(label="load processing queue", silent=not args.time):
            load_procq(config=cp, verbose=args.verbose)
            load_depids(config=cp, verbose=args.verbose)

    if args.extras:
        with loader.timer(label="load additional CSVs", silent=not args.time):
            load_extras(config=cp, verbose=args.verbose)

#
# eof
