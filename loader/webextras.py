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

DB = "web"


def _schema(config):
    """The web schema to write to -- the shadow one during a shadow load."""

    return shadow.target(config, DB)


# wrapper
#
#
def load(config, verbose=False):

    create_schema(config, verbose)
    load_procq(config, verbose)
    load_depids(config, verbose)
    load_extras(config, verbose)
    load_bmrb_pdb_map(config, start=1, verbose=verbose)
    generate_stats(config, verbose)
    if config.has_option(DB, "rouser"):
        db.add_ro_grants(db.dsn(config, DB), schema=_schema(config),
                         user=config.get(DB, "rouser"), config=config, verbose=verbose)


def _script(config, section, option):
    script = os.path.realpath(config.get(section, option))
    if not os.path.exists(script):
        raise IOError("File not found: %s" % (script,))
    return script


#
#
#
def _workdir(config):
    """Where rewritten copies of the SQL scripts go."""

    return os.path.join(os.path.realpath(config.get(DB, "shadowdir")
                                         if config.has_option(DB, "shadowdir")
                                         else "."), "shadow")


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

    return db.run_sql_file(db.dsn(config, DB), script, config=config, verbose=verbose)


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

    return db.run_sql_file(db.dsn(config, DB), script, config=config, verbose=verbose)


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
    for name in config.get(DB, "csvfiles").split():
        f = os.path.realpath(name)
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
