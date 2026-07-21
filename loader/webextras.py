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

DB = "web"


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
        db.add_ro_grants(db.dsn(config, DB), schema=config.get(DB, "schema"),
                         user=config.get(DB, "rouser"), config=config, verbose=verbose)


def _script(config, section, option):
    script = os.path.realpath(config.get(section, option))
    if not os.path.exists(script):
        raise IOError("File not found: %s" % (script,))
    return script


#
#
#
def create_schema(config, verbose=False):
    if verbose:
        sys.stdout.write("create_schema()\n")

    return db.run_sql_file(db.dsn(config, DB), _script(config, DB, "ddlfile"),
                           config=config, verbose=verbose)


# The SQL script creates the statistics tables in the web schema from the
# tables in the macromolecules schema, then dumps them to CSV.  Both the CSV
# paths and the schema names are hardcoded in the SQL.
#
def generate_stats(config, verbose=False):
    if verbose:
        sys.stdout.write("generate_stats()\n")

    return db.run_sql_file(db.dsn(config, DB), _script(config, "macromolecules", "csstats"),
                           config=config, verbose=verbose)


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

    sql = "insert into web.procque (accno,received,onhold,status,released)" \
          " values (%(id)s,%(recv)s,%(hld)s,%(st)s,%(rel)s)"

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

    sql = "insert into web.dep2accno (depno,accno) values (%(dep)s,%(id)s)"

    # tuples: deposition id, bmrb id
    rows = ((row, {"dep": row[0], "id": row[1]}) for row in loader.depids_itr(config))

    _insert_all(config, sql, rows, truncate="web.dep2accno", verbose=verbose)


# BMRB-PDB ID map
#
def load_bmrb_pdb_map(config, start, verbose=False):
    if verbose:
        sys.stdout.write("load_bmrb_pdb_map()\n")

    sql = "insert into web.pdb_link (bmrb_id, pdb_id) values (%(bmrbid)s,%(pdbid)s)"

    # tuples: bmrb id, pdb id
    rows = ((row, {"bmrbid": row[0], "pdbid": row[1]})
            for row in loader.bmrb_pdb_ids_itr(config, start))

    _insert_all(config, sql, rows, truncate="web.pdb_link", verbose=verbose)


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
        db.copy_from_csv(dsn, filename=f, schema=m.group(1), table=m.group(2),
                         config=config, verbose=verbose)


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
            db.add_ro_grants(db.dsn(cp, DB), schema=cp.get(DB, "schema"),
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
