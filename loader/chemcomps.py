#!/usr/bin/env python3
#
# Chem comps live in a separate database (ccdb) that also holds unreleased
# ones, so they cannot simply be copied across:
#  1. dump the released ones to CSV,
#  2. load those CSVs into the bmrbeverything database.
#
# The target tables are generated from the dictionary the same way entry tables
# are (see loader/starschema.py) -- but only the subset in TABLES below, and
# always typed.
#

import argparse
import glob
import os
import pprint
import shutil
import sys
import tempfile
from configparser import ConfigParser

_UP = os.path.abspath(os.path.join(os.path.split(__file__)[0], ".."))
sys.path.append(_UP)
from loader import csvio, db, starschema

DB = "chemcomps"

# the dictionary schema the chem-comp tables are generated from
DICT_SCHEMA = "dict"

# schema the chem comps are dumped from, in the source database
SRCSCHEMA = "chem_comp"

TABLES = ("Atom_nomenclature",
          "Characteristic",
          "Chem_comp",
          "Chem_comp_SMILES",
          "Chem_comp_angle",
          "Chem_comp_atom",
          "Chem_comp_bio_function",
          "Chem_comp_bond",
          "Chem_comp_citation",
          "Chem_comp_common_name",
          "Chem_comp_db_link",
          "Chem_comp_descriptor",
          "Chem_comp_identifier",
          "Chem_comp_keyword",
          "Chem_comp_systematic_name",
          "Chem_comp_tor",
          "Chem_struct_descriptor",
          "Entity",
          "Entity_atom_list",
          "Entity_biological_function",
          "Entity_bond",
          "Entity_chem_comp_deleted_atom",
          "Entity_chimera_segment",
          "Entity_citation",
          "Entity_common_name",
          "Entity_comp_index",
          "Entity_comp_index_alt",
          "Entity_db_link",
          "Entity_keyword",
          "Entity_poly_seq",
          "Entity_systematic_name",
          "PDBX_chem_comp_feature",
          )

# tables keyed by "Comp_ID" rather than by "ID" or "Sf_ID"
COMP_ID_TABLES = tuple(t for t in TABLES if t.startswith("Chem_comp")) \
    + ("Atom_nomenclature", "Characteristic", "Chem_struct_descriptor",
       "PDBX_chem_comp_feature")

############################################################################################


def _src_dsn(config):
    """Connection parameters for the source (ccdb) database.

    Its own set of options, prefixed `src`, in the [chemcomps] section -- the
    section's unprefixed ones point at the database being loaded.
    """

    if not config.has_section(DB):
        raise Exception("No [%s] section in config file" % (DB,))
    if not config.has_option(DB, "srcdatabase"):
        raise Exception("No srcdatabase in [%s] section in config file" % (DB,))

    dsn = {"dbname": config.get(DB, "srcdatabase")}
    for opt, key in (("srcuser", "user"), ("srcpassword", "password"),
                     ("srchost", "host"), ("srcport", "port")):
        if config.has_option(DB, opt):
            val = config.get(DB, opt)
            if val is not None and str(val).strip() != "":
                dsn[key] = val
    return dsn


def list_tables(dsn, verbose=False):
    """Tables present in the source schema."""

    tables = db.list_tables(dsn, SRCSCHEMA, verbose=verbose)
    if verbose:
        for t in tables:
            sys.stdout.write("%s\n" % (t,))
    return tables


# main
#
def dump_and_load(config, verbose=False):

    wd = tempfile.mkdtemp()
    try:
        dump(config, where=wd, verbose=verbose)
        fix_inchi_column(where=wd, verbose=verbose)
        load(config, where=wd, verbose=verbose)
        fix_entry_id(config, verbose=verbose)
        if config.has_option(DB, "rouser"):
            db.add_ro_grants(db.dsn(config, DB), schema=config.get(DB, "schema"),
                             user=config.get(DB, "rouser"), config=config, verbose=verbose)
    finally:
        shutil.rmtree(wd)


############################################################################################
# dump released chem comps to CSV files
#
def dump(config, where=None, verbose=False):

    if verbose:
        sys.stdout.write("dump()\n")

    dsn = _src_dsn(config)
    if verbose:
        pprint.pprint(dsn)

    tables = list_tables(dsn, verbose=verbose)
    if len(tables) < 1:
        raise Exception("no tables to dump in %s" % (SRCSCHEMA,))

    if where is not None:
        assert os.path.isdir(where)
        for f in glob.glob(os.path.join(where, "*")):
            os.unlink(f)

    # unreleased chem comps, and the entities that point at them, are excluded
    # by subquery rather than by a generated ID list: the lists ran to tens of
    # thousands of literals, and the old code built them and then threw them
    # away in favour of exactly these two subqueries.
    unreleased = """(select "ID" from %s."Chem_comp" where "Release_status"<>'REL')""" \
        % (SRCSCHEMA,)
    unreleased_sfids = """(select "Sf_ID" from %s."Entity" where "Nonpolymer_comp_ID" in %s)""" \
        % (SRCSCHEMA, unreleased,)

    for table in tables:
        tbl = '%s."%s"' % (SRCSCHEMA, table,)
        if table == "Chem_comp":
            sql = 'select * from %s where "ID" not in %s' % (tbl, unreleased,)
        elif table in COMP_ID_TABLES:
            sql = 'select * from %s where "Comp_ID" not in %s' % (tbl, unreleased,)
        else:
            sql = 'select * from %s where "Sf_ID" not in %s' % (tbl, unreleased_sfids,)

        outfile = table + ".csv" if where is None else os.path.join(where, table + ".csv")
        csvio.tocsv(dsn, table="(%s)" % (sql,), outfile=outfile,
                    config=config, verbose=verbose)

    return True


# spec. case: InChI code was originally misspelled
#
def fix_inchi_column(where, verbose=False):

    infile = os.path.join(os.path.realpath(where), "Chem_comp.csv")
    if not os.path.exists(infile):
        sys.stderr.write("File not found: %s\n" % (infile,))
        return False

    outfile = infile + ".tmp"
    with open(outfile, "w") as out, open(infile) as f:
        for line in f:
            out.write(line.replace("InCHi_code", "InChI_code"))
    os.rename(outfile, infile)
    return True


def _target(config):
    """Connection parameters for the database being loaded into."""

    if not config.has_section("dictionary"):
        raise Exception("No [dictionary] section in config file")
    if not config.has_option("dictionary", "database"):
        raise Exception("No database in [dictionary] section in config file")

    dsn = db.dsn(config, DB)
    if "host" not in dsn:
        pprint.pprint(dsn)
        raise Exception("No host in DSN")
    return dsn


# load (previously dumped) chem comps from CSV files
#
def load(config, where, verbose=False):

    if verbose:
        sys.stdout.write("load(%s)\n" % (where,))
    indir = os.path.realpath(where)
    assert os.path.isdir(indir)

    dsn = _target(config)
    schema = config.get(DB, "schema")

    # the tables are in sub-schemas, just drop and re-create the whole thing.
    # no other option for now
    #
    with db.connection(dsn, autocommit=True) as conn:
        with conn.cursor() as curs:
            curs.execute("set client_min_messages=WARNING")
            curs.execute("drop schema if exists %s cascade" % (schema,))
            curs.execute("create schema %s" % (schema,))
        # only the chem-comp subset of the dictionary, and always typed
        starschema.create_tables(conn, schema, DICT_SCHEMA, use_types=True,
                                 only=TABLES, verbose=verbose)

    for f in sorted(glob.glob(os.path.join(indir, "*.csv"))):
        table = os.path.splitext(os.path.split(f)[1])[0]
        # no table was created for these, so loading one would just fail --
        # the legacy code warned and then tried anyway
        if table not in TABLES:
            sys.stderr.write("%s.csv not in tables, skipping\n" % (table,))
            continue
        db.copy_from_csv(dsn, filename=f, schema=DB, table=table, config=config, verbose=verbose)


# There are no Entry_IDs in chem comps, but Entry_ID is part of the primary key
# in the "big" databases.  Internally that is set to "NEED_ACC_NUM"; for the
# public database, fill it in from the comp ID.
#
def fix_entry_id(config, verbose=False):
    if verbose:
        sys.stdout.write("fix_entry_id()\n")

    dsn = _target(config)
    scam = config.get(DB, "schema")

    with db.connection(dsn) as conn:
        # ugh
        #
        for (table, column) in starschema.entryid_columns(conn, DICT_SCHEMA, only=TABLES):

            tbl = db.qualified(scam, table)
            col = db.quote(column)

            # in chem_comp it's id
            if table == "Chem_comp":
                sql = 'update %s set %s="ID"' % (tbl, col,)

            # in other chem_comp tables it's comp_id
            elif table in COMP_ID_TABLES:
                sql = 'update %s set %s="Comp_ID"' % (tbl, col,)

            # in entity tables it's entity_comp_index.comp_id -> entity_comp_index.entity_id,
            # except in entity itself, where it's the id
            elif table == "Entity_comp_index":
                sql = 'update %s e set %s="Comp_ID"' % (tbl, col,)

            elif table == "Entity":
                sql = 'update %s e set %s="Nonpolymer_comp_ID"' % (tbl, col,)

            else:
                sql = 'update %s e set %s=' % (tbl, col,) \
                    + '(select "Nonpolymer_comp_ID" from %s where "Sf_ID"=e."Sf_ID")' \
                    % (db.qualified(scam, "Entity"),)

            if verbose:
                sys.stdout.write(sql)
            with conn.cursor() as curs:
                curs.execute(sql)
                if verbose:
                    sys.stdout.write(": %d rows updated\n" % (curs.rowcount,))
        conn.commit()


####################################################################################################
#
#
if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="Dump public chem comps from ligand expo database")
    ap.add_argument("-v", "--verbose", default=False, action="store_true",
                    help="print lots of messages to stdout", dest="verbose")
    ap.add_argument("-t", "--time", help="time the operatons", dest="time",
                    action="store_true", default=False)
    ap.add_argument("-c", "--config", help="config file", dest="conffile", required=True)
    ap.add_argument("-d", "--outdir", help="directory for temporary files", dest="outdir")
    ap.add_argument("--load-only", help="don't dump the database", dest="dump",
                    action="store_false", default=True)
    ap.add_argument("--dump-only", help="don't load the database", dest="load",
                    action="store_false", default=True)
    args = ap.parse_args()

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    import loader
    if args.dump:
        with loader.timer(label="dump chemcomps", silent=not args.time):
            dump(config=cp, where=args.outdir, verbose=args.verbose)

    if args.load:
        with loader.timer(label="load chemcomps", silent=not args.time):
            load(config=cp, where=args.outdir, verbose=args.verbose)
            fix_entry_id(config=cp, verbose=args.verbose)

#
# eof
