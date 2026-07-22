#!/usr/bin/env python3
#
# Post-load cleanups for macromolecule entries.
#
# These normalize values the depositors typed in freely -- software names,
# task names, vendor names -- against hand-maintained maps (software.js,
# task.js, swauthors.js), and patch up a few entries by hand.  All of it runs
# in one transaction: either the archive is fixed up or it is left as loaded.
#

import argparse
import json
import os
import sys
from configparser import ConfigParser

_UP = os.path.abspath(os.path.join(os.path.split(__file__)[0], ".."))
sys.path.append(_UP)
from loader import db
from loader import shadow
from loader import datafiles

DB = "macromolecules"

# `DB` is the config *section*; the schema it names is a separate thing, and
# the two are only identical by convention.  These fixups used to qualify
# their tables with the section name, which silently wrote to a schema called
# `macromolecules` whatever the config said -- including, once shadow loads
# existed, the live schema while the data was still in the shadow.  fixup()
# resolves it once, here.
_SCHEMA = DB


def _table(name):
    return db.qualified(_SCHEMA, name)


def _log(verbose, sql, curs=None):
    if not verbose:
        return
    sys.stdout.write(sql if curs is None else "%s : %d\n" % (sql, curs.rowcount,))


# wrapper for misc. fixes
#
#
def fixup(config, verbose=False):
    if verbose:
        sys.stdout.write("fixup()\n")

    # Normally this runs between the load and the swap, so the schema to fix
    # up is the shadow.  Run on its own after a swap there is no shadow left,
    # and the live schema is the thing that was loaded -- so fall back to it
    # rather than failing on a schema that does not exist.
    global _SCHEMA
    conn = db.connect(db.dsn(config, DB))
    _SCHEMA = shadow.target(config, DB)
    with conn.cursor() as curs:
        curs.execute("select 1 from pg_namespace where nspname = %s", (_SCHEMA,))
        if curs.fetchone() is None:
            _SCHEMA = config.get(DB, "schema")
    if verbose:
        sys.stdout.write("fixing up schema %s\n" % (_SCHEMA,))

    try:
        with conn.cursor() as curs:
            fix_software(curs, config, verbose=verbose)
            fix_task(curs, config, verbose=verbose)
            fix_software_authors(curs, config, verbose=verbose)
            fix_entry(curs, verbose=verbose)
            fix_entities(curs, verbose=verbose)
            fix_csref(curs, verbose=verbose)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


#
#
def fix_entry(curs, verbose=False):

    entry = _table("Entry")
    small = """update %s set "Type"='small molecule structure' where """ % (entry,)

    for sql in (small + """"ID"='15443' and "Type" is null""",
                small + """"ID"='16041' and "Type" is null""",
                small + """cast("ID" as integer)>=20000 and cast("ID" as integer)<25000"""
                        """ and "Type" is null""",
                """update %s set "Type"='macromolecule'"""
                """ where cast("ID" as integer)<20000 and "Type" is null""" % (entry,),
                # these are non-public and shouldn't be there; wipe them just in case
                "truncate %s" % (_table("Contact_person"),),
                "truncate %s" % (_table("Upload_data"),)):
        _log(verbose, sql)
        curs.execute(sql)
        _log(verbose, sql, curs)


# this one takes a dictionary "map" and reduces to the key
#  to normalize all different spellings etc.
# If the map's value list is empty, this just fixes the case.
#
def _fix_map(curs, table, column, which, verbose=False):

    sql = """update %s set %s=%%s where regexp_replace( trim( lower( %s ) ), '[[:space:]]+', ' ' )=%%s""" \
        % (_table(table), db.quote(column), db.quote(column))

    for name in sorted(which.keys()):
        for spelling in [name] + list(which[name] or []):
            _log(verbose, sql % (name, spelling.lower()))
            curs.execute(sql, (name, spelling.lower()))
            _log(verbose, sql, curs)


# names don't need to be barewords,
# sequences are line-wrapped in the entries
#
def fix_entities(curs, verbose=False):

    for table in ("Entity", "Assembly", "Chem_comp"):
        sql = """update %s set "Name"=regexp_replace("Name", '_+', ' ', 'g')""" % (_table(table),)
        _log(verbose, sql)
        curs.execute(sql)
        _log(verbose, sql, curs)

    sql = """update %s set "Polymer_seq_one_letter_code_can"=regexp_replace( "Polymer_seq_one_letter_code_can",'\n','','g'),
          "Polymer_seq_one_letter_code"=regexp_replace( "Polymer_seq_one_letter_code",'\n','','g')""" \
        % (_table("Entity"),)
    _log(verbose, sql)
    curs.execute(sql)
    _log(verbose, sql, curs)


#
#
def fix_csref(curs, verbose=False):

    # TODO!
    #    with open( "chem_shift_ref_todo.csv" ) as f :
    #        cs = csv.DictReader( f )
    #        for row in cs :
    #            if verbose : print( row )
    pass


def _mapfile(config, option, verbose=False):
    """Read one of the hand-maintained JSON maps; None if it isn't there.

    Defaults to the copy in this checkout -- see loader/datafiles.py.
    """

    try:
        f = datafiles.path(config, DB, option)
    except IOError as e:
        if verbose:
            sys.stderr.write("%s\n" % (e,))
        return None
    with open(f) as inf:
        return json.load(inf)


# software/task fixup originally done for nmrbox
#
def fix_software(curs, config, verbose=False):
    if verbose:
        sys.stdout.write("fix_software()\n")

    dat = _mapfile(config, "software_mapfile", verbose)
    if dat is not None:
        _fix_map(curs, table="Software", column="Name", which=dat, verbose=verbose)


def fix_task(curs, config, verbose=False):
    if verbose:
        sys.stdout.write("fix_task()\n")

    dat = _mapfile(config, "task_mapfile", verbose)
    if dat is not None:
        _fix_map(curs, table="Task", column="Task", which=dat, verbose=verbose)


def fix_software_authors(curs, config, verbose=False):
    if verbose:
        sys.stdout.write("fix_software_authors()\n")

    dat = _mapfile(config, "software_authors_mapfile", verbose)
    if dat is None:
        return

    qry = 'select "Sf_ID","Entry_ID" from %s where "Name"=%%s' % (_table("Software"),)
    upd = 'update %s set "Name"=%%s where "Sf_ID"=%%s and "Entry_ID"=%%s' % (_table("Vendor"),)

    for (sw, vendor) in sorted(dat.items()):
        _log(verbose, qry % (sw,) + "\n")
        curs.execute(qry, (sw,))
        for (sfid, entryid) in curs.fetchall():
            curs.execute(upd, (vendor, sfid, entryid))
            _log(verbose, upd % (vendor, sfid, entryid), curs)

    # special
    #
    qry = """select "Sf_ID","Entry_ID" from %s where "Name"='PyMol'""" % (_table("Software"),)
    curs.execute(qry)
    pymol = curs.fetchall()

    vendor = _table("Vendor")
    up1 = """update %s set "Name"='DeLano Scientific LLC.'""" % (vendor,) \
        + """ where "Sf_ID"=%s and "Entry_ID"=%s and "Name" like '%%delano%%'"""
    up2 = """update %s set "Name"='Schrodinger, LLC'""" % (vendor,) \
        + """ where "Sf_ID"=%s and "Entry_ID"=%s and "Name" like '%%dinger%%'"""

    for (sfid, entryid) in pymol:
        for sql in (up1, up2):
            curs.execute(sql, (sfid, entryid))
            _log(verbose, sql % (sfid, entryid), curs)


#
#
#
if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="post-load fixups for macromolecule entries")
    ap.add_argument("-t", "--time", help="print out timings", dest="time", action="store_true",
                    default=False)
    ap.add_argument("-v", "--verbose", help="print lots of messages to stdout", dest="verbose",
                    action="store_true", default=False)
    ap.add_argument("-c", "--config", help="config file", dest="conffile", required=True)
    args = ap.parse_args()

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    import loader
    with loader.timer(label="Macromolecule fixup", silent=not args.time):
        fixup(config=cp, verbose=args.verbose)

#
# eof
