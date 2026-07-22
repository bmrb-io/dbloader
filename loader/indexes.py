#!/usr/bin/env python3
#
# Indexes and planner statistics for the entry schemas.
#
# The entry tables are generated from the dictionary (loader/starschema.py),
# which describes columns and types and says nothing about indexes -- so until
# something adds them, every query over the archive is a sequential scan.  Two
# consumers cared enough to add their own afterwards:
#
#   * cs_stats.sql, run by the web stage a few minutes later, whose six
#     `num_outliers` updates are a correlated subquery per statistics row --
#     275 sequential scans of Atom_chem_shift for one statement, and nine
#     statements of that shape.  Measured on a 222k-row Atom_chem_shift, one
#     (Comp_ID, Atom_ID) index took the whole script from 24s to 7s, and the
#     planner's cost for that subquery from 12730 to 116.  The archive is
#     around sixty times that size and the pattern is superlinear.
#
#   * BMRB-API's reloaders/sql/initialize.sql, which creates six indexes on
#     these tables as its first act, then builds the API's own derived tables.
#     It runs as a separate job after the reload -- against the *live* schema,
#     so the archive is queryable but unindexed in between, and the index
#     builds take exclusive locks on tables the website is reading.
#
# Building them here instead means they are built on the shadow schema, before
# the swap: no locks anyone can see, and the schema arrives indexed.  The API
# job then finds them already there and skips its block -- note that block is
# all-or-nothing (the first CREATE INDEX that fails aborts the rest via its
# EXCEPTION handler), so this must create every one of the six, not some.
#
# Indexes are built after the entries are loaded, never before: filling a
# table and maintaining an index at the same time is slower than doing them in
# order.
#

import os
import sys

_UP = os.path.abspath(os.path.join(os.path.split(__file__)[0], ".."))
sys.path.append(_UP)
from loader import db
from loader import shadow

# (table, index expression) per schema.  The first is ours; the rest mirror
# BMRB-API initialize.sql, which is why they are spelled the way they are --
# including the CAST, which is what the API queries on.
#
# `Val` is text in macromolecules and typed in metabolomics; casting is a
# no-op in the second case and the index is still what the API asks for.
COMMON = (
    # cs_stats.sql: `where "Comp_ID" = ... and "Atom_ID" = ...`, once per
    # statistics row
    ("Atom_chem_shift", '("Comp_ID", "Atom_ID")'),
    # BMRB-API initialize.sql
    ("Atom_chem_shift", '(CAST("Val" AS FLOAT), "Atom_type")'),
    ("Chem_shift_experiment", '("Entry_ID", "Sample_ID")'),
    ("Sample_component", '("Entry_ID", "Sample_ID")'),
)

INDEXES = {
    "macromolecules": COMMON,
    "metabolomics": COMMON,
}


def _key(schema):
    """The INDEXES key for a schema name, shadow or live.

    Indexes are built on <schema>_new, before the swap, but the table set is a
    property of the archive, not of which copy of it this is.  Looking the
    shadow name up directly found nothing and quietly created no indexes at
    all, so the lookup is explicit and a miss is loud.
    """

    return shadow.live_name(schema)


def create(conn, schema, verbose=False):
    """Create the entry-table indexes for `schema`.  Returns how many were made.

    Each is attempted separately and a failure is reported rather than raised:
    an expression index can fail on data the loader was happy to store (the
    CAST above will not survive a non-numeric `Val`), and losing an hour-long
    reload over an index would be the wrong trade.  The API's own block makes
    the same choice.
    """

    key = _key(schema)
    if key not in INDEXES:
        sys.stderr.write("no index set defined for schema %s (%s)\n" % (schema, key,))
        return 0

    made = 0
    for (table, expression) in INDEXES[key]:
        # let PostgreSQL name it: the API does the same, and a generated name
        # cannot collide with the one index it does name (error_on_duplicates)
        sql = "create index on %s %s" % (db.qualified(schema, table), expression,)
        try:
            with conn.cursor() as curs:
                curs.execute(sql)
            made += 1
            if verbose:
                sys.stdout.write("%s\n" % (sql,))
        except Exception as e:
            conn.rollback()
            sys.stderr.write("index not created: %s\n    %s\n"
                             % (sql, str(e).strip(),))
        else:
            conn.commit()
    return made


def analyze(conn, schema, verbose=False):
    """ANALYZE every table in `schema`.  Returns how many were analyzed.

    Nothing else in the loader does this, and the schema is queried within
    minutes of being filled -- cs_stats.sql runs in the same reload. Whether
    autovacuum has got to a table by then is a race, and the plans above
    depend entirely on the answer: with no statistics the planner has no
    reason to prefer the indexes this module just built.
    """

    conn.rollback()
    old = conn.autocommit
    conn.autocommit = True
    try:
        with conn.cursor() as curs:
            curs.execute("select table_name from information_schema.tables"
                         " where table_schema = %s and table_type = 'BASE TABLE'",
                         (schema,))
            tables = [r[0] for r in curs.fetchall()]
            for table in tables:
                curs.execute("analyze %s" % (db.qualified(schema, table),))
        if verbose:
            sys.stdout.write("analyzed %d tables in %s\n" % (len(tables), schema,))
    finally:
        conn.autocommit = old
    return len(tables)


def prepare(dsn, schema, verbose=False):
    """Index and analyze one freshly loaded entry schema."""

    with db.connection(dsn) as conn:
        made = create(conn, schema, verbose)
        n = analyze(conn, schema, verbose)
    return (made, n)

#
# eof
