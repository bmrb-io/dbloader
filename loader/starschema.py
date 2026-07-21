#!/usr/bin/env python3
#
# The NMR-STAR dictionary, read as a relational schema definition.
#
# `dict.adit_item_tbl` is the materialized dictionary: one row per tag, with
# the table (`tagcategory`) and column (`tagfield`) it maps to, its SQL type
# (`dbtype`), and the order tags are defined in (`dictionaryseq`).  That is all
# it takes to generate the tables an entry loads into -- which is why the
# dictionary has to be loaded before any entries are.
#
# This replaces the parts of starobj's StarDictionary and NMRSTAREntry that
# dbloader used.  Column order matters: it is `dictionaryseq`, because the CSV
# dumps are `select *`.
#

import sys

from loader import db

# The one type mapping, reproduced from starobj/entry.py:create_tables.
#
# Floats are stored as varchar(63) deliberately: it keeps trailing zeros as
# deposited and sidesteps precision and rounding differences.  Do not "fix" it.
#
# `use_types = False` makes every column text -- that is how the macromolecule
# archive is loaded, while metabolomics gets real types.
_TYPES = (("char", "text"),
          ("varchar", "text"),
          ("vchar", "text"),
          ("boolean", "text"),      # 2018-06-07: the dictionary says boolean for
          ("text", "text"),         #   what used to be yes/no char(3), and the
          ("date", "date"),         #   values are still yes/no
          ("int", "integer"))


def sqltype(dbtype, use_types=True):
    """PostgreSQL type for a dictionary dbtype."""

    dbtype = dbtype.lower()
    if dbtype.startswith("float"):
        return "varchar(63)"
    if not use_types:
        return "text"
    for (prefix, sql) in _TYPES:
        if dbtype.startswith(prefix):
            return sql
    sys.stderr.write("Unsupported DBTYPE %s\n" % (dbtype,))
    return "text"


def tables(conn, schema="dict", only=None):
    """{table: [(column, dbtype), ...]}, both in dictionary order.

    `only` restricts it to the named tables (chemcomps loads a subset).
    """

    sql = "select tagcategory, tagfield, dbtype from %s.adit_item_tbl" % (schema,)
    args = None
    if only is not None:
        sql += " where tagcategory = any(%s)"
        args = (list(only),)
    sql += " order by dictionaryseq"

    rc = {}
    with conn.cursor() as curs:
        curs.execute(sql, args)
        for (table, column, dbtype) in curs:
            rc.setdefault(table, []).append((column, dbtype))
    return rc


def saveframe_categories(conn, schema="dict"):
    """{table: saveframe category} -- the `originalcategory` of its tags."""

    with conn.cursor() as curs:
        curs.execute("select tagcategory, min(originalcategory) from %s.adit_item_tbl"
                     " group by tagcategory" % (schema,))
        return dict(curs.fetchall())


def pointer_tags(conn, schema="dict"):
    """{(table, column), ...} for every tag the dictionary calls a saveframe pointer.

    These are the values written `$framecode`; the `$` is not part of the value
    and is stripped on load.  322 tags carry the flag.
    """

    with conn.cursor() as curs:
        curs.execute("select tagcategory, tagfield from %s.adit_item_tbl"
                     " where sfpointerflg = 'Y'" % (schema,))
        return set(curs.fetchall())


def entryid_columns(conn, schema="dict", only=None):
    """[(table, column), ...] for every tag flagged as the entry ID."""

    sql = "select tagcategory, tagfield from %s.adit_item_tbl where entryidflg = 'Y'" % (schema,)
    args = None
    if only is not None:
        sql += " and tagcategory = any(%s)"
        args = (list(only),)
    sql += " order by dictionaryseq"

    with conn.cursor() as curs:
        curs.execute(sql, args)
        return curs.fetchall()


# The saveframe index.  Not a dictionary table -- starobj created it alongside
# the generated ones and the loader assigns Sf_ID from it, so it has to exist
# wherever entry tables do.
SAVEFRAMES = "entry_saveframes"
SAVEFRAMES_DDL = "(category text,entryid text,sfid integer primary key,name text,line integer)"


def create_tables(conn, target, dict_schema="dict", use_types=True, only=None, verbose=False):
    """Create one table per tag category in `target`, plus entry_saveframes.

    The caller is responsible for the schema itself existing and for the
    transaction; nothing here commits.
    """

    defs = tables(conn, dict_schema, only)
    created = 0

    with conn.cursor() as curs:
        for (table, columns) in defs.items():
            if len(columns) < 1:
                sys.stderr.write("No columns in %s\n" % (table,))
                continue
            cols = ",".join('%s %s' % (db.quote(c), sqltype(t, use_types),)
                            for (c, t) in columns)
            stmt = "create table %s (%s)" % (db.qualified(target, table), cols,)
            if verbose:
                sys.stdout.write("%s\n" % (stmt,))
            curs.execute(stmt)
            created += 1

        curs.execute("create table %s %s"
                     % (db.qualified(target, SAVEFRAMES), SAVEFRAMES_DDL,))

    return created

#
# eof
