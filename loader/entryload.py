#!/usr/bin/env python3
#
# Parse an NMR-STAR entry and insert it into its relational tables.
#
# Replaces starobj's StarParser/NMRSTAREntry/DbWrapper load path (and with it
# the `sas` parser).  Two differences that matter:
#
#  * pynmrstar parses the whole entry into memory first, in a C extension --
#    about 12x faster than the sas/ply lexer it replaces;
#  * rows are batched.  starobj issued one INSERT per row, which for the
#    macromolecule archive is ~2,300 statements per entry; here every row of a
#    table that shares a column set goes in one execute_values call.
#
# Sf_ID assignment is unchanged and is the part to be careful with: each
# saveframe takes the next value from max(sfid) in entry_saveframes -- so IDs
# run across the whole archive, not per entry -- and every row of that
# saveframe that does not carry its own Sf_ID inherits it.
#

import re
import sys

import pynmrstar
from psycopg2.extras import execute_values

from loader import db, starschema

# what starobj read entry files as: a single-byte codec that decodes any byte
# sequence, so a stray latin-1 byte cannot cost us the whole file
ENCODING = "iso8859-15"

# rows per execute_values call
PAGESIZE = 500

_SPACE = re.compile(r"\s")


def _value(value, pointer=False):
    """Normalize one STAR value, as starobj's InsertStatement did.

    Blank, `?` and `.` are the STAR null markers and become SQL NULL.

    On a saveframe pointer the leading `$` is stripped -- `$sample_1` is stored
    as `sample_1`, which is what joins to Saveframe.Name.  Which tags are
    pointers comes from the dictionary (`sfpointerflg`), not from the shape of
    the value: sas stripped `$` lexically, off any bare `\\$\\S+` token, so it
    also stripped it from tags that are not pointers at all.  Across both
    archives that is one tag, `Entity_assembly.Entity_assembly_name`, where 203
    entries carry a framecode in a free-text name field by mistake -- those now
    keep the `$` they were deposited with.  See DATA_REMEDIATION.md.

    The no-whitespace test remains because pynmrstar does not report whether a
    value was quoted, and a quoted value is not a framecode token.
    """

    if value is None:
        return None
    val = str(value).strip()
    if val == "" or val == "?" or val == ".":
        return None
    if pointer and val.startswith("$") and not _SPACE.search(val):
        return val.lstrip("$")
    return val


def _category(prefix):
    """`_Assembly` (or `_Assembly.Name`) -> `Assembly`."""

    return prefix.lstrip("_").split(".")[0]


class EntryLoader(object):
    """Loads entries into one schema, against one connection."""

    def __init__(self, conn, schema, dict_schema="dict", verbose=False):
        self._conn = conn
        self._schema = schema
        self._verbose = bool(verbose)
        # tagcategory -> saveframe category, for the entry_saveframes index
        self._categories = starschema.saveframe_categories(conn, dict_schema)
        # (table, column) of every tag whose value is a `$framecode`
        self._pointers = starschema.pointer_tags(conn, dict_schema)
        # loop tag lists repeat across entries, so the per-column pointer mask
        # is worth caching: (table, tags) -> tuple of bools
        self._masks = {}

    def _mask(self, table, tags):
        key = (table, tags)
        mask = self._masks.get(key)
        if mask is None:
            mask = tuple((table, t) in self._pointers for t in tags)
            self._masks[key] = mask
        return mask

    #
    # the schema
    #
    def create_tables(self, dict_schema="dict", use_types=True, only=None):
        return starschema.create_tables(self._conn, self._schema, dict_schema,
                                        use_types=use_types, only=only,
                                        verbose=self._verbose)

    def last_sfid(self):
        """Highest Sf_ID in use. Saveframe IDs continue from here."""

        with self._conn.cursor() as curs:
            curs.execute("select coalesce(max(sfid), 0) from %s"
                         % (db.qualified(self._schema, starschema.SAVEFRAMES),))
            return max(0, curs.fetchone()[0])

    #
    # the load
    #
    def load_file(self, filename):
        """Parse and insert one entry. Returns the number of rows inserted.

        Raises on anything that would leave the entry half-loaded; the caller
        owns the transaction and rolls back.
        """

        with open(filename, "r", encoding=ENCODING) as f:
            entry = pynmrstar.Entry.from_string(f.read())

        return self.load_entry(entry)

    def load_entry(self, entry):

        sfid = self.last_sfid()
        saveframes = []
        # (table, column tuple) -> list of value tuples
        batches = {}

        def add(table, row):
            cols = tuple(sorted(row.keys()))
            batches.setdefault((table, cols), []).append(tuple(row[c] for c in cols))

        for frame in entry.frame_list:
            sfid += 1

            # the saveframe's own tags are its "free table" row
            free_table = _category(frame.tag_prefix)
            row = dict((tag, _value(val, (free_table, tag) in self._pointers))
                       for (tag, val) in frame.tags)
            if row.get("Sf_ID") is None:
                row["Sf_ID"] = sfid
            add(free_table, row)

            category = self._categories.get(free_table)
            if category is None:
                raise Exception("No saveframe category for free table %s" % (free_table,))
            # `line` is deliberately NULL: pynmrstar does not report source line
            # numbers, and the ones sas reported were wrong anyway (off by one
            # per preceding saveframe).  `entryid` was always NULL here too.
            saveframes.append((category, None, sfid, frame.name, None))

            for loop in frame.loops:
                table = _category(loop.category)
                cols = tuple(loop.tags)
                mask = self._mask(table, cols)
                has_sfid = "Sf_ID" in cols
                for data in loop.data:
                    row = dict(zip(cols, (_value(v, p) for (v, p) in zip(data, mask))))
                    if not has_sfid or row.get("Sf_ID") is None:
                        row["Sf_ID"] = sfid
                    add(table, row)

        return self._insert(saveframes, batches)

    def _insert(self, saveframes, batches):

        count = 0
        with self._conn.cursor() as curs:

            if len(saveframes) > 0:
                execute_values(
                    curs,
                    "insert into %s (category,entryid,sfid,name,line) values %%s"
                    % (db.qualified(self._schema, starschema.SAVEFRAMES),),
                    saveframes, page_size=PAGESIZE)
                count += len(saveframes)

            for ((table, cols), rows) in batches.items():
                stmt = "insert into %s (%s) values %%s" \
                    % (db.qualified(self._schema, table),
                       ",".join(db.quote(c) for c in cols),)
                if self._verbose:
                    sys.stdout.write("%s : %d rows\n" % (stmt, len(rows),))
                execute_values(curs, stmt, rows, page_size=PAGESIZE)
                count += len(rows)

        return count

#
# eof
