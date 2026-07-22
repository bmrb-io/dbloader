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
#    table that shares a column set goes to the server in one call.
#
# Sf_ID assignment is unchanged and is the part to be careful with: each
# saveframe takes the next value from max(sfid) in entry_saveframes -- so IDs
# run across the whole archive, not per entry -- and every row of that
# saveframe that does not carry its own Sf_ID inherits it.
#
# Batching alone left the time split about evenly between the server round trip
# and building the rows in Python (300 macromolecule entries, 52 MB, 496k rows,
# 9.6M values, cProfile).  Both halves are dealt with here -- `_write` sends
# batches with COPY instead of INSERT, and `_plan` turns a loop's tag list into
# a row layout once instead of building a dict per row -- which took that load
# from 16.3 s to 4.8 s.  The rows produced are unchanged, byte for byte.
#

import io
import re
import sys

import pynmrstar

from loader import db, starschema

# what starobj read entry files as: a single-byte codec that decodes any byte
# sequence, so a stray latin-1 byte cannot cost us the whole file
ENCODING = "iso8859-15"

_SPACE = re.compile(r"\s")

# STAR null markers, as `_value` reads them
_NULLS = frozenset(("", "?", "."))

# COPY text format: backslash escapes, `\N` for NULL.  Values needing any of it
# are rare, and testing for them is much cheaper than translating every value.
_ESCAPES = str.maketrans({"\\": "\\\\", "\n": "\\n", "\r": "\\r", "\t": "\\t"})


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
    if val in _NULLS:
        return None
    if pointer and val.startswith("$") and not _SPACE.search(val):
        return val.lstrip("$")
    return val


def _category(prefix):
    """`_Assembly` (or `_Assembly.Name`) -> `Assembly`."""

    return prefix.lstrip("_").split(".")[0]


def _line(row):
    """One row as a COPY text-format line."""

    out = []
    for v in row:
        if type(v) is str:
            # the four characters COPY reads as syntax; almost no value has one
            if "\\" in v or "\t" in v or "\n" in v or "\r" in v:
                v = v.translate(_ESCAPES)
            out.append(v)
        elif v is None:
            out.append("\\N")
        else:
            out.append(str(v))
    return "\t".join(out) + "\n"


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
        # loop tag lists repeat across entries, so the work of turning one into
        # a row layout is worth caching: (table, tags) -> plan, see _plan
        self._plans = {}
        # next Sf_ID, or None before the first entry.  Read from the database
        # once (see last_sfid) and counted forward from there.
        self._sfid = None

    def _plan(self, table, tags):
        """How to turn one loop row into one insert row.

        Returns (columns, sources, pointers, sfid_at).  `columns` is the column
        list the batch is keyed and inserted by, sorted the way the old
        dict-per-row build sorted it, and always including Sf_ID; `sfid_at` is
        where Sf_ID lands in it.  `sources` is the index into the loop's data
        for each of those columns, or -1 for an Sf_ID the loop does not carry.
        `pointers` is None when no column in the loop is a saveframe pointer,
        which is the common case and the one with a fast path in load_entry.

        A tag repeated in one loop keeps its *last* column, which is what
        building the row as a dict did.
        """

        key = (table, tags)
        plan = self._plans.get(key)
        if plan is None:
            index = dict((t, i) for (i, t) in enumerate(tags))
            index.setdefault("Sf_ID", -1)
            columns = tuple(sorted(index))
            sources = tuple(index[c] for c in columns)
            pointers = tuple((table, c) in self._pointers for c in columns)
            plan = (columns, sources, (pointers if any(pointers) else None),
                    columns.index("Sf_ID"))
            self._plans[key] = plan
        return plan

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

    def resync(self):
        """Forget the counted Sf_ID and re-read it from the database.

        The counter advances only on an entry that inserted, so a failed entry
        leaves it where it was and the next entry reuses its IDs -- exactly
        what re-reading max(sfid) per entry used to do.  Call this anyway after
        rolling back, so that the numbering is right even if what failed was
        the commit rather than the load.
        """

        self._sfid = None

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

        if self._sfid is None:
            self._sfid = self.last_sfid()
        sfid = self._sfid
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
                (columns, sources, pointers, sfid_at) = \
                    self._plan(table, tuple(loop.tags))
                batch = batches.setdefault((table, columns), [])

                if pointers is None:
                    # No saveframe pointer in this loop -- so `_value` reduces
                    # to strip-and-null-check, and it is worth inlining: this
                    # runs once per value, ~9.6M times per 300 entries.  The
                    # values a parsed loop holds are always str; anything else
                    # (a Loop built by hand, say) raises and takes the general
                    # path, after backing the half-built rows out of the batch.
                    start = len(batch)
                    try:
                        for data in loop.data:
                            row = [sfid if i < 0
                                   else (None if (v := data[i].strip()) in _NULLS else v)
                                   for i in sources]
                            if row[sfid_at] is None:
                                row[sfid_at] = sfid
                            batch.append(tuple(row))
                        continue
                    except AttributeError:
                        del batch[start:]
                        pointers = (False,) * len(sources)

                for data in loop.data:
                    row = [sfid if i < 0 else _value(data[i], p)
                           for (i, p) in zip(sources, pointers)]
                    if row[sfid_at] is None:
                        row[sfid_at] = sfid
                    batch.append(tuple(row))

        count = self._insert(saveframes, batches)
        # only now: a failed insert must leave the numbering where it was
        self._sfid = sfid
        return count

    def _insert(self, saveframes, batches):

        count = 0
        with self._conn.cursor() as curs:

            if len(saveframes) > 0:
                self._write(curs, db.qualified(self._schema, starschema.SAVEFRAMES),
                            "category,entryid,sfid,name,line", saveframes)
                count += len(saveframes)

            for ((table, cols), rows) in batches.items():
                self._write(curs, db.qualified(self._schema, table),
                            ",".join(db.quote(c) for c in cols), rows)
                count += len(rows)

        return count

    def _write(self, curs, table, collist, rows):
        """One batch into one table, with `COPY ... FROM STDIN`.

        COPY rather than a batched INSERT because it skips per-row `mogrify` on
        the client and statement parsing on the server, which together were
        most of what the inserts cost.  It wins at *every* batch size here --
        measured against `execute_values`, including on the 2-row batches that
        are the common case -- so there is no size threshold and no second
        path.  `COPY FROM STDIN` is client-side and needs no superuser.
        """

        if self._verbose:
            sys.stdout.write("%s (%s) : %d rows\n" % (table, collist, len(rows),))

        curs.copy_expert("copy %s (%s) from stdin" % (table, collist,),
                         io.StringIO("".join([_line(r) for r in rows])))

#
# eof
