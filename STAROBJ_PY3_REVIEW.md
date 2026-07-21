# Review: starobj's Python 3 conversion

Per the decision recorded in `MODERNIZATION_PLAN.md` §2a/§9.2, dbloader **keeps
`starobj`** and moves to its Python 3 `master`. That conversion was never fully
validated, so its correctness is now part of what the dbloader golden tests must
prove. This is a read of the diff ahead of those tests.

Diff reviewed: `starobj` `89925a1..98b3436` (11 commits, 16 files,
+225/−129) — `89925a1` is the last pgdb/py2 commit and the base of the golden
stack. The conversion is small and almost entirely mechanical, which is good
news; the defects below are the exceptions.

Environment: findings marked **verified** were reproduced against
`~/git/starobj@master` + `~/git/sas@master` on Python 3.12.

> **Status: findings 1–6 and 8 are fixed** on `starobj` branch
> **`dbloader-py3-fixes`** (commit `eb5382b`, off `master`). That branch is
> deliberately *not* for merging back: other projects run against `master` as
> coded, and these are behaviour changes. Finding 7 is a dbloader-side decision,
> not a starobj bug. Each fix is annotated below.

---

## Blocker

### 1. `StarParser` no longer implements the `sas` handler interface — every parse raises `AssertionError` (verified)

`starobj/parser.py`, the entry-load path dbloader depends on:

```python
-class StarParser( starobj.BaseClass, starobj.sas.ContentHandler, starobj.sas.ErrorHandler ) :
+class StarParser:
```

but `StarParser.parse()` still hands itself to `sas` as both handlers:

```python
p = starobj.sas.SansParser.parse( lexer = l, content_handler = h, error_handler = h, ... )
```

and `sas` type-checks them — `sas/parsebase.py:36`
`assert isinstance( ch, sas.ContentHandlerBase )`, with a second pair of asserts
in `nmrstar/sansparser.py:_parse_file` (`sas.ContentHandler`, `sas.ErrorHandler`).

Reproduced:

```
File "sas/parsebase.py", line 36, in __init__
    assert isinstance( ch, sas.ContentHandlerBase )
AssertionError
```

So `StarParser.parse_file()` — the single call dbloader makes to load an entry —
fails immediately on Python 3. Running with `python -O` would skip the asserts
and *appear* to work, which is worse: it hides the real question of whether the
handler contract is still satisfied by duck typing.

`comment()` and `endData()` (called unconditionally by `_parse_file`) *are*
defined on `StarParser` itself, so restoring the base classes should be
sufficient — but the full `ContentHandlerBase` surface should be checked, not
assumed.

**FIXED.** The cause was neither MRO nor metaclass: `__init__.py` had moved
`import sas` *below* `from .parser import StarParser`, so `starobj.sas` did not
exist when the class statement was evaluated — dropping the base classes made
the import error go away. The sas import is back above the submodule imports
and the bases are restored; all six `ContentHandler` abstract methods are
implemented, so the class instantiates. This also restores the `verbose`
property from `BaseClass`, which `parse()` assigns to.

## Behaviour changes that will show up as golden diffs

### 2. All encoding validation was commented out, not ported

`parser.py` `startData` / `startSaveframe` / `data` each had a
`value.decode(starobj.ENCODING)` guard that appended a `starobj.Error` to
`errlist` for non-ASCII data block IDs, saveframe names, tags and values. All
three are now commented-out blocks. On py3 `str` has no `.decode`, so this was
"convert by deletion".

Consequences: entries that used to be *reported* now load silently, and in the
`data` case the old code did `return True` (skip the tag) on a bad tag — that
skip is gone too, so a previously-rejected tag now gets inserted. Both are
observable in a golden diff if any test entry has non-ASCII content.

**FIXED.** Re-implemented via a `_check_encoding()` helper using `.encode()`,
with the `return True` tag-skip restored in `data`.

### 3. `parse_file` opens entries as text with the platform default encoding

```python
-with open( filename, "rb" ) as inf :
+with open( filename, "r" ) as inf :
```

Text mode is right for py3, but no `encoding=` means the locale default. In this
container that's UTF-8, so any archive entry containing latin-1 bytes now raises
`UnicodeDecodeError` and the whole entry fails to load, where the py2 code
processed the bytes. Combined with finding 2 there is no longer any encoding
diagnostic at all.

**FIXED.** `parse_file` now takes an `encoding` argument defaulting to
`starobj.ENCODING` (`iso8859-15`). Being a single-byte codec it decodes any
byte sequence, so input never fails to read — matching the py2 behaviour of
feeding raw bytes to the lexer.

> **Open question for the golden diff — measured, and it turns out to be moot
> for these archives.** py2 inserted raw bytes; py3 decodes to `str` and
> psycopg2 re-encodes on insert (UTF-8 by default), so for non-ASCII values the
> bytes reaching PostgreSQL could differ even with nothing erroring. But a full
> scan of both corpora — 14,772 macromolecule + 3,629 metabolomics entries —
> found **zero files containing a single non-ASCII byte**:
>
> ```
> LC_ALL=C grep -rlP '[\x80-\xff]' --include="*.str" bmrb_entries bmrb_metabolomics
> ```
>
> ASCII round-trips identically through any of these paths, so this cannot
> produce a golden diff on the current archives, and findings 2 and 3 are
> defensive rather than actively biting. Re-run that scan if the corpus is ever
> refreshed — a single Ã© in a citation author's name brings the question back.

## Latent / lower severity

### 4. `collections.abc` used but never imported

`stardict.py` (8 sites), `entry.py` (2), `startable.py`, `unparser.py` were
changed `collections.Iterable → collections.abc.Iterable`, but the modules only
do `import collections`. `import collections` does **not** guarantee the `abc`
attribute — bare `python3.12 -c "import collections; collections.abc"` raises
`AttributeError`. It happens to work whenever anything else in the process has
already imported `collections.abc` (common, and true in my test), so this is
order-dependent and will surface as a mysterious `AttributeError` in some other
deployment.

Directly on dbloader's path: `stardict.iter_tags(columns=("dbtype",))`.

**FIXED.** `import collections.abc` in all four modules.

### 5. Duplicate `__next__` in `DataTable` — one of them is infinite recursion

`startable.py` now defines `__next__` twice: line 116
(`def __next__(self): return next(self)` — converted from the py2 forwarding
shim `return self.next()`, and now a self-call) and line 385 (the real one,
renamed from `next`). The later definition wins, so the recursive one is dead
code — by luck, not design. Any reordering turns it into a `RecursionError`.

**FIXED.** Deleted, with a comment left in its place.

### 6. `configparser.SafeConfigParser` — removed in Python 3.12

Five sites: `db.py:375`, `unparser.py:674`, `stardict.py:1025`,
`parser.py:384`, `scripts/passthru.py:84`. All but the last are `__main__`
demo blocks, so the library is unaffected — but those entry points and
`scripts/passthru.py` are broken on 3.12 (`AttributeError`). **The py3 dbloader
must not copy this pattern**: today's `loader/dictionary.py` both constructs
`SafeConfigParser` and asserts `isinstance(config, ConfigParser.SafeConfigParser)`.
Use `configparser.ConfigParser`. **FIXED** in starobj (all five sites); still
to do in dbloader when `loader/dictionary.py` is ported.

### 7. `autocommit` silently changes meaning with the driver swap

The deleted `db.py` header comment said it out loud:

```
-# note that connection.autocommit does not work in pgdb but setting it is not an error.
```

`dbloader/loader/entries.py:_load_entries` relies on exactly that attribute —
it sets `conn.autocommit = True` around the `drop schema` / `create schema` /
`create_tables` block, then back to `False` for the per-entry commits. Under
pgdb that assignment **did nothing**, so the schema rebuild ran inside the
ambient transaction; under psycopg2 it takes effect and each DDL statement
commits immediately.

The end state is probably identical for a successful run, but the failure
semantics are not: a crash midway through `create_tables` now leaves a
half-built schema behind instead of rolling back. Worth deciding explicitly
rather than inheriting by accident — and it's a place where golden (pgdb) and
test (psycopg2) can legitimately differ without either being "wrong".

### 8. `print` conversion artifact

`startable.py:334`: `print "Table is Peak_char:", self._table` became
`print(("Table is Peak_char:", self._table))` — prints a tuple. Verbose output
only. **FIXED.**

## Not a regression, but noted

`unparser.py:pretty_print_file` calls `cls.pretty_print( cls, entry, ... )` —
passing `cls` as the first positional argument to a classmethod — and references
an undefined `alltags` (the parameter is spelled `alltage`). Both predate the
py3 work and are unreachable from dbloader; flagging only so they aren't
mistaken for conversion damage later.

## What this means for the port

- Finding 1 must be fixed before **any** Phase 2 golden run — the entry loader
  cannot execute otherwise.
- Findings 2 and 3 are exactly the class of thing a row-for-row golden diff is
  designed to catch, and they argue for a broad test corpus (not 5–10 entries)
  when checking the entry loader.
- These fixes belong in `starobj`, not in dbloader. They want their own branch
  and their own regression check against py2 `starobj@89925a1` output.
