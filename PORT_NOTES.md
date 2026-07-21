# dbloader Python 3 port — what changed and why

> **Part two: `starobj` and `sas` are gone.** The port first moved to
> Python 3 keeping `starobj` (§0.4 of the plan). That decision was then
> reversed: entry loading is now `pynmrstar` + `psycopg2` directly, and neither
> BMRB library is a runtime dependency any more. See
> [The starobj removal](#the-starobj-removal) at the end — everything above it
> describes the port and still holds.

Companion to [`MODERNIZATION_PLAN.md`](MODERNIZATION_PLAN.md) (the brief) and
[`STAROBJ_PY3_REVIEW.md`](STAROBJ_PY3_REVIEW.md) (the starobj defects this port
depends on being fixed). This is the record of the port itself: the shape of
the result, every behaviour change, and exactly what has and has not been
proven against the golden.

## What the port is

Python 2.7 + `pgdb` (PyGreSQL) → Python 3 + `psycopg2`, keeping `starobj` and
moving it to its Python 3 branch. No functional redesign: the stages, the
config file, the command line and the resulting database are the same.

```
__main__.py            switchboard, argparse
loader/
  db.py         NEW    the only module that talks to PostgreSQL
  csvio.py      NEW    csvdump.py + csvdump_better.py + csvload.py, merged
  dictionary.py        load dict schema
  entries.py           entry discovery + load (via starobj)
  chemcomps.py         ccdb dump-and-load (via starobj)
  macromol.py          post-load fixups
  webextras.py         web schema + CS stats
  metabolomicsextras.py  meta schema
  ets.py               ETS tracking-DB readers
fastalib.py            standalone FASTA library generator
```

Deleted: `loader/csvdump.py`, `loader/csvdump_better.py`, `loader/csvload.py`
(merged into `csvio.py`) and `pacsy_schema.sql` (byte-identical to
`nmr-star-dictionary-scripts/conf/sql_pacsy_schema.sql` and referenced by
neither repo).

> `load_postgres_db.py` was deleted too, on the grounds that nothing in the
> package imports it. That was wrong: **three HTCondor jobs in `updater_dag`
> invoke it directly** (160, 260, 410) — it is the second half of the pipeline,
> the step that loads the dump into the serving database. It has been restored
> and ported; see "Where this runs in production" in `CLAUDE.md`. "Imported by
> nothing" is not the same as "called by nothing" when the callers live in
> another repo.

> **This branch vs `origin/python3`.** `MODERNIZATION_PLAN.md` §0.1 says "No
> dbloader code has been ported." That is true of `master` but not of the repo:
> `origin/python3` is a mechanical py2→py3 + pgdb→psycopg2 port forked from the
> same commit, and it is what runs in production as
> `/projects/BMRB/software/dbloader3`. Both efforts start from 41293e9, so the
> golden this branch is verified against is the right baseline either way.
> Reviewed commit by commit, everything on `origin/python3` is superseded here
> except one deliberate behaviour change — "No more metabolomics db"
> (`0d68fd1`) — which has been carried across. Its py3 fixes to
> `load_postgres_db.py` (`out.decode()`, `open(..., "r")`) are the ones
> predicted above; the repo was not left behind after all, only `master` was.

Requires: `psycopg2`, `starobj` (Python 3, branch `dbloader-py3-fixes`, which
pulls in `sas` + `ply`), and a `psql`/`pg_dump` client.

## Verified against the golden

`tests/regression.sh` loads the same inputs with the rewritten code and diffs
every table of every schema, ordered by every column, against the golden built
by the unmodified Python 2 loader (`tests/golden_*.sh`). See `tests/README.md`.

| what | scale | result |
|---|---|---|
| `dict` schema | 16 tables | identical |
| `macromolecules` schema | 465 tables | identical |
| `metabolomics` schema | 465 tables | identical |
| same, via the **new truncate path** | 930 tables | identical |
| `bmrbeverything` CSV dump | 247 files | identical |
| "old-style" macromolecule dump | 159 files | identical |
| "old-style" metabolomics dump | 105 files | identical |

Corpus: 300 macromolecule + 300 metabolomics entries, a stride sample across
both archives (`tests/subset.*.txt`), NMR-STAR v3.2.14.0 built from
`internal_106_source`. 0 entries failed to load on either side.

The dumps are compared by pointing the old dumper and the new one at the same
database (`sh tests/regression.sh dumps`); the only difference in any of the
three layouts is the random `\restrict` token pg_dump stamps into `schema.sql`.

That covers the `dict` stage, both entry stages, both schema-preparation
paths, the connection layer, the CSV loader, the three dump layouts, and
starobj's own Python 3 conversion.

**Not covered, because the external systems are not reachable from here**
(deferred by design, MODERNIZATION_PLAN.md §6): `chemcomps.py` needs the ccdb
database on `octopus`; `ets.py`, and with it the three ETS-fed tables in
`webextras.py`, needs the tracking database on `ets.bmrb.wisc.edu`. The rest of
the web schema, the `meta` schema and `macromol.fixup` are exercised (they run
clean and load the expected row counts) but not diffed against a golden.
Anything below marked **unverified** falls in that gap and should be
re-checked on a host that has those systems.

## Behaviour changes

Deliberate, in the order they matter.

1. **`entries.py`: the truncate path exists now.** The non-`--drop-tables`
   branch was `raise Exception( "FIXME!!!! Not implemented" )`, so the
   documented default aborted every run and only `--drop-tables` worked. It now
   truncates every table in the schema in one statement, and falls back to
   creating them if the schema is empty. Both paths leave `entry_saveframes`
   empty, and starobj takes the next `Sf_ID` from `max(sfid)` there, so both
   number from 1 — the truncate path produces the same database as the drop
   path against an unchanged dictionary. Dropping is still the right choice
   when the dictionary has changed, since only a re-create picks up added or
   removed tables.

2. **`entries.py`: failures are counted instead of swallowed.** The per-file
   handler was a bare `except:` that logged a traceback and continued — which
   also caught `KeyboardInterrupt`, and left a run that loaded nothing looking
   successful. It now catches `Exception`, collects the failed files, and
   returns them; `__main__` prints them and exits non-zero.

3. **`ets.py`: `processing_queue_itr` no longer writes the string `"None"`.**
   For an on-hold entry with a NULL `onhold_status`, the old code set
   `rel = None` and then fell through a second, non-`elif` `if` into
   `rel = str( row[3] ).strip()` — i.e. the four characters `None` — and
   inserted that into `web.procque.status`. Now such rows get a real NULL.
   **Unverified** (needs ETS).

4. **`macromol.py`: `fix_software_authors` updates every matching row.** It
   collected `Sf_ID`s into a dict keyed by `Entry_ID`, so when one entry had
   two `Software` saveframes with the same name, only the last one's `Vendor`
   row was normalized and the other kept the raw value. It now updates all of
   them. **Unverified** — the golden does not include the fixup stage.

5. **`chemcomps.py`: CSVs for unknown tables are skipped, not loaded.** `load()`
   warned `"%s.csv not in tables, skipping"` and then loaded the file anyway.
   No table had been created for it (`create_tables` is called with
   `tables = TABLES`), so the `\copy` could only fail. **Unverified** (needs ccdb).

6. **`chemcomps.py`: `fix_entry_id` no longer re-runs a stale statement.** The
   `if/elif` chain assigning `sql` had no final `else`; a table outside every
   listed group would have re-executed the previous table's `UPDATE` against
   itself. In practice `iter_tags` only yields tables in `TABLES`, so the chain
   was exhaustive — the last branch is now `else` so it stays that way.
   **Unverified** (needs ccdb).

7. **`psql` and `pg_dump` are configurable.** They were hard-coded to
   `/bin/psql` and `/bin/pg_dump`, with the real production paths
   (`/usr/pgsql-10/bin/...`) commented out just above. Now: `[tools] psql` /
   `[tools] pg_dump` in the properties file, else `$PSQL` / `$PG_DUMP`, else
   `PATH`.

8. **`starobj`'s location is no longer hard-coded.** `loader/__init__.py` did
   `sys.path.append( "/projects/BMRB/software/starobj" )` unconditionally, so
   the package could not be imported anywhere else. That path is now the
   fallback, after `$STAROBJ_PATH` and whatever is already importable.

9. **Host and port are no longer packed into one string.** `dsn()` built a
   pgdb-style `"host:port"` that every caller then re-split — and
   `csvload._fromcsv` did not, passing `-h host:port` to `psql`, which cannot
   work. `db.dsn()` returns psycopg2 keyword arguments with `host` and `port`
   separate, and every consumer takes them as such.

10. **Errors from `psql -f` are surfaced.** `psql -f` keeps going after a
    failed statement and still exits 0, so `runscript()`'s return value was
    close to meaningless and the DDL's stderr was discarded. `db.run_command()`
    now prints any `ERROR`/`FATAL`/`PANIC`/`WARNING` lines even on success
    (`NOTICE`s, which every drop-then-create script produces by the dozen, are
    filtered out).

11. **`__main__.py`: `--no-web` is honoured and verbosity is not forced.**
    `load_web_schema` was called with a hard-coded `verbose = True`. The `--no-*`
    flags are now generated from one list of stages rather than repeated five
    times, and the required-argument combinations (`--dictdir` when loading the
    dictionary, `-d` when dumping) are checked up front instead of raising an
    `AttributeError` half way through a run.

12. **`fastalib.py`: the checksums work now.** The comment said md5s came out
    wrong "no matter how many flush()es and os.fsync()s I add", and the code was
    left commented out. The cause was that each writer coroutine holds its
    `with open(...)` across the `yield`, so its output file is still open — and
    partly unwritten — when the checksum is taken. Closing the coroutines first
    fixes it, and the `.md5` files are written again.

13. **Iteration order is now deterministic** where it was arbitrary: files are
    globbed in sorted order and dict iteration in `macromol.py`/`csvio.py` is
    sorted. No effect on the loaded rows, but two runs now produce identical
    logs.

14. **`cs_stats.sql`: one output path was missing `ftp/`.** Six of the seven
    statistics files are written to
    `/projects/BMRB/public/ftp/pub/bmrb/statistics/chem_shifts/`; `dna_filt.csv`
    went to `/projects/BMRB/public/pub/bmrb/...`. Item 10 above is what made it
    visible — under the old code that `\copy` failed silently on every run, so
    `dna_filt.csv` has presumably been missing from the FTP site all along.
    **Unverified**: whether the wrong directory exists on production is worth a
    look before the next run.

15. **A misconfigured entry load fails instead of doing nothing.** Missing
    config sections, a missing `entrydir`, or an `entrydir` with no entries in
    it made `load_entries` write a line to stderr and return `False`, which
    nobody checked — so the run carried on and reported success having loaded
    an empty schema. Those are exceptions now.

16. **Connections are closed.** `list_tables`/`tocsv` opened one connection per
    table and relied on `with conn:`, which in both pgdb and psycopg2 ends the
    transaction but leaves the connection open — a couple of hundred of them
    per dump. `db.connection()` is a context manager that actually closes, and
    the starobj wrappers are closed in a `finally`.

17. **Packaging and the condor jobs run Python 3.** `packaging/setup.py` is
    version 2.0, declares `psycopg2`, and no longer needs the sources edited
    before building (its instruction to comment out every `sys.path.append`
    first is obsolete — those lines are harmless inside an egg). The submit
    files call `/usr/bin/python3` and a stable `dbloader.egg` symlink rather
    than the literal `dbloader-1.0-py2.7.egg`, since setuptools names the egg
    after the interpreter it was built with.

## Things left alone on purpose

- **`float → varchar(63)`** in starobj's type mapping. It looks wrong and it is
  deliberate: it preserves trailing zeros as deposited.
- **Macromolecules load as all-`text`, metabolomics as typed.** Reproduced
  exactly (`use_types`).
- **Shelling out to `psql` for `COPY`.** Server-side `COPY` needs superuser and
  reads files on the server; `\copy` needs neither. `psycopg2.copy_expert`
  would work, but changing it is a separate decision from this port.
- **`entries.py` cross-checks the file list against ETS.** Loud, and it does not
  stop the load, but it is how a withdrawn entry left on the website gets
  noticed.

## The production pipeline

`CLAUDE.md` now documents it, because misreading it is what caused the
`load_postgres_db.py` mistake above. Two findings from reading `updater_dag`
that are worth acting on independently of this port:

1. **The committed `load_postgres_db.py` could not run under Python 3 at all** —
   `open(infile, "rU")` raises `ValueError` on 3.11+, and `add_ro_grants` did
   `bytes.split(",")`, a `TypeError`. The DAG has been running it with
   `/usr/bin/python3` since the `dbloader3` deployment, so the deployed copy
   must have been patched in place and the repo left behind. Worth diffing
   `/projects/BMRB/software/dbloader3/` against this branch before deploying.
   The restored version fixes both and round-trips losslessly (dump → load →
   dump leaves 463 macromolecule tables identical to the golden).

2. **The publish step was not atomic, and the commented-out attempt at one
   would not have helped.** Fixed; recorded here because the diagnosis is the
   interesting part. Each table was `truncate table only` + `\copy` with no
   transaction, so the truncate committed first: a failed copy left that table
   empty on the live server, and readers could see a partly reloaded database
   throughout.

   `fromcsv` carried a commented-out `-c begin ... -c truncate ... -c \copy
   ... -c commit`. Multiple `-c` options *do* share one session and one
   transaction -- but psql does not stop on error by default, so after the
   `\copy` failed it carried on to the `-c commit` and committed the truncate
   anyway. Measured, on the same failing copy:

   | form | table after a failed copy |
   |---|---|
   | no transaction (what was running) | **empty** |
   | `-c begin ... -c commit`, as commented out | **empty** |
   | same, plus `-v ON_ERROR_STOP=1` | **intact** |

   So the wrapper as written bought nothing, which is the likeliest reason it
   was abandoned. Locking was not the reason for *that* snippet -- it is
   per-table, so `TRUNCATE`'s `ACCESS EXCLUSIVE` would be held only for one
   table's copy. It would be the reason to avoid wrapping the *whole* load: a
   single transaction over ~250 tables holds `ACCESS EXCLUSIVE` on all of them
   until it commits.

   Both paths are now fixed:

   - `ON_ERROR_STOP=1` plus the per-table `begin`/`commit`, so a failed copy
     leaves the old rows in place.
   - **`--shadow`** (item 3) for the atomic version.

3. **The reload is a schema swap now, and rebuilds the schema from the
   dictionary.** Instead of truncating the live tables, build `<schema>_new`
   from the dump's own `schema.sql`, load into that, and swap with
   `ALTER SCHEMA ... RENAME` in one transaction. **This is the default**, with
   no flag: a dump that cannot be swapped falls back to loading in place and
   says why in the log.

   - **Atomic.** The swap transaction touches no rows: measured at **~1 ms**
     per schema, against a reload that takes minutes. Readers see the old
     database or the new one, never half of each.
   - **Safe to fail.** If any table fails to load, nothing is swapped and the
     live database is untouched -- verified.
   - **No gap in privileges.** Grants are applied to the shadow schemas
     *before* the swap; they follow the objects through the rename.
   - **It is the dictionary-driven schema rebuild.** `schema.sql` is dumped by
     `csvio.dump_ddl()` from the build database, whose schema dbloader
     generated from the dictionary (`loader/starschema.py`). Creating the
     shadow schema from it means a dictionary change reaches the serving
     database on the next reload instead of having to be applied by hand.

   What made this cheap: the dumped DDL is 974 tables, 14 sequences, 9 views
   and 57 primary keys, with **no foreign keys, materialized views, functions
   or triggers**, and no view crossing a schema. And PostgreSQL tracks view
   dependencies by OID, so renaming a schema does not disturb them.

   A swap must only bring in schemas the load actually filled. `-s dict`
   therefore swaps `dict` alone and throws the other shadow schemas away —
   in the first, opt-in cut of this it swapped all six, **taking the other
   five live empty**, which is the bug that made auto-detection worth having.

   `can_shadow()` declines in two cases now: a dump with no `schema.sql`, and
   one whose CSVs are not schema-qualified. The second is the old-style
   layouts, whose entry tables live in the search_path — swapping those would
   mean renaming `public` and taking anything else in it out of the live
   database. **That is the only remaining reason the in-place path exists**;
   when the old-style serving databases are retired it can be deleted and the
   swap becomes the only path.

   The DDL is rewritten textually: `<schema>.` becomes `<schema>_new.`
   everywhere including inside view bodies, and the `-c` clean section is
   dropped by position (everything between the `SET` header and the first
   `CREATE`) rather than by enumerating `DROP ...`,
   `ALTER TABLE ... DROP CONSTRAINT` and
   `ALTER TABLE ... ALTER COLUMN ... DROP DEFAULT`. That holds for this dump
   because it has no function bodies and no string literal containing a
   schema-qualified name; adding a stored function would break the assumption.

   `tests/shadow_swap.sh` covers all of it: first load, reload over a live
   database, a failed shadow load, a failed truncate load, a good truncate
   load, and the three fallback decisions.

   No change to `updater_dag` is needed -- job 410 gets the swap because it is
   the default, and job 260 (old-style `bmrb`) keeps loading in place because
   its dump cannot be swapped.

## Follow-on work

- Run the regression on a host with ccdb and ETS to close items 3–6 and 14.
- A golden for `macromol.fixup` — the one stage that runs here but is not
  diffed. It needs the fixup added to `tests/load_entries.py` and the entry
  goldens rebuilt.
- The production `loader.properties` still carries live hosts,
  `/share/dmaziuk/...` paths and a plaintext ETS password;
  `loader.example.properties` is the annotated version to deploy from.
- ~~`starobj`'s `dbloader-py3-fixes` branch~~ — moot: starobj is gone.
- ~~`execute_values` for the entry insert path~~ — done, see below.

---

# The starobj removal

The port above kept `starobj` (plan §0.4). That was then reversed: entry
loading is `pynmrstar` + `psycopg2` directly, and `starobj` and `sas` are no
longer dependencies of anything dbloader runs. They are still needed by
`tests/golden_*.sh`, which runs the *legacy Python 2 loader* to produce the
comparison golden — that is the only place they appear now.

## What replaced what

| starobj | replacement |
|---|---|
| `StarDictionary.iter_tables` / `iter_tags` / `get_saveframe_category` | `loader/starschema.py` — queries over `dict.adit_item_tbl` |
| `NMRSTAREntry.create_tables` (the type mapping) | `starschema.create_tables` / `starschema.sqltype` |
| `NMRSTAREntry.last_sfid` / `insert_saveframe` | `EntryLoader.last_sfid` + the `entry_saveframes` batch |
| `StarParser` (SAX over `sas`) + `DbWrapper.InsertStatement` | `loader/entryload.py` — `pynmrstar.Entry` walked, rows batched |
| `DbWrapper` (connections, the `[entry]` config section) | `loader/db.py`, which was already there |

The `[entry]` section the loader used to synthesize for starobj is gone, and so
is the `engine` option — there is one driver now and it is not configurable.

`float → varchar(63)`, `use_types` (macromolecules all-text, metabolomics
typed), the `entry_saveframes` DDL, `Sf_ID` inheritance and the `?`/`.`/blank
null handling are all reproduced exactly; `loader/starschema.py` cites the
starobj source each rule came from.

## Speed

Same driver, same corpus, same database, back to back — the 300-entry
macromolecule subset (52 MB) and the 300-entry metabolomics subset (13.7 MB):

| | macromolecules | metabolomics |
|---|---|---|
| Python 2 + starobj + pgdb | 110 s | 19 s |
| Python 3 + starobj + psycopg2 | 79 s | 15 s |
| Python 3 + pynmrstar, batched | **15 s** | **3.2 s** |
| | **5.3x** faster | **4.6x** faster |

Two things were slow, and only one of them was the parser:

- **Parsing.** `sas`/`ply` manages 2.6 MB/s; pynmrstar's C extension manages
  32 MB/s, about **12x**. But parsing was only ~17% of the old load, so on its
  own that is worth ~1.2x.
- **Inserts.** starobj issued one `INSERT` per row — 115,579 statements for 50
  macromolecule entries, ~2,300 per entry — and a profile put
  `cursor.execute` at the top by a wide margin. `entryload.py` groups every row
  of a table that shares a column set and sends it in one `execute_values`.
  That is where the rest of the factor comes from.

Extrapolating to the full archives (14,772 macromolecule + 3,629 metabolomics
entries), the entry load goes from about **68 minutes to about 13** — or from
90 minutes, if you start from where this began, Python 2.

## The three differences from the golden

928 of 930 tables are still byte-identical. The exceptions are
`entry_saveframes` (two columns) and `metabolomics.Entity_assembly` (one
column, 18 rows in the test subset); `tests/regression.sh` checks both
separately — `check_saveframes` and `check_entity_assembly` — rather than by
fingerprint.

**`line` is NULL** (owner's decision). pynmrstar reports no source line
numbers. They could be recovered by scanning the file for `save_` — but the
numbers in the golden are not the true ones anyway: `sas` under-counts by one
line per preceding saveframe, exactly and reproducibly (420/420 saveframes
checked), so `save_system_insulin_A_chain`, really on line 173 of
`bmr1000_3.str`, is recorded as 171. Rather than carry a deleted lexer's
off-by-N forward or silently change the numbers, the column is now empty.

**`category` is fixed.** starobj set it in `endSaveframe` with

```sql
update entry_saveframes set category = :cat where name = :nip
```

which matches by saveframe **name across every entry loaded so far** — so two
entries with a saveframe of the same name overwrite each other's category, and
the last one loaded wins for both. In the 300-entry macromolecule golden that
is wrong for **59 of 5690** rows: 58 saveframes whose free table is
`NMR_spectrometer_list` are labelled `NMR_spectrometer`, and one `method` is
labelled `software`. Entry 4054's `save_spectrometer_list` declares
`_NMR_spectrometer_list.Sf_category NMR_spectrometer_list` in the file and the
golden contradicts it.

The new loader writes each saveframe's own category, and the regression asserts
the strong version of that: every `entry_saveframes.category` must equal the
`Sf_category` the entry itself declares, joined over all 109 free tables that
have one. New: 0 mismatches of 5690 (macromolecules) and 0 of 4838
(metabolomics). Golden: 59 and 0.

**`Entity_assembly.Entity_assembly_name` keeps its `$`.** The dictionary says
that tag is not a saveframe pointer, so the `$` is no longer stripped from it;
`sas` stripped it lexically and hid the fact that 203 metabolomics entries have
a framecode in a free-text name field. The regression asserts that every
`$`-prefixed name is exactly the row's own `Entity_label` with the `$` still
attached — i.e. that nothing but this known defect is involved.
[`DATA_REMEDIATION.md`](DATA_REMEDIATION.md) lists the 203 entries and the
one-character edit each needs.

## Risks this takes on

- **Quoted values beginning with `$`.** `sas` stripped the `$` off saveframe
  pointers in the lexer, from any bare `\$\S+` token, whatever tag it was in.
  This strips it only from tags the dictionary marks `sfpointerflg='Y'`, which
  is the narrower and better-defined rule — a value is a pointer because the
  dictionary says the *tag* is one, not because the value happens to start with
  a `$`. (The no-whitespace test is still applied on top, since pynmrstar does
  not report whether a value was quoted and a quoted value is not a framecode
  token.)

  Across all 18,401 entries the two rules disagree on exactly one tag:
  **`Entity_assembly.Entity_assembly_name`**, a `VARCHAR(127)` free-text name
  correctly flagged `'N'`, where 203 metabolomics entries carry a framecode by
  mistake. `sas` stripped those silently; they now load with the `$` as
  deposited. That is a deliberate change — the loader stopped hiding a data
  error — and it is the third documented difference from the golden, checked by
  `check_entity_assembly` in `regression.sh`. The fix belongs in the source
  files: see [`DATA_REMEDIATION.md`](DATA_REMEDIATION.md).

- **Empty loops.** starobj treated a loop with no rows as an insert error and
  failed the whole entry; `entryload.py` inserts nothing and carries on. No
  entry in either archive has one, so the corpus cannot tell them apart.
- **Encoding.** Entry files are read as `iso8859-15`, the codec starobj used,
  so no byte sequence can fail to decode. Both corpora are pure ASCII, so this
  is untested by the golden either way.
- **pynmrstar is now a hard dependency** of the entry load, and its parser is a
  C extension — a wheel per platform rather than pure Python.
