# dbloader

## What this is

The **last stage** of the pipeline for the *database* branch. It (re)builds the
BMRB **PostgreSQL** database `bmrb` from:

- the **dictionary artifacts** produced by `../nmr-star-dictionary-scripts`
  (`dictionary.sql` + `dict.*.csv`), and
- the **actual BMRB entries** (NMR-STAR files on disk) for the macromolecule and
  metabolomics archives,
- plus chem-comp, web, and metabolomics-"meta" side data.

It then **dumps** selected schemas back out to CSV. The dictionary here is not
just reference data — it *defines the relational schema* the entries are loaded
into (see "How the dictionary is interpreted" below).

See [`../ORGANIZATION.md`](../ORGANIZATION.md) for the whole pipeline.

> **Python 3, psycopg2, pynmrstar.** Ported from Python 2.7/`pgdb`, and the
> BMRB `starobj`/`sas` libraries have been removed — see
> [`PORT_NOTES.md`](PORT_NOTES.md) for what changed and what is verified,
> [`tests/README.md`](tests/README.md) for how to run the regression against
> the Python 2 golden, and [`DATA_REMEDIATION.md`](DATA_REMEDIATION.md) for
> defects in the deposited entries that the old loader used to hide. Bulk CSV
> in and out shells out to `psql`/`pg_dump` (server-side `copy` needs
> superuser; `psql \copy` does not) — while the entry loader runs its own
> `COPY ... FROM STDIN` in process through psycopg2's `copy_expert`, which is
> client-side and needs no superuser either. That is what makes the entry load
> fast; bigger *insert* batches are not — see `PORT_NOTES.md` §Speed before
> reaching for them.

## How to run it

```bash
python __main__.py -c loader.properties --dictdir <dir-with-dictionary.sql+dict.*.csv> -d <outdir>
```

`__main__.py` is a big switchboard of `--no-*` flags; by default it loads
**everything** and dumps `bmrbeverything`. Key stages, each a `loader.*` call:

Every stage builds `<schema>_new` and the run ends by renaming them all into
place in one transaction (`loader/shadow.py`) — so a reload is invisible to
readers, and a failure anywhere leaves the previous contents serving. This is
not optional: the drop-and-refill and truncate-and-refill paths that used to
load the live schema directly are gone, the same cleanup
`load_postgres_db.py` did on the serving side. `--drop-tables` is still
accepted, ignored, and warns (condor jobs 120/220 still pass it).

`chemcomps` and `meta` are the exceptions — both load from sources that are
unreachable outside BMRB, so they still load in place.

| Stage | Function | What it does |
|-------|----------|--------------|
| dictionary | `load_dict` (`loader/dictionary.py`) | rewrite `dictionary.sql` to build `dict_new`/`validict_new`, run it, then `COPY` every `dict.*.csv` in `--dictdir` into `dict_new.<table>`. |
| chem comps | `load_chem_comps` (`chemcomps.py`) | dump released chem comps from the `ccdb` database, load into `chemcomps` schema. |
| metabolomics | `load_metabolomics` + `load_meta_schema` | parse metabolomics NMR-STAR entries into the `metabolomics` schema; load `meta` extras from CSV. |
| macromolecules | `load_macromolecules` + `fix_macromolecules` | parse macromolecule entries into the `macromolecules` schema, then cleanups. |
| web | `load_web_schema` (`webextras.py`) | `web` schema: chemical-shift statistics (`cs_stats.sql`) + CSV extras. |
| swap | `shadow.swap` (`loader/shadow.py`) | rename every `<schema>_new` built above into place, in one transaction; drop the retired ones afterwards, outside it. |
| dump | `dump_new` / `dump_macromolecules` / `dump_metabolomics` | write schema contents back out to CSV in `-d <outdir>`. |

Connection details (host, db, user, schema) per stage come from
`loader.properties` sections (`[dictionary]`, `[macromolecules]`,
`[metabolomics]`, `[chemcomps]`, `[web]`, `[meta]`, `[ets]`).

## How the dictionary is interpreted (the important bit)

Entry loading is dictionary-driven (`loader/starschema.py`, `loader/entryload.py`):

```python
starschema.create_tables(conn, "macromolecules")   # tables come FROM the dictionary
EntryLoader(conn, "macromolecules").load_file(f)   # pynmrstar parse + batched insert
```

`dict.adit_item_tbl` is one row per tag: the table it maps to (`tagcategory`),
the column (`tagfield`), the SQL type (`dbtype`) and the order tags are defined
in (`dictionaryseq`, which is the column order). That is the whole schema
definition — change the dictionary → change the schema. Each saveframe takes
the next `Sf_ID` from `max(sfid)` in `entry_saveframes`, and every row of that
saveframe inherits it.

## Where this runs in production

Driven by HTCondor DAGs in [`~/git/updater_dag`](../../updater_dag), deployed as
`/projects/BMRB/software/dbloader3/` (its own venv; config `uconn.properties`,
not in this repo). **The database is built in one place and served from
another**, and both halves live here:

```
        entry files (CVS/SVN, validated by updater_dag/update_check.py)
                              │
   ┌──────────────────────────▼───────────────────────────┐
   │ BUILD DB  -- __main__.py -c uconn.properties         │
   │  100 --dictdir .../nmr-star-dictionary-scripts/csv   │  dict
   │  110 --drop-tables (chemcomps only)                  │  chemcomps
   │  131 --drop-tables (metabolomics)                    │  metabolomics + meta
   │  231 --drop-tables (macromolecules)                  │  macromolecules + web
   │  401 BMRB-API reloaders (not this repo)              │  API extras
   └──────────────────────────┬───────────────────────────┘
                              │  __main__.py --no-load -d <dir>
                              ▼
        /projects/BMRB/staging/dbdump/{bmrb,metabolomics,bmrbeverything}
                    CSV per table + schema.sql          (151 / 251 / 400)
                              │
                              │  load_postgres_db.py -d <db> -g
                              ▼          (160 / 260 / 410)
   ┌──────────────────────────────────────────────────────┐
   │ SERVING DB  bmrb-staging.cam.uchc.edu                │
   │  databases: bmrb, metabolomics, bmrbeverything       │
   └──────────────────────────────────────────────────────┘
                              │  602 rsync
                              ▼
        ftp/pub/bmrb/relational_tables/nmr-star3.1/   (the public dump)
```

Three dumps, two layouts (see `loader/csvio.py`). `bmrbeverything` is
**"new-style"**: every file `<schema>.<table>.csv`. The `bmrb` and
`metabolomics` dumps are **"old-style"**: entry tables unqualified, each with
its own copy of `dict`. That layout was shaped by the separate website
databases, which are retired — but it is *also* the format published on the
FTP site, which is why it stays.

Things to know before changing any of this:

- **`load_postgres_db.py` is not dead code.** It is invoked directly by three
  DAG jobs, with the *system* `/usr/bin/python3` rather than the dbloader venv
  — so it must keep working with the standard library and `psql` alone, and
  must not import `loader` (which needs psycopg2).
- **The serving host is hard-coded** in its `CONF`, not read from a properties
  file. `-H/--host`, `-U/--user` and `--psql` can override it.
- **The reload is a schema swap, and that is the only path.** It builds
  `<schema>_new` from the dump's `schema.sql`, loads into that, and renames it
  into place: atomic (~1 ms of locking instead of a whole reload), a failure
  leaves the live database untouched, and since `schema.sql` comes from the
  build database — whose schema dbloader generates from the dictionary — it
  rebuilds the serving schema from the dictionary as a side effect, so a
  dictionary change no longer has to be applied by hand. Job 410 gets this
  with no change to `updater_dag`. A dump that cannot be swapped (unqualified
  CSVs) is an error, not a fallback to something weaker.
- **`-d bmrb` and `-d metabolomics` load nothing.** Both serving databases are
  retired; everything is served from `bmrbeverything`. Jobs 160 and 260 still
  call them and stay green no-ops that say so.
- **Job 251 must stay: it feeds the public FTP relational tables.** It dumps
  the *build* database — `csvio.dump()` connects via `[dictionary]` and reads
  the `dict`/`macromolecules`/`web` schemas out of it — so it never touched
  the retired `bmrb` serving database. Verified by renaming that database out
  of existence and re-running the dump: byte-identical, 159 files.

      251 → staging/dbdump/bmrb → 602 rsync → ftp/…/relational_tables/nmr-star3.1
                                            → rsync_to_library.sh → /librarym/BMRB

- **The metabolomics relational tables have no publication step.** The old
  condor jobs in `condor/` dumped *straight* to the FTP directories
  (`relational_tables/nmr-star3.1` and `relational_tables/metabolomics`). When
  updater_dag moved to dump-to-staging-then-rsync, job 602 was added for the
  macromolecule dump and nothing was added for the metabolomics one — so job
  151 writes `staging/dbdump/metabolomics` and no job copies it anywhere.
  Either add an rsync alongside 602 or point 151 at the FTP directory the way
  the old job did; until then `relational_tables/metabolomics` is frozen.
- **`origin/python3` is the deployed branch** (`dbloader3`): a mechanical
  py2→py3 + pgdb→psycopg2 port of the same base commit this branch forked
  from. Everything in it is superseded here except the metabolomics
  retirement, which has been carried across.
- `--no-web` on job 100 is redundant (the web stage only runs inside the
  macromolecules branch) but harmless.

## Layout

| Path | Role |
|------|------|
| `__main__.py` | CLI switchboard; sequences the load/dump stages. |
| `loader/db.py` | **The only module that talks to PostgreSQL**: `dsn()`, `connect()`, `run_sql_file()`, `copy_from_csv()`/`copy_to_csv()` (via `psql \copy`), `add_ro_grants()`, identifier quoting. |
| `loader/__init__.py` | `timer`; re-exports every stage. |
| `loader/dictionary.py` | Load `dictionary.sql` + `dict.*.csv` into the `dict` schema. |
| `loader/entries.py` | Find entry files, build the shadow schema, keep score. |
| `loader/shadow.py` | Build alongside and swap in: DDL rewriting, and the swap transaction. |
| `loader/starschema.py` | The dictionary read as a schema definition: type mapping, `create_tables`. |
| `loader/entryload.py` | pynmrstar parse + batched insert of one entry. |
| `loader/chemcomps.py` | Dump-and-load chem comps from `ccdb`. |
| `loader/macromol.py` | Post-load fixups for macromolecule entries. |
| `loader/metabolomicsextras.py`, `webextras.py` | `meta` and `web` schema extras (CS stats, term/pulse lists). |
| `loader/csvio.py` | CSV in and out: per-schema dumps and bulk loads. |
| `loader/ets.py` | Iterators over the ETS tracking DB (released/deposited/queued IDs). |
| `fastalib.py` | Standalone FASTA library generator (not imported by the loader). |
| `*.sql` | Schemas: `webschema.sql`, `metabolomics_meta_schema.sql`, `cs_stats.sql`. |
| `*.js`, `*.csv`, `metabolomics_meta_files/` | Static mapping/side-data loaded into `web`/`meta`. |
| `condor/*.sub` | HTCondor submit files that run these stages in production. |
| `loader.properties` | Per-schema DB connection + file-path config. |
| `tests/` | The golden harness and regression runner — see `tests/README.md`. |

Note the paths in `loader.properties` (`/share/dmaziuk/...`, hosts `irukandji`,
`octopus`, `ets.bmrb.wisc.edu`) are production-specific.
