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
> defects in the deposited entries that the old loader used to hide. It shells
> out to `psql`/`pg_dump` for `COPY` (server `copy` needs superuser; `psql
> \copy` does not).

## How to run it

```bash
python __main__.py -c loader.properties --dictdir <dir-with-dictionary.sql+dict.*.csv> -d <outdir>
```

`__main__.py` is a big switchboard of `--no-*` flags; by default it loads
**everything** and dumps `bmrbeverything`. Key stages, each a `loader.*` call:

| Stage | Function | What it does |
|-------|----------|--------------|
| dictionary | `load_dict` (`loader/dictionary.py`) | run `dictionary.sql` DDL into `dict` schema, then `COPY` every `dict.*.csv` in `--dictdir` into `dict.<table>`. |
| chem comps | `load_chem_comps` (`chemcomps.py`) | dump released chem comps from the `ccdb` database, load into `chemcomps` schema. |
| metabolomics | `load_metabolomics` + `load_meta_schema` | parse metabolomics NMR-STAR entries into the `metabolomics` schema; load `meta` extras from CSV. |
| macromolecules | `load_macromolecules` + `fix_macromolecules` | parse macromolecule entries into the `macromolecules` schema, then cleanups. |
| web | `load_web_schema` (`webextras.py`) | `web` schema: chemical-shift statistics (`cs_stats.sql`) + CSV extras. |
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

Three dumps, three target databases, two layouts (see `loader/csvio.py`):
`bmrb` and `metabolomics` are the **"old-style"** website databases — entry
tables unqualified, each with its own copy of `dict` — while `bmrbeverything`
is **"new-style"**, every file `<schema>.<table>.csv`. That is why the dump
code has both layouts and why `load_postgres_db.py` handles a missing schema
prefix.

Things to know before changing any of this:

- **`load_postgres_db.py` is not dead code.** It is invoked directly by three
  DAG jobs, with the *system* `/usr/bin/python3` rather than the dbloader venv
  — so it must keep working with the standard library and `psql` alone, and
  must not import `loader` (which needs psycopg2).
- **The serving host is hard-coded** in its `CONF`, not read from a properties
  file. `-H/--host`, `-U/--user` and `--psql` can override it.
- **The DAG never passes `-c/--create`**, so `schema.sql` is dumped but never
  used: the serving tables must already exist. The load truncates and refills
  them, so a schema change has to be applied there by hand.
- **Each table is truncated and copied by a separate `psql -c`**, so the
  TRUNCATE commits before the `\copy` runs. A copy that fails leaves that
  table **empty** on the live server until the next run (verified), and there
  is no transaction spanning the ~250 tables — readers can see a partly
  reloaded database.
- `--no-web` on job 100 is redundant (the web stage only runs inside the
  macromolecules branch) but harmless.

## Layout

| Path | Role |
|------|------|
| `__main__.py` | CLI switchboard; sequences the load/dump stages. |
| `loader/db.py` | **The only module that talks to PostgreSQL**: `dsn()`, `connect()`, `run_sql_file()`, `copy_from_csv()`/`copy_to_csv()` (via `psql \copy`), `add_ro_grants()`, identifier quoting. |
| `loader/__init__.py` | `timer`; re-exports every stage. |
| `loader/dictionary.py` | Load `dictionary.sql` + `dict.*.csv` into the `dict` schema. |
| `loader/entries.py` | Find entry files, prepare the schema, keep score. |
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
