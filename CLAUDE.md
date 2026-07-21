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
> [`PORT_NOTES.md`](PORT_NOTES.md) for what changed and what is verified, and
> [`tests/README.md`](tests/README.md) for how to run the regression against
> the Python 2 golden. It shells out to `psql`/`pg_dump` for `COPY` (server
> `copy` needs superuser; `psql \copy` does not).

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
