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

**Every** schema goes through this, `chemcomps` and `meta` included. Those two
were the last in-place loads (each opened with `drop schema <s> cascade` on the
live name), which only ever looked harmless because the reload happened on a
build database nobody read.

`--host`/`--port` override the server for the sections that name the target
database, so a DAG job or a test run can say where it is loading without
editing the deployed properties file. `[ets]` and the chem-comp source (`ccdb`)
are deliberately left where the config puts them — they are different servers,
read-only, and following the override would either fail or silently read the
wrong data. Note `ccdb` shares the `[chemcomps]` section with the target and
falls back to the unprefixed options, so the override pins `srchost` first.

| Stage | Function | What it does |
|-------|----------|--------------|
| dictionary | `load_dict` (`loader/dictionary.py`) | rewrite `dictionary.sql` to build `dict_new`/`validict_new`, run it, then `COPY` every `dict.*.csv` in `--dictdir` into `dict_new.<table>`. |
| chem comps | `load_chem_comps` (`chemcomps.py`) | dump released chem comps from the `ccdb` database, load into `chemcomps` schema. |
| metabolomics | `load_metabolomics` + `load_meta_schema` | parse metabolomics NMR-STAR entries into the `metabolomics` schema; load `meta` extras from CSV. |
| macromolecules | `load_macromolecules` + `fix_macromolecules` | parse macromolecule entries into the `macromolecules` schema, then cleanups. |
| web | `load_web_schema` (`webextras.py`) | `web` schema: chemical-shift statistics (`cs_stats.sql`) + CSV extras, the time domain scan, then the API's derived tables (`webapi.sql`). |
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
`/projects/BMRB/software/dictionary-meta/dbloader/` (its own venv; config
`uconn.properties`, not in this repo). **One database.** The reload loads the
served database directly, building every schema alongside the live ones and
renaming them all into place in a single transaction:

```
        entry files (CVS/SVN, validated by updater_dag/update_check.py)
                              │
   ┌──────────────────────────▼───────────────────────────┐
   │ __main__.py -c uconn.properties --host --database    │
   │   --no-swap on every stage: nothing becomes visible  │
   │  100 --dictdir .../nmr-star-dictionary-scripts/csv   │  dict_new, validict_new
   │  110                                                 │  chemcomps_new
   │  131                                                 │  metabolomics_new, meta_new
   │  231                                                 │  macromolecules_new, web_new
   │        (231's web stage also runs cs_stats.sql, the  │
   │         timedomain scan and webapi.sql)              │
   └──────────────────────────┬───────────────────────────┘
                              │  300: loader/shadow.py -- ONE transaction
                              ▼
   ┌──────────────────────────────────────────────────────┐
   │ bmrbeverything @ bmrb-staging.nmrbox.org             │
   │   the whole release appears at once                  │
   └──────────────────────────┬───────────────────────────┘
                              │
              ┌───────────────┼────────────────┐
              ▼               ▼                ▼
        401 redis/xml    151 dump         251 dump
                              │                │
                              ▼ 602 rsync      ▼ 602 rsync
              ftp/pub/bmrb/relational_tables/{metabolomics,nmr-star3.1}
```

Two dumps, one layout each (see `loader/csvio.py`). Both are **"old-style"** —
entry tables unqualified, each with its own copy of `dict` — which was shaped
by the separate website databases, long retired, but is *also* the format
published on the FTP site, which is why it stays. The "new-style"
`bmrbeverything` dump (`dump_new`, every file `<schema>.<table>.csv`) existed
only to feed the serving database and is no longer produced by any job.

Things to know before changing any of this:

- **`load_postgres_db.py` is now dead code.** It existed to load a CSV dump
  into the separate serving database; jobs 160, 260 and 410 called it and all
  three are gone. Nothing invokes it. Delete it once a release has run green on
  the single-server DAG — it is kept for one cycle only so there is something
  to fall back to. Its hard-coded `CONF["host"]` is also stale
  (`bmrb-staging.cam.uchc.edu`; the host is `bmrb-staging.nmrbox.org`).
- **Where the release lands is stated in the DAG, not here.** Every job passes
  `--host`/`--database`, set by the `VARS` lines in `update_bmrb_db.dag` — one
  place to repoint a whole release. `[ets]` and the chem-comp source never
  follow the override (`loader/db.py:repoint`).
- **Job 300 is the release.** It swaps `dict validict chemcomps metabolomics
  meta macromolecules web` — spelled out, because all of them have to arrive
  together (validict is views over dict, web is built from macromolecules, meta
  from metabolomics) and one silently missing from an auto-detected list would
  be stranded in its `_new` form while the live schema served the old data. A
  missing shadow is an error, not a partial swap.
- **Jobs 151 and 251 feed the public FTP relational tables**, and now run
  *after* the swap, since they dump the live schemas.

- **The metabolomics relational tables are published now.** They were not: the
  old condor jobs in `condor/` dumped *straight* to the FTP directories, and
  when updater_dag moved to dump-to-staging-then-rsync, job 602 was added for
  the macromolecule dump and nothing for the metabolomics one — so job 151
  wrote `staging/dbdump/metabolomics` and nothing copied it anywhere.
  `602_copy_metabolomics_dump_to_ftp.sub` closes that.
- **`origin/python3` is the deployed branch** (`dbloader3`): a mechanical
  py2→py3 + pgdb→psycopg2 port of the same base commit this branch forked
  from. Everything in it is superseded here except the metabolomics
  retirement, which has been carried across.
- `--no-web` on job 100 is redundant (the web stage only runs inside the
  macromolecules branch) but harmless.
- **Two BMRB-API reloaders moved here.** `webapi.sql` was
  `bmrbapi/reloaders/sql/initialize.sql`, and `webextras.load_timedomain()` was
  `bmrbapi/reloaders/timedomain.py`. Both had to be inside the swap: they build
  on `macromolecules`/`metabolomics`/`web`, which the loader replaces
  wholesale, so run afterwards they would rebuild objects the swap had just
  destroyed — in public. The API keeps `--sql` and `--timedomain` as warning
  no-ops so the deployed job 401 does not die on an unrecognized argument;
  drop them from both sides once `updater_dag` stops sending them. The other
  API writers into `web` — `inext`, `csrosetta`, `uniprot` — have **not**
  moved, and are still destroyed by every reload with nothing rebuilding them.
- **`[web] timedomain_dir`** names the per-entry time domain directory, with
  `%s` for the entry ID. It exists because BMRB-API's
  `macromolecule_entry_directory` (`…/bmr%s/clean`) and dbloader's `entrydir`
  (`…/bmr%s`) disagree about the layout. The scan reports how many entries it
  found and warns loudly at zero — a wrong pattern is otherwise
  indistinguishable from an archive with no time domain data.

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
| `*.sql` | Schemas: `webschema.sql`, `metabolomics_meta_schema.sql`, `cs_stats.sql`, `webapi.sql`. |
| `*.js`, `*.csv`, `metabolomics_meta_files/` | Static mapping/side-data loaded into `web`/`meta`. |
| `condor/*.sub` | HTCondor submit files that run these stages in production. |
| `loader.properties` | Per-schema DB connection + file-path config. |
| `tests/` | The golden harness and regression runner — see `tests/README.md`. |

Note the paths in `loader.properties` (`/share/dmaziuk/...`, hosts `irukandji`,
`octopus`, `ets.bmrb.wisc.edu`) are production-specific.
