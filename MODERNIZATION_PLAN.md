# dbloader modernization plan

> ## STATUS: the port is done. Phases 0–4 complete; see `PORT_NOTES.md`.
>
> `loader/` is Python 3 + psycopg2, `starobj` is kept (§0.4), and the `dict`,
> `macromolecules` and `metabolomics` schemas the rewrite builds are **identical
> row for row to the Python 2 golden** — 946 tables over a 300+300 entry subset
> (`sh tests/regression.sh`). Phase 3's chemcomps/ETS/web stages and Phase 4's
> dump path are ported but **not diffed**, because ccdb and the ETS database are
> not reachable from here; those, plus packaging (Phase 5), are what is left.
>
> Read `PORT_NOTES.md` for the result and every behaviour change, and
> `tests/README.md` for how to re-run the comparison. The rest of this document
> is the original brief, kept for the reasoning behind the decisions.

Plan for porting `dbloader` to Python 3 the same way `nmr-star-dictionary-scripts`
was done: rewrite, replace `pgdb`, fix latent bugs, simplify, and verify
row-for-row against the current code. This document is the brief.

> **The scripts port is the template.** It succeeded because we (1) captured a
> golden from a reconstructed Python 2 run, (2) did a faithful port, and (3)
> diffed outputs byte-for-byte. dbloader is bigger and its "output" is a
> **PostgreSQL database**, so the golden and the diff are the hard parts — read
> §6 before writing any code.

> **§0 is new and supersedes parts of §2a, §6, §7 and §8.** It records what a
> Phase 0 session actually built and decided. Read it first: it will save you
> re-deriving several non-obvious things about the sibling repos.

---

## 0. State of play (end of the Phase 0 session)

### 0.1 What exists — and what does not

**No dbloader code has been ported.** `loader/*.py`, `__main__.py` and
`fastalib.py` are untouched Python 2. The only additions to this repo are
`tests/`, `CLAUDE.md`, `MODERNIZATION_PLAN.md` and `STAROBJ_PY3_REVIEW.md`, none
of them committed. Phase 1 has not started.

What *was* done: the golden harness (§0.5), and unblocking `starobj` (§0.4).

### 0.2 The sibling repos, and how the dictionary actually reaches dbloader

All repos live under `~/git/dictionary/`. `../ORGANIZATION.md` is the map; this
is the part that matters for dbloader, because it is easy to get wrong.

```
nmr-star-dictionary/NMR-STAR/
  internal_106_source/          the real source: master spreadsheet + release
                                files + the hand-maintained static inputs
  internal_106_distribution/    a checked-in SNAPSHOT of some past build
        │
        │  dictionary-converter:  python -m dict_builder.cli -s <source> -o <ANY DIR>
        ▼
  a complete CSV distribution  (wherever -o points; ours is dbloader/tests/build/dist)
        ▼
  nmr-star-dictionary-scripts   (run out-of-tree; reads a csv.dir, writes csv/)
        ▼
  dict.*.csv + dictionary.sql   ->  dbloader load_dict  ->  PostgreSQL `dict`
```

**The thing that confuses everyone:** `internal_106_distribution/` is **not** a
location the converter maintains. The converter writes to whatever `-o` says;
that directory is simply where the output of *a* build was committed once, like
a `build/` folder checked into git. Our chain **builds fresh from
`internal_106_source` and does not read the distribution at all.**

That last part is newly true. Four hand-maintained inputs — `comments.str`,
`extra_enumerations.str`, `val_overide_add.csv`, `default-entry.cif` — used to
live *only* in `internal_106_distribution/`, so a consumer had to splice them in
from a checked-in release while taking everything else from a fresh build. They
have since been copied into `internal_106_source/` (**uncommitted**, see below)
and added to `dictionary-converter`'s `PASSTHROUGH_FILES`, so one build now
emits everything the scripts need. The golden is byte-identical either way.

Why not just consume the checked-in distribution wholesale? Because it had
drifted badly from source — `dictionary-converter/tests/conftest.py` calls it
"an older Jun-2021 snapshot [that] will NOT match the current source". Measured
against a fresh build: 20 files identical, 4 different, 3 absent. Two of the
differences would have corrupted the golden (§0.3).

### 0.3 Dictionary version — read before touching the harness

`nmr-star-dictionary-scripts/input/` is a **v3.2.6.0** reference copy. The entry
archive is current, so loading entries against a 3.2.6.0 `dict` schema fails on
every tag added since — `_Experiment.Details` alone appears in **1,005** of the
14,772 macromolecule entries. Building from `internal_106_source` (**v3.2.14.0**)
took a 739-entry sweep from *684 ok / 55 failed* to **739 ok / 0 failed**.

Two upstream data problems were found and are **not yet resolved**:

1. **`item_type_units.txt`** — the distribution snapshot still had the
   `'milliseconds` unclosed-quote typo; `internal_106_source` has the fix.
2. **`adit_enum_dtl.csv`** — the distribution had lost commas *inside*
   enumeration values (`Solvent Extr. Res. Dev. Jpn.` where `enumerations.txt`,
   the source of record, says `'Solvent Extr. Res. Dev., Jpn.'`). The legacy VB
   wrote that file as raw comma-joined CSV with no quoting, so any value
   containing a comma had to be mangled to keep the column count. The converter
   writes properly quoted CSV and preserves the source value — it is the fix,
   not the bug (`dictionary-converter/DISCREPANCIES.md` E5). 337 values affected.

> **Uncommitted local state.** The working tree of `nmr-star-dictionary` carries
> a v3.2.14.1 bump (`version.json`, restamped source master), a regenerated
> `internal_106_distribution`, and the four static inputs newly copied into
> `internal_106_source/` (§0.2 — the converter's passthrough change IS committed,
> so a clean checkout of the dictionary repo will not build until those four
> files are committed too). **The owner deliberately did not commit this** —
> the regeneration also rewrites `NMR-STAR.dic` (~15.6k lines, a deliberate
> CIF-validity fix: the historical file was not loadable CIF) and
> `commonDA_tag.dic`, and three build outputs marked "Decide — no consumer
> located" in `OUTPUTS.md` were left unshipped. Those decisions are pending. The
> harness does not depend on any of it: it builds from `internal_106_source`.

### 0.4 starobj — decision made, and it needed fixing

**Decision (owner, this session): keep `starobj`; do NOT reimplement the load
path on pynmrstar.** This reverses §2a below. Use starobj's Python 3 `master`.
Moving to pynmrstar may happen later as a separate project.

But that py3 conversion was never validated, and **entry loading did not work at
all**: `StarParser` had lost its `sas.ContentHandler`/`ErrorHandler` base classes
(root cause: `import sas` was moved below `from .parser import StarParser` in
`starobj/__init__.py`), so `sas`'s `isinstance` asserts fired on the first file.

Fixed on branch **`starobj@dbloader-py3-fixes`** (commit `eb5382b`, off
`master`). **Not for merging to master** — other projects run against `master`
as coded. Full write-up with all 8 findings: `STAROBJ_PY3_REVIEW.md`.
Validating that conversion is part of what the dbloader golden must prove.

After the fix: 739/739 macromolecule entries and 91/91 metabolomics entries load
clean.

### 0.5 The harness that exists (see `tests/README.md` for detail)

| piece | what |
|---|---|
| `tests/build_dict_inputs.sh` | source → converter → scripts → `tests/build/csv` |
| `tests/golden_dict.sh` | the above + **unmodified py2** `load_dict` + dump |
| `tests/dump_schema.sh` | deterministic per-table CSV dump (ordered by every column) |
| `tests/golden/dict.md5` | golden fingerprints, 16 tables |
| `tests/loader.test.properties` | prod config repointed at a local `pg-tmp` server |
| `../.golden-stack/` | `starobj`@`89925a1` + `sas`@`34243e9` py2 worktrees |

Entry corpora (owner-provided, large, outside the repos):
`~/git/query-bmrb/bmrb_entries` — 14,772 `bmr<ID>/bmr<ID>_3.str`;
`~/git/query-bmrb/bmrb_metabolomics` — 3,629 `bms[et]<ID>/<ID>.str`.
Both match `entries.py:_gen_file_list`'s expected layout. **All 18,401 files are
pure ASCII** — so the encoding questions in `STAROBJ_PY3_REVIEW.md` (findings 2
and 3) cannot produce a golden diff on this corpus. Re-check if it is refreshed.

Environment (claude-box): `gcc` + python2.7 headers are present, so real
**PyGreSQL builds** — `python2 -m pip install --user ply "PyGreSQL<6"`. Postgres
is `pg-tmp` (ephemeral, nothing survives a container restart; re-run the setup in
`tests/README.md`). Python 3 deps live in `dbloader/venv`.

### 0.6 Cleanup facts established

- `pacsy_schema.sql` is byte-identical to
  `nmr-star-dictionary-scripts/conf/sql_pacsy_schema.sql` and is referenced by
  **nothing** in either repo. Dead; delete in Phase 4.
- Every other data file in this repo (`cs_stats.sql`, `webschema.sql`,
  `metabolomics_meta_schema.sql`, `software.js`, `swauthors.js`, `task.js`,
  `web.*.csv`, `metabolomics_meta_files/*`) **is** referenced from
  `loader.properties` and exists nowhere else in the pipeline. It is
  dbloader-owned side data, not duplicated dictionary content — do not try to
  source it from the dictionary repo.
- No dictionary CSVs are checked into this repo; `dict.*.csv` has always come
  from the scripts' output at build time.

---

## 1. What dbloader does today

`dbloader` (re)builds the BMRB `bmrb` PostgreSQL database and dumps parts of it
back to CSV. Entry point `__main__.py` is a switchboard of `--no-*` flags over
these stages (all in `loader/`, orchestrated via `loader/__init__.py`):

| Stage | Function (module) | What it does | Uses |
|-------|-------------------|--------------|------|
| dictionary | `load_dict` (`dictionary.py`) | run `dictionary.sql` DDL + `COPY` `dict.*.csv` into `dict` schema | psql |
| **entries** | `load_macromolecules` / `load_metabolomics` (`entries.py`) | parse NMR-STAR entry files → per-archive relational tables | **starobj** |
| chem comps | `load_chem_comps` (`chemcomps.py`) | dump released chem comps from `ccdb`, load into `chemcomps` | **starobj**, pgdb |
| macro fixups | `fix_macromolecules` (`macromol.py`) | post-load SQL cleanups / stats | pgdb |
| web / meta | `webextras.py`, `metabolomicsextras.py` | `web` (CS stats) + `meta` schema extras | pgdb, psql |
| dumps | `dump_new` / `Dumper` (`csvdump.py`, `csvdump_better.py`) | dump schemas back to CSV | pgdb, psql |
| ETS reads | `ets.py` | iterate released/deposited/queued IDs from the ETS DB | pgdb |

The **dict schema these load is produced by `../nmr-star-dictionary-scripts`**
(now Python 3) — so we can generate test inputs ourselves.

`dictionary-converter/OUTPUTS.md` and `dbloader/CLAUDE.md` have the wider context.

## 2. The dependency surface to remove

> Post-§0.4 this is really *one* removal: **`pgdb` → `psycopg2`** (§2b) plus the
> py2→py3 mechanics (§2c). §2a is retained as reference, not as work.

Three things, in rough order of difficulty:

### 2a. ~~`starobj` (the hard one)~~ — SUPERSEDED by §0.4; reference only

*starobj is being kept. Read this subsection to understand the contract dbloader
relies on — not as a list of work to do.* Used only in `entries.py` + `chemcomps.py`:
`entries.py:_load_entries` uses exactly this contract:
- `DbWrapper(config)` / `.connect()` / `._connections[C]["conn"]` / `.schema(C)` — a
  connection abstraction (see §2b).
- `StarDictionary(db)` — reads the loaded `dict` schema; passed opaquely to the two
  calls below (dbloader never calls its methods directly).
- `NMRSTAREntry.create_tables(dictionary, db, use_types, verbose)` — generates
  `CREATE TABLE schema."<TagCategory>" (...)` for every entry table from the
  dictionary, plus `entry_saveframes`.
- `StarParser.parse_file(db, dictionary, filename, errlist, types, create_tables=False)`
  — SAX-parses the `.str` file (via `starobj.sas`) and inserts each free-table row /
  loop row into its table, assigning `Sf_ID`.

`chemcomps.py` uses `DbWrapper` + `NMRSTAREntry` similarly for schema drop/create.

**What "drop starobj" actually requires** — a focused reimplementation of the
*load path only* (not starobj's 1136-line query API, which dbloader doesn't call
directly). Reference behaviour to port, verbatim, from `~/git/starobj`:

- **Type mapping** (`starobj/entry.py:create_tables`, lines ~76–105):
  `float → varchar(63)` (preserves trailing zeros — **keep this**);
  and when `use_types` is true: `char/varchar/vchar/boolean/text → text`,
  `date → date`, `int → integer`, else → `text` (+ stderr warning);
  when `use_types` is false: **everything `text`**. Plus the fixed
  `entry_saveframes (category text, entryid text, sfid integer primary key,
  name text, line integer)`.
- **`use_types` flag** (`entries.py:197`): **macromolecules → false (all text)**,
  **metabolomics → true (typed)**. Reproduce exactly.
- **The table/column model**: `TagCategory` → table name, `TagField` → column,
  `dbtype` per column. In the `dict` schema this is `adit_item_tbl` /
  `val_item_tbl`. A ~30-line query replaces `StarDictionary.iter_tables()` /
  `iter_tags(columns=("dbtype",))`.
- **Insert** (`starobj/db.py:InsertStatement.insert`, lines ~191–232):
  columns quoted, values by dict; **auto-fill `Sf_ID`** from
  `NMRSTAREntry.last_sfid` when absent; sort columns for stable statements.
  Reproduce the `Sf_ID`/`Entry_ID` handling and the per-saveframe `sfid`
  assignment + `entry_saveframes` bookkeeping (`starobj/parser.py` handlers
  `startData/startSaveframe/startLoop/endLoop/endSaveframe/data`, lines ~201–330).
- **Type coercion**: pynmrstar yields strings; `"."`/`"?"`/empty → NULL; integer/date
  columns need coercion. Confirm how the legacy parser coerces (it inserts via the
  DB driver; check whether it passes strings and lets PostgreSQL cast, or coerces
  in Python) and match it.

Replacement: parse with **pynmrstar** (`Entry.from_file`) and walk saveframes/loops,
inserting with `psycopg2` (batched via `execute_values` for speed). New module,
e.g. `loader/entryload.py`, ~400–600 lines. **This is the bulk of the work and the
main correctness risk.**

> ### RESOLVED — §2a no longer applies. Do not implement it.
>
> The owner decided to **keep `starobj` and move to its Python 3 `master`**
> (via branch `dbloader-py3-fixes`, see §0.4). `loader/entryload.py` is
> **cancelled**: there is no pynmrstar reimplementation of the load path, and
> the "XL risk" line in §5 goes away with it. Everything above in §2a is kept
> only as a description of the starobj contract dbloader relies on — which is
> still worth reading, because validating starobj's py3 conversion is now part
> of the job. Revisit pynmrstar later as a separate project if desired.

### 2b. `pgdb` (PyGreSQL) → `psycopg2`
Used directly in `csvdump.py`, `ets.py`, `chemcomps.py`, and inside starobj's
`DbWrapper`. Standardize on **`psycopg2`** (matches starobj master; mature;
`execute_values` for bulk). Replace `DbWrapper` with a small connection helper
(the codebase already opens `pgdb.connect(**dsn)` directly in most places — unify
those on one `connect(config, section)` returning a psycopg2 connection).
`dsn()` in `loader/__init__.py` builds `pgdb`-style kwargs and a `"host:port"`
string it re-splits later — rewrite for psycopg2's separate `dbname`/`host`/`port`.
(psycopg3 is a viable alternative; psycopg2 recommended for least surprise.)

### 2c. Python 2 → 3
Mechanical, same as the scripts: `ConfigParser → configparser`; `print` stmts;
`.next()`/`__next__` (ets.py already has both — drop the `.next` shim);
`except X, e:` → `except X as e:`; `open(...,"rU")`; string/bytes around `COPY`
and subprocess I/O. **`macromol.py` is the most py2-dense (≈21 sites).**

## 3. Replacement architecture

```
__main__.py  (py3 argparse switchboard)
   └── loader/  (package)
        ├── db.py         NEW: thin psycopg2 helper (connect, run_sql_file,
        │                 copy_from/to via `psql \copy`, add_ro_grants, dsn)
        ├── dictionary.py load dict schema (psql) + dict.*.csv          [no starobj]
        ├── entries.py    file discovery + orchestration; calls starobj  [starobj KEPT]
        ├── chemcomps.py  ccdb dump+load; schema mgmt via db.py          [starobj KEPT]
        ├── macromol.py   post-load SQL fixups                           [psycopg2]
        ├── webextras.py / metabolomicsextras.py  schema extras + stats  [psycopg2]
        ├── ets.py        ETS iterators                                  [psycopg2]
        └── csvio.py      consolidated CSV dump/load (see §5)            [psql/psycopg2]
```

Deps: `psycopg2` + `starobj` (py3, which pulls `sas` + `ply`) + a PostgreSQL
client for `\copy`. Only `pgdb` goes away. See §0.4.

## 4. Latent bugs & simplifications to fix (verify each against golden)

- **`entries.py:215`** — the non-`--drop-tables` branch is
  `raise Exception("FIXME!!!! Not implemented")`. So the tool *only* works with
  `--drop-tables`; plain "truncate + reload" crashes. Implement it (or make drop
  the documented default) — decide with owner.
- **`entries.py:234`** — bare `except:` per file (logs traceback, continues).
  Keep the continue-on-error behaviour but catch `Exception` and record which
  entries failed (return a nonzero summary).
- **`csvdump.py` vs `csvdump_better.py`** — two implementations of the same dump;
  `__main__` calls `dump_new` (csvdump) yet also imports `Dumper` (csvdump_better,
  the "better" one). Pick one, delete the other. Fold `csvload.py` in → `csvio.py`.
- **`load_postgres_db.py`** (top-level, 331 lines, `PgLoader`) — standalone,
  imported by nothing in the package. Confirm it's dead and **retire** it.
- **`__main__.py`** — `load_web_schema(..., verbose=True)` hardcodes verbose;
  the `--no-*`/`args.load` flag interplay is convoluted — simplify the switchboard.
- **Hardcoded paths** — `PSQL="/bin/psql"`, `PGDUMP="/bin/pg_dump"`
  (`loader/__init__.py`), and prod paths in `loader.properties`
  (`/share/dmaziuk/...`, hosts `irukandji`/`octopus`/`ets.bmrb.wisc.edu`). Make
  the psql/pg_dump binaries configurable; ship a `loader.example.properties`.
- **`dsn()`** host:port packing/splitting — simplify (§2b).
- Consider `execute_values`/`COPY`-based bulk insert for the entry loader (the old
  path inserts row-by-row) — but **only after** row-for-row parity is proven.

## 5. Module-by-module effort

| Module | LOC | Action | Risk |
|--------|-----|--------|------|
| `__main__.py` | 101 | py3; simplify flag logic | S |
| `loader/__init__.py` | 229 | split: keep helpers, move DB access to `db.py`; keep the starobj import but stop hard-coding `/projects/BMRB/software/starobj` | M |
| `loader/dictionary.py` | 95 | py3; use `db.py` | S |
| ~~`loader/entryload.py` (new)~~ | — | **CANCELLED** — starobj is kept (§0.4) | — |
| `loader/entries.py` | 279 | py3; file discovery + call entryload; implement truncate path | M |
| `loader/chemcomps.py` | 428 | py3; pgdb→psycopg2; starobj kept | L |
| `loader/macromol.py` | 247 | py3 (heaviest); psycopg2 | M |
| `loader/webextras.py` | 265 | py3; psycopg2 | M |
| `loader/metabolomicsextras.py` | 110 | py3; psycopg2 | S |
| `loader/ets.py` | 280 | py3; psycopg2; drop `.next` shim | S |
| `loader/csvdump*.py`, `csvload.py` | ~1000 | consolidate → `csvio.py`; py3 | L |
| `load_postgres_db.py` | 331 | retire (confirm dead) | — |
| `fastalib.py` | 202 | py3 | S |

Total ≈ 3.6 kLOC to port + ≈0.5 kLOC new (entryload) − ≈0.3 kLOC retired.
Materially larger than the scripts port, and gated on live-DB testing.

## 6. Testing strategy (read first)

> **Largely built — see §0.5 and `tests/README.md`.** The `dict` stage of this
> is done and fingerprinted; the entries stage is scripted but not yet run. Read
> the rest of §6 for the reasoning, but take the concrete commands from
> `tests/README.md`, which reflects what actually exists.

The "output" is a PostgreSQL database, so the golden and the diff are the crux.
Mirror the scripts approach: **golden from a reconstructed Python 2 run, then
row-for-row diff.**

**Golden (old code):** run the current `dbloader` unchanged under
- Python 2.7,
- `starobj` @ the **last pgdb/py2 commit `89925a1`** (2020-05-28) — the py3
  rewrite is `837b692`+ and switches to psycopg2 at `b7c1f21`; use a worktree at
  `89925a1`, exactly like the `sas`@`34243e9` trick used for the scripts,
- `PyGreSQL` (`pgdb`) + `ply` (sas needs it) under py2,
- a local PostgreSQL populated by the same inputs.

**Inputs we can generate ourselves:** the `dict` schema comes from the now-py3
`../nmr-star-dictionary-scripts` (`dict.*.csv` + `dictionary.sql`) → `load_dict`.

**Sample data to obtain (owner-provided, small):** a handful of macromolecule
entries (`bmr<ID>_3.str`) and metabolomics entries (`bmse<ID>.str`) laid out as
`entry_directories/bmr<ID>/bmr<ID>_3.str`. 5–10 of each exercises free tables,
loops, typed vs all-text, and `Sf_ID` assignment.

**Diff:** load into two schemas (`*_golden`, `*_test`). For every table, dump
ordered rows and compare:
```
COPY (SELECT * FROM <schema>.<table> ORDER BY 1,2,...) TO STDOUT WITH CSV
```
(or `pg_dump --data-only --inserts` with a stable `ORDER BY`), then `diff`. A
`tests/regression.sh` should: build dict inputs → load_dict → load entries (new)
→ dump every table → diff against committed golden dumps (or golden md5s, as the
scripts port does). Watch for ordering non-determinism (load files in **sorted**
order — the old code already does; keep it) and `sfid` assignment order.

**Stage the diffing** — don't wait for a full build:
1. `dict` schema (load_dict) — trivial, no starobj; prove the connection layer.
2. **entries** — macromolecules (all-text) then metabolomics (typed); the main event.
3. chemcomps / macromol fixups / web / meta.
4. dumps (csvio) — diff dumped CSVs.

**External systems** — full runs also touch the **ETS** tracking DB
(`ets.bmrb.wisc.edu`) and **ccdb** (`octopus`). For first-pass testing, **stub**
`released_ids_itr` (feed the sample IDs) and defer chemcomps/ETS to a later phase
against owner-provided instances. The core entry-load path needs only PostgreSQL
+ sample `.str` + the generated dict schema.

## 7. Prerequisites for the implementing session

**All satisfied — see §0.5 for how to re-create each.** PostgreSQL via `pg-tmp`;
py3 deps in `dbloader/venv`; the py2 golden stack in `../.golden-stack/` with
real PyGreSQL (the box now has `gcc` + python2.7 headers); both entry corpora
under `~/git/query-bmrb/`; dict artifacts generated by `tests/build_dict_inputs.sh`.

Note `pg-tmp` is **ephemeral** — a container restart loses the cluster, roles and
database. Re-run the setup block in `tests/README.md`, then `sh tests/golden_dict.sh`.

Still deferred by design: ETS DB access, ccdb access.

## 8. Phasing

- **Phase 0 — harness. DONE (§0.5).** PostgreSQL up, dict inputs generated from
  current source, both entry corpora in place, `dict` golden produced by the
  unmodified py2 loader and fingerprinted, ordered dumps scripted. Remaining
  Phase-0 work: the **entries** golden (run py2 `load_macromolecules` /
  `load_metabolomics` over a fixed entry subset and dump those schemas) — the
  corpora and both code paths are ready, nobody has run it yet.
- **Phase 1 — connection layer + dict.** `loader/db.py` (psycopg2); port
  `dictionary.py`; diff the `dict` schema. No starobj involved. *Proves the plumbing.*
- **Phase 2 — entries.** Much smaller than originally scoped: starobj is kept
  (§0.4), so `entryload.py` is cancelled and `entries.py` only needs py3 +
  psycopg2 + the missing truncate path. The *risk* did not vanish, it moved:
  the golden must now prove **starobj's own py3 conversion** is faithful
  (`STAROBJ_PY3_REVIEW.md`), including the `autocommit` semantics change in
  finding 7 — `conn.autocommit = True` was a silent no-op under pgdb and is real
  under psycopg2. Row-for-row diff macromolecules then metabolomics.
- **Phase 3 — chemcomps + fixups + extras.** Port `chemcomps.py`,
  `macromol.py`, `webextras.py`, `metabolomicsextras.py`, `ets.py` to py3 +
  psycopg2 (starobj stays). Diff each schema (chemcomps/ETS against provided instances).
- **Phase 4 — dumps + cleanup.** Consolidate `csvdump*/csvload` → `csvio.py`;
  diff dumped CSVs; simplify `__main__`; retire `load_postgres_db.py`.
- **Phase 5 — finish.** `tests/regression.sh` + fixtures; update `CLAUDE.md`;
  `loader.example.properties`; commit on a `python3-rewritten` branch (mirror the
  scripts PR).

## 9. Risks & open decisions

1. **Fidelity of starobj's py3 conversion** — replaces the old "reimplement the
   load path" risk. 8 findings in `STAROBJ_PY3_REVIEW.md`, fixed on
   `dbloader-py3-fixes`; the golden diff is what proves them. Watch finding 7
   (`autocommit` was a no-op under pgdb, real under psycopg2).
2. ~~Drop starobj vs adopt py3 starobj~~ — **RESOLVED**, see §0.4.
3. ~~Test infrastructure~~ — **RESOLVED**, see §0.5. Note the corpus is
   all-ASCII, so encoding-related divergence cannot be detected with it.
4. **Driver choice** — psycopg2 (recommended) vs psycopg3.
5. **Determinism** — keep sorted file order; verify `sfid`/serial sequences start
   clean per drop-create so golden and test agree.
6. **Scope of first correctness pass** — core = `dict` + entry load; treat
   chemcomps/ETS/web/meta as follow-on once the entry loader has parity.

---

### Appendix: reference commits/paths
- Old py2 `sas` for the golden stack: `~/git/sas` @ `34243e9`.
- Old pgdb/py2 `starobj` for the golden stack: `~/git/starobj` @ `89925a1`
  (py3 begins `837b692`; pgdb→psycopg2 at `b7c1f21`).
- **starobj to run against: `~/git/starobj` @ branch `dbloader-py3-fixes`**
  (`eb5382b`); `master` cannot load entries (§0.4). `sas` runs off `master`.
- Dict-schema producer: `../nmr-star-dictionary-scripts` (Python 3), driven by
  `dbloader/tests/build_dict_inputs.sh` — do not run it in-tree.
- Dictionary source of record: `../nmr-star-dictionary/NMR-STAR/internal_106_source`
  (§0.2 — *not* `internal_106_distribution`, except for 4 static inputs).
- starobj load path dbloader depends on:
  `~/git/starobj/starobj/{entry.py,parser.py,db.py,stardict.py}`.
- Companion docs in this repo: `CLAUDE.md` (what dbloader is),
  `STAROBJ_PY3_REVIEW.md` (the starobj findings), `tests/README.md` (how to run
  the harness). `../ORGANIZATION.md` maps all six repos.
- `dictionary-converter` docs worth knowing: `OUTPUTS.md` (per-file
  producer/consumer/ship table), `DISCREPANCIES.md` (E5 = the enum-comma issue,
  E6 = `commonDA_tag.dic` staleness), `tests/conftest.py` (why the checked-in
  distribution is not a golden).
