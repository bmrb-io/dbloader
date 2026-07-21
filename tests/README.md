# dbloader test harness

The Python 3 rewrite ([`../MODERNIZATION_PLAN.md`](../MODERNIZATION_PLAN.md),
[`../PORT_NOTES.md`](../PORT_NOTES.md)) is verified by building the *same*
database twice — once with the unmodified legacy Python 2 code, once with the
rewrite — and diffing it row for row.

```
tests/golden_dict.sh      py2 -> dict schema           -> golden/dict.md5
tests/golden_entries.sh   py2 -> macromolecules,       -> golden/*.md5
                                 metabolomics             golden/entries.failed
tests/regression.sh       py3 -> the same schemas      -> diff vs golden
```

Status: **all three schemas match, 946 tables** (16 `dict`, 465
`macromolecules`, 465 `metabolomics`), over a 300+300 entry subset, with the
same 0 entries failing to load on both sides. `chemcomps` and most of `web`
are not covered — they need ccdb and the ETS tracking database, deferred by
design.

## Setup

### PostgreSQL

Throwaway server, per `pg-tmp` (unix socket in `/tmp`, trust auth, **nothing
persists across container restarts** — re-run this after one):

```sh
eval "$(pg-tmp)"
psql -c "create role bmrb login superuser" -c "create role web login"
createdb -O bmrb bmrb
export PGHOST=/tmp PGUSER=bmrb PGDATABASE=bmrb
```

### The golden stack

The legacy loader needs Python 2.7, `starobj`, `sas` and PyGreSQL's `pgdb`, and
it has to keep running after the port has rewritten `loader/` in place — so it
runs out of git worktrees in `../.golden-stack/` (not checked in):

| Piece | What | Why |
|-------|------|-----|
| `dbloader/` | worktree of this repo @ `41293e9` | the pre-port Python 2 loader |
| `starobj/` | worktree of `~/git/starobj` @ `89925a1` | last pgdb/Python 2 commit (`master` is py3 now) |
| `sas/` | worktree of `~/git/sas` @ `34243e9` | the py2 STAR parser starobj imports |

```sh
G=~/git/dictionary/.golden-stack
git -C ~/git/dictionary/dbloader worktree add --detach $G/dbloader 41293e9
git -C ~/git/starobj              worktree add --detach $G/starobj  89925a1
git -C ~/git/sas                  worktree add --detach $G/sas      34243e9
python2 -m pip install --user ply "PyGreSQL<6"
```

`PyGreSQL` is a C extension; it builds here because the box has `gcc` and the
python2.7 headers.

### The Python 3 side

`../venv/` (gitignored) has `psycopg2-binary` and `ply`; py3 `starobj` and
`sas` come off `PYTHONPATH`:

```sh
export PYTHONPATH=~/git/starobj:~/git/sas/python
```

with `starobj` on branch **`dbloader-py3-fixes`** — `master`'s py3 conversion
cannot load entries at all (see [`../STAROBJ_PY3_REVIEW.md`](../STAROBJ_PY3_REVIEW.md)).
`regression.sh` sets both of these itself if they are not already set.

### Config

`tests/loader.properties.in` is the production `loader.properties` pointed at
the throwaway server, as a template: `tests/render_config.sh` substitutes the
checkout path, the tests directory and the driver (`pgdb` for the golden,
`psycopg2` for the rewrite) into `tests/build/loader.properties`. Absolute
paths throughout, because the golden runs out of a different checkout.

## Running it

```sh
sh tests/golden_dict.sh            # build inputs + golden dict   (~2 min)
sh tests/golden_entries.sh         # golden entry schemas         (~3 min)
sh tests/regression.sh             # the rewrite, diffed          (~4 min)
sh tests/regression.sh truncate    # again, via the truncate path (~4 min)
```

`golden_dict.sh --no-build` reuses `tests/build/csv` instead of rebuilding the
dictionary artifacts. `regression.sh dict` / `entries` run one stage.

A failure prints the tables whose fingerprints differ and the first few
differing rows of each; the full dumps are left in `tests/build/test/`.

## What the golden is built from

### Entry corpus

`~/git/query-bmrb/bmrb_entries` holds **14,772 released macromolecule entries**
(944 MB) already in the `bmr<ID>/bmr<ID>_3.str` layout `entries.py` expects;
`~/git/query-bmrb/bmrb_metabolomics` holds **3,629** more (3,409 `bmse` + 220
`bmst`, 222 MB) as `bmse<ID>/bmse<ID>.str`.

`tests/make_subset.sh` takes a **stride sample** of each — sort the entry
directories, keep every Nth — and materializes it as a tree of symlinks under
`tests/build/entries/`. 300 of each takes ~90 s to load instead of ~75 min for
the full archives, and striding rather than `head` keeps the sample spread
across the whole archive (hand-curated legacy entries, ADIT-era entries, recent
depositions). The chosen IDs are committed as `tests/subset.*.txt` — that file
is what makes the golden reproducible. Regenerate with `MACRO_N`/`METAB_N` set
if you want a different size; the goldens must then be rebuilt.

Both corpora are **pure ASCII**, so findings 2 and 3 in
`../STAROBJ_PY3_REVIEW.md` (encoding handling) cannot produce a golden diff
here. Re-check if the corpus is ever refreshed.

### Dictionary version

**v3.2.14.0 — built from source, not the reference `input/`.**

`nmr-star-dictionary-scripts/input/` is a **v3.2.6.0** reference copy, but the
entry archive is current. Loading entries against a 3.2.6.0 `dict` schema fails
on every tag added since: `_Experiment.Details` alone is absent from 3.2.6.0
and present in **1,005** macromolecule entries. (Not a code defect — py2 and
py3 fail identically, and the old `entries.py` swallowed it in a bare
`except:` — but proving parity on entries that load incomplete is worthless.)

So `tests/build_dict_inputs.sh` builds the whole chain from
`nmr-star-dictionary/NMR-STAR/internal_106_source`:

```
internal_106_source ──dictionary-converter──▶ complete CSV distribution
                     ──nmr-star-dictionary-scripts──▶ tests/build/csv/
                                                      dict.*.csv + dictionary.sql
```

**One source, one generated distribution.** `dictionary-converter` passes the
hand-maintained inputs (`comments.str`, `extra_enumerations.str`,
`val_overide_add.csv`, `default-entry.cif`) through verbatim alongside what it
generates, so the build output is everything the scripts need — nothing is
spliced in from a checked-in release. Nothing is written inside any sibling
repo: the scripts run out-of-tree in `tests/build/`.

#### Why not take everything from `internal_106_distribution/`?

That directory is a checked-in *snapshot* of a past build, not a location the
converter maintains (it writes wherever `-o` points). It had drifted from
source — `dictionary-converter/tests/conftest.py` calls it "an older Jun-2021
snapshot [that] will NOT match the current source" — and two of the differences
would have corrupted the golden: `item_type_units.txt` still carried the
`'milliseconds` unclosed-quote typo, and `adit_enum_dtl.csv` had lost commas
inside 337 enumeration values (`Solvent Extr. Res. Dev. Jpn.` vs the source's
`'Solvent Extr. Res. Dev., Jpn.'` — the legacy VB wrote that file as unquoted
comma-joined CSV; see DISCREPANCIES E5). Always build from source.

#### 3.2.6.0 → 3.2.14.0, in the loaded schema

| table | 3.2.6.0 | 3.2.14.0 | |
|-------|--------:|---------:|--|
| `adit_item_tbl` | 6713 | 6760 | +47 tags |
| `query_interface` | 6713 | 6760 | |
| `validator_printflags` | 6713 | 6760 | |
| `nmrcifmatch` | 3815 | 5097 | |
| `aditenumtie` | 10324 | 10355 | |
| `aditenumdtl` | 4786 | 4695 | |
| `aditmanoverride` | 1059 | 258 | ADIT rules pruned |
| `validationlinks` | 905 | 209 | ADIT rules pruned |

The two large drops are genuine upstream data evolution, not truncation:
`adit_man_over.csv` and `adit_tag_validation.csv` are passed through unchanged
by the converter, and their git history in `nmr-star-dictionary` is a run of
commits deliberately removing ADIT rules (ADIT-NMR was retired in 2020).

## How the diff works

`tests/dump_schema.sh <schema> <outdir>` writes one CSV per table, every row
ordered by every column (by ordinal). That makes the dump independent of
insertion order, of physical row order, and of which loader produced it — so a
matching md5 means the same rows, not merely the same row count. Views are
skipped; they are derived from the tables.

To compare any database against a golden by hand:

```sh
sh tests/dump_schema.sh dict /tmp/test-dict
( cd /tmp/test-dict && md5sum *.csv ) | diff -u tests/golden/dict.md5 -
```

## The ETS stub

The macromolecule load cross-checks its file list against the released-ID list
in the ETS tracking database, which is not reachable from here.
`tests/load_entries.py` — the driver both sides run — replaces
`loader.released_ids_itr` with one that yields exactly the IDs present in
`entrydir`, which is what ETS would report for a corpus of released entries. It
is the only thing stubbed; everything else is the real code path.

## What's still missing

- **ccdb / ETS access** for the chemcomps and ETS/web stages, and the
  behaviour changes that depend on them (PORT_NOTES.md items 3–6).
- **A golden for the CSV dump path** (`csvio.dump_new`) and for
  `macromol.fixup`.
