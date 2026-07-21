#!/bin/sh
#
# Phase 0 harness, stage 2: produce the *golden* `macromolecules` and
# `metabolomics` schemas with the UNMODIFIED legacy Python 2 dbloader
# (MODERNIZATION_PLAN.md §6, §8), then dump them for row-for-row comparison
# against the Python 3 rewrite.
#
# Unlike the dict stage this depends on the dict schema already being loaded --
# starobj builds the entry tables *from* it -- so run tests/golden_dict.sh
# first, or pass --with-dict to chain them.
#
# Steps:
#   1. (--with-dict) load the dict schema;
#   2. materialize the fixed entry subset (tests/make_subset.sh);
#   3. run tests/load_entries.py under Python 2.7 out of the py2 worktree, with
#      the golden-stack PYTHONPATH and the ETS lookup stubbed;
#   4. dump both schemas to deterministic CSV + md5s under tests/golden/.
#
#     sh tests/golden_entries.sh [--with-dict]
#
# Exit status: 0 on success.

set -eu

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/.." && pwd)
top=$(cd "$repo/.." && pwd)

golden="$top/.golden-stack"
py2repo="$golden/dbloader"
[ -d "$py2repo/loader" ] || { echo "no py2 worktree at $py2repo -- see tests/README.md"; exit 1; }

PYTHONPATH="$golden/sas/python:$golden/starobj"
export PYTHONPATH
: "${PGHOST:=/tmp}" ; : "${PGUSER:=bmrb}" ; : "${PGDATABASE:=bmrb}"
export PGHOST PGUSER PGDATABASE

if [ "${1:-}" = "--with-dict" ]; then
    sh "$here/golden_dict.sh" --no-build
fi
psql -tAc "select 1 from information_schema.tables
            where table_schema='dict' and table_name='adit_item_tbl'" | grep -q 1 || {
    echo "dict schema not loaded -- run tests/golden_dict.sh first"; exit 1; }

echo "== materializing entry subset =="
sh "$here/make_subset.sh"

echo "== loading entries (legacy python2 dbloader) =="
conf=$(sh "$here/render_config.sh" "$py2repo" pgdb)
log="$here/build/golden_entries.log"
python2 "$here/load_entries.py" -c "$conf" -r "$py2repo" > "$log" 2>&1
tail -2 "$log"

# entries.py swallows per-file failures in a bare `except:` (MODERNIZATION_PLAN
# §4), so a "successful" run can silently have loaded nothing.  Record which
# files failed: the rewrite has to fail on exactly the same ones.
grep '^Exception on ' "$log" | sed 's|.*/||' | sort > "$here/golden/entries.failed"
echo "$(wc -l < "$here/golden/entries.failed") entries failed to load (see $log)"

for schema in macromolecules metabolomics; do
    echo "== dumping $schema =="
    rm -rf "$here/golden/$schema"
    sh "$here/dump_schema.sh" "$schema" "$here/golden/$schema" 2>/dev/null
    ( cd "$here/golden/$schema" && md5sum *.csv ) > "$here/golden/$schema.md5"
    echo "wrote $here/golden/$schema.md5 ($(wc -l < "$here/golden/$schema.md5") tables,"\
         "$(cat "$here/golden/$schema"/*.csv | wc -l) data lines)"
done
