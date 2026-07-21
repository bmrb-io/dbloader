#!/bin/sh
#
# Phase 0 harness: produce the *golden* `dict` schema with the UNMODIFIED
# legacy Python 2 dbloader, then dump it for comparison against the Python 3
# rewrite (MODERNIZATION_PLAN.md §6, §8).
#
# Steps:
#   1. build the dictionary artifacts (dictionary.sql + dict.*.csv) from the
#      CURRENT dictionary source -- see tests/build_dict_inputs.sh;
#   2. run loader/dictionary.py under Python 2.7 against a throwaway
#      PostgreSQL, with the golden-stack PYTHONPATH (see below);
#   3. dump every dict table to deterministic CSV + md5s under tests/golden/.
#
# The golden stack lives in ../.golden-stack (created once, outside the repos):
#   dbloader @ 41293e9 (pre-port master)        git worktree
#   starobj  @ 89925a1 (last pgdb/py2 commit)   git worktree
#   sas      @ 34243e9 (py2)                    git worktree
#
# The loader is run out of that worktree, not out of this checkout, so the
# golden stays reproducible once the port has rewritten loader/ in place.
#
# Prerequisites:  pg-tmp already running, roles `bmrb` (superuser) and `web`,
# database `bmrb` (see tests/README.md);  python2 with `ply` and `PyGreSQL`.
#
#     sh tests/golden_dict.sh [--no-build]
#
# Exit status: 0 on success.

set -eu

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/.." && pwd)
top=$(cd "$repo/.." && pwd)

golden="$top/.golden-stack"
build="$here/build"
py2repo="$golden/dbloader"
[ -d "$py2repo/loader" ] || { echo "no py2 worktree at $py2repo -- see tests/README.md"; exit 1; }

PYTHONPATH="$golden/sas/python:$golden/starobj"
export PYTHONPATH
: "${PGHOST:=/tmp}" ; : "${PGUSER:=bmrb}" ; : "${PGDATABASE:=bmrb}"
export PGHOST PGUSER PGDATABASE

# 1. dictionary artifacts (skip with --no-build to reuse an existing build/)
#
if [ "${1:-}" != "--no-build" ]; then
    sh "$here/build_dict_inputs.sh" "$build"
fi
[ -f "$build/csv/dictionary.sql" ] || { echo "no $build/csv -- run without --no-build"; exit 1; }

# 2. the golden load, by the legacy code, unmodified
#
echo "== loading dict schema (legacy python2 dbloader) =="
conf=$(sh "$here/render_config.sh" "$py2repo" pgdb)
( cd "$py2repo" && python2 loader/dictionary.py -c "$conf" -d "$build/csv" )

# 3. dump + fingerprint
#
echo "== dumping dict schema =="
rm -rf "$here/golden/dict"
sh "$here/dump_schema.sh" dict "$here/golden/dict"
( cd "$here/golden/dict" && md5sum *.csv ) > "$here/golden/dict.md5"
echo "wrote $here/golden/dict.md5 ($(wc -l < "$here/golden/dict.md5") tables)"
