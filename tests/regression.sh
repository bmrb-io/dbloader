#!/bin/sh
#
# Run the Python 3 loader and diff the database it builds, row for row, against
# the golden built by the legacy Python 2 code (MODERNIZATION_PLAN.md §6).
#
# Both sides load the same inputs into the same throwaway PostgreSQL and are
# dumped by tests/dump_schema.sh, which orders every table by every column --
# so a match means the two loaders produce the same rows, not merely the same
# row counts.
#
# Prerequisites: the goldens exist (tests/golden_dict.sh, tests/golden_entries.sh),
# pg-tmp is running with roles `bmrb`/`web` and database `bmrb`, and the py3
# starobj + sas are importable.  See tests/README.md.
#
#     sh tests/regression.sh [dict|entries|truncate|dumps]  # default: dict + entries
#
# `truncate` re-runs the entry load over the schema the previous run left
# behind, using the truncate-and-reload path the rewrite implemented (the
# legacy code raised "FIXME!!!! Not implemented" there), and checks it lands on
# the same golden.  It needs an entries run before it.
#
# `dumps` needs no golden: it points the old dumper and the new one at the same
# database and diffs their output directly.
#
# Exit status: 0 if every schema matches its golden.

set -eu

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/.." && pwd)
what=${1:-all}

: "${PGHOST:=/tmp}" ; : "${PGUSER:=bmrb}" ; : "${PGDATABASE:=bmrb}"
export PGHOST PGUSER PGDATABASE

# python3 with psycopg2, and the py3 starobj/sas
python=${PYTHON:-$repo/venv/bin/python}
gitdir=$(cd "$repo/../.." && pwd)
PYTHONPATH=${PYTHONPATH:-$gitdir/starobj:$gitdir/sas/python}
export PYTHONPATH

conf=$(sh "$here/render_config.sh")
out=$here/build/test
rc=0

# dump schema $1 and compare its fingerprints with the golden
check() {
    schema=$1
    [ -f "$here/golden/$schema.md5" ] || { echo "no golden for $schema -- run tests/golden_*.sh"; exit 1; }
    rm -rf "$out/$schema"
    sh "$here/dump_schema.sh" "$schema" "$out/$schema" 2>/dev/null
    if ( cd "$out/$schema" && md5sum *.csv ) | diff -u "$here/golden/$schema.md5" - > "$out/$schema.diff"; then
        echo "OK    $schema matches golden ($(wc -l < "$here/golden/$schema.md5") tables)"
    else
        echo "FAIL  $schema differs from golden:"
        # the md5 diff names the tables; show the first rows that actually
        # differ, if this run rebuilt the golden dumps (they are not committed)
        awk '/^\+[0-9a-f]{32}/ { print $2 }' "$out/$schema.diff" | while read -r t; do
            echo "      --- $t"
            [ -f "$here/golden/$schema/$t" ] || { echo "      (no golden dump -- rerun tests/golden_*.sh)"; continue; }
            diff -u "$here/golden/$schema/$t" "$out/$schema/$t" | sed -n '3,8p' | sed 's/^/      /'
        done
        rc=1
    fi
}

mkdir -p "$out"

if [ "$what" = "all" ] || [ "$what" = "dict" ]; then
    echo "== loading dict schema (python3) =="
    "$python" "$repo/loader/dictionary.py" -c "$conf" -d "$here/build/csv"
    check dict
fi

if [ "$what" = "all" ] || [ "$what" = "entries" ] || [ "$what" = "truncate" ]; then
    if [ "$what" = "truncate" ]; then
        echo "== reloading entries into the existing tables (python3, --truncate) =="
        "$python" "$here/load_entries.py" -c "$conf" --truncate > "$out/entries.log" 2>&1 || true
    else
        echo "== loading entries (python3) =="
        "$python" "$here/load_entries.py" -c "$conf" > "$out/entries.log" 2>&1 || true
    fi
    tail -2 "$out/entries.log"

    grep '^Exception on ' "$out/entries.log" | sed 's|.*/||' | sort > "$out/entries.failed"
    if diff -u "$here/golden/entries.failed" "$out/entries.failed" > /dev/null; then
        echo "OK    the same $(wc -l < "$out/entries.failed") entries failed to load as in the golden"
    else
        echo "FAIL  a different set of entries failed to load:"
        diff -u "$here/golden/entries.failed" "$out/entries.failed" | sed -n '3,20p' | sed 's/^/      /'
        rc=1
    fi

    check macromolecules
    check metabolomics
fi

# The dump path needs no golden of its own: both dumpers read whatever is in
# the database right now, so running them side by side compares them directly.
#
if [ "$what" = "dumps" ]; then
    py2repo=$(cd "$repo/../.golden-stack/dbloader" && pwd)
    py2conf=$(sh "$here/render_config.sh" "$py2repo" pgdb)
    py3conf=$(sh "$here/render_config.sh")

    for mode in "" --macromol --metabol; do
        name=${mode:---all}
        echo "== dumping $name =="
        rm -rf "$out/dump$name.py2" "$out/dump$name.py3"

        ( cd "$py2repo" \
          && PYTHONPATH="$repo/../.golden-stack/sas/python:$repo/../.golden-stack/starobj" \
             python2 loader/csvdump.py -c "$py2conf" -d "$out/dump$name.py2" $mode )
        "$python" "$repo/loader/csvio.py" -c "$py3conf" -d "$out/dump$name.py3" $mode

        # pg_dump stamps each dump with a random \restrict token, so schema.sql
        # cannot be compared byte for byte; everything else can.
        if diff -r -I '^\\\(un\)\?restrict ' "$out/dump$name.py2" "$out/dump$name.py3" \
             > "$out/dump$name.diff"; then
            echo "OK    dump $name matches the legacy dumper" \
                 "($(ls "$out/dump$name.py2" | wc -l) files)"
        else
            echo "FAIL  dump $name differs from the legacy dumper:"
            sed -n '1,20p' "$out/dump$name.diff" | sed 's/^/      /'
            rc=1
        fi
    done
fi

exit $rc
