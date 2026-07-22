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
# Prerequisites: the goldens exist (tests/golden_dict.sh, tests/golden_entries.sh)
# and pg-tmp is running with roles `bmrb`/`web` and database `bmrb`.  See
# tests/README.md.
#
#     sh tests/regression.sh [dict|entries|dumps]           # default: dict + entries
#
# There used to be a `truncate` mode here, for the truncate-and-reload path the
# rewrite implemented over the live schema.  Both in-place paths are gone --
# every load now builds <schema>_new and is swapped in (loader/shadow.py) --
# so there is nothing left for it to exercise.
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

# python3 with psycopg2 and pynmrstar
python=${PYTHON:-$repo/venv/bin/python}

conf=$(sh "$here/render_config.sh")
out=$here/build/test
rc=0

# `entry_saveframes` is the one table the rewrite deliberately does not
# reproduce, so it is compared by check_saveframes() instead of by fingerprint:
#
#   `line`     NULL now.  pynmrstar reports no line numbers, and the ones sas
#              reported were wrong anyway -- off by one per preceding saveframe.
#   `category` fixed.  starobj set it with `update entry_saveframes set
#              category=... where name=...`, which matches by saveframe NAME
#              across every entry loaded so far, so entries sharing a saveframe
#              name overwrote each other.  59 of 5690 rows in the golden
#              contradict the Sf_category the entry itself declares.
#
# Every other column of it, and every other table, still has to match exactly --
# except `Entity_assembly`, below.
EXCEPT=entry_saveframes.csv

# `Entity_assembly` differs for the same kind of reason: sas stripped the `$`
# off a saveframe pointer lexically, so it also stripped it from
# Entity_assembly_name, which the dictionary does not call a pointer
# (sfpointerflg='N') and which is a free-text name.  203 metabolomics entries
# have a framecode in that field by mistake and now keep the `$` they were
# deposited with -- 18 of them are in this subset.  See DATA_REMEDIATION.md.
EXCEPT2=Entity_assembly.csv

# dump schema $1 and compare its fingerprints with the golden
check() {
    schema=$1
    [ -f "$here/golden/$schema.md5" ] || { echo "no golden for $schema -- run tests/golden_*.sh"; exit 1; }
    rm -rf "$out/$schema"
    sh "$here/dump_schema.sh" "$schema" "$out/$schema" 2>/dev/null
    grep -v " $EXCEPT\$" "$here/golden/$schema.md5" | grep -v " $EXCEPT2\$" > "$out/$schema.golden.md5"
    ( cd "$out/$schema" && md5sum *.csv ) | grep -v " $EXCEPT\$" | grep -v " $EXCEPT2\$" > "$out/$schema.new.md5"
    n=$(wc -l < "$out/$schema.golden.md5")
    # the dict schema has no entry_saveframes, so no exception to mention
    skipped=$(($(wc -l < "$here/golden/$schema.md5") - n))
    if [ "$skipped" -gt 0 ]; then
        note=" + $skipped checked separately"
    else
        note=""
    fi
    if diff -u "$out/$schema.golden.md5" "$out/$schema.new.md5" > "$out/$schema.diff"; then
        echo "OK    $schema matches golden ($n tables$note)"
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

# the two assertions that replace a golden comparison for entry_saveframes
check_saveframes() {
    schema=$1
    golden=$here/golden/$schema/entry_saveframes.csv
    new=$out/$schema/entry_saveframes.csv

    # 1. same rows as the golden, ignoring `line`.  Differences are expected --
    #    but only in `category`, and only where the golden is wrong; check 2 is
    #    what decides.
    if [ -f "$golden" ]; then
        sed 's/,[^,]*$//' "$golden" > "$out/$schema.sf.golden"
        sed 's/,[^,]*$//' "$new"    > "$out/$schema.sf.new"
        if ! diff -q "$out/$schema.sf.golden" "$out/$schema.sf.new" > /dev/null; then
            echo "      $(diff "$out/$schema.sf.golden" "$out/$schema.sf.new" | grep -c '^<') rows differ from the golden (excluding line):"
            diff "$out/$schema.sf.golden" "$out/$schema.sf.new" | head -4 | sed 's/^/      /'
        fi
    fi

    # 2. every category agrees with the Sf_category the entry itself declares --
    #    which is the thing the golden gets wrong, so this is the real check
    union=$(psql -tAc "select string_agg(format('select \"Sf_ID\"::int, \"Sf_category\" from $schema.%I',
                                               table_name), ' union all ')
                        from information_schema.columns
                       where table_schema = '$schema' and column_name = 'Sf_category'")
    bad=$(psql -tAc "select count(*) from $schema.entry_saveframes s
                       join ($union) t(sfid, cat) on t.sfid = s.sfid
                      where s.category is distinct from t.cat")
    total=$(psql -tAc "select count(*) from $schema.entry_saveframes")
    if [ "$bad" = "0" ]; then
        echo "OK    $schema.entry_saveframes: all $total categories agree with the entries' own Sf_category"
    else
        echo "FAIL  $schema.entry_saveframes: $bad of $total categories contradict the entry's Sf_category"
        rc=1
    fi

    if [ "$(awk -F, '{print $NF}' "$new" | grep -c .)" != "0" ]; then
        echo "FAIL  $schema.entry_saveframes: expected the line column to be NULL"
        rc=1
    fi
}

# Entity_assembly must differ from the golden in exactly one way: the `$` that
# sas stripped from Entity_assembly_name is back, and nothing else moved.
check_entity_assembly() {
    schema=$1
    golden=$here/golden/$schema/$EXCEPT2
    new=$out/$schema/$EXCEPT2
    [ -f "$golden" ] || return 0
    [ -f "$new" ] || return 0

    # every $-name must be the row's own Entity_label with the $ still on it --
    # Entity_label IS a pointer, so it was stripped; the name was not
    bad=$(psql -tAc "select count(*) from $schema.\"Entity_assembly\"
                      where \"Entity_assembly_name\" like '\$%'
                        and \"Entity_assembly_name\" is distinct from '\$' || \"Entity_label\"")
    kept=$(psql -tAc "select count(*) from $schema.\"Entity_assembly\"
                       where \"Entity_assembly_name\" like '\$%'")
    other=$(diff "$golden" "$new" | grep -c '^<' || true)

    if [ "$bad" = "0" ]; then
        echo "OK    $schema.$EXCEPT2: $kept rows keep a \$ in Entity_assembly_name"\
             "(deposited that way; $other rows differ from the golden, all in that column)"
    else
        echo "FAIL  $schema.$EXCEPT2: $bad rows have a \$ name that is not a copy of Entity_label"
        rc=1
    fi
}

mkdir -p "$out"

if [ "$what" = "all" ] || [ "$what" = "dict" ]; then
    echo "== loading dict schema (python3) =="
    "$python" "$repo/loader/dictionary.py" -c "$conf" -d "$here/build/csv"
    check dict
fi

if [ "$what" = "all" ] || [ "$what" = "entries" ]; then
    echo "== loading entries (python3) =="
    "$python" "$here/load_entries.py" -c "$conf" > "$out/entries.log" 2>&1 && loadrc=0 || loadrc=$?
    tail -2 "$out/entries.log"

    # a crash in the loader must not read as "0 entries failed" further down:
    # per-entry failures are counted, anything else kills the run
    if [ "$loadrc" -ne 0 ]; then
        echo "FAIL  the loader exited $loadrc:"
        tail -20 "$out/entries.log" | sed 's/^/      /'
        exit 1
    fi

    grep '^Exception on ' "$out/entries.log" | sed 's|.*/||' | sort > "$out/entries.failed"
    if diff -u "$here/golden/entries.failed" "$out/entries.failed" > /dev/null; then
        echo "OK    the same $(wc -l < "$out/entries.failed") entries failed to load as in the golden"
    else
        echo "FAIL  a different set of entries failed to load:"
        diff -u "$here/golden/entries.failed" "$out/entries.failed" | sed -n '3,20p' | sed 's/^/      /'
        rc=1
    fi

    check macromolecules
    check_saveframes macromolecules
    check_entity_assembly macromolecules
    check metabolomics
    check_saveframes metabolomics
    check_entity_assembly metabolomics
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
