#!/bin/sh
#
# Test the publish step: load_postgres_db.py loading a dbloader CSV dump into
# the serving database.
#
# There is one way to do it -- build <schema>_new from the dump's schema.sql,
# load into that, then swap it in with renames -- so what is tested is that it
# swaps the right things and, above all, that a load which fails part way
# leaves the live database exactly as it was.
#
#     sh tests/shadow_swap.sh
#
# Needs a dump to load -- `sh tests/regression.sh dumps` makes one -- and
# pg-tmp running.  Creates and drops a database called `shadowtest`.
#
# Exit status: 0 if every assertion holds.

set -eu

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/.." && pwd)

: "${PGHOST:=/tmp}" ; : "${PGUSER:=bmrb}"
export PGHOST PGUSER

dump=$here/build/test/dump--all.py3
[ -d "$dump" ] || { echo "no dump at $dump -- run: sh tests/regression.sh dumps"; exit 1; }

db=shadowtest
out=$here/build/test
rc=0
loader="/usr/bin/python3 $repo/load_postgres_db.py -H $PGHOST -U $PGUSER -d $db"

say() { printf '%-58s %s\n' "$1" "$2"; }
check() {
    if [ "$2" = "$3" ]; then say "OK    $1" "$2"
    else say "FAIL  $1" "got $2, expected $3"; rc=1; fi
}

rows() { psql -d "$db" -tAc "$1" 2>/dev/null || echo "ERR"; }

dropdb --if-exists "$db" >/dev/null 2>&1 || true
createdb -O "$PGUSER" "$db"

# a dump with one unloadable CSV, for the failure cases
broken=$out/dump-broken
rm -rf "$broken"; cp -r "$dump" "$broken"
printf 'Sf_category,Sf_framecode,ID\nnot,enough\n' > "$broken/macromolecules.Entry.csv"

echo "== into an empty database =="
$loader -g -i "$dump" > "$out/shadow1.log" 2>&1
check "schemas created" \
    "$(rows "select count(*) from pg_namespace where nspname in ('dict','macromolecules','metabolomics','web','meta','chemcomps')")" 6
check "entries loaded" "$(rows 'select count(*) from macromolecules."Entry"')" 300
check "views survived the swap" "$(rows "select count(*) from pg_views where schemaname='dict'")" 9
check "read-only user can select" \
    "$(psql -d $db -U web -tAc 'select count(*) from macromolecules."Entry"' 2>&1 | tail -1)" 300

echo "== again: the live schemas have to be renamed out of the way =="
$loader -g -i "$dump" > "$out/shadow2.log" 2>&1
check "entries still there" "$(rows 'select count(*) from macromolecules."Entry"')" 300
check "no _new/_old schemas left behind" \
    "$(rows "select count(*) from pg_namespace where nspname like '%\\_new' or nspname like '%\\_old'")" 0

echo "== a broken CSV: the live database must not change =="
set +e
$loader -g -i "$broken" > "$out/shadow3.log" 2>&1
set -e
check "live entries untouched" "$(rows 'select count(*) from macromolecules."Entry"')" 300
check "refused to swap" "$(grep -c 'NOT swapping' "$out/shadow3.log")" 1

echo "== one schema only: swap that one, throw the other shadows away =="
$loader -s dict -i "$dump" > "$out/decide2.log" 2>&1
check "single-schema load swaps only that schema" \
    "$(grep -c 'building dict_new and swapping it in' "$out/decide2.log")" 1
check "the other schemas were not swapped in empty" \
    "$(rows 'select count(*) from macromolecules."Entry"')" 300
check "dict is still there" "$(rows 'select count(*) from dict.adit_item_tbl')" 6760
check "no shadow schemas left over" \
    "$(rows "select count(*) from pg_namespace where nspname like '%\\_new'")" 0

echo "== the retired databases, and dumps that cannot be swapped =="
# updater_dag jobs 160 and 260 still pass these; they must stay green no-ops
for retired in bmrb metabolomics; do
    set +e
    /usr/bin/python3 "$repo/load_postgres_db.py" -H "$PGHOST" -U "$PGUSER" \
        -d $retired -g > "$out/retired.$retired.log" 2>&1
    check "-d $retired is a green no-op" "$?" 0
    set -e
    check "  and says so" "$(grep -c 'is retired' "$out/retired.$retired.log")" 1
done

# an old-style dump has unqualified CSVs: there is no path for it any more
old=$here/build/test/dump--macromol.py3
if [ -d "$old" ]; then
    set +e
    $loader -i "$old" > "$out/decide3.log" 2>&1
    check "old-style dump is refused" "$?" 1
    set -e
    check "  with a usable message" "$(grep -c 'no schema prefix' "$out/decide3.log")" 1
fi

dropdb "$db"
exit $rc
