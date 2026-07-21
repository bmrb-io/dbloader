#!/bin/sh
#
# Test the publish step: load_postgres_db.py loading a dbloader CSV dump into a
# serving database, both ways round.
#
#   --shadow   build <schema>_new from the dump's schema.sql, load into that,
#              swap it in with renames.  Atomic, and rebuilds the schema from
#              the dictionary (schema.sql came from the build database, whose
#              schema dbloader generated from the dictionary).
#   default    truncate and refill the live tables in place.
#
# The cases that matter are the failures: neither mode may leave the serving
# database emptier than it found it.  Before ON_ERROR_STOP was added, a failed
# \copy left the table it was loading empty on the live server.
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

echo "== --shadow into an empty database =="
$loader --shadow -g -i "$dump" > "$out/shadow1.log" 2>&1
check "schemas created" \
    "$(rows "select count(*) from pg_namespace where nspname in ('dict','macromolecules','metabolomics','web','meta','chemcomps')")" 6
check "entries loaded" "$(rows 'select count(*) from macromolecules."Entry"')" 300
check "views survived the swap" "$(rows "select count(*) from pg_views where schemaname='dict'")" 9
check "read-only user can select" \
    "$(psql -d $db -U web -tAc 'select count(*) from macromolecules."Entry"' 2>&1 | tail -1)" 300

echo "== --shadow again: the live schemas have to be renamed out of the way =="
$loader --shadow -g -i "$dump" > "$out/shadow2.log" 2>&1
check "entries still there" "$(rows 'select count(*) from macromolecules."Entry"')" 300
check "no _new/_old schemas left behind" \
    "$(rows "select count(*) from pg_namespace where nspname like '%\\_new' or nspname like '%\\_old'")" 0

echo "== --shadow with a broken CSV: the live database must not change =="
set +e
$loader --shadow -g -i "$broken" > "$out/shadow3.log" 2>&1
set -e
check "live entries untouched" "$(rows 'select count(*) from macromolecules."Entry"')" 300
check "refused to swap" "$(grep -c 'NOT swapping' "$out/shadow3.log")" 1

echo "== truncate-in-place with a broken CSV: the table must keep its rows =="
set +e
$loader -i "$broken" > "$out/shadow4.log" 2>&1
set -e
check "live entries untouched" "$(rows 'select count(*) from macromolecules."Entry"')" 300

echo "== truncate-in-place with a good dump still loads =="
psql -q -d "$db" -c 'delete from macromolecules."Entry"'
$loader -i "$dump" > "$out/shadow5.log" 2>&1
check "entries reloaded" "$(rows 'select count(*) from macromolecules."Entry"')" 300

dropdb "$db"
exit $rc
