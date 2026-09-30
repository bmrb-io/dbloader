#!/bin/sh
#
# Test the `carry` option of the swap (loader/shadow.py:carried()): a table in
# a swapped schema that the reload does not build -- web.webserver_logs -- has
# to come through the swap with its rows, sequence, indexes and grants, and a
# write to it that is in flight during the swap must not be lost.
#
#     sh tests/carry_swap.sh
#
# Runs loader/shadow.py the way job 300 does.  Needs pg-tmp running with roles
# `bmrb` and `web`; creates and drops a database called `carrytest`.
#
# Exit status: 0 if every assertion holds.

set -eu

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/.." && pwd)

: "${PGHOST:=/tmp}" ; : "${PGUSER:=bmrb}"
export PGHOST PGUSER

db=carrytest
out=$here/build/test
mkdir -p "$out"
rc=0

say() { printf '%-58s %s\n' "$1" "$2"; }
check() {
    if [ "$2" = "$3" ]; then say "OK    $1" "$2"
    else say "FAIL  $1" "got $2, expected $3"; rc=1; fi
}
rows() { psql -d "$db" -tAc "$1" 2>/dev/null || echo "ERR"; }

conf=$out/carry.properties
cat > "$conf" <<EOF
[DEFAULT]
database = $db
host = $PGHOST
user = $PGUSER
[dictionary]
schema = dict
[web]
schema = web
carry = webserver_logs
EOF

swap() { "$repo/venv/bin/python" "$repo/loader/shadow.py" -c "$conf" -v web; }

# what the reload builds: a web_new without the logs
new_release() {
    psql -q -d "$db" -c "create schema web_new" \
        -c "create table web_new.query_grid (id int)" -c "insert into web_new.query_grid values (1)"
}

dropdb --if-exists "$db" >/dev/null 2>&1 || true
createdb -O "$PGUSER" "$db"

# the live schema: the logs, and a table the reload no longer builds
psql -q -d "$db" \
    -c "create schema web" \
    -c "create table web.webserver_logs (id serial primary key, year int, hits bigint)" \
    -c "create index on web.webserver_logs (year)" \
    -c "insert into web.webserver_logs (year, hits) values (2024, 10), (2025, 20), (2026, 30)" \
    -c "grant usage on schema web to web" -c "grant select on web.webserver_logs to web" \
    -c "create table web.retired (id int)"

echo "== first release =="
new_release
swap > "$out/carry1.log" 2>&1
check "logs carried, rows intact" "$(rows 'select sum(hits) from web.webserver_logs')" 60
check "the reload's own tables are live" "$(rows 'select count(*) from web.query_grid')" 1
check "a table not carried is gone" "$(rows "select to_regclass('web.retired') is null")" t
check "index came with it" \
    "$(rows "select count(*) from pg_indexes where schemaname='web' and tablename='webserver_logs'")" 2
check "sequence came with it" \
    "$(rows "with i as (insert into web.webserver_logs (year, hits) values (2027, 40) returning id) select id from i")" 4
check "no _new/_old schemas left behind" \
    "$(rows "select count(*) from pg_namespace where nspname in ('web_new','web_old')")" 0
psql -q -d "$db" -c "grant usage on schema web to web"   # the reload's grants, in real life
check "read-only user can still select it" \
    "$(psql -d "$db" -U web -tAc 'select count(*) from web.webserver_logs' 2>&1 | tail -1)" 4

echo "== second release, with the log loader mid-transaction =="
new_release
# holds a lock on the table across the swap; the swap has to wait, not lose it
psql -q -d "$db" -c "begin" -c "update web.webserver_logs set hits = hits + 1 where year = 2027" \
    -c "select pg_sleep(3)" -c "commit" > /dev/null 2>&1 &
writer=$!
sleep 1
swap > "$out/carry2.log" 2>&1
wait $writer
check "carried again" "$(rows 'select count(*) from web.webserver_logs')" 4
check "the in-flight update survived" "$(rows 'select hits from web.webserver_logs where year = 2027')" 41

echo "== a reload that builds the table itself =="
new_release
psql -q -d "$db" -c "create table web_new.webserver_logs (id int)"
swap > "$out/carry3.log" 2>&1 && swaprc=0 || swaprc=$?
check "refused" "$swaprc" 1
check "the live logs untouched" "$(rows 'select count(*) from web.webserver_logs')" 4
check "the shadow not swapped in" "$(rows "select count(*) from pg_namespace where nspname = 'web_new'")" 1
psql -q -d "$db" -c "drop schema web_new cascade"

echo "== the table has moved elsewhere =="
psql -q -d "$db" -c "create schema logs" -c "alter table web.webserver_logs set schema logs"
new_release
swap > "$out/carry4.log" 2>&1 && swaprc=0 || swaprc=$?
check "swapped anyway" "$swaprc" 0
check "and said so" "$(grep -c 'no table web.webserver_logs to carry' "$out/carry4.log")" 1
check "the moved table is left alone" "$(rows 'select count(*) from logs.webserver_logs')" 4

dropdb "$db"
exit $rc
