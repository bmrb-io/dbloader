#!/bin/sh
#
# Dump every table of a PostgreSQL schema to deterministic CSV, one file per
# table, for golden/test comparison (MODERNIZATION_PLAN.md §6).
#
# Rows are ordered by every column (by ordinal), so the dump does not depend on
# insertion order, on the physical row order, or on which loader produced it.
# Views are skipped -- they are derived from the tables.
#
#     sh tests/dump_schema.sh <schema> <outdir>
#
# Connection comes from the usual PG* environment (PGHOST/PGUSER/PGDATABASE),
# e.g. after `eval "$(pg-tmp)"`:  PGHOST=/tmp PGUSER=bmrb PGDATABASE=bmrb
#
# Exit status: 0 on success.

set -eu

schema=${1:?usage: dump_schema.sh <schema> <outdir>}
outdir=${2:?usage: dump_schema.sh <schema> <outdir>}

mkdir -p "$outdir"

tables=$(psql -tAc "select table_name from information_schema.tables
                     where table_schema = '$schema' and table_type = 'BASE TABLE'
                     order by table_name")

for t in $tables; do
    ncols=$(psql -tAc "select count(*) from information_schema.columns
                        where table_schema = '$schema' and table_name = '$t'")
    order=$(seq -s, 1 "$ncols")
    psql -q -c "\\copy (select * from \"$schema\".\"$t\" order by $order) to '$outdir/$t.csv' with csv"
    # lines, not rows: CSV fields may contain embedded newlines (e.g. dict.comments)
    printf "%-28s %s lines\n" "$t" "$(wc -l < "$outdir/$t.csv")" >&2
done
