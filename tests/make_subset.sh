#!/bin/sh
#
# Build the fixed entry subset the golden/regression runs use.
#
# The full corpora (14,772 macromolecule + 3,629 metabolomics entries, ~1.2 GB)
# are far too slow to load on every iteration, so both goldens are taken over a
# deterministic stride sample: sort the entry directories by name, keep every
# Nth.  Striding rather than head -n keeps the sample spread over the whole
# archive (old hand-curated entries, ADIT-era entries, recent depositions) --
# `head` would give 300 consecutive low-numbered legacy entries.
#
# The sample is materialized as a tree of symlinks so `entries.py:_gen_file_list`
# reads it unmodified, and the chosen IDs are written to tests/subset.*.txt,
# which IS committed: it is what makes the golden reproducible.
#
#     sh tests/make_subset.sh [outdir]        # default tests/build/entries
#
# Exit status: 0 on success.

set -eu

here=$(cd "$(dirname "$0")" && pwd)
out=${1:-$here/build/entries}

# ~/git, i.e. the parent of the `dictionary` checkout these repos live under
gitdir=$(cd "$here/../../.." && pwd)

# source corpora (owner-provided, outside the repos) and sample sizes
macro_src=${MACRO_SRC:-$gitdir/query-bmrb/bmrb_entries}
metab_src=${METAB_SRC:-$gitdir/query-bmrb/bmrb_metabolomics}
macro_n=${MACRO_N:-300}
metab_n=${METAB_N:-300}

for d in "$macro_src" "$metab_src" ; do
    [ -d "$d" ] || { echo "no such corpus: $d" >&2 ; exit 1 ; }
done

# stride-sample $2 of the directories in $1 into $3, listing them in $4
sample() {
    src=$1 ; n=$2 ; dst=$3 ; list=$4
    rm -rf "$dst" ; mkdir -p "$dst"
    ls "$src" | sort | awk -v n="$n" '
        { a[NR] = $0 }
        END {
            if (NR < n) n = NR
            for (i = 0; i < n; i++) print a[int(i * NR / n) + 1]
        }' > "$list"
    while read -r e ; do
        ln -s "$src/$e" "$dst/$e"
    done < "$list"
    printf "%-14s %5s of %5s entries -> %s\n" \
        "$(basename "$dst")" "$(wc -l < "$list")" "$(ls "$src" | wc -l)" "$dst" >&2
}

# where the ID lists land; only the canonical tests/ copies are committed
listdir=${LISTDIR:-$here}

sample "$macro_src" "$macro_n" "$out/macromolecules" "$listdir/subset.macromolecules.txt"
sample "$metab_src" "$metab_n" "$out/metabolomics"   "$listdir/subset.metabolomics.txt"
