#!/bin/sh
#
# Build the dictionary artifacts dbloader loads (dictionary.sql + dict.*.csv)
# from the CURRENT dictionary source, without touching either sibling repo.
#
#     sh tests/build_dict_inputs.sh [outdir]        (default: tests/build)
#
# Chain (see ../../ORGANIZATION.md):
#
#   nmr-star-dictionary/NMR-STAR/internal_106_source
#        │  dictionary-converter        -> a complete CSV distribution
#        ▼  nmr-star-dictionary-scripts -> dict.*.csv + dictionary.sql
#   <outdir>/csv
#
# Why not just use nmr-star-dictionary-scripts/input/ ? That is the **v3.2.6.0**
# reference copy, while the entry archive is current. Loading entries against a
# 3.2.6.0 dict schema fails on tags added since -- e.g. _Experiment.Details,
# absent from 3.2.6.0 and present in 1,005 macromolecule entries. See
# tests/README.md.
#
# The scripts repo is driven entirely from its properties file, and csv.dir is
# relative, so we assemble a work directory (symlinked conf/, generated input/,
# copied properties) and run there. Nothing is written inside the repos.

set -eu

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/.." && pwd)
top=$(cd "$repo/.." && pwd)

converter="$top/dictionary-converter"
scripts="$top/nmr-star-dictionary-scripts"
dict_repo="$top/nmr-star-dictionary/NMR-STAR"
source_dir="$dict_repo/internal_106_source"

out=${1:-$here/build}

# This directory is emptied below. Callers pass a work directory, but a typo
# or a stray argument could name a checkout or a home directory, so refuse
# anything that is obviously not scratch space.
case $out in
    ""|/|/*/..|.|..) echo "refusing to empty '$out'" >&2; exit 2 ;;
esac
if [ -e "$out/.git" ] || [ -d "$out/.hg" ]; then
    echo "refusing to empty '$out': it looks like a checkout, not a work directory" >&2
    exit 2
fi
# ${HOME:-} because `set -u` plus a condor job's empty environment turns the
# guard itself into the failure -- "HOME: parameter not set", before anything
# is built.
for keep in "${HOME:-}" / /usr /etc /var /projects; do
    [ -n "$keep" ] || continue
    [ "$(cd "$out" 2>/dev/null && pwd)" = "$keep" ] || continue
    echo "refusing to empty '$out'" >&2; exit 2
done

# Everything the scripts need now comes out of one generated distribution:
# dictionary-converter passes the hand-maintained inputs (comments.str,
# extra_enumerations.str, val_overide_add.csv, default-entry.cif, ...) through
# verbatim alongside the files it generates. Previously those four lived only in
# internal_106_distribution and had to be spliced in from a checked-in release.
#
# The files the scripts read, for reference when debugging a missing input:
#   generated: adit_item_tbl_o.csv adit_cat_grp_o.csv adit_super_grp_o.csv
#              adit_enum_{hdr,dtl,ties}.csv adit_tag_validation.csv
#              query_interface.csv xlschem_ann.csv
#   passthru:  adit_man_over.csv adit_interface_dict.txt adit_nmr_upload_tags.csv
#              nmr_cif_match.csv item_type_units.txt comments.str
#              extra_enumerations.str val_overide_add.csv default-entry.cif

version=$(sed -n 's/.*"version"[^"]*"\([^"]*\)".*/\1/p' "$source_dir/version.json")
echo "== dictionary source version $version =="

rm -rf "$out"
mkdir -p "$out/input"

# 1. CSV distribution from the current source
#
echo "== dictionary-converter =="
( cd "$converter" && ./venv/bin/python -m dict_builder.cli -s "$source_dir" -o "$out/dist" ) \
    >"$out/converter.log" 2>&1 \
    || { echo "converter FAILED -- see $out/converter.log"; tail -20 "$out/converter.log"; exit 1; }

# 2. the scripts' input dir IS the generated distribution
#
cp "$out"/dist/* "$out/input/"

# 3. run the scripts out-of-tree
#
echo "== nmr-star-dictionary-scripts =="
ln -sfn "$scripts/conf" "$out/conf"
cp "$scripts/dictionary.properties" "$out/dictionary.properties"
mkdir -p "$out/csv" "$out/production"

( cd "$out" && python3 "$scripts/__main__.py" -c dictionary.properties >"$out/scripts.log" 2>&1 ) \
    || { echo "scripts FAILED -- see $out/scripts.log"; tail -20 "$out/scripts.log"; exit 1; }

echo "wrote $out/csv: $(ls "$out"/csv/dict.*.csv | wc -l) dict.*.csv + dictionary.sql (v$version)"
