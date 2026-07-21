#!/bin/sh
#
# Render tests/loader.properties.in -> tests/build/loader.properties with
# absolute paths, so the same config works from any working directory and from
# the py2 golden worktree (which has no tests/ of its own).
#
#     sh tests/render_config.sh [repo [engine]]
#
# `repo` is the checkout the *data files* (cs_stats.sql, software.js, ...) come
# from, default this one; the entry/dict inputs always come from this
# checkout's tests/build.  `engine` is the driver starobj should use --
# psycopg2 (default) for the rewrite, pgdb for the Python 2 golden.
#
# Prints the path of the rendered file on stdout.

set -eu

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "${1:-$here/..}" && pwd)
engine=${2:-psycopg2}
out=$here/build/loader.properties

mkdir -p "$here/build"
sed -e "s|@REPO@|$repo|g" -e "s|@TESTS@|$here|g" -e "s|@ENGINE@|$engine|g" \
    "$here/loader.properties.in" > "$out"
echo "$out"
