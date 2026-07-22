#!/usr/bin/env python3
#
# Build a reload alongside the live schemas and swap it in atomically.
#
# The problem: every load stage starts with `drop schema <s> cascade` (see
# entries.py) and only grants the read-only user access at the end, so for the
# length of a reload -- an hour or so for the macromolecule archive -- readers
# get "permission denied for schema macromolecules", and before that a
# half-loaded database.
#
# The fix is the one load_postgres_db.py already applies on the serving host:
# build everything as <schema>_new while the live schemas carry on serving,
# then rename them into place in a single transaction that touches no rows.
# Readers block for microseconds instead of an hour, and a load that dies part
# way leaves the live database untouched.
#
# This is not optional and not configurable.  There used to be a second path
# that dropped each schema and refilled it in place (and a third that truncated
# and refilled), which is what produced the outage; they are gone, for the same
# reason load_postgres_db.py dropped its in-place path -- "one way to load, and
# it is the safe one".  Every stage builds `<schema>_new` and the caller swaps.
#
# `schema` in the config keeps naming the schema that ends up live; SUFFIX is
# what the stage actually builds into in the meantime.
#
# Costs about twice the disk of the schema being rebuilt, at peak -- the same
# price load_postgres_db.py already pays on the serving host.
#
# Not covered: `chemcomps` and `meta`.  Both load from sources that are not
# reachable outside BMRB (the ccdb database on octopus, and static CSVs), so
# the change could not be exercised here; they still load in place.
#

import os
import re
import sys

_UP = os.path.abspath(os.path.join(os.path.split(__file__)[0], ".."))
sys.path.append(_UP)
from loader import db

SUFFIX = "_new"
RETIRING = "_old"


def suffix(config=None, section=None):
    """The shadow suffix.  Constant; the arguments are accepted for callers
    that used to ask per-section."""

    return SUFFIX


def target(config, section):
    """The schema a stage builds into: `schema` plus the shadow suffix."""

    return config.get(section, "schema") + SUFFIX


def shadow_of(schema):
    """The shadow name for a live schema name."""

    return schema + SUFFIX


def live_name(schema):
    """The live name for a schema name that may already be a shadow."""

    return schema[:-len(SUFFIX)] if schema.endswith(SUFFIX) else schema


def live(config, section):
    """The schema a stage's output ends up as, once swapped in."""

    return config.get(section, "schema")


# Rewriting a DDL script to build the shadow instead of the live schema.
#
# The scripts name their schema themselves -- dictionary.sql opens with `drop
# schema if exists dict cascade; create schema dict;` and then `set search_path
# = dict`, and webschema.sql does the same for `web` -- so pointing a stage at
# a different schema means rewriting the script, not just passing a name.
#
# This is deliberately NOT a blanket rename of the schema name: dictionary.sql
# also contains `create view dict as select * from adit_item_tbl`, a view named
# `dict` inside schema `dict`, which must keep its name.  Only the three places
# a schema name can appear as a schema are rewritten:
#
#   1. `... schema <name>`      create/drop schema, grant ... on schema,
#                               alter default privileges in schema
#   2. `search_path = <name>`   the unqualified section that follows
#   3. `<name>.`                qualified references
#
# Case 3 is what makes the cross-schema views work.  dictionary.sql builds
# `validict` out of views over `dict` (`create view sfcats as select * from
# dict.validator_sfcats`), and a view binds to the *table OID* at creation.
# Rewritten to `dict_new.validator_sfcats`, the view binds to the shadow
# table, and the later rename of dict_new -> dict leaves that OID untouched --
# so the view is correct on the other side of the swap.  Left as `dict.`, it
# would bind to the live dictionary that the swap is about to drop.
#
_SCHEMA_KW = r"(?:\bschema\s+(?:if\s+(?:not\s+)?exists\s+)?)"
_PATH_KW = r"(?:\bsearch_path\s*=\s*)"


def declared_schemas(path):
    """Schema names a DDL script creates, in the order it creates them."""

    pat = re.compile(r"\bcreate\s+schema\s+(?:if\s+not\s+exists\s+)?([A-Za-z_][A-Za-z0-9_]*)",
                     re.IGNORECASE)
    seen = []
    with open(path) as f:
        for m in pat.finditer(f.read()):
            if m.group(1) not in seen:
                seen.append(m.group(1))
    return seen


def rewrite_sql(infile, outfile, mapping):
    """Copy a SQL script, renaming the schemas in `mapping` ({old: new}).

    Returns the number of substitutions made.
    """

    subs = []
    # longest first, so `validict` is never half-matched by a `dict` rule
    for old in sorted(mapping, key=len, reverse=True):
        new = mapping[old]
        q = re.escape(old)
        subs.append((re.compile(r"(%s)%s\b" % (_SCHEMA_KW, q), re.IGNORECASE),
                     lambda m, new=new: m.group(1) + new))
        subs.append((re.compile(r"(%s)%s\b" % (_PATH_KW, q), re.IGNORECASE),
                     lambda m, new=new: m.group(1) + new))
        subs.append((re.compile(r"\b%s\." % (q,)), lambda m, new=new: new + "."))

    n = 0
    with open(infile) as f, open(outfile, "w") as out:
        for line in f:
            for (pat, rep) in subs:
                (line, k) = pat.subn(rep, line)
                n += k
            out.write(line)
    return n


# psql's `\copy <table> to '<path>'` writes on the client.  cs_stats.sql ends
# with seven of them, pointed at /projects/BMRB/public/ftp/..., which exists
# only on the production host; redirecting them is what lets the script run
# anywhere.  Only the directory changes -- the file names are the published
# ones and callers depend on them.
_COPY_TO = re.compile(r"""(^\s*\\copy\s+.*?\s+to\s+')([^']*/)?([^'/]+)(')""",
                      re.IGNORECASE | re.MULTILINE)


def rewrite_copy_targets(text, outdir):
    """Repoint `\\copy ... to '<dir>/<file>'` at `outdir`."""

    return _COPY_TO.sub(
        lambda m: m.group(1) + os.path.join(outdir, m.group(3)) + m.group(4), text)


def rewritten_copy(script, mapping, workdir, copy_outdir=None, verbose=False):
    """`script` rewritten per `mapping` into `workdir`; the original if nothing to do."""

    if not mapping and copy_outdir is None:
        return script

    if not os.path.isdir(workdir):
        os.makedirs(workdir)
    out = os.path.join(workdir, "shadow." + os.path.basename(script))
    n = rewrite_sql(script, out, mapping)

    if n == 0 and mapping:
        raise Exception("%s: nothing to rewrite for %s -- wrong script?"
                        % (script, sorted(mapping),))

    if copy_outdir is not None:
        if not os.path.isdir(copy_outdir):
            os.makedirs(copy_outdir)
        with open(out) as f:
            text = f.read()
        with open(out, "w") as f:
            f.write(rewrite_copy_targets(text, copy_outdir))

    if verbose:
        sys.stdout.write("shadow: %s -> %s (%d substitutions: %s%s)\n"
                         % (script, out, n,
                            ", ".join("%s->%s" % kv for kv in sorted(mapping.items())),
                            "" if copy_outdir is None else "; \\copy -> " + copy_outdir,))
    return out


# The swap itself.
#
def swap(dsn, schemas, config=None, verbose=False):
    """Rename every <schema><suffix> to <schema>, in one transaction.

    `schemas` is a list of (live, shadow) pairs.  Nothing is dropped inside
    the transaction: the old schemas are renamed aside and dropped afterwards,
    so a multi-gigabyte `drop ... cascade` cannot hold the exclusive locks that
    readers are queued behind.

    Returns the list of schemas actually swapped.
    """

    pairs = [(l, s) for (l, s) in schemas if l != s]
    if not pairs:
        return []

    with db.connection(dsn) as conn:
        with conn.cursor() as curs:
            missing = []
            for (_, shadow) in pairs:
                curs.execute("select 1 from pg_namespace where nspname = %s", (shadow,))
                if curs.fetchone() is None:
                    missing.append(shadow)
            if missing:
                raise Exception("nothing to swap in: no schema %s"
                                % (", ".join(missing),))

            # one transaction, no rows touched
            retired = []
            for (livename, shadow) in pairs:
                curs.execute("select 1 from pg_namespace where nspname = %s", (livename,))
                if curs.fetchone() is not None:
                    old = livename + RETIRING
                    curs.execute("drop schema if exists %s cascade" % (db.quote(old),))
                    curs.execute("alter schema %s rename to %s"
                                 % (db.quote(livename), db.quote(old),))
                    retired.append(old)
                curs.execute("alter schema %s rename to %s"
                             % (db.quote(shadow), db.quote(livename),))
                if verbose:
                    sys.stdout.write("swap: %s -> %s\n" % (shadow, livename,))
        conn.commit()

    # outside the transaction, so it holds no locks the readers care about
    if retired:
        with db.connection(dsn, autocommit=True) as conn:
            with conn.cursor() as curs:
                for old in retired:
                    if verbose:
                        sys.stdout.write("swap: dropping %s\n" % (old,))
                    curs.execute("drop schema if exists %s cascade" % (db.quote(old),))

    return [l for (l, _) in pairs]

#
# main -- swap the shadow schemas in, after every stage has loaded
#
if __name__ == "__main__":

    import argparse
    from configparser import ConfigParser

    ap = argparse.ArgumentParser(
        description="rename <schema><suffix> to <schema>, atomically")
    ap.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False)
    ap.add_argument("-c", "--config", dest="conffile", required=True)
    ap.add_argument("-s", "--section", dest="section", default="dictionary",
                    help="config section to take the connection from (default: %(default)s)")
    ap.add_argument("--suffix", dest="suffix", default=None,
                    help="shadow suffix (default: the `shadow` option of --section)")
    ap.add_argument("schemas", nargs="+",
                    help="live schema names to swap the shadows in for")
    args = ap.parse_args()

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    sfx = args.suffix if args.suffix is not None else suffix(cp, args.section)
    if not sfx:
        sys.stderr.write("no shadow suffix: nothing to swap\n")
        sys.exit(0)

    done = swap(db.dsn(cp, args.section), [(s, s + sfx) for s in args.schemas],
                config=cp, verbose=args.verbose)
    sys.stdout.write("swapped in: %s\n" % (", ".join(done),))

#
# eof
