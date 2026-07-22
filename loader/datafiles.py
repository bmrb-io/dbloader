#!/usr/bin/env python3
#
# The static data files that ship with dbloader, found relative to the code.
#
# `cs_stats.sql`, `webschema.sql`, the three JSON name maps, the two web CSVs
# and the metabolomics `meta` files all live in this repo, next to the code
# that loads them.  They used to have to be named in the properties file by
# absolute path -- one line each, seven lines of a production config that said
# nothing except where the checkout is:
#
#     csstats = /projects/BMRB/software/dbloader3/cs_stats.sql
#     software_mapfile = /projects/BMRB/software/dbloader3/software.js
#     ...
#
# which then went stale the moment the checkout moved.  They now default to
# the copy alongside the code, so a config only has to mention one if it
# deliberately points somewhere else.
#
# Overrides are still honoured, and a relative override is taken relative to
# the repo rather than to the current directory -- the loader is run from
# condor, from cron and by hand, and those disagree about what "here" means.
#

import os
import sys

# the checkout this module is part of
REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

# (section, option) -> name in the repo
DEFAULTS = {
    ("macromolecules", "csstats"): "cs_stats.sql",
    ("macromolecules", "software_mapfile"): "software.js",
    ("macromolecules", "software_authors_mapfile"): "swauthors.js",
    ("macromolecules", "task_mapfile"): "task.js",
    ("web", "ddlfile"): "webschema.sql",
    ("web", "apiddl"): "webapi.sql",
    ("web", "csvfiles"): "web.pulsefilelist.csv\nweb.termlist.csv",
    ("meta", "ddlfile"): "metabolomics_meta_schema.sql",
    ("meta", "csvdir"): "metabolomics_meta_files",
}


def default_for(section, option):
    """The shipped value for a config option, or None if it has no default."""

    return DEFAULTS.get((section, option))


def _resolve(value):
    return value if os.path.isabs(value) else os.path.join(REPO, value)


def path(config, section, option, must_exist=True):
    """Path to one data file: the config's value if it has one, else the
    copy that ships with the code."""

    value = None
    if config is not None and config.has_section(section) \
            and config.has_option(section, option):
        value = config.get(section, option).strip()

    if not value:
        value = default_for(section, option)
        if value is None:
            raise Exception("No %s in [%s] section in config file, and no default"
                            % (option, section,))

    out = os.path.realpath(_resolve(value))
    if must_exist and not os.path.exists(out):
        raise IOError("File not found: %s (from [%s] %s)" % (out, section, option,))
    return out


def paths(config, section, option, must_exist=True):
    """Same, for an option holding a whitespace-separated list."""

    value = None
    if config is not None and config.has_section(section) \
            and config.has_option(section, option):
        value = config.get(section, option).strip()

    if not value:
        value = default_for(section, option)
        if value is None:
            raise Exception("No %s in [%s] section in config file, and no default"
                            % (option, section,))

    out = []
    for name in value.split():
        p = os.path.realpath(_resolve(name))
        if must_exist and not os.path.exists(p):
            raise IOError("File not found: %s (from [%s] %s)" % (p, section, option,))
        out.append(p)
    return out


#
# main -- print what the current config resolves to, which is the quickest way
# to find out whether a config still needs any of these lines
#
if __name__ == "__main__":

    import argparse
    from configparser import ConfigParser

    ap = argparse.ArgumentParser(description="show where the shipped data files resolve to")
    ap.add_argument("-c", "--config", dest="conffile", default=None,
                    help="properties file to check (default: none -- show the defaults)")
    args = ap.parse_args()

    cp = None
    if args.conffile is not None:
        cp = ConfigParser()
        cp.read(os.path.realpath(args.conffile))

    sys.stdout.write("repo: %s\n\n" % (REPO,))
    for (section, option) in sorted(DEFAULTS):
        overridden = (cp is not None and cp.has_section(section)
                      and cp.has_option(section, option)
                      and cp.get(section, option).strip())
        try:
            if option in ("csvfiles",):
                got = ", ".join(paths(cp, section, option, must_exist=False))
            else:
                got = path(cp, section, option, must_exist=False)
            missing = "" if all(os.path.exists(p) for p in
                                (got.split(", ") if option == "csvfiles" else [got])) \
                      else "   ** MISSING **"
        except Exception as e:
            got, missing = str(e), ""
        sys.stdout.write("[%s] %s\n    %s%s%s\n"
                         % (section, option, got,
                            "   (from config)" if overridden else "   (default)", missing,))

#
# eof
