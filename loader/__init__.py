#!/usr/bin/env python3
#
# BMRB database loader.
#
# Stage functions are re-exported here so callers (__main__.py, the condor
# jobs) can treat this as one flat namespace, the way they always have.
#
# All database access goes through `loader.db` (psycopg2 + psql); `starobj`
# does its own, configured from the same properties file.
#

import os
import sys
import time
from contextlib import contextmanager

# starobj is not pip-installable; production keeps it in a fixed location.
# It used to be hard-coded here, which meant the package could not be imported
# anywhere else at all -- now that location is only a fallback, after whatever
# $STAROBJ_PATH or PYTHONPATH already provide.
STAROBJ_PATH = os.environ.get("STAROBJ_PATH", "/projects/BMRB/software/starobj")
if os.path.isdir(STAROBJ_PATH):
    sys.path.append(STAROBJ_PATH)
import starobj

from . import db
from .db import add_ro_grants, connect, dsn, run_sql_file

from .ets import released_ids_itr, depids_itr, processing_queue_itr, removed_ids_itr, \
    bmrb_pdb_ids_itr
from .csvio import dump_new, dump_macromolecules, dump_metabolomics, fromcsv, tocsv

from .dictionary import load as load_dict
from .chemcomps import dump_and_load as load_chem_comps
from .metabolomicsextras import load as load_meta_schema
from .entries import load_metabolomics, load_macromolecules
from .macromol import fixup as fix_macromolecules
from .webextras import load as load_web_schema

#######################################


# simple timings
#
@contextmanager
def timer(label, silent=False):
    start = time.time()
    try:
        yield
    finally:
        end = time.time()
        if not silent:
            sys.stdout.write("%s: %0.3f\n" % (label, (end - start)))


#######################################
#
#

__all__ = ["starobj",
           "db", "connect", "dsn", "run_sql_file", "add_ro_grants",
           "timer",
           "released_ids_itr", "depids_itr", "processing_queue_itr", "removed_ids_itr",
           "bmrb_pdb_ids_itr",
           "fromcsv", "tocsv",
           "load_dict",
           "load_chem_comps",
           "load_meta_schema",
           "load_metabolomics", "load_macromolecules",
           "fix_macromolecules",
           "load_web_schema",
           "dump_new", "dump_macromolecules", "dump_metabolomics",
           ]

#
# eof
