#!/usr/bin/env python3
#
# BMRB database loader.
#
# Stage functions are re-exported here so callers (__main__.py, the condor
# jobs) can treat this as one flat namespace, the way they always have.
#
# All database access goes through `loader.db` (psycopg2 + psql).  NMR-STAR
# parsing is pynmrstar; the entry tables are generated from the loaded
# dictionary by `loader.starschema` and filled by `loader.entryload`.
#

import sys
import time
from contextlib import contextmanager

from . import db
from .db import add_ro_grants, connect, dsn, run_sql_file

from . import shadow
from .shadow import swap as swap_shadow

from .ets import released_ids_itr, depids_itr, processing_queue_itr, removed_ids_itr, \
    bmrb_pdb_ids_itr
from .csvio import dump_new, dump_macromolecules, dump_metabolomics, fromcsv, tocsv

from .dictionary import load as load_dict, _ddlfile as dictionary_ddlfile
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

__all__ = ["db", "connect", "dsn", "run_sql_file", "add_ro_grants",
           "timer",
           "released_ids_itr", "depids_itr", "processing_queue_itr", "removed_ids_itr",
           "bmrb_pdb_ids_itr",
           "fromcsv", "tocsv",
           "load_dict", "dictionary_ddlfile",
           "load_chem_comps",
           "load_meta_schema",
           "load_metabolomics", "load_macromolecules",
           "fix_macromolecules",
           "load_web_schema",
           "dump_new", "dump_macromolecules", "dump_metabolomics",
           "shadow", "swap_shadow",
           ]

#
# eof
