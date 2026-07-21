#!/usr/bin/env python3
#
# Readers for the ETS entry tracking database (the deposition system's own
# database, on a different host from `bmrb`).  Everything here is read-only.
#
# These were four near-identical iterator classes -- __init__/​__iter__/
# __next__/next/__del__ around one query each -- which is what a generator is.
# The public names and what they yield are unchanged.
#

import argparse
import os
import pprint
import sys
from configparser import ConfigParser

_UP = os.path.abspath(os.path.join(os.path.split(__file__)[0], ".."))
sys.path.append(_UP)
from loader import db

SECTION = "ets"


def _query(config, sql, params=None):
    """Run a query against ETS, yielding rows; connection closed on exhaustion."""

    conn = db.connect(db.dsn(config, SECTION))
    try:
        with conn.cursor() as curs:
            curs.execute(sql, params)
            for row in curs:
                yield row
    finally:
        conn.close()


def _nonempty(value):
    """ETS columns are char(n) and full of blanks and NULLs."""

    return (value is not None) and (len(str(value).strip()) > 0)


#
# released BMRB IDs
#
def released_ids_itr(config):
    """iterator for released BMRB IDs"""

    sql = "select bmrbnum from entrylog where status like 'rel%' order by bmrbnum"

    for row in _query(config, sql):
        if _nonempty(row[0]):
            yield str(row[0]).strip()


# this is probably long obsolete but might still be somewhere on the website?
# -- 2020-03-27 - pulled the plug on it but am not updating DB schema (TODO)
# dep. id is not null in ets
#
def depids_itr(config):
    """iterator for deposition id to bmrb id map"""

    sql = "select nmr_dep_code,bmrbnum from entrylog" \
          " where nmr_dep_code is not null and bmrbnum is not null order by bmrbnum"

    for row in _query(config, sql):
        if not (_nonempty(row[0]) and _nonempty(row[1])):
            continue
        if "based_on_existing" in str(row[0]).lower():
            continue
        # the deposition code itself is deliberately not published
        yield ("", str(row[1]).strip())


# iterator for "in processing" entries
# tuples: bmrb id, date received, on hold, release status, date when returned to author
# the ugly parts:
#   some status codes have '_1', '_2', etc. appended,
#   for the last date we need the most recent one (there may be several),
#   last date applies only to entries waiting for author's response,
#   we've added several 'in processing' status codes over the years -- but no new 'released'
#     ones so far.  So this excludes known 'released' codes rather than include known
#     'in processing' ones.
#
def processing_queue_itr(config):
    """iterator for entries in the processing queue"""

    sql = """select e.bmrbnum,e.submission_date,e.status,e.onhold_status,cast(NULL as timestamp)
            from entrylog e join logtable l on l.depnum=e.depnum
            where l.newstatus='nd' and e.status like 'oh%'
            union
            select e.bmrbnum,e.submission_date,e.status,e.onhold_status,cast(NULL as timestamp)
            from entrylog e join logtable l on l.depnum=e.depnum
            where l.newstatus='nd' and e.status not in ( 'awd', 'obs', 'sa', 'nd' )
            and e.status not like 'oh%' and e.status not like 'rel%' and e.status not like 'rta%'
            union
            select e.bmrbnum,e.submission_date,e.status,e.onhold_status,max(l.logdate)
            from entrylog e join logtable l on l.depnum=e.depnum
            where l.newstatus='rta' and e.status like 'rta%'
            group by e.bmrbnum,e.submission_date,e.status,e.onhold_status
            order by bmrbnum"""

    for row in _query(config, sql):

        if not _nonempty(row[0]):
            raise Exception("ETS error: no BMRB ID")
        if not _nonempty(row[2]):
            continue

        bmrbid = str(row[0]).strip()
        status = str(row[2]).strip().lower()
        when = None

        if "oh" in status:
            hold = "Y"
            onhold = str(row[3]).lower() if row[3] is not None else ""
            if not _nonempty(row[3]):
                rel = None
            elif "pub" in onhold:
                rel = "On publication"
            elif "wwpdb" in onhold:
                rel = "On release of PDB structure"
            else:
                rel = str(row[3]).strip()
        elif "rta" in status:
            hold = "N"
            rel = "Returned to author"
            when = row[4]
        else:
            hold = "N"
            rel = "Being processed"

        yield (bmrbid, row[1], hold, rel, when)


# obsolete/withdrawn IDs.
#
def removed_ids_itr(config):
    """iterator for obsolete and withdrawn BMRB IDs"""

    sql = "select bmrbnum,coalesce(submission_date,accession_date,'1969-12-31')" \
          " from entrylog where status in ('obs','awd') order by bmrbnum"

    for row in _query(config, sql):
        if _nonempty(row[0]):
            yield (str(row[0]).strip(), row[1],)


#
# matching BMRB ID, PDB ID pairs from ETS.
#
def bmrb_pdb_ids_itr(config, start=11000):
    """iterator for BMRB - PDB ID pairs"""

    assert int(start) > 0

    sql = "select bmrbnum,pdb_code from entrylog where status like 'rel%%'" \
          " and bmrbnum>%s and pdb_code is not null and trim(pdb_code)<>'?'" \
          " and trim(pdb_code)<>'' and trim(pdb_code)<>'.'" \
          " order by cast(bmrbnum as integer)"

    for row in _query(config, sql, (start,)):
        if not _nonempty(row[1]):
            continue
        pdbids = str(row[1]).strip()
        bmrbid = str(row[0])

        # SMSDep numbers have pdb id = bmrb id
        if bmrbid == pdbids:
            continue
        for pdbid in pdbids.upper().replace(",", " ").split():
            yield (bmrbid, pdbid)


#
#
#
if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="ETS wrapper")
    ap.add_argument("-v", "--verbose", help="print lots of messages to stdout", dest="verbose",
                    action="store_true", default=False)
    ap.add_argument("-c", "--config", help="config file", dest="conffile", required=True)
    args = ap.parse_args()

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    for itr in (released_ids_itr(cp), depids_itr(cp), processing_queue_itr(cp),
                removed_ids_itr(cp), bmrb_pdb_ids_itr(cp, start=1)):
        for i in itr:
            pprint.pprint(i)

#
# eof
