#!/usr/bin/python -u
#
# stuff that goes into web schema,
# chemical shift statistics
#
# NOTE that this only works if "web" is a schema in the same DB as "macromolecules"
#

from __future__ import absolute_import
import os
import sys
import pgdb
import ConfigParser
import argparse

import pprint

_UP = os.path.abspath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import loader

#
#
class released_ids_itr( object ) :
    """iterator for released BMRB IDs"""

    sql = "select bmrbnum from entrylog where status like 'rel%' order by bmrbnum"

    _conn = None
    _curs = None

    def __init__( self, config ) :
        self._conn = pgdb.connect( **(loader.dsn( config, "ets" )) )
        self._curs = self._conn.cursor()
        self._curs.execute( self.sql )

    def __iter__( self ) :
        return self

    def __next__( self ) :
        while True :
            row = self._curs.fetchone()
            if row is None :
                self._curs.close()
                self._conn.close()
                raise StopIteration()
            if row[0] is None : continue
            if len( str( row[0] ).strip() ) < 1 : continue
            return str( row[0] ).strip()

    def next( self ) :
        return self.__next__()

    def __del__( self ) :
        self._curs = None
        if not self._conn.closed : self._conn.close()

# this is probably long obsolete but might still be somewhere on the website?
# -- 2020-03-27 - pulled the plus on it but am not updating DB schema (TODO)
# dep. id is not null in ets
#
class depids_itr( object ) :
    """iterator for deposition id to bmrb id map"""

    sql = "select nmr_dep_code,bmrbnum from entrylog where nmr_dep_code is not null and bmrbnum is not null order by bmrbnum"

    _conn = None
    _curs = None

    def __init__( self, config ) :
        self._conn = pgdb.connect( **(loader.dsn( config, "ets" )) )
        self._curs = self._conn.cursor()
        self._curs.execute( self.sql )

    def __iter__( self ) :
        return self

    def __next__( self ) :
        while True :
            row = self._curs.fetchone()
            if row is None :
                self._curs.close()
                self._conn.close()
                raise StopIteration()
            if len( str( row[0] ).strip() ) < 1 : continue
            if len( str( row[1] ).strip() ) < 1 : continue
            if str( row[0] ).lower().find( "based_on_existing" ) != -1 : continue
#            return (str( row[0] ).strip(),str( row[1] ).strip())
            return ("",str( row[1] ).strip())

    def next( self ) :
        return self.__next__()

    def __del__( self ) :
        self._curs = None
        if not self._conn.closed : self._conn.close()

# iterator for "in processing" entries
# tuples: bmrb id, date received, on hold, release status, date when returned to author
# the ugly parts:
#   some status codes have '_1', '_2', etc. appended,
#   for the last date we need the most recent one (there may be several),
#   last date applies only to entries waiting for author's response,
#   we've added several 'in processing' status codes over the years -- but no new 'released' ones so far.
#     So this excludes known 'released' codes rather than include known 'in processing' ones.
#
class processing_queue_itr( object ) :

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

    _conn = None
    _curs = None
    _hold = True

    def __init__( self, config ) :
        self._conn = pgdb.connect( **(loader.dsn( config, "ets" )) )
        self._curs = self._conn.cursor()
        self._curs.execute( self.sql )

    def __iter__( self ) :
        return self

    def __next__( self ) :
        while True :
            row = self._curs.fetchone()
            if row is None :
                self._curs.close()
                self._conn.close()
                raise StopIteration()

            if (row[0] is None) or (len( str( row[0] ).strip() ) < 1) :
                raise Exception( "ETS error: no BMRB ID" )

            if (row[2] is None) or (len( str( row[2] ).strip() ) < 1) :
                continue

            bmrbid = str( row[0] ).strip()
            status = str( row[2] ).strip().lower()
            when = None
            if status.find( "oh" ) != -1 :
                hold = "Y"
                if (row[3] is None) or (len( str( row[3] ).strip() ) < 1) :
                    rel = None
                if str( row[3] ).lower().find( "pub" ) != -1 :
                    rel = "On publication"
                elif str( row[3] ).lower().find( "wwpdb" ) != -1 :
                    rel = "On release of PDB structure"
                else : rel = str( row[3] ).strip()
            elif status.find( "rta" ) != -1 :
                hold = "N"
                rel = "Returned to author"
                when = row[4]
            else :
                hold = "N"
                rel = "Being processed"

            return (bmrbid,row[1],hold,rel,when)

    def next( self ) :
        return self.__next__()

    def __del__( self ) :
        self._curs = None
        if not self._conn.closed : self._conn.close()

# obsolete/withdrawn IDs. 
#
class removed_ids_itr( object ) :
    """iterator for obsolete and withdrawn BMRB IDs"""

    sql = "select bmrbnum,coalesce(submission_date,accession_date,'1969-12-31') " \
        + "from entrylog where status in ('obs','awd') order by bmrbnum"

    _conn = None
    _curs = None

    def __init__( self, config ) :
        self._conn = pgdb.connect( **(loader.dsn( config, "ets" )) )
        self._curs = self._conn.cursor()
        self._curs.execute( self.sql )

    def __iter__( self ) :
        return self

    def __next__( self ) :
        while True :
            row = self._curs.fetchone()
            if row is None :
                self._curs.close()
                self._conn.close()
                raise StopIteration()
            if row[0] is None : continue
            if len( str( row[0] ).strip() ) < 1 : continue
            return (str( row[0] ).strip(),row[1],)

    def next( self ) :
        return self.__next__()

    def __del__( self ) :
        self._curs = None
        if not self._conn.closed : self._conn.close()

#
# matching BMRB ID, PDB ID pairs from ETS.
#
def bmrb_pdb_ids_itr( config, start = 11000 ) :
    """iterator for BMRB - PDB ID pairs"""

    sql = "select bmrbnum,pdb_code from entrylog where status like 'rel%%' " \
        + "and bmrbnum>%s and pdb_code is not null and trim(pdb_code)<>'?' " \
        + "and trim(pdb_code)<>'' and trim(pdb_code)<>'.' " \
        + "order by cast(bmrbnum as integer)"

    assert int( start ) > 0

    with pgdb.connect( **(loader.dsn( config, "ets" )) ) as conn :
        with conn.cursor() as curs :
            curs.execute( sql, (start,) )
            for row in curs :
                if row[1] is None : continue
                pdbids = str( row[1] ).strip()
                if len( pdbids ) < 1 : continue
                bmrbid = str( row[0] )

# SMSDep numbers have pdb id = bmrb id
#
                if bmrbid == pdbids : continue
                tmp = pdbids.upper().replace( ",", " " ).split()
                for pdbid in tmp :
                    yield (bmrbid,pdbid)

#
#
#
if __name__ == "__main__" :

    ap = argparse.ArgumentParser( description = "ETS wrapper" )
    ap.add_argument( "-v", "--verbose", help = "print lots of messages to stdout", dest = "verbose",
        action = "store_true", default = False )
    ap.add_argument( "-t", "--time", help = "time the operatons", dest = "time",
        action = "store_true", default = False )

    ap.add_argument( "-c", "--config", help = "config file", dest = "conffile", required = True )

    args = ap.parse_args()

    cp = ConfigParser.SafeConfigParser()
    f = os.path.realpath( args.conffile )
    cp.read( f )

    for i in released_ids_itr( cp ) :
        pprint.pprint( i )

    for i in depids_itr( cp ) :
        pprint.pprint( i )

    for i in processing_queue_itr( cp ) :
        pprint.pprint( i )

    for i in removed_ids_itr( cp ) :
        pprint.pprint( i )

    for i in  bmrb_pdb_ids_itr( cp ) :
        pprint.pprint( i )

#
# eof
