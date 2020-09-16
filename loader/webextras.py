#!/usr/bin/python -u
#
# stuff that goes into web schema,
# chemical shift statistics
#
# NOTE that this only works if "web" is a schema in the same DB as "macromolecules"
#   CS statistics are generated off the macromolecules and dumped to files, but we also put them in
#     web schema for API queries.
#

from __future__ import absolute_import
import os
import sys
import pgdb
import re
import ConfigParser
import argparse

_UP = os.path.abspath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import loader

DB = "web"

# wrapper
#
#
def load( config, verbose = False ) :

    global DB

    create_schema( config, verbose )
    load_procq( config, verbose )
    load_depids( config, verbose )
    load_extras( config, verbose )
    load_bmrb_pdb_map( config, start = 1, verbose )
    generate_stats( config, verbose )
    if config.has_option( DB, "rouser" ) :
        loader.add_ro_grants( dsn = loader.dsn( config, DB ), schema = config.get( DB, "schema" ),
                user = config.get( DB, "rouser" ), verbose = verbose )  

#
#
#
def create_schema( config, verbose = False ) :
    if verbose :
        sys.stdout.write( "create_schema()\n" )

    assert isinstance( config, ConfigParser.SafeConfigParser )

    global DB

    script = config.get( DB, "ddlfile" )
    script = os.path.realpath( script )
    if not os.path.exists( script ) :
        raise IOError( "File not found: %s" % (script,) )

    return loader.runscript( loader.dsn( config, section = DB ), script, verbose = verbose )

# SQL script creates statistics tables in web schema, from tables in macromolecules schema
#  it then dus them to CSV
#  CSV file paths as well as DB schema names are hardcoded in the SQL
#
def generate_stats( config, verbose = False ) :
    if verbose :
        sys.stdout.write( "generate_stats()\n" )

    assert isinstance( config, ConfigParser.SafeConfigParser )

    global DB

    script = config.get( "macromolecules", "csstats" )
    script = os.path.realpath( script )
    if not os.path.exists( script ) :
        raise IOError( "File not found: %s" % (script,) )

    return loader.runscript( loader.dsn( config, section = DB ), script, verbose = verbose )

# processing queue from ETS
#
def load_procq( config, verbose = False ) :
    if verbose :
        sys.stdout.write( "load_procq()\n" )

    global DB

    assert isinstance( config, ConfigParser.SafeConfigParser )

    sql = "insert into web.procque (accno,received,onhold,status,released)" \
        + " values (%(id)s,%(recv)s,%(hld)s,%(st)s,%(rel)s)"

    with pgdb.connect( **(loader.dsn( config, DB )) ) as conn :
        conn.autocommit = False
        with conn.cursor() as curs :

            try :
                for row in loader.processing_queue_itr( config ) :

# tuples: bmrb id, date received, on hold, release status, date when returned to author
#
                    vals = { "id" : row[0], "recv" : row[1], "hld" : row[2], "st" : row[3], "rel" : row[4] }
                    if verbose :
                        sys.stdout.write( ",".join( str( i ) for i in row ) )
                        sys.stdout.write( "\n" )
                    curs.execute( sql, vals )
                    if verbose : sys.stdout.write( ": inserted %d\n" % (curs.rowcount,) )

                for row in loader.removed_ids_itr( config ) :

# tuples: bmrb id, date received
#
                    vals = { "id" : row[0], "recv" : row[1], "hld" : "N", "st" : "Withdrawn", "rel" : None }
                    if verbose :
                        sys.stdout.write( ",".join( str( i ) for i in row ) )
                        sys.stdout.write( "\n" )
                    curs.execute( sql, vals )
                    if verbose : sys.stdout.write( ": inserted %d\n" % (curs.rowcount,) )

                conn.commit()
            except :
                conn.rollback()
                raise

# deposition id to accession id map
#
def load_depids( config, verbose = False ) :
    if verbose :
        sys.stdout.write( "load_depids()\n" )

    assert isinstance( config, ConfigParser.SafeConfigParser )

    global DB

    with pgdb.connect( **(loader.dsn( config, DB )) ) as conn :
        conn.autocommit = False
        with conn.cursor() as curs :

            try :

                sql = "truncate web.dep2accno"
                curs.execute( sql )

                sql = "insert into web.dep2accno (depno,accno) values (%(dep)s,%(id)s)"
                for row in loader.depids_itr( config ) :

# tuples: deposition id, bmrb id
#
                    vals = { "dep" : row[0], "id" : row[1] }
                    if verbose :
                        sys.stdout.write( ",".join( str( i ) for i in row ) )
                        sys.stdout.write( "\n" )
                    curs.execute( sql, vals )
                    if verbose : sys.stdout.write( ": inserted %d\n" % (curs.rowcount,) )

                conn.commit()

            except :
                conn.rollback()
                raise

# couple of extra files
#
def load_extras( config, verbose = False ) :
    if verbose :
        sys.stdout.write( "load_extras()\n" )

    global DB

    assert isinstance( config, ConfigParser.SafeConfigParser )

    pat = re.compile( r"([^.]+)\.([^.]+)\.csv$" )
    extras = config.get( DB, "csvfiles" )
    for name in extras.split() :
        f = os.path.realpath( name )
        m = pat.search( os.path.split( f )[1] )
        if not m :
            sys.stderr.write( "%s does nto match pattern\n" % (f,) )
            continue
        loader.fromcsv( loader.dsn( config, DB ), filename = f, schema = m.group( 1 ),
                table = m.group( 2 ), verbose = verbose )

# BMRB-PDB ID map
#
def load_bmrb_pdb_map( config, start, verbose = False ) :
    if verbose :
        sys.stdout.write( "load_bmrb_pdb_map()\n" )

    assert isinstance( config, ConfigParser.SafeConfigParser )

    global DB

    with pgdb.connect( **(loader.dsn( config, DB )) ) as conn :
        conn.autocommit = False
        with conn.cursor() as curs :

            try :
#                sql = "delete from web.db_links where upper(db_code)='PDB' and upper(link_type)='ETS'"
                sql = "truncate web.pdb_link"

                curs.execute( sql )

#                sql = "insert into web.db_links (bmrb_id,db_code,db_id,link_type) " \
#                    + "values (%(bmrbid)s,'PDB',%(pdbid)s,'ETS')"
                sql = "insert into web.pdb_link (bmrb_id, pdb_id) values (%(bmrbid)s,%(pdbid)s)"
                for row in loader.bmrb_pdb_ids_itr( config, start ) :

# tuples: deposition id, bmrb id
#
                    vals = { "bmrbid" : row[0], "pdbid" : row[1] }
                    if verbose :
                        sys.stdout.write( ",".join( str( i ) for i in row ) )
                        sys.stdout.write( "\n" )
                    curs.execute( sql, vals )
                    if verbose : sys.stdout.write( ": inserted %d\n" % (curs.rowcount,) )

                conn.commit()

            except :
                conn.rollback()
                raise

#
#
#
if __name__ == "__main__" :

    ap = argparse.ArgumentParser( description = "load NMR-STAR dictionary into PostgreSQL database" )
    ap.add_argument( "-v", "--verbose", help = "print lots of messages to stdout", dest = "verbose",
        action = "store_true", default = False )
    ap.add_argument( "-t", "--time", help = "time the operatons", dest = "time",
        action = "store_true", default = False )

    ap.add_argument( "-c", "--config", help = "config file", dest = "conffile", required = True )

    ap.add_argument( "--no-cs-stats", help = "don't generate chemical shift statistics", dest = "genstats",
        action = "store_false", default = True )
    ap.add_argument( "--no-proc-queue", help = "don't load processing queue", dest = "procq",
        action = "store_false", default = True )
    ap.add_argument( "--no-extras", help = "don't load extra tables", dest = "extras",
        action = "store_false", default = True )

    args = ap.parse_args()

    cp = ConfigParser.SafeConfigParser()
    f = os.path.realpath( args.conffile )
    cp.read( f )

    with loader.timer( label = "make web schema", silent = not args.time ) :
        create_schema( config = cp, verbose = args.verbose )
        if cp.has_option( "web", "rouser" ) :
            loader.add_ro_grants( dsn = loader.dsn( cp, "web" ), schema = cp.get( "web", "schema" ),
                user = cp.get( "web", "rouser" ),  verbose = args.verbose )

    if args.genstats :
        with loader.timer( label = "generate CS stats", silent = not args.time ) :
            generate_stats( config = cp, verbose = args.verbose )

    if args.procq :
        with loader.timer( label = "load processing queue", silent = not args.time ) :
            load_procq( config = cp, verbose = args.verbose )
            load_depids( config = cp, verbose = args.verbose )

    if args.extras :
        with loader.timer( label = "load additional CSVs", silent = not args.time ) :
            load_extras( config = cp, verbose = args.verbose )
