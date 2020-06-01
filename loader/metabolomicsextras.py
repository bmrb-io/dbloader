#!/usr/bin/python -u
#
# stuff that goes into metabolomics "meta" schema
#  - until we replace that old setup with something better.
#

from __future__ import absolute_import
import os
import sys
#import pgdb
import ConfigParser
import argparse
import glob
import re

_UP = os.path.abspath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import loader

DB = "meta"

# main
#
#
def load( config, verbose = False ) :
    create_schema( config, verbose )
    add_grants( config, verbose )
    load_files( config, verbose )

#######################################
# run DDL script
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

# these files are named meta.tablename.csv
#
#
def load_files( config, verbose = False ) :
    if verbose :
        sys.stdout.write( "load_files()\n" )

    assert isinstance( config, ConfigParser.SafeConfigParser )

    global DB

    d = config.get( DB, "csvdir" )
    datadir = os.path.realpath( d )
    if not os.path.isdir( datadir ) :
        raise IOError( "Not a directory: %s" % (datadir,) )

    pat = re.compile( r"([^.]+)\.([^.]+)\.csv$" )
    for name in glob.glob( os.path.join( datadir, "meta.*.csv" ) ) :
        m = pat.search( os.path.split( name )[1] )
        if not m :
            sys.stderr.write( "%s does not match pattern, skipping\n" % (name,) )
            continue
        loader.fromcsv( loader.dsn( config, section = DB ), filename = name, schema = m.group( 1 ),
                table = m.group( 2 ), verbose = verbose )

# add grants
#
#
def add_grants( config, verbose = False ) :
    if verbose :
        sys.stdout.write( "add_grants()\n" )

    assert isinstance( config, ConfigParser.SafeConfigParser )

    global DB

    if config.has_option( DB, "rouser" ) :
        loader.add_ro_grants( dsn = loader.dsn( config, DB ), schema = config.get( DB, "schema" ),
                user = config.get( DB, "rouser" ),  verbose = verbose )

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

    args = ap.parse_args()

    cp = ConfigParser.SafeConfigParser()
    f = os.path.realpath( args.conffile )
    cp.read( f )

    with loader.timer( label = "load additional metabolomics CSVs", silent = not args.time ) :
        load( config = cp, verbose = args.verbose )

