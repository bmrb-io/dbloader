#!/usr/bin/python -u
#
# these functions exec psql instead of using a python adapter because
#  copy from/to don't work without root permissions but psql's \copy does.
#

from __future__ import absolute_import
import os
import sys
import re
import glob
#import subprocess
import ConfigParser
import argparse

_UP = os.path.abspath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import loader

#
# main
#
def load( config, path, verbose = False ) :
    assert isinstance( config, ConfigParser.SafeConfigParser )

    if not config.has_section( "dictionary" ) :
        raise Exception( "No [dictionary] section in config file\n" )

    if not config.has_option( "dictionary", "ddlfile" ) :
        raise Exception( "No ddlfile in [dictionary] section in config file\n" )

    wd = os.path.realpath( path )
    if not os.path.isdir( wd ) :
        raise Exception( "Not a directory: %s\n", (wd,) )

    dscript = config.get( "dictionary", "ddlfile" )
    ds = os.path.realpath( os.path.join( wd, dscript ) )

    if not loader.runscript( loader.dsn( config, section = "dictionary" ), script = ds, verbose = verbose ) :
        return False

    if not fromcsv( loader.dsn( config, section = "dictionary" ), path = wd, verbose = verbose ) :
        return False

    if config.has_option( "dictionary", "rouser" ) :
        loader.add_ro_grants( dsn = loader.dsn( config, "dictionary" ), schema = config.get( "dictionary", "schema" ), 
            user = config.get( "dictionary", "rouser" ),  verbose = verbose )

# files are named dict.<table>.csv
# 1st row is column headers
# column order may not match database
#
# loading is not ordered, foreign keys should be turned off or not defined
#
def fromcsv( dsn, path, verbose = False ) :

    d = os.path.realpath( path )
    if not os.path.isdir( d ) : raise IOError( "Not a directory: %s" % (d,) )
    pat = re.compile( r"([^.]+)\.([^.]+)\.csv$" )
    errs = 0
    for i in glob.glob( os.path.join( d, "dict.*.csv" ) ) :
        m = pat.search( os.path.split( i )[1] )
        if not m :
            sys.stderr.write( "%s does not match pattern, skipping\n" % (i,) )
            errs += 1
            continue

        if not loader.fromcsv( dsn, filename = i, schema = "dict", table = m.group( 2 ), verbose = verbose ) :
            errs += 1

    return (errs == 0)

#
#
#
if __name__ == "__main__" :

    ap = argparse.ArgumentParser( description = "load NMR-STAR dictionary into PostgreSQL database" )
    ap.add_argument( "-v", "--verbose", help = "print lots of messages to stdout", dest = "verbose",
        action = "store_true", default = False )

    ap.add_argument( "-c", "--config", help = "config file", dest = "conffile", required = True )
    ap.add_argument( "-d", "--dir", help = "directory with input files", dest = "indir", required = True )
    args = ap.parse_args()

    wd = os.path.realpath( args.indir )
    if not os.path.isdir( wd ) :
        sys.stderr.write( "Not a directory: %s\n", (wd,) )
        sys.exit( 1 )

    cp = ConfigParser.SafeConfigParser()
    f = os.path.realpath( args.conffile )
    cp.read( f )

    load( config = cp, path = wd, verbose = args.verbose )
