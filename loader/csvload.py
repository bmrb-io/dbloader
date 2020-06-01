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
import subprocess
import ConfigParser
import argparse

_UP = os.path.abspath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import loader


#
# 1st row in file is column headers
# column order may not match database
#
# if table has upper-case letter, quote it
# ditto for column names
#
def _fromcsv( filename, dsn, schema, table, verbose = False ) :

# pgdb vs psycopg2
#
    if "database" in dsn.keys() : cmd = [ loader.PSQL, "-d", dsn["database"] ]
    else : cmd = [ loader.PSQL, "-d", dsn["dbname"] ]
    if "user" in dsn.keys() : cmd.extend( ["-U", dsn["user"]] )
    if "host" in dsn.keys() : cmd.extend( ["-h", dsn["host"]] )
    if "port" in dsn.keys() : cmd.extend( ["-p", dsn["port"]] )
    if not verbose : cmd.append( "-q" )

    infile = os.path.realpath( filename )
    if not os.path.exists( infile ) : raise IOError( "Not found: %s" % (infile,) )

# column names
#
    cols = []
    with open( infile, "rU" ) as f :
        l = f.readline()
        for col in l.split( "," ) :
            cols.append( col.strip().strip( "'\"" ) )

    if len( cols ) < 1 :
        sys.stderr.write( "no columns in %s\n" % (infile,) )
        return False

    colstr = ""
    for c in cols :
        if c.islower() : colstr += c
        else : colstr += '"' + c + '"'
        colstr += ","
    colstr = colstr[:-1]

    if (schema is None) or (str( schema ).strip() == "") :
        scam = ""
    else :
        scam = "%s." % (str( schema ).strip(),)

    if table.islower() :
        tbl = table
    else :
        tbl = '"%s"' % (table,)

    stmt = "\\copy %s%s (%s) from '%s' csv header" % (scam,tbl,colstr,infile,)

    cmd.extend( ["-c", stmt] )

    if verbose :
        sys.stderr.write( " ".join( j for j in cmd ) )
        sys.stderr.write( "\n" )

    p = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
    (out, err) = p.communicate()
    if p.returncode != 0 :
        sys.stderr.write( "ERR: psql returned %d\n" % (p.returncode,))
        sys.stderr.write( " ".join( j for j in cmd ) )
        sys.stderr.write( "\n" )
        sys.stderr.write( "** STDERR **\n" )
        for j in err.splitlines() :
            sys.stderr.write( j )
            sys.stderr.write( "\n" )
        sys.stderr.write( "** STDOUT **\n" )
        for j in out.splitlines() :
            sys.stderr.write( j )
            sys.stderr.write( "\n" )

        return False

    if verbose :
        sys.stderr.write( "** STDERR **\n" )
        for j in err.splitlines() :
            sys.stderr.write( j )
            sys.stderr.write( "\n" )
        sys.stderr.write( "** STDOUT **\n" )
        for j in out.splitlines() :
            sys.stderr.write( j )
            sys.stderr.write( "\n" )

    return True

# public wrapper
#
def fromcsv( dsn, filename, schema, table, verbose = False ) :
 return _fromcsv( filename, dsn, schema, table, verbose )

#TODO
#
# tables are <schema>.<table>.csv
#
#
def all_fromcsv( path, config, verbose = False ) :

    d = os.path.realpath( path )
    if not os.path.isdir( d ) : raise IOError( "Not a directory: %s" % (d,) )
    pat = re.compile( r"([^.]+)\.([^.]+)\.csv$" )
    errs = 0
    for i in glob.glob( os.path.join( d, "*.csv" ) ) :
        m = pat.search( os.path.split( i )[1] )
        if not m :
            sys.stderr.write( "%s does not match pattern, skipping\n" % (i,) )
            errs += 1
            continue

        if not _fromcsv( i, dsn = loader.dsn( config, "bmrbeverything" ), 
                schema = m.group( 1 ), table = m.group( 2 ), verbose = verbose ) :
            errs += 1

    return (errs == 0)

#
#
#
if __name__ == "__main__" :

    ap = argparse.ArgumentParser( description = "load 'bmrbeverything' PostgreSQL database" )
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

    if not all_fromcsv( path = wd, config = cp, verbose = args.verbose ) :
        sys.exit( 1 )

#    if cp.has_option( "dictionary", "rouser" ) :
#        loader.add_ro_grants( dsn = loader.dsn( cp, "dictionary" ), schema = cp.get( "dictionary", "schema" ), 
#            user = cp.get( "dictionary", "rouser" ),  verbose = args.verbose )
#
#
# eof
