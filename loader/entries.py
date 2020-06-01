#!/usr/bin/python -u
#
#

from __future__ import absolute_import
import os
import sys
import re
import glob
#import subprocess
import ConfigParser
import argparse
import pprint
import traceback

_UP = os.path.abspath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import loader

# wrappers
#
#
def load_metabolomics( config, drop_tables, verbose = False ) :

    return load_db( "metabolomics", config, drop_tables, verbose )

#
#
def load_macromolecules( config, drop_tables, verbose = False ) :

    return load_db( "macromolecules", config, drop_tables, verbose )

#
#
def load_db( db, config, drop_tables, verbose = False ) :

    load_entries( db, config, drop_tables, verbose )
    if config.has_option( db, "rouser" ) :
        loader.add_ro_grants( dsn = loader.dsn( config, db ), schema = config.get( db, "schema" ),
                user = config.get( db, "rouser" ), verbose = verbose )

#
#
#
def load_entries( db, config, drop_tables = False, verbose = False ) :

    if verbose : 
        sys.stdout.write( "load_entries( %s, %s )\n" % (db,(drop_tables and "drop_tables" or "keep_tables"),) )

    assert db in ("macromolecules","metabolomics",)

    if not config.has_section( db ) :
        sys.stderr.write( "No [%s] section in config file\n" % (db,) )
        return False

# need dictionary section
#
    if not config.has_section( "dictionary" ) :
        sys.stderr.write( "No [dictionary] section in config file\n" )
        return False

    if not config.has_option( "dictionary", "database" ) :
        sys.stderr.write( "No database in [dictionary] section in config file\n" )
        return False

# and a database
#
    if not config.has_option( db, "database" ) :
        sys.stderr.write( "No database in [%s] section in config file\n" % (db,) )
        return False

    dbname = config.get( db, "database" )
    duser = None
    if config.has_option( db, "user" ) : duser = config.get( db, "user" )
    dhost = None
    if config.has_option( db, "host" ) : dhost = config.get( db, "host" )

# input file list
#
    if not config.has_option( db, "entrydir" ) :
        sys.stderr.write( "No entrydir in [%s] section in config file\n" % (db,) )
        return False

    files = _gen_file_list( db, directory = config.get( db, "entrydir" ), verbose = verbose )
    if (files is None) or (len( files ) < 1) :
        sys.stderr.write( "Nothing to load\n" )
        return False

    toload = []
    if db == "macromolecules" :
# compare w/ released
#
        released = []
        for i in loader.released_ids_itr( config ) :
            released.append( i )

        pat = re.compile( r"bmr(\d+)_3\.str$" )
        for f in files :
            m = pat.search( f )
            if not m :
                raise Exception( "this should never happen: %s in file list" % (f,) )
            bmrbid = m.group( 1 )
            if bmrbid in released :
                released.remove( bmrbid )
                toload.append( f )
            else :
                sys.stderr.write( "*************** ERROR ******************\n" )
                sys.stderr.write( "BMRB ID of %s is not in released IDs!\n" % (f,) )
                sys.stderr.write( "Delete from public website!\n" )
                sys.stderr.write( "****************************************\n" )
#                raise Exception()

        if len( released ) > 0 :
            sys.stderr.write( "Following BMRB IDs are released but not in file list:\n" )
            for i in released :
                sys.stderr.write( "%s\n" % (i,) )

    if verbose :
        sys.stdout.write( "*********\nFiles to load:\n" )
        pprint.pprint( toload )
        sys.stdout.write( "*********\nFile list:\n" )
        pprint.pprint( files )

# starobj needs [entry] section with 
# engine = pgdb
# database = <dbname>
# schema = <db>
# user and host
#
    if not config.has_section( "entry" ) : config.add_section( "entry" )
    config.set( "entry", "engine", "pgdb" )
    config.set( "entry", "database", dbname )
    config.set( "entry", "schema", db )
    if duser is not None : config.set( "entry", "user", duser )
    if dhost is not None : config.set( "entry", "host", dhost )

#    _load_entries( config, files, drop_tables, verbose = verbose )

    if db == "macromolecules" :
        _load_entries( config, toload, drop_tables, verbose = verbose )
    else :
        _load_entries( config, files, drop_tables, verbose = verbose )

    del files[:]

#

# list input files for metabolomics or macromolecule database
# this reads files actually on the website, without checking ETS status
#
def _gen_file_list( db, directory, verbose = False ) :

    assert db in ("macromolecules","metabolomics",)
    entrydir = os.path.realpath( directory )
    if not os.path.isdir( entrydir ) :
        sys.stderr.write( "Not a directory: %s\n" % (entrydir,) )
        return []

    filelist = []
    if db == "macromolecules" : 
        dirpat = re.compile( r"bmr(\d+)$" )
        filename = "bmr%s_3.str"
    else :
        dirpat = re.compile( r"(bms[et]\d+)$" )
        filename = "%s.str"

    for i in glob.glob( os.path.join( entrydir, "*" ) ) :
        m = dirpat.search( i )
        if not m :
            sys.stderr.write( "%s does not match patern\n" % (i,) )
            continue
        if not os.path.isdir( i ) :
            sys.stderr.write( "%s: not a directory\n" % (i,) )
            continue
        infile = os.path.join( i, filename % (m.group( 1 ),) )
        if not os.path.exists( infile ) :
            sys.stderr.write( "Not found: %s\n" % (infile,) )
            continue

        filelist.append( infile )

#    pprint.pprint( filelist )
    return sorted( filelist )

#
#
#
def _load_entries( config, filelist, drop_tables = False, verbose = False ) :

    errs = []
    db = loader.starobj.DbWrapper( config, verbose = False ) # verbose )
    db.connect()

    sd = loader.starobj.StarDictionary( db, verbose = False ) # verbose )
    se = loader.starobj.NMRSTAREntry( db, verbose = False ) # verbose )

    if config.get( "entry", "schema" ) == "macromolecules" :
        types = False
    else : types = True

# the tables are in sub-schemas, just drop and re-create the whole thing
#
    if drop_tables :
        db._connections[se.CONNECTION]["conn"].autocommit = True
        schema = db.schema( se.CONNECTION )
        stmt = "set client_min_messages=WARNING"
        se.execute( stmt )
        stmt = "drop schema if exists %s cascade" % (schema,)
        se.execute( stmt )
        stmt = "create schema %s" % (schema,)
        se.execute( stmt )

        se.create_tables( dictionary = sd, db = db, use_types = types, verbose = verbose )
    else :
        raise Exception( "FIXME!!!! Not implemented" )

    db._connections[se.CONNECTION]["conn"].autocommit = False
    for f in filelist :
        del errs[:]
        if verbose :
            sys.stdout.write( "> %s\n" % (f,) )
        try :
            if loader.starobj.StarParser.parse_file( db = db, dictionary = sd, filename = f,
                        errlist = errs, types = types, create_tables = False, verbose = False ) : # verbose ) :
                db._connections[se.CONNECTION]["conn"].commit()
            else :
                sys.stderr.write( "** Errors parsing %s\n" % (f,) )
                db._connections[se.CONNECTION]["conn"].rollback()
            if len( errs ) > 0 :
                sys.stderr.write( "** Parser output for %s\n" % (f,) )
                for e in errs :
                    sys.stderr.write( str( e ) )
                    sys.stderr.write( "\n" )
        except :
            db._connections[se.CONNECTION]["conn"].rollback()
            sys.stderr.write( "Exception on %s\n" % (f,) )
            traceback.print_exc()

#
#
#
if __name__ == "__main__" :

    ap = argparse.ArgumentParser( description = "load NMR-STAR files into PostgreSQL database" )
    ap.add_argument( "--time", help = "print out timings", dest = "time", action = "store_false",
        default = True )
    ap.add_argument( "-v", "--verbose", help = "print lots of messages to stdout", dest = "verbose",
        action = "store_true", default = False )

    ap.add_argument( "-c", "--config", help = "config file", dest = "conffile", required = True )
    ap.add_argument( "-s", "--schema", help = "database to load: macromolecules or metabolomics", 
        dest = "db", default = "all" )

    ap.add_argument( "-d", "--drop-tables", help = "drop and re-create tables (default: truncate existing)", 
        dest = "droptables", action = "store_true", default = False )

    args = ap.parse_args()

    cp = ConfigParser.SafeConfigParser()
    f = os.path.realpath( args.conffile )
    cp.read( f )

    if args.db != "all" :
        if not cp.has_section( args.db ) :
            sys.stderr.write( "Don't know how to load %s (no config section)\n", (args.db,) )
            sys.exit( 1 )

    if args.db in ("macromolecules","all" ) :
        with loader.timer( label = "Load macromolecules", silent = args.time ) :
            load_macromolecules( config = cp, drop_tables = args.droptables, verbose = args.verbose )

    if args.db in ("metabolomics","all") :
        with loader.timer( label = "Load metabolomics", silent = args.time ) :
            load_metabolomics( config = cp, drop_tables = args.droptables, verbose = args.verbose )





