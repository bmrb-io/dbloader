#!/usr/bin/python -u
#
# there's 2 layouts:
#  "old" has macromolecules and metabolomics tables in separate dirs,
#  each has its own copy of "dict"
#  NMR-STAR table CSVs have no schema prefix
#  DDL file has only select schemas, and need to be edited (remove schema
#   for NMR-STAR tables)
#
# for "new" we just dump everything as <schema>.<table>.csv
#

from __future__ import absolute_import
import os
import sys
import subprocess
import pgdb
import ConfigParser
import argparse
import glob
#import pprint
import re

_UP = os.path.abspath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import loader

# config sections
#
ALLSECTIONS = ( "dictionary", "macromolecules", "metabolomics", "web", "meta", "chemcomps" )
MACROSECTIONS = ( "dictionary", "macromolecules", "web" )
METASECTIONS = ( "dictionary", "metabolomics", "meta" )

# this only works if all schemas are in the same database as "dictionary"
#
#
def dump_new( config, path, verbose = False ) :
    if verbose :
        sys.stdout.write( "dump_new( %s )\n" % (path,) )

    global ALLSECTIONS

    return dump( config, path, sections = ALLSECTIONS, verbose = verbose )

# "old-style" dump of macromolecule database
#
#
def dump_macromolecules( config, path, verbose = False ) :
    if verbose :
        sys.stdout.write( "dump_macromolecules( %s )\n" % (path,) )

    global MACROSECTIONS

    return dump( config, path, sections = MACROSECTIONS, verbose = verbose )

# "old-style" dump of metabolomics database
#
#
def dump_metabolomics( config, path, verbose = False ) :
    if verbose :
        sys.stdout.write( "dump_metabolomics( %s )\n" % (path,) )

    global METASECTIONS

    return dump( config, path, sections = METASECTIONS, verbose = verbose )

#
#
#
def dump( config, path, sections, verbose = False ) :

    global ALLSECTIONS
    global MACROSECTIONS
    global METASECTIONS

    assert isinstance( config, ConfigParser.SafeConfigParser )
    assert sections in (ALLSECTIONS,MACROSECTIONS,METASECTIONS)

    if not os.path.exists( os.path.realpath( loader.PGDUMP ) ) :
        sys.stderr.write( "Run this where postgresql-10 is installed!\n" )
        sys.stderr.write( "Not found: %s\n" % (loader.PGDUMP,) )
        raise Exception( "Need postgresql 10 for this" )

    outdir = os.path.realpath( path )
    os.umask( 0o002 )
    if not os.path.exists( outdir ) :
        os.makedirs( outdir )
    else :
        if not os.path.isdir( outdir ) :
            raise IOError( "Not  directory: %s" % (outdir,) )
        else :
            for i in glob.glob( os.path.join( outdir, "*" ) ) :
                os.unlink( i )

# schemas
# can also get all schemas from
# select schema_name from information_schema.schemata
#
    scams = []
    for key in sections :
        scams.append( config.get( key, "schema" ) )
    outfile = os.path.join( outdir, "schema.sql" )
    dsn = loader.dsn( config, "dictionary" )

    if sections == ALLSECTIONS :
        if not dump_ddl( dsn, scams, outfile, old = None, verbose = verbose ) :
            sys.stderr.write( "Error dumping DDL\n" )
            return False
    elif sections == MACROSECTIONS :
        if not dump_ddl( dsn, scams, outfile, old = "macromolecules", verbose = verbose ) :
            sys.stderr.write( "Error dumping DDL\n" )
            return False
    elif sections == METASECTIONS :
        if not dump_ddl( dsn, scams, outfile, old = "metabolomics", verbose = verbose ) :
            sys.stderr.write( "Error dumping DDL\n" )
            return False

# tables
#
    tables = {}
    for scam in scams :
        tables[scam] = list_tables( dsn, scam, verbose )

# dump
#
    for scam in tables.keys() :
        for table in tables[scam] :
            if table.islower() :
                tbl = "%s.%s" % (scam,table,)
            else :
                tbl = '%s."%s"' % (scam,table,)

            if sections == ALLSECTIONS :
                outfile = os.path.join( outdir, "%s.%s.csv" % (scam,table,) )
            elif sections == MACROSECTIONS :
                if scam == "macromolecules" :
                    outfile = os.path.join( outdir, "%s.csv" % (table,) )
                else :
                    outfile = os.path.join( outdir, "%s.%s.csv" % (scam,table,) )
            elif sections == METASECTIONS :
                if scam == "metabolomics" :
                    outfile = os.path.join( outdir, "%s.csv" % (table,) )
                else :
                    outfile = os.path.join( outdir, "%s.%s.csv" % (scam,table,) )

            tocsv( dsn, tbl, outfile, verbose )

# pg_dump only really works between same postgres versions
#
def dump_ddl( dsn, schemata, outfile, old = None, verbose = False ) :

# "clean", no-owner, no ACLs, plain SQL output, schema only
#
    cmd = [loader.PGDUMP, "-c", "-O", "-x", "-F", "p", "-s"]
    for s in schemata :
        cmd.extend( ["-n", s] )

#    cmd.extend( ["-f", outfile] )

# it'll fail if there is a password
#
    if "user" in dsn.keys() : cmd.extend( ["-U", dsn["user"]] )
    if "host" in dsn.keys() :
        if ":" in dsn["host"] :
            (host,port) = dsn["host"].split( ":" )
        else :
            host = dsn["host"]
            port = None
        cmd.extend( ["-h", host] )
        if port is not None : cmd.extend( ["-p", port] )

# last
#
    cmd.append( dsn["database"] )

    if verbose :
        sys.stdout.write( "%s\n" % (" ".join( i for i in cmd ),) )

    p = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
    (out, err) = p.communicate()
    if p.returncode != 0 :
        sys.stderr.write( "ERR: pg_dump returned %d\n" % (p.returncode,))
        sys.stderr.write( " ".join( i for i in cmd ) )
        sys.stderr.write( "\n" )
        sys.stderr.write( "** STDERR **\n" )
        for i in err.splitlines() :
            sys.stderr.write( i )
            sys.stderr.write( "\n" )
#        sys.stderr.write( "** STDOUT **\n" )
#        for i in out.splitlines() :
#            sys.stderr.write( i )
#            sys.stderr.write( "\n" )
        return False

    if verbose :
        sys.stderr.write( "** STDERR **\n" )
        for i in err.splitlines() :
            sys.stderr.write( i )
            sys.stderr.write( "\n" )

# "new": just dump everything as is. it is for "bmrbeverything" database where everything is in the
# same database in different schemas.
#
    if old is None :
        with open( outfile, "w" ) as fout :
            for line in out.splitlines() :
                fout.write( line )
                fout.write( "\n" )
        return True

# "old": strip off postgres-10-isms and schema names for website databases: separate ones for
# metabolomics+"meta" and mactomolecules+"web", each with its own "dict".
# (hopefully this will go away soon. -ish...)
#
    assert old in ("macromolecules", "metabolomics")

    drop_pat = re.compile( r"^drop\s+(?:\S+\s+)?(\S+)\s+(\S+);$", re.IGNORECASE )
#                              DROP MATERIALIZED VIEW web.hupo_psi_id;
    create_pat = re.compile( r"^create\s+(\S+)\s+(\S+)", re.IGNORECASE )

    with open( outfile, "w" ) as fout :
        firstline = True
        for line in out.splitlines() :

# actual DDL commands start with "ALTER TABLE"
#
            if firstline :
                if line.upper().startswith( "ALTER" ) :
                    fout.write( "SET search_path = public, pg_catalog;\n\n" )
                    firstline = False

# these don't exist in 9.x
#
            if line.upper().startswith( "SET" ) :
                if line.startswith( "SET lock_timeout" ) :
                    continue
                if line.startswith( "SET idle_in_transaction_session_timeout" ) :
                    continue
                if line.startswith( "SET row_security" ) :
                    continue

# ALTER: there is only one to alter
#
            if line.upper().startswith( "ALTER" ) :
                fout.write( line.replace( old + ".entry_saveframes", "entry_saveframes" ) )
                fout.write( "\n" )
                continue
# DROP
#
            if line.upper().startswith( "DROP" ) :
                m = drop_pat.search( line )
                if not m :
                    raise Exception( "DROP no match: %s" % (line,) )

# strip "main" schema
#
                if m.group( 1 ).upper() == "TABLE" :
                    fout.write( "DROP TABLE IF EXISTS " )
                    if m.group( 2 ).startswith( old + "." ) :
                        fout.write( m.group( 2 ).replace( old + ".", "" ) )
                    else :
                        fout.write( m.group( 2 ) )
                    fout.write( " CASCADE;\n" )
                    continue

# omit this: no such schema
#
                if m.group( 1 ).upper() == "SCHEMA" :
                    if m.group( 2 ) == old :
                        pass
                    else :
                        fout.write( "DROP SCHEMA IF EXISTS " )
                        fout.write( m.group( 2 ) )
                        fout.write( " CASCADE;\n" )
                    continue

# else add "if exists" and "cascade"
#
                fout.write( "DROP " )
                fout.write( m.group( 1 ) )
                fout.write( " IF EXISTS " )
                fout.write( m.group( 2 ) )
                fout.write( " CASCADE;\n" )
                continue

# CREATE:
#
            if line.upper().startswith( "CREATE" ) :
                m = create_pat.search( line )
                if not m :
                    raise Exception( "CREATE no match: %s" % (line,) )

# strip "main" schema
#
                if m.group( 1 ).upper() == "TABLE" :
                    if m.group( 2 ).startswith( old + "." ) :
                        fout.write( line.replace( old + ".", "" ) )
                    else :
                        fout.write( line )
                    fout.write( "\n" )
                    continue

# omit this: no such schema
#
                if m.group( 1 ).upper() == "SCHEMA" :
                    if line.startswith( "CREATE SCHEMA " + old ) :
                        pass
                    else :
                        fout.write( line )
                        fout.write( "\n" )
                    continue

# else write out as is
#
            fout.write( line )
            fout.write( "\n" )

    return True

#
#
#
def list_tables( dsn, schema, verbose = False ) :
    if verbose :
        sys.stdout.write( "list_tables(%s)\n" % (schema,) )

    tables = []
    rc = []
    with pgdb.connect( **dsn ) as conn :
        sql = "select table_name from information_schema.tables where table_schema=%s" \
            + " and table_type='BASE TABLE' order by table_name"
        with conn.cursor() as curs :
            curs.execute( sql, (schema,) )
            for row in curs :
                tables.append( row[0] )

# if table name has uppercase letters, it has to be double-quoted
#
            for table in tables :
                if table.islower() :
                    sql = "select count(*) from %s.%s" % (schema,table)
                else :
                    sql = 'select count(*) from %s."%s"' % (schema,table)
                curs.execute( sql )
                row = curs.fetchone()
                if verbose :
                    sys.stdout.write( "%s : %s\n" % (sql, (row is None and "NULL" or row[0]),) )
                if (row is not None) and (row[0] > 0) :
                    rc.append( table )

    return rc

#
#
#
def tocsv( dsn, table, outfile, verbose = False ) :
    if verbose :
        sys.stdout.write( "tocsv(%s,%s)\n" % (table,outfile,) )

    numrows = 0

# a subselect has to be "as alias"ed
#

    if table.lower().startswith( "select" ) or table.lower().startswith( "(select" ) :
        sql= "select count(*) from %s as foo" % (table,)
    else :
        sql = "select count(*) from %s" % (table,)

# skip empty tables
#
    conn = pgdb.connect( **dsn )
    curs = conn.cursor()
    if verbose : sys.stdout.write( ">%s\n" % (sql,) )
    try :
        curs.execute( sql )
        row = curs.fetchone()
        numrows = row[0]
        curs.close()
        conn.close()
    except :
        sys.stderr.write( ">> %s <<\n" % (sql,) )
        raise

    if numrows < 1 :
        if verbose : sys.stdout.write( "No rows to dump: %s\n" % (numrows,) )
        return

    cmd = [ loader.PSQL, "-d", dsn["database"] ]
    if "user" in dsn.keys() : cmd.extend( ["-U", dsn["user"]] )
    if "host" in dsn.keys() :
        if ":" in dsn["host"] :
            (host,port) = dsn["host"].split( ":" )
        else :
            host = dsn["host"]
            port = None
        cmd.extend( ["-h", host] )
        if port is not None : cmd.extend( ["-p", port] )
    if not verbose : cmd.append( "-q" )

    stmt = "\\copy %s to '%s' csv header" % (table,outfile,)
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

#######################################################################################
#
#
if __name__ == "__main__" :

    ap = argparse.ArgumentParser( description = "Dump NMR-STAR PostgreSQL database" )
    ap.add_argument( "-v", "--verbose", help = "print lots of messages to stdout", dest = "verbose",
        action = "store_true", default = False )
    ap.add_argument( "-t", "--time", help = "time the operatons", dest = "time",
        action = "store_true", default = False )

    ap.add_argument( "-c", "--config", help = "config file", dest = "conffile", required = True )
    ap.add_argument( "-d", "--outdir", help = "directory for output files", dest = "outdir", required = True )

    ap.add_argument( "--macromol", help = "dump macromolecule database", dest = "macromol", 
        action = "store_true", default = False )
    ap.add_argument( "--metabol", help = "dump metabolomics database", dest = "metabol", 
        action = "store_true", default = False )

    args = ap.parse_args()

    cp = ConfigParser.SafeConfigParser()
    f = os.path.realpath( args.conffile )
    cp.read( f )

    if args.macromol :
        with loader.timer( label = "dump macromolecules", silent = not args.time ) :
            dump_macromolecules( config = cp, path = args.outdir, verbose = args.verbose )

    elif args.metabol :
        with loader.timer( label = "dump metabolomics", silent = not args.time ) :
            dump_metabolomics( config = cp, path = args.outdir, verbose = args.verbose )

    else :
        with loader.timer( label = "dump new-style", silent = not args.time ) :
            dump_new( config = cp, path = args.outdir, verbose = args.verbose )

