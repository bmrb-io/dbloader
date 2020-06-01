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
import shutil

import glob
import pprint
import re

_UP = os.path.abspath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import loader


#
#
class Dumper( object ) :

# config sections
#
    ALLSECTIONS = ( "dictionary", "macromolecules", "metabolomics", "web", "meta", "chemcomps" )
    STRSECTIONS = ( "macromolecules", "metabolomics", "chemcomps" )

# move these to config someday?
#
    DDLFILE = "schema.sql"
    SQLFILE = "_load_all.sql"

    #
    #
    def __init__( self, config, outdir, verbose = False ) :

        assert isinstance( config, ConfigParser.SafeConfigParser )

        if (not os.path.exists( os.path.realpath( loader.PGDUMP ) )) \
        or (not os.path.exists( os.path.realpath( loader.PSQL ) )) :
            sys.stderr.write( "Run this where postgresql-10 is installed!\n" )
            sys.stderr.write( "Not found: %s\n" % (loader.PGDUMP,) )
            raise Exception( "Need postgresql 10 for this" )

        self._config = config
        self._outdir = os.path.realpath( outdir )
        self._verbose = bool( verbose )
        self._tables = {}
        self._dumpscript = []

    #
    #
    @classmethod
    def dump( cls, config, outdir, verbose = False ) :

        d = cls( config = config, outdir = outdir, verbose = verbose )
        d._dump()
        return d

    ##################################################################
    #
    #
    def _dump( self ) :

        if os.path.exists( self._outdir ) :
            shutil.rmtree( self._outdir )
        os.umask( 0o002 )
        os.makedirs( self._outdir )

        for k in self.ALLSECTIONS :
            self._tables[self._config.get( k, "schema" )] = {}

        dsn = loader.dsn( self._config, "dictionary" )

        self._list_tables( dsn )
        if self._verbose :
            sys.stdout.write( "******\n" )
            pprint.pprint( self._tables )

        ddlfile = os.path.join( self._outdir, self.DDLFILE )
        sqlfile = os.path.join( self._outdir, self.SQLFILE )

        lines = self._read_ddl( dsn, self._tables.keys() )
        if lines is None :
            raise Exception( "No output from pg_dump" )
        if len( lines ) < 1 :
            raise Exception( "Empty output from pg_dump" )

        self._write_ddl( lines, ddlfile, ddl = True )
        self._write_ddl( lines, sqlfile, ddl = False )

        lines = self._to_csv()
        if lines is None :
            raise Exception( "No output from csv dump" )
        if len( lines ) < 1 :
            raise Exception( "Empty output from csv dump" )

        with open( sqlfile, "a" ) as out :

            for line in lines :
                out.write( "%s;\n" % (line,) )

            out.write( "\nCOMMIT;\n\n--\n" )

        for line in self._dumpscript :
            sys.stdout.write( "%s;\n" % (line,) )

    # list tables with data
    #
    def _list_tables( self, dsn ) :
        if self._verbose :
            sys.stdout.write( "%s.list_tables()\n" % (self.__class__.__name__,) )

        tmp = []
        tblqry = "select table_name from information_schema.tables where table_schema=%s" \
            + " and table_type='BASE TABLE' order by table_name"
        colqry = "select column_name from information_schema.columns" \
            + " where table_schema=%s and table_name=%s order by ordinal_position"
        with pgdb.connect( **dsn ) as conn :
            with conn.cursor() as curs :
                for schema in self._tables.keys() :
                    del tmp[:]
                    curs.execute( tblqry, (schema,) )
                    for row in curs :
                        tmp.append( row[0] )

# if table name has uppercase letters, it has to be double-quoted
#
                    for table in tmp :
                        if table.islower() :
                            sql = "select count(*) from %s.%s" % (schema,table)
                        else :
                            sql = 'select count(*) from %s."%s"' % (schema,table)
                        curs.execute( sql )
                        row = curs.fetchone()
                        if self._verbose :
                            sys.stdout.write( "%s : %s\n" % (sql, (row is None and "NULL" or row[0]),) )
                        if (row is not None) and (row[0] > 0) :
                            self._tables[schema][table] = []

#                            sys.stdout.write( "%s %s %s\n" % (colqry,schema,table,) )
                            curs.execute( colqry, (schema,table,) )
                            for row in curs :
                                self._tables[schema][table].append( row[0] )

    # run pg_dump, return its output or none
    #
    def _read_ddl( self, dsn, schemata ) :

# "clean", no-owner, no ACLs, plain SQL output, schema only
#
        cmd = [loader.PGDUMP, "-c", "--if-exists", "-O", "-x", "-F", "p", "-s"]
        for s in schemata :
            cmd.extend( ["-n", s] )

# it'll fail if there is a password
#
        if "user" in dsn.keys() : cmd.extend( ["-U", dsn["user"]] )

# pygresql-style is host:port
#
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

        if self._verbose :
            sys.stdout.write( "%s\n" % (" ".join( i for i in cmd ),) )

        p = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
        (out, err) = p.communicate()
        if (p.returncode != 0) or (len( out ) < 1) :
            sys.stderr.write( "ERR: pg_dump returned %d\n" % (p.returncode,))
            sys.stderr.write( " ".join( i for i in cmd ) )
            sys.stderr.write( "\n" )
            sys.stderr.write( "** STDERR **\n" )
            for i in err.splitlines() :
                sys.stderr.write( i )
                sys.stderr.write( "\n" )
            return None

        if self._verbose :
            sys.stderr.write( "** STDERR **\n" )
            for i in err.splitlines() :
                sys.stderr.write( i )
                sys.stderr.write( "\n" )

        return out.splitlines()

    #
    #
    def _write_ddl( self, lines, outfile, ddl = True ) :
        with open( outfile, "w" ) as out :
            if not ddl :
                out.write( "--\n-- drop and reload script\n--\n\nBEGIN;\n\n" )
            for line in lines :
                out.write( line )
                out.write( "\n" )

    #
    #
    def _to_csv( self ) :

        ldr = []
        colstr = ""
        for schema in self._tables.keys() :
            for table in self._tables[schema].keys() :
                if schema in self.STRSECTIONS :
                    colstr = '("%s")' % ('","'.join( c for c in self._tables[schema][table] ),)
                else :
                    colstr = '(%s)' % (','.join( c for c in  self._tables[schema][table] ),)

                outfname = "%s.%s.csv" % (schema,table,)
                outfile = os.path.join( self._outdir, "%s.%s.csv" % (schema,table,) )

                if schema in self.STRSECTIONS :
                    trunc = 'truncate %s."%s"' % (schema,table)
                    load = '\\copy %s."%s" %s from \'%s\' csv header' \
                        % (schema,table,colstr,outfname,)
                    dump = '\\copy %s."%s" %s to \'%s\' csv header' \
                        % (schema,table,colstr,outfile,)
                else :
                    trunc = 'truncate %s.%s' % (schema,table)
                    load = "\\copy %s.%s %s from '%s' csv header" \
                        % (schema,table,colstr,outfname,)
                    dump = '\\copy %s.%s %s to \'%s\' csv header' \
                        % (schema,table,colstr,outfile,)

                ldr.append( "begin" )
                ldr.append( trunc )
                ldr.append( load )
                ldr.append( "commit" )
                self._dumpscript.append( dump )

        return ldr

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
    curs.execute( sql )
    row = curs.fetchone()
    numrows = row[0]
    curs.close()    
    conn.close()

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

    ap = argparse.ArgumentParser( description = "load NMR-STAR dictionary into PostgreSQL database" )
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

