#!/usr/bin/python -u
#
# read [foo.]bar.csv into [foo.]bar DB table
#  if bar is not all lowercase, load into [foo.]"bar"
#  get column order from 1st line in case it does not match between CSV and DB
#

import sys
import os
import re
import glob
import subprocess
import pprint

#import smtplib
#from email import MIMEText

import argparse

from contextlib import contextmanager
import time

@contextmanager
def timer( label, verbose = True ) :
    start = time.time()
    try :
        yield
    finally :
        end = time.time()
        if verbose :
            sys.stdout.write( "%s: %0.3f\n" % (label,(end - start)) )

#
#
#
class PgLoader( object ) :

    CONF = {
        "psql" : "/usr/bin/psql",
        "rwuser" : "bmrb",
        "rouser" : "web",
        "host": "bmrb-staging.cam.uchc.edu",
        "ddlfile" : "schema.sql",
        "mailfrom" : "web@bmrb.wisc.edu",
        "databases" : {
            "bmrb" : {
                "dir" : "/projects/BMRB/staging/dbdump/bmrb",
            },
            "metabolomics" : {
                "dir" : "/projects/BMRB/staging/dbdump/metabolomics",
            },
            "bmrbeverything" : {
                "dir" : "/projects/BMRB/staging/dbdump/bmrbeverything",
#               "schemata" : (
#                   "dict",
#                   "macromolecules",
#                   "metabolomics",
#                   "chemcomps",
#                   "web",
#                   "meta"
#               )
            }
        }
    }

    # wrapper for subprocess call
    #
    @staticmethod
    def psql( database, command, verbose = False ) :

        cmd = [PgLoader.CONF["psql"]]
        cmd.extend( ["-U", PgLoader.CONF["rwuser"]] )
        cmd.extend( ["-d", database] )
        if "host" in PgLoader.CONF.keys() :
            cmd.extend( ["-h", PgLoader.CONF["host"]] )

        if not verbose : cmd.append( "-q" )

        cmd.extend( command )

        if verbose :
            sys.stdout.write( " ".join( i for i in cmd ) )
            sys.stdout.write( "\n" )

        p = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
        (out, err) = p.communicate()
        if p.returncode != 0 :
            sys.stderr.write( "ERR: psql returned %d\n" % (p.returncode,))
            sys.stderr.write( " ".join( i for i in cmd ) )
            sys.stderr.write( "\n" )

        if (p.returncode != 0) or verbose :
            sys.stderr.write( "** STDERR **\n" )
            for i in err.splitlines() :
                sys.stderr.write( i )
                sys.stderr.write( "\n" )
            sys.stderr.write( "** STDOUT **\n" )
            for i in out.splitlines() :
                sys.stderr.write( i )
                sys.stderr.write( "\n" )

        return p.returncode

    # load one table from csv
    # read column order from 1st line: it may not match the db
    # truncate table before load: it's 0-cost if it's empty
    #
    @staticmethod
    def fromcsv( filename, database, schema, table, verbose = False ) :

        assert database in PgLoader.CONF["databases"].keys()

        infile = os.path.realpath( filename )
        if not os.path.exists( infile ) :
            raise IOError( "Not found: %s" % (infile,) )

# column names
#
        cols = []
        with open( infile, "rU" ) as f :
            l = f.readline()
            for col in l.split( "," ) :
                cols.append( col.strip().strip( "'\"" ) )

        if len( cols ) < 1 :
            sys.stderr.write( "no columns in %s\n" % (infile,) )
            return -1

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

#        stmt = "\\copy %s%s (%s) from '%s' csv header" % (scam,tbl,colstr,infile,)
#        cmd = ["-c", stmt]

        trunc = "truncate table only %s%s" % (scam,tbl)
        stmt = "\\copy %s%s (%s) from '%s' csv header" % (scam,tbl,colstr,infile,)
#        cmd = ["-c", "begin", "-c", trunc, "-c", stmt, "-c", "commit"]
        if verbose :
            cmd = ["-c", "\\timing on", "-c", trunc, "-c", stmt]
        else :
            cmd = ["-c", trunc, "-c", stmt]

        return PgLoader.psql( database = database, command = cmd, verbose = verbose )

    # add read-only grants for RO user
    #
    @staticmethod
    def add_ro_grants( db = "bmrb", verbose = False ) :

        assert db in PgLoader.CONF["databases"].keys()

        sqls = ("grant usage on schema %s to %s",
            "grant select on all tables in schema %s to %s",
            "alter default privileges in schema %s grant select on tables to %s",
            "grant usage on all sequences in schema %s to %s",
            "alter default privileges in schema %s grant usage on sequences to %s",)

        rc = ""

# want stdout from psql
#
        psql = [PgLoader.CONF["psql"]]
        psql.extend( ["-U", PgLoader.CONF["rwuser"]] )
        if not verbose : psql.append( "-q" )
        psql.extend( ["-d", db] )

        cmd = psql[:]
        cmd.extend( ["-A", "-t", "-F,"] )  # CSV output, tuples only
        cmd.extend( ["-c", r"\dn"] )
        if verbose :
            sys.stdout.write( " ".join( i for i in cmd ) )
            sys.stdout.write( "\n" )

        p = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
        (out, err) = p.communicate()
        if p.returncode != 0 :
            rc += "%s: psql -c \dn returned %d\n" % (db,p.returncode,)
            return rc

        for i in out.splitlines() :
            (schema, owner) = i.split( "," )
#        if owner == RWUSER :
            for sql in sqls :
                cmd = psql[:]
                cmd.extend( ["-c", sql % (schema, PgLoader.CONF["rouser"],)] )
                if verbose :
                    sys.stdout.write( " ".join( i for i in cmd ) )
                    sys.stdout.write( "\n" )
                p = subprocess.Popen( cmd )
                p.wait()
                if p.returncode != 0 :
                    rc += "%s: psql -c 'grant r/o privs' returned %d\n" % (db,p.returncode,)

        return rc

    # run schema.sql to drop and recreate tables
    #
    @staticmethod
    def runscript( scriptfile, db = "bmrb", verbose = False ) :

        assert db in PgLoader.CONF["databases"].keys()
        script = os.path.realpath( scriptfile )
        if not os.path.exists( script ) :
            raise IOError( "Not found: %s" % (script,) )

        cmd = ["-f", script]

        return PgLoader.psql( database = db, command = cmd, verbose = verbose )

    # glob files and decide which to load where
    #
    @staticmethod
    def update_db( db = "bmrb", create = False, schema = "any", path = None, verbose = False ) :

        assert db in PgLoader.CONF["databases"].keys()
        if path is not None :
            inputdir = os.path.realpath( path )
        else :
            inputdir = os.path.realpath( PgLoader.CONF["databases"][db]["dir"] )
        if not os.path.isdir( inputdir ) :
            raise IOError( "Not a directory: %s" % (inputdir,) )

        script = os.path.realpath( os.path.join( inputdir, PgLoader.CONF["ddlfile" ] ) )
        rc = ""

#
#
        if create :
            if not os.path.exists( script ) :
                raise IOError( "Not found: %s" % (script,) )
            x = PgLoader.runscript( scriptfile = script, db = db, verbose = verbose )
            if x != 0 :
                sys.stderr.write( "runscript %s returned %s\n" % (script, x,) )
#                return "ERR: runscript %s returned %s\n" % (script, x,)

# could be table.csv or schema.table.csv
#
        pat = re.compile( r"(?:([^.]+)\.)?([^.]+)\.csv$" )

        wild = os.path.join( inputdir, "*.csv" )

        files = []
        for f in glob.glob( wild ) :
            m = pat.search( os.path.split( f )[1] )
            if m :
                files.append( f )

#        pprint.pprint( files )
        if len( files ) < 1 :
            rc += "No input files for %s - %s\n" % (db,schema,)
            return rc

        for f in files :
            m = pat.search( os.path.split( f )[1] )
            if not m : continue # can never happen

# skip schema?
#
            if (schema is not None) and (schema != "any") :
                if schema != m.group( 1 ) :
                    continue

            x = PgLoader.fromcsv( filename = f, database = db, schema = m.group( 1 ), 
                table = m.group( 2 ), verbose = verbose )
            if x != 0 :
                rc += "\npsql load of %s returned %s\n" % (f,x,)

        return rc

########################################################################################
#
#
if __name__ == "__main__" :

    ap = argparse.ArgumentParser( description = "Load BMRB database" )
    ap.add_argument( "-v", "--verbose", default = False, action = "store_true",
        help = "print lots of messages to stdout", dest = "verbose" )
    ap.add_argument( "-d", "--database", dest = "db", default = "all",
        help = "DB to load: bmrb, metabolomics, or bmrbeverything",
        required = True )
    ap.add_argument( "-s", "--schema", dest = "schema", default = "any",
        help = "load only given schema (e.g. dict)" )
    ap.add_argument( "-c", "--create", default = False, action = "store_true",
        help = "run schema.sql first to drop and re-create all objects",
        dest = "create" )
    ap.add_argument( "-i", "--input", dest = "filedir",
        help = "directory with input files" )
    ap.add_argument( "-g", "--grants", default = False, action = "store_true",
        help = "add read-only grants for web user",
        dest = "grant" )
#    ap.add_argument( "-m", "--mail", dest = "email",
#        help = "e-mail to send errors/output" )

    args = ap.parse_args()
    messages = ""

    if (args.db.lower() == "bmrb") or (args.db.lower() == "all") :
        with timer( "Load BMRB", verbose = True ) :
            PgLoader.update_db( db = "bmrb", create = args.create,
                schema = args.schema, path = args.filedir, verbose = args.verbose )
            if args.grant :
                PgLoader.add_ro_grants( db = "bmrb", verbose = args.verbose )
    if (args.db.lower() == "bmrbeverything") or (args.db.lower() == "all") :
        with timer( "Load API", verbose = True ) :
            PgLoader.update_db( db = "bmrbeverything", create = args.create,
                schema = args.schema, path = args.filedir, verbose = args.verbose )
            if args.grant :
                PgLoader.add_ro_grants( db = "bmrbeverything", verbose = args.verbose )
    if (args.db.lower() == "metabolomics") or (args.db.lower() == "all") :
        with timer( "Load metabolomics", verbose = True ) :
            PgLoader.update_db( db = "metabolomics", create = args.create,
                schema = args.schema, path = args.filedir, verbose = args.verbose )
            if args.grant :
                PgLoader.add_ro_grants( db = "metabolomics", verbose = args.verbose )

#
# eof
#
