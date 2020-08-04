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

#
#
#
class PgLoader( object ) :

    CONF = {
        "psql" : "/usr/bin/psql",
        "rwuser" : "bmrb",
        "rouser" : "web",
        "ddlfile" : "schema.sql",
        "mailfrom" : "web@bmrb.wisc.edu",
        "databases" : {
            "bmrb" : {
                "dir" : "/websites/www/ftp/pub/bmrb/relational_tables/nmr-star3.1",
            },
            "metabolomics" : {
                "dir" : "/websites/www/ftp/pub/bmrb/relational_tables/metabolomics",
            },
            "bmrbeverything" : {
                "dir" : "/websites/webapi/admin/dbdump",
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
        cmd.extend( "-U", PgLoader.CONF["rwuser"] )
        cmd.extend( ["-d", database] )

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
    def fromcsv( filename, database, schema, table ) :

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
        if self._verbose :
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
        psql.extend( "-U", PgLoader.CONF["rwuser"] )
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

        return PgLoader.psql( databse = db, command = cmd, verbose = verbose )

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
        rc = ""

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

            print "PgLoader.fromcsv( filename=", f, ",database=", db, ",schema=", m.group( 1 ), "table=", m.group( 2 ), ")"

#    psql = PSQL[:]
    # if verbose : psql.append( "-a" )
    # else : psql.append( "-q" )
    # if db == "bmrb" : psql.extend( ["-d", "bmrb"] )
    # elif db == "metabolomics" : psql.extend( ["-d", "metabolomics"] )

    # if create :
        # scriptfile = SCRIPTFILE % ((db == "bmrb" and STAR31 or METABOLOMICS),)
        # if not os.path.exists( scriptfile ) :
            # rc += "%s: script file not found: %s\n" % (db, scriptfile,)
            # return rc


        # cmd = psql[:]
        # cmd.extend( ["-f", scriptfile] )

        # if verbose :
            # sys.stderr.write( " ".join( i for i in cmd ) )
            # sys.stderr.write( "\n" )
        # p = subprocess.Popen( cmd )
        # p.wait()
        # if p.returncode != 0 :
            # rc += "%s returned %d\n" % ( " ".join( i for i in cmd ), p.returncode,)
            # return rc

    # for i in files :
        # if verbose : print i
        # parts = os.path.split( i )[1].split( "." )
        # if len( parts ) == 2 : # table name (case-sensitive) . csv
            # cmd1 = psql[:]
            # cmd1.extend( ["-c", 'truncate "%s" cascade' % (parts[0],)] )
            # cmd2 = psql[:]
            # cmd2.extend( ["-c", '\\copy "%s" from %s csv header' % (parts[0],i)] )
        # elif len( parts ) == 3 : # schema . table . csv
            # cmd1 = psql[:]
            # cmd1.extend( ["-c", 'truncate %s.%s cascade' % (parts[0],parts[1])] )
            # cmd2 = psql[:]
            # cmd2.extend( ["-c", '\\copy %s.%s from %s csv header' % (parts[0],parts[1],i)] )

        # if verbose : print cmd1
        # p = subprocess.Popen( cmd1 )

        # p.wait()
        # if p.returncode != 0 :
            # rc += "%s returned %d\n" % ( " ".join( i for i in cmd1 ), p.returncode,)
# #            continue

        # if verbose : print cmd2
        # p = subprocess.Popen( cmd2 )
        # p.wait()
        # if p.returncode != 0 :
            # rc += "%s returned %d\n" % ( " ".join( i for i in cmd2 ), p.returncode,)
# #            continue

        return rc






########################################################################################
DIR = "/websites/www/ftp/pub/bmrb/relational_tables"
METABOLOMICS ="%s/metabolomics" % (DIR)
STAR31 = "%s/nmr-star3.1" % (DIR)
SCRIPTFILE = "%s/schema.sql"
ROUSER = "web"
RWUSER = "bmrb"
PSQL = ["/usr/pgsql-10/bin/psql", "-U", RWUSER]

FROM = "web@bmrb.wisc.edu"

#
# Run: grant connect on databased $db to $ROUSER
# foreach schema :
#  grant select on all tables in schema $SCHEMA to $ROUSER
#  alter default privileges in schema $SCHEMA grant select on tables to $ROUSER
#
#  grant usage on all sequences in schema $SCHEMA to $ROUSER
#  alter default privileges in schema $SCHEMA grant usage on sequences to $ROUSER
#
def update_grants( db = "bmrb", verbose = False ) :
    global ROUSER
    global RWUSER
    global PSQL

    assert db in ("bmrb", "metabolomics")

    sqls = ("grant usage on schema %s to %s",
            "grant select on all tables in schema %s to %s",
            "alter default privileges in schema %s grant select on tables to %s",
            "grant usage on all sequences in schema %s to %s",
            "alter default privileges in schema %s grant usage on sequences to %s",)

    rc = ""

    psql = PSQL[:]
    if not verbose : psql.append( "-q" )
    psql.extend( ["-d", db] )

    cmd = psql[:]
    cmd.extend( ["-A", "-t", "-F,"] )  # CSV output, tuples only
    cmd.extend( ["-c", r"\dn"] )
    if verbose :
        sys.stderr.write( " ".join( i for i in cmd ) )
        sys.stderr.write( "\n" )
    p = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
    (out, err) = p.communicate()
    if p.returncode != 0 :
        rc += "%s: psql -c \dn returned %d\n" % (db,p.returncode,)
        return rc

    for i in out.splitlines() :
        (schema, owner) = i.split( "," )
#        if owner == RWUSER :
        if True :
            for sql in sqls :
                cmd = psql[:]
                cmd.extend( ["-c", sql % (schema, ROUSER,)] )
                if verbose :
                    sys.stderr.write( " ".join( i for i in cmd ) )
                    sys.stderr.write( "\n" )
                p = subprocess.Popen( cmd )
                p.wait()
                if p.returncode != 0 :
                    rc += "%s: psql -c 'grant r/o privs' returned %d\n" % (db,p.returncode,)

    return rc

#
#
def update_db( db = "bmrb", create = False, schema = "all", verbose = False ) :
    global STAR31
    global METABOLOMICS
    global SCRIPTFILE
    global PSQL

    assert db in ("bmrb", "metabolomics")
    rc = ""

    if (schema == "all") or (schema == "public" ) : wild = "%s/*.csv" % ((db == "bmrb" and STAR31 or METABOLOMICS),)
    else : wild = "%s/%s.*.csv" % ((db == "bmrb" and STAR31 or METABOLOMICS), schema)
    files = glob.glob( wild )

    pat = re.compile( r"^[^.]+\.csv$" )
    tmp = []
    if schema == "public" :
        for f in files :
#            if verbose : print f
            m = pat.search( os.path.split( f )[1] )
            if m : tmp.append( f )
        files = tmp[:]
#        if verbose : print tmp

    if len( files ) < 1 :
        rc += "No input files for %s - %s\n" % (db,schema,)
        return rc

    psql = PSQL[:]
    if verbose : psql.append( "-a" )
    else : psql.append( "-q" )
    if db == "bmrb" : psql.extend( ["-d", "bmrb"] )
    elif db == "metabolomics" : psql.extend( ["-d", "metabolomics"] )

    if create :
        scriptfile = SCRIPTFILE % ((db == "bmrb" and STAR31 or METABOLOMICS),)
        if not os.path.exists( scriptfile ) :
            rc += "%s: script file not found: %s\n" % (db, scriptfile,)
            return rc


        cmd = psql[:]
        cmd.extend( ["-f", scriptfile] )

        if verbose :
            sys.stderr.write( " ".join( i for i in cmd ) )
            sys.stderr.write( "\n" )
        p = subprocess.Popen( cmd )
        p.wait()
        if p.returncode != 0 :
            rc += "%s returned %d\n" % ( " ".join( i for i in cmd ), p.returncode,)
            return rc

    for i in files :
        if verbose : print i
        parts = os.path.split( i )[1].split( "." )
        if len( parts ) == 2 : # table name (case-sensitive) . csv
            cmd1 = psql[:]
            cmd1.extend( ["-c", 'truncate "%s" cascade' % (parts[0],)] )
            cmd2 = psql[:]
            cmd2.extend( ["-c", '\\copy "%s" from %s csv header' % (parts[0],i)] )
        elif len( parts ) == 3 : # schema . table . csv
            cmd1 = psql[:]
            cmd1.extend( ["-c", 'truncate %s.%s cascade' % (parts[0],parts[1])] )
            cmd2 = psql[:]
            cmd2.extend( ["-c", '\\copy %s.%s from %s csv header' % (parts[0],parts[1],i)] )

        if verbose : print cmd1
        p = subprocess.Popen( cmd1 )

        p.wait()
        if p.returncode != 0 :
            rc += "%s returned %d\n" % ( " ".join( i for i in cmd1 ), p.returncode,)
#            continue

        if verbose : print cmd2
        p = subprocess.Popen( cmd2 )
        p.wait()
        if p.returncode != 0 :
            rc += "%s returned %d\n" % ( " ".join( i for i in cmd2 ), p.returncode,)
#            continue

    return rc

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
    ap.add_argument( "-m", "--mail", dest = "email",
        help = "e-mail to send errors/output" )

    args = ap.parse_args()
    messages = ""

    if (args.db.lower() == "bmrb") or (args.db.lower() == "all") :
        PgLoader.update_db( db = "bmrb", create = args.create,
            schema = args.schema, path = args.filedir, verbose = args.verbose )
        if args.grant :
            PgLoader.add_ro_grants( db = "bmrb", verbose = args.verbose )
    if (args.db.lower() == "bmrbeverything") or (args.db.lower() == "all") :
        PgLoader.update_db( db = "bmrbeverything", create = args.create,
            schema = args.schema, path = args.filedir, verbose = args.verbose )
        if args.grant :
            PgLoader.add_ro_grants( db = "bmrbeverything", verbose = args.verbose )
    if (args.db.lower() == "metabolomics") or (args.db.lower() == "all") :
        PgLoader.update_db( db = "metabolomics", create = args.create,
            schema = args.schema, path = args.filedir, verbose = args.verbose )
        if args.grant :
            PgLoader.add_ro_grants( db = "metabolomics", verbose = args.verbose )



        # update_db( db = "bmrb", create = options.create, schema = options.schema, verbose = options.verbose )
        # if options.grant :
            # messages += update_grants( db = "bmrb", verbose = options.verbose )
    # if (options.db.lower() == "metabolomics") or (options.db.lower() == "all") :
        # messages += update_db( db = "metabolomics", create = options.create, schema = options.schema, verbose = options.verbose )
        # if options.grant :
            # messages += update_grants( db = "metabolomics", verbose = options.verbose )

    # if len( messages.strip() ) > 0 :
        # if options.email is None :
            # sys.stderr.write( messages )
            # sys.stderr.write( "\n" )
        # else :
            # msg = MIMEText.MIMEText( messages )
            # msg["To"] = options.email
            # msg["Subject"] = "BMRB DB update errors"
            # msg["From"] = FROM
            # msg["Reply-To"] = FROM

            # sm = smtplib.SMTP( "mail.bmrb.wisc.edu" )
            # if options.verbose :
                # sm.set_debuglevel( 1 )
                # sys.stderr.write( msg.as_string() )
                # sys.stderr.write( "\n" )
            # sm.sendmail( msg["From"], options.email, msg.as_string() )

#
#
#
