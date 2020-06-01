#!/usr/bin/python -u
#
#

from __future__ import absolute_import

from contextlib import contextmanager
import time
import sys
import os
import subprocess
#import re
import ConfigParser

#STAROBJ_PATH = "/bmrb/lib/python27"
STAROBJ_PATH = "/share/dmaziuk/projects/starobj"
sys.path.append( STAROBJ_PATH )
import starobj

from .ets import released_ids_itr, depids_itr, processing_queue_itr, removed_ids_itr
from .csvdump import tocsv
from .csvload import fromcsv

from .dictionary import load as load_dict
from .chemcomps import dump_and_load as load_chem_comps
from .metabolomicsextras import load as load_meta_schema
from .entries import load_metabolomics, load_macromolecules
from .macromol import fixup as fix_macromolecules
from .webextras import load as load_web_schema
from .csvdump import dump_new, dump_macromolecules, dump_metabolomics
from .csvdump_better import Dumper

PSQL = "/usr/pgsql-10/bin/psql"
PGDUMP = "/usr/pgsql-10/bin/pg_dump"

#######################################

# simple timings
#
@contextmanager
def timer( label, silent = False ) :
    start = time.time()
    try :
        yield
    finally :
        end = time.time()
        if not silent :
            sys.stdout.write( "%s: %0.3f\n" % (label,(end - start)) )

#######################################

# read config file into pgdb connection kwargs
#
def dsn( config, section ) :

    assert isinstance( config, ConfigParser.SafeConfigParser )
    rc = {}
    if not config.has_section( section ) :
        sys.stderr.write( "No [%s] section in config file\n" % (section,) )
        raise Exception( "Bad config" )
    if not config.has_option( section, "database" ) :
        sys.stderr.write( "No database in [%s] section in config file\n" % (section,) )
        raise Exception( "Bad config" )

    rc["database"] = config.get( section, "database" )

# for psql
#
    rc["dbname"] = rc["database"]
    if config.has_option( section, "user" ) :
        rc["user"] = config.get( section, "user" )
    if config.has_option( section, "password" ) :
        rc["password"] = config.get( section, "password" )

# (if there's port we need to go via localhost. if not: it's a unix socket)
#
    host = None
    if config.has_option( section, "host" ) :
        host = config.get( section, "host" )
    if config.has_option( section, "port" ) :
        if host is None : host = "localhost:%s" % (config.get( section, "port" ),)
        else : host = "%s:%s" % (host,config.get( section, "port" ),)

    if host is not None : rc["host"] = host

    return rc

#######################################

# wrapper for `psql -f <script>`
#
def runscript( dsn, script, verbose = False ) :

    global PSQL
    cmd = [ PSQL, "-d", dsn["database"] ]
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

    f = os.path.realpath( script )
    if not os.path.exists( f ) : raise IOError( "File not found: %s" % (f,) )

    cmd.extend( ["-f", f] )

    if verbose :
        sys.stderr.write( " ".join( i for i in cmd ) )
        sys.stderr.write( "\n" )

    p = subprocess.Popen( cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
    (out, err) = p.communicate()
    if p.returncode != 0 :
        sys.stderr.write( "ERR: psql returned %d\n" % (p.returncode,))
        sys.stderr.write( " ".join( i for i in cmd ) )
        sys.stderr.write( "\n" )
        sys.stderr.write( "** STDERR **\n" )
        for i in err.splitlines() :
            sys.stderr.write( i )
            sys.stderr.write( "\n" )
        sys.stderr.write( "** STDOUT **\n" )
        for i in out.splitlines() :
            sys.stderr.write( i )
            sys.stderr.write( "\n" )
        return False

    if verbose :
        sys.stderr.write( "** STDERR **\n" )
        for i in err.splitlines() :
            sys.stderr.write( i )
            sys.stderr.write( "\n" )
        sys.stderr.write( "** STDOUT **\n" )
        for i in out.splitlines() :
            sys.stderr.write( i )
            sys.stderr.write( "\n" )

    return True

#######################################

# adds read-only grants using `psql -c "grant ..."`
#
def add_ro_grants( dsn, schema, user, verbose = False ) :

    sqls = ("grant usage on schema %s to %s",
            "grant select on all tables in schema %s to %s",
            "alter default privileges in schema %s grant select on tables to %s",
            "grant usage on all sequences in schema %s to %s",
            "alter default privileges in schema %s grant usage on sequences to %s",)

    global PSQL
    cmd = [ PSQL, "-d", dsn["database"] ]
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

    for sql in sqls :

        c = cmd[:]
        c.extend( ["-c", sql % (schema,user,)] )

        if verbose :
            sys.stderr.write( " ".join( i for i in c ) )
            sys.stderr.write( "\n" )

        p = subprocess.Popen( c, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
        (out, err) = p.communicate()
        if p.returncode != 0 :
            sys.stderr.write( "ERR: psql returned %d\n" % (p.returncode,))
            sys.stderr.write( " ".join( i for i in cmd ) )
            sys.stderr.write( "\n" )
            sys.stderr.write( "** STDERR **\n" )
            for i in err.splitlines() :
                sys.stderr.write( i )
                sys.stderr.write( "\n" )
            sys.stderr.write( "** STDOUT **\n" )
            for i in out.splitlines() :
                sys.stderr.write( i )
                sys.stderr.write( "\n" )
            return False

        if verbose :
            sys.stderr.write( "** STDERR **\n" )
            for i in err.splitlines() :
                sys.stderr.write( i )
                sys.stderr.write( "\n" )
            sys.stderr.write( "** STDOUT **\n" )
            for i in out.splitlines() :
                sys.stderr.write( i )
                sys.stderr.write( "\n" )

    return True

#######################################
#
#

__all__ = [ "starobj",
    "PSQL", "PGDUMP",
    "timer", 
    "dsn", "runscript", "add_ro_grants",
    "released_ids_itr", "depids_itr", "processing_queue_itr", "removed_ids_itr",
    "fromcsv", "tocsv",
    "load_dict",
    "load_chem_comps",
    "load_meta_schema",
    "load_metabolomics", "load_macromolecules",
    "fix_macromolecules",
    "load_web_schema",
    "dump_new", "dump_macromolecules", "dump_metabolomics",
    "Dumper"
    ]

#
# eof
