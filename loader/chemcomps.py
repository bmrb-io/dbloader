#!/usr/bin/python -u
#
# chem comps live in a separate database that includes unreleased ones.
# 1. dump the out to CSV (only released ones)
# 2. load from those CSVs into bmrbeverything DB
#

from __future__ import absolute_import

import sys
assert ((sys.version_info[0] == 2) and (sys.version_info[1] > 6))

import os
import argparse
import ConfigParser
import pgdb
import pprint
import glob
import tempfile
import shutil

_UP = os.path.abspath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import loader

DB = "chemcomps"

TABLES = ( "Atom_nomenclature",
        "Characteristic",
        "Chem_comp",
        "Chem_comp_SMILES",
        "Chem_comp_angle",
        "Chem_comp_atom",
        "Chem_comp_bio_function",
        "Chem_comp_bond",
        "Chem_comp_citation",
        "Chem_comp_common_name",
        "Chem_comp_db_link",
        "Chem_comp_descriptor",
        "Chem_comp_identifier",
        "Chem_comp_keyword",
        "Chem_comp_systematic_name",
        "Chem_comp_tor",
        "Chem_struct_descriptor",
        "Entity",
        "Entity_atom_list",
        "Entity_biological_function",
        "Entity_bond",
        "Entity_chem_comp_deleted_atom",
        "Entity_chimera_segment",
        "Entity_citation",
        "Entity_common_name",
        "Entity_comp_index",
        "Entity_comp_index_alt",
        "Entity_db_link",
        "Entity_keyword",
        "Entity_poly_seq",
        "Entity_systematic_name",
        "PDBX_chem_comp_feature"
        )

############################################################################################

#FIXME!! put scrschema in the config file
#
def list_tables( curs, verbose = False ) :
    rc = []
    sql = "select table_name from information_schema.tables where table_schema='chem_comp'"
    if verbose : sys.stdout.write( "%s\n" % (sql,) )
    curs.execute( sql )
    for row in curs :
        rc.append( row[0] )
        if verbose : sys.stdout.write( "%s\n" % (row[0],) )
    if len( rc ) < 1 : 
        return None
    return rc

# main
#
def dump_and_load( config, verbose = False ) :
    assert isinstance( config, ConfigParser.SafeConfigParser )
    global DB
    wd = tempfile.mkdtemp()
    try :
        dump( config, where = wd, verbose = verbose )
#        fix_inchi_column( where = wd, verbose = verbose )
        load( config, where = wd, verbose = verbose )
        fix_entry_id( config, verbose = verbose )
        if config.has_option( DB, "rouser" ) :
            loader.add_ro_grants( dsn = loader.dsn( config, DB ), schema = config.get( DB, "schema" ),
                    user = config.get( DB, "rouser" ), verbose = verbose )  

    finally :
        shutil.rmtree( wd )

############################################################################################
# dump released chem comps to CSV files
#
def dump( config, where = None, verbose = False ) :

    if verbose : sys.stdout.write( "dump()\n" )
    if where is not None : assert os.path.isdir( where )

    global DB
#    global TABLES

    assert isinstance( config, ConfigParser.SafeConfigParser )
    if not config.has_section( DB ) :
        sys.stderr.write( "No [%s] section in config file\n" % (DB,) )
        return False

    dsn = {}
    if not config.has_option( DB, "srcdatabase" ) :
        sys.stderr.write( "No srcdatabase in [%s] section in config file\n" % (DB,) )
        return False
    dsn["database"] = config.get( DB, "srcdatabase" )

# for psql
#
    dsn["dbname"] = dsn["database"]
    if config.has_option( DB, "srcuser" ) :
        dsn["user"] = config.get( DB, "srcuser" )
    if config.has_option( DB, "srcpassword" ) :
        dsn["password"] = config.get( DB, "srcpassword" )
    if config.has_option( DB, "srchost" ) :
        host = config.get( DB, "srchost" )
    else :
        host = None
    if config.has_option( DB, "srcport" ) :
        if host is None : dsn["host"] = "localhost:%s" % (config.get( DB, "srcport" ),)
        else : dsn["host"] = "%s:%s" % (host,config.get( DB, "srcport" ),)
    else :
        if host is not None : dsn["host"] = host

    if verbose : pprint.pprint( dsn )

    tables = None
    cidstr = ""
    cidstr1 = ""
    eidstr = ""

    with pgdb.connect( **dsn ) as conn :
        with conn.cursor() as curs :

            tables = list_tables( curs, verbose = verbose )
            if tables is None :
                raise Exception( "no tables to dump in chem_comp" )

# unreleased chem comps/entities
#
            cids = set()

# can't join if entity table's empty
#
#            sql = 'select c."ID",e."ID",c."Release_status" from chem_comp."Chem_comp" c ' \
#                + 'join chem_comp."Entity" e on e."Nonpolymer_comp_ID"=c."ID" ' \
#                + """where c."Release_status"<>'REL'"""

            sql = """select "ID" from chem_comp."Chem_comp" where "Release_status"<>'REL'"""
            curs.execute( sql )
            for row in curs :
                cids.add( row[0] )

            if len( cids ) > 0 :
                cidstr = """ where "ID" not in ('%s')""" % ("','".join( str( i ) for i in cids ),)
                cidstr1 = """ where "Comp_ID" not in ('%s')""" % ("','".join( str( i ) for i in cids ),)

# entities only have unique Sf_ID
#
            eids = set()
            sql = 'select "Sf_ID" from chem_comp."Entity"' \
                + """ where "Nonpolymer_comp_ID" not in ('%s')""" % ("','".join( str( i ) for i in cids ),)
            curs.execute( sql )
            for row in curs :
                eids.add( row[0] )

            if len( eids ) > 0 :
                eidstr = ' where "Sf_ID" not in (%s)' % (",".join( str( i ) for i in eids ),)

# clean up
#
    if where is not None : 
        for f in glob.glob( os.path.join( where, "*" ) ) :
            os.unlink( f )

# make queries for writecsv()
#
    for table in tables : # TABLES :

        if table == "Chem_comp" :
            sql = 'select * from chem_comp."Chem_comp"' + cidstr
        elif table.startswith( "Chem_comp" ) or (table in ("Atom_nomenclature","Characteristic",
                "Chem_struct_descriptor","PDBX_chem_comp_feature")) :
            sql = ('select * from chem_comp."%s"' % (table,)) + cidstr1
        else :
            sql = ('select * from chem_comp."%s"' % (table,)) + eidstr

        if where is None : outfile = table + ".csv"
        else : outfile = os.path.join( where, table + ".csv" )

        loader.tocsv( dsn, table = "(%s)" % sql, outfile = outfile, verbose = verbose )

    return True

# spec. case: InChI code was originally misspelled
#
def fix_inchi_column( where, verbose = False ) :

    indir = os.path.realpath( where )
    assert os.path.isdir( indir )
    infile = os.path.join( indir, "Chem_comp.csv" )
    if not os.path.exists( infile ) :
        sys.stderr.write( "File not found: %s\n" % (infile,) )
        return False
    outfile = infile + ".tmp"
    with open( outfile, "wb" ) as out :
        with open( infile, "rU" ) as f :
            for line in f :
                out.write( line.replace( "InCHi_code", "InChI_code" ) )
    os.rename( outfile, infile )
    return True

# load (previously dumped) chem comps from CSV files
#
def load( config, where, verbose = False ) :

    if verbose : sys.stdout.write( "load(%s)\n" % (where,) )
    indir = os.path.realpath( where )
    assert os.path.isdir( indir )

    global DB
    global TABLES

# need dictionary section
#
    if not config.has_section( "dictionary" ) :
        sys.stderr.write( "No [dictionary] section in config file\n" )
        return False

    if not config.has_option( "dictionary", "database" ) :
        sys.stderr.write( "No database in [dictionary] section in config file\n" )
        return False

    dsn = loader.dsn( config, DB )

# starobj needs [entry] section with
# engine = pgdb
# database = <dbname>
# schema = <db>
# user and host
#
    if not config.has_section( "entry" ) : config.add_section( "entry" )
    config.set( "entry", "engine", "pgdb" )
    config.set( "entry", "database", dsn["database"] )
    config.set( "entry", "schema", DB )
    if ("user" in dsn) and (dsn["user"] is not None) : 
        config.set( "entry", "user", dsn["user"] )
    if ("host" in dsn) and (dsn["host"] is not None) : 
        config.set( "entry", "host", dsn["host"] )
    else :
        pprint.pprint( dsn )
        raise Exception( "No host in DSN" )

    if verbose :
        pprint.pprint( config.items( "entry" ) )

    db = loader.starobj.DbWrapper( config, verbose = verbose )
    db.connect()

    sd = loader.starobj.StarDictionary( db, verbose = verbose )
    se = loader.starobj.NMRSTAREntry( db, verbose = verbose )

# the tables are in sub-schemas, just drop and re-create the whole thing
# no other option for now
#
    if True :
        db._connections[se.CONNECTION]["conn"].autocommit = True
        schema = db.schema( se.CONNECTION )
        stmt = "set client_min_messages=WARNING"
        se.execute( stmt )
        stmt = "drop schema if exists %s cascade" % (schema,)
        se.execute( stmt )
        stmt = "create schema %s" % (schema,)
        se.execute( stmt )

        se.create_tables( dictionary = sd, db = db, use_types = True, tables = TABLES, verbose = verbose )

    for f in glob.glob( os.path.join( indir, "*.csv" ) ) :
        table = os.path.splitext( os.path.split( f )[1] )[0]
        if not table in TABLES :
            sys.stderr.write( "%s.csv not in tables, skipping\n" % (table,) )
        loader.fromcsv( dsn, f, DB, table, verbose )

# there's no Entry_IDs in chem comps, but it is a part of primary key in "big" databases.
# for internal use that's set to "NEED_ACC_NUM" but for public DB: add one, use comp.id
#
def fix_entry_id( config, verbose = False ) :
    if verbose : sys.stdout.write( "fix_entry_id()\n" )

    global DB
    global TABLES

# use starobj wrapper
#

# need dictionary section
#
    if not config.has_section( "dictionary" ) :
        sys.stderr.write( "No [dictionary] section in config file\n" )
        return False

    if not config.has_option( "dictionary", "database" ) :
        sys.stderr.write( "No database in [dictionary] section in config file\n" )
        return False

    dsn = loader.dsn( config, DB )

# starobj needs [entry] section with
# engine = pgdb
# database = <dbname>
# schema = <db>
# user and host
#
    if not config.has_section( "entry" ) : config.add_section( "entry" )
    config.set( "entry", "engine", "pgdb" )
    config.set( "entry", "database", dsn["database"] )
    config.set( "entry", "schema", DB )
    if ("user" in dsn) and (dsn["user"] is not None) : 
        config.set( "entry", "user", dsn["user"] )
    if ("host" in dsn) and (dsn["host"] is not None) : 
        config.set( "entry", "host", dsn["host"] )
    else :
        pprint.pprint( dsn )
        raise Exception( "No host in DSN" )

    if verbose :
        pprint.pprint( config.items( "entry" ) )

    db = loader.starobj.DbWrapper( config, verbose = verbose )
    db.connect()

    sd = loader.starobj.StarDictionary( db, verbose = verbose )
    se = loader.starobj.NMRSTAREntry( db, verbose = verbose )
    scam = db.schema( se.CONNECTION )

# ugh
#
    for (table,column) in sd.iter_tags( which = ("entryid",), tables = TABLES ) :

        if scam is None : tbl = '"%s"' % (table,)
        else : tbl = '%s."%s"' % (scam,table,)

# in chem_comp it's id
#
        if table == "Chem_comp" :
            sql = 'update %s set "%s"="ID"' % (tbl,column,)

# in other chem_comp tables it's comp_id
#
        elif table in ("Atom_nomenclature","Characteristic","Chem_comp_SMILES","Chem_comp_angle",
                "Chem_comp_atom","Chem_comp_bio_function","Chem_comp_bond","Chem_comp_citation",
                "Chem_comp_common_name","Chem_comp_db_link","Chem_comp_descriptor","Chem_comp_identifier",
                "Chem_comp_keyword","Chem_comp_systematic_name","Chem_comp_tor","Chem_struct_descriptor",
                "PDBX_chem_comp_feature") :
            sql = 'update %s set "%s"="Comp_ID"' % (tbl,column,)

# in entity tables it's entity_comp_index.comp_id -> entity_comp_index.entity_id
# except in entity it's id
#
        elif table == "Entity_comp_index" :
            sql = 'update %s e set "%s"="Comp_ID"' % (tbl,column)

        elif table == "Entity" :
            if scam is None : tbl2 = '"Entity_comp_index"'
            else : tbl2 = '%s."Entity_comp_index"' % (scam,)
            sql = 'update %s e set "%s"=(select "Comp_ID" from %s where "Entity_ID"=e."ID")' % (tbl,column,tbl2,)

        elif table in ("Entity_atom_list","Entity_biological_function","Entity_bond","Entity_chem_comp_deleted_atom",
                "Entity_chimera_segment","Entity_citation","Entity_common_name","Entity_comp_index_alt",
                "Entity_db_link","Entity_keyword","Entity_poly_seq","Entity_systematic_name") :
            if scam is None : tbl2 = '"Entity_comp_index"'
            else : tbl2 = '%s."Entity_comp_index"' % (scam,)
            sql = 'update %s e set "%s"=(select "Comp_ID" from %s where "Entity_ID"=e."Entity_ID")' % (tbl,column,tbl2)

        if verbose : sys.stdout.write( sql )
        rc = se.execute( sql, commit = True )
        if verbose : sys.stdout.write( ": %d rows updated\n" % (rc.rowcount,) )

####################################################################################################
#
#
if __name__ == "__main__" :

    ap = argparse.ArgumentParser( description = "Dump public chem comps from ligand expo database" )
    ap.add_argument( "-v", "--verbose", default = False, action = "store_true",
        help = "print lots of messages to stdout", dest = "verbose" )
    ap.add_argument( "-t", "--time", help = "time the operatons", dest = "time",
        action = "store_true", default = False )

    ap.add_argument( "-c", "--config", help = "config file", dest = "conffile", required = True )
    ap.add_argument( "-d", "--outdir", help = "directory for temporary files", dest = "outdir" )

    ap.add_argument( "--load-only", help = "don't dump the database", dest = "dump",
        action = "store_false", default = True )
    ap.add_argument( "--dump-only", help = "don't load the database", dest = "load",
        action = "store_false", default = True )

    args = ap.parse_args()

    cp = ConfigParser.SafeConfigParser()
    f = os.path.realpath( args.conffile )
    cp.read( f )

    if args.dump :
        with loader.timer( label = "dump chemcomps", silent = not args.time ) :
            dump( config = cp, where = args.outdir, verbose = args.verbose )

    if args.load :
        with loader.timer( label = "load chemcomps", silent = not args.time ) :
#            fix_inchi_column( where = args.outdir, verbose = args.verbose )
            load( config = cp, where = args.outdir, verbose = args.verbose )
            fix_entry_id( config = cp, verbose = args.verbose )

#
#
