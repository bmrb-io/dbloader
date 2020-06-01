#!/usr/bin/python -u
#
# stuff specific to macromolecule entries:
#  table cleanups,
#  statistics
#

from __future__ import absolute_import
import os
import sys
import json
import pgdb
import argparse
import ConfigParser

_UP = os.path.abspath( os.path.join( os.path.split( __file__ )[0], ".." ) )
sys.path.append( _UP )
import loader

DB = "macromolecules"

# wrapper for misc. fixes
#
#
def fixup( config, verbose = False ) :
    if verbose :
        sys.stdout.write( "fixup()\n" )

    assert isinstance( config, ConfigParser.SafeConfigParser )

    global DB

    with pgdb.connect( **(loader.dsn( config, DB )) ) as conn :
        conn.autocommit = False
        with conn.cursor() as curs :
            try :
                fix_software( curs, config, verbose = verbose )
                fix_task( curs, config, verbose = verbose )
                fix_software_authors( curs, config, verbose = verbose )
                fix_entry( curs, verbose = verbose )
                fix_entities( curs, verbose = verbose )
                fix_csref( curs, verbose = verbose )
                conn.commit()
            except :
                conn.rollback()
                raise
    
#
#
def fix_entry( curs, verbose = False ) :

    global DB

    sql = """update %s."Entry" set "Type"='small molecule structure' where "ID"='15443' and "Type" is null""" % (DB,)
    if verbose : print (sql),
    curs.execute( sql )
    if verbose : print curs.rowcount
    sql = """update %s."Entry" set "Type"='small molecule structure' where "ID"='16041' and "Type" is null""" % (DB,)
    if verbose : print (sql),
    curs.execute( sql )
    if verbose : print curs.rowcount
    sql = """update %s."Entry" set "Type"='small molecule structure' where cast("ID" as integer)>=20000 and cast("ID" as integer)<25000 and "Type" is null""" % (DB,)
    if verbose : print (sql),
    curs.execute( sql )
    if verbose : print curs.rowcount
    sql = """update %s."Entry" set "Type"='macromolecule' where cast("ID" as integer)<20000 and "Type" is null""" % (DB,)
    if verbose : print (sql),
    curs.execute( sql )
    if verbose : print curs.rowcount

# these are non-public and shouldn't be there
# wipe them out just in case
#
    sql = 'truncate %s."Contact_person"' % (DB,)
    if verbose : print (sql),
    curs.execute( sql )
    if verbose : print curs.rowcount
    sql = 'truncate %s."Upload_data"' % (DB,)
    if verbose : print (sql),
    curs.execute( sql )
    if verbose : print curs.rowcount

# this one takes a dictionary "map" and reduces to the key
#  to normalize all different spellings etc.
# If map is empty, just fixes the case.
#
def _fix_map( curs, table = None, column = None, which = None, verbose = False ) :

    assert table is not None

    global DB

    sql = """update %s."%s" set "%s"=%%s where regexp_replace( trim( lower( "%s" ) ), '[[:space:]]+', ' ' )=%%s""" \
        % (DB,table, column, column)

    for i in sorted( which.keys() ) :
        if verbose : print (sql % (i, i.lower()) ),
        curs.execute( sql, (i, i.lower()) )
        if verbose : print curs.rowcount
        if which[i] != None :
            for j in which[i] :
                task = j.lower()
                if verbose : print (sql % (i,task)),
                curs.execute( sql, (i,task) )
                if verbose : print curs.rowcount

# names don't need to be barewords,
# sequences are line-wrapped in the entries
#
def fix_entities( curs, verbose = False ) :

    global DB

    for table in ( "Entity", "Assembly", "Chem_comp" ) :
        sql = """update %s."%s" set "Name"=regexp_replace("Name", '_+', ' ', 'g')""" % (DB,table,)
        if verbose : print sql,
        curs.execute( sql )
        if verbose : print curs.rowcount

    sql = 'update %s."Entity"'  % (DB,)
    sql += """  set "Polymer_seq_one_letter_code_can"=regexp_replace( "Polymer_seq_one_letter_code_can",'\n','','g'),
          "Polymer_seq_one_letter_code"=regexp_replace( "Polymer_seq_one_letter_code",'\n','','g')"""
    if verbose : print sql,
    curs.execute( sql )
    if verbose : print curs.rowcount

#
#
def fix_csref( curs, verbose = False ) :

# TODO!
#    with open( "chem_shift_ref_todo.csv", "rU" ) as f :
#        cs = csv.DictReader( f )
#        for row in cs :
#            if verbose : print row
    pass

# softwre/task fixup originally done for nmrbox
#
def fix_software( curs, config, verbose = False ) :
    if verbose : sys.stdout.write( "fix_software()\n" )

    global DB

    infile = config.get( DB, "software_mapfile" )
    f = os.path.realpath( infile )
    if not os.path.exists( f ) :
        if verbose : sys.stderr.write( "File not found: %s\n" % (f,) )
        return
    with open( f ) as inf : dat = json.load( inf )
    _fix_map( curs, table = "Software", column = "Name", which = dat, verbose = verbose )

def fix_task( curs, config, verbose = False ) :
    if verbose : sys.stdout.write( "fix_task()\n" )

    global DB

    infile = config.get( DB, "task_mapfile" )
    f = os.path.realpath( infile )
    if not os.path.exists( f ) :
        if verbose : sys.stderr.write( "File not found: %s\n" % (f,) )
        return
    with open( f ) as inf : dat = json.load( inf )
    _fix_map( curs, table = "Task", column = "Task", which = dat, verbose = verbose )

def fix_software_authors( curs, config, verbose = False ) :
    if verbose : sys.stdout.write( "fix_software_authors()\n" )

    global DB

    infile = config.get( DB, "software_authors_mapfile" )
    f = os.path.realpath( infile )
    if not os.path.exists( f ) :
        if verbose : sys.stderr.write( "File not found: %s\n" % (f,) )
        return
    with open( f ) as inf : dat = json.load( inf )

# redundant
#
    qry = 'select "Sf_ID","Entry_ID" from %s."Software"' % (DB,)
    qry += ' where "Name"=%s'
    upd = 'update %s."Vendor"' % (DB,)
    upd += ' set "Name"=%s where "Sf_ID"=%s and "Entry_ID"=%s'

    entries = {}
    for (sw, vend) in dat.iteritems() :
        entries.clear()
        if verbose :
            sys.stdout.write( qry % (sw,) )
            sys.stdout.write( "\n" )
        curs.execute( qry, (sw,) )
        while True :
            row = curs.fetchone()
            if row == None : break
            entries[row[1]] = row[0]

        for (eid, sfid) in entries.iteritems() :
            if verbose :
                sys.stdout.write( upd % (vend, sfid, eid) )
            curs.execute( upd, (vend, sfid, eid) )
            if verbose :
                sys.stdout.write( " : %d\n" % (curs.rowcount,) )

# special
#
    qry = """select "Sf_ID","Entry_ID" from %s."Software" where "Name"='PyMol'""" % (DB,)
    entries.clear()
    curs.execute( qry )
    while True :
        row = curs.fetchone()
        if row == None : break
        entries[row[1]] = row[0]

    up1 = 'update %s."Vendor"' % (DB,)
    up1 += """ set "Name"='DeLano Scientific LLC.' where "Sf_ID"=%s and "Entry_ID"=%s and "Name" like '%%delano%%'"""
    up2 = 'update %s."Vendor"' % (DB,)
    up2 += """ set "Name"='Schrodinger, LLC' where "Sf_ID"=%s and "Entry_ID"=%s and "Name" like '%%dinger%%'"""
    for (eid, sfid) in entries.iteritems() :
        if verbose : sys.stdout.write( up1 % (sfid,eid) )
        curs.execute( up1, (sfid,eid) )
        if verbose : sys.stdout.write( " : %d\n" % (curs.rowcount,) )

        if verbose : sys.stdout.write( up2 % (sfid,eid) )
        curs.execute( up2, (sfid,eid) )
        if verbose : sys.stdout.write( " : %d\n" % (curs.rowcount,) )

#
#
#
if __name__ == "__main__" :

    ap = argparse.ArgumentParser( description = "load NMR-STAR files into PostgreSQL database" )
    ap.add_argument( "-t", "--time", help = "print out timings", dest = "time", action = "store_true",
        default = False )
    ap.add_argument( "-v", "--verbose", help = "print lots of messages to stdout", dest = "verbose",
        action = "store_true", default = False )

    ap.add_argument( "-c", "--config", help = "config file", dest = "conffile", required = True )

    args = ap.parse_args()

    cp = ConfigParser.SafeConfigParser()
    f = os.path.realpath( args.conffile )
    cp.read( f )

    with loader.timer( label = "Macromolecule fixup", silent = args.time ) :
        fixup( config = cp, verbose = args.verbose )
