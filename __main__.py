#!/usr/bin/python2.7 -u
#
# -*- coding: utf-8 -*-
#
#
from __future__ import absolute_import

import os
import argparse
import ConfigParser
import loader
#import pprint

if __name__ == '__main__':

    ap = argparse.ArgumentParser( description = "Reload  BMRB database" )
    ap.add_argument( "-v", "--verbose", default = False, action = "store_true",
        help = "print lots of messages to stdout", dest = "verbose" )
    ap.add_argument( "-t", "--time", help = "time the operatons", dest = "time",
        action = "store_false", default = True )
    ap.add_argument( "-c", "--config", help = "config file", dest = "conffile", required = True )

    ap.add_argument( "-d", "--outdir", help = "directory for  output CSV files", dest = "outdir" )
    ap.add_argument( "--dictdir", help = "directory with dictionary files", dest = "dictdir" )

    ap.add_argument( "--no-dump", help = "don't dump the database", dest = "dump",
        action = "store_false", default = True )
    ap.add_argument( "--no-load", help = "don't load the database", dest = "load",
        action = "store_false", default = True )

    ap.add_argument( "--dump-macromolecule-db", help = "dump macromolecules database", dest = "dump_macro",
        action = "store_true", default = False )
    ap.add_argument( "--dump-metabolomics-db", help = "dump macromolecules database", dest = "dump_metab",
        action = "store_true", default = False )

    ap.add_argument( "--no-dict", help = "don't load dictionary schema", dest = "load_dict",
        action = "store_false", default = True )
    ap.add_argument( "--no-chemcomps", help = "don't load chem comp schema", dest = "load_ccdb",
        action = "store_false", default = True )
    ap.add_argument( "--no-metabolomics", help = "don't load metabolomics schema", dest = "load_metab",
        action = "store_false", default = True )
    ap.add_argument( "--no-macromolecules", help = "don't load macromolecules schema", dest = "load_macro",
        action = "store_false", default = True )
    ap.add_argument( "--no-web", help = "don't load web schema", dest = "load_web",
        action = "store_false", default = True )
    ap.add_argument( "--drop-tables", help = "drop and re-create database tables instead of truncating",
        dest = "drop_tables", action = "store_true", default = False )

    args = ap.parse_args()

    cp = ConfigParser.SafeConfigParser()
    f = os.path.realpath( args.conffile )
    cp.read( f )

    if not args.load :
        args.load_dict = False
        args.load_ccdb = False
        args.load_metab = False
        args.load_macro = False
        args.load_web = False

    with loader.timer( label = "total", silent = args.time ) :

        if args.load_dict :
            dictdir = os.path.realpath( args.dictdir )
            assert os.path.isdir( dictdir )
            with loader.timer( label = "load dictionary", silent = args.time ) :
                loader.load_dict( config = cp, path = dictdir, verbose = args.verbose )

        if args.load_ccdb :
            with loader.timer( label = "load chem. comps", silent = args.time ) :
                loader.load_chem_comps( config = cp, verbose = args.verbose )

        if args.load_metab :
            with loader.timer( label = "load metabolomics", silent = args.time ) :
                loader.load_metabolomics( config = cp, drop_tables = args.drop_tables, verbose = args.verbose )
                loader.load_meta_schema( config = cp, verbose = args.verbose )

        if args.load_macro :
            with loader.timer( label = "load macromolecules", silent = args.time ) :
                loader.load_macromolecules( config = cp, drop_tables = args.drop_tables, verbose = args.verbose )
                loader.fix_macromolecules( config = cp, verbose = args.verbose )

            if args.load_web :
                with loader.timer( label = "load web extras", silent = args.time ) :
                    loader.load_web_schema( config = cp, verbose = args.verbose )

        if args.dump :
            if args.dump_macro :
                with loader.timer( label = "dump macromolecule database", silent = args.time ) :
                    loader.dump_macromolecules( config = cp, path = args.outdir, verbose = args.verbose )
            elif args.dump_metab :
                with loader.timer( label = "dump metabolomics database", silent = args.time ) :
                    loader.dump_metabolomics( config = cp, path = args.outdir, verbose = args.verbose )
            else :
                with loader.timer( label = "dump bmrbeverything database", silent = args.time ) :
                    loader.dump_new( config = cp, path = args.outdir, verbose = args.verbose )
#                    loader.Dumper.dump( config = cp, outdir = args.outdir, verbose = args.verbose )

# EOF
#
