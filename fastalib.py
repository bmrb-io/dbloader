#!/usr/bin/python -u
#
# -*- coding: utf-8 -*-
#

"""make BMRB sequence databases"""

import sys
import os
import re
import glob
import logging
import argparse
import time
#import hashlib
import subprocess

# coroutine decorator
#
def coroutine( func ) :
    def start( *args, **kwargs ) :
        cr = func( *args, **kwargs )
        cr.next()
        return cr
    return start

#
# globals
#
CONFIG = {
    "entrydirglob" : "/projects/BMRB/private/entrydirs/macromolecules/bmr*",
    "libs" : {
        "aa" : {
            "tooshort" : 15,
            "seqfile" : "%s/clean/bmr%s.prot.fasta",
            "libfile" : "bmrb.prot.%s"
        },
#
# FIXME: how short is too short for rna & dna?
#
        "dna" : {
            "tooshort" : 5,
            "seqfile" : "%s/clean/bmr%s.dna.fasta",
            "libfile" : "bmrb.dna.%s"
        },
        "rna" : {
            "tooshort" : 5,
            "seqfile" : "%s/clean/bmr%s.rna.fasta",
            "libfile" : "bmrb.rna.%s"
        }
    }
}

# filename generator
#
def list_files( config ) :

    pat = re.compile( r"/bmr(\d+)$" )
    files = {}

    for i in glob.glob( config["entrydirglob"] ) :
        if not os.path.isdir( i ) :
            logging.info( "Not a directory: %s", i )
            continue
        m = pat.search( i )
        if not m :
            logging.info( "No BMRB ID in %s", i )
            continue
        bmrbid = m.group( 1 )
        files.clear()
        for j in ("aa", "dna", "rna") :
            fname = config["libs"][j]["seqfile"] % (i, bmrbid)
            if os.path.exists( fname ) :
                files[j] = os.path.realpath( fname )

        if len( files ) < 1 :
            logging.info( "No FASTA file in %s", bmrbid )
        for j in files.keys() :
            yield (bmrbid, j, files[j])

# "targets" can be multiple to write to differens destinations in one run
#
#
@coroutine
def check_seq( targets, config ) :

    while True :
        (bmrbid,restype,name) = (yield)
        if not os.path.exists( os.path.realpath( name ) ) : continue
        tooshort = config["libs"][restype]["tooshort"]
        hdr = None
        seq = ""
        with open( os.path.realpath( name ), "rb" ) as f :
            for line in f :
# new
#
                if line.startswith( ">" ) :
                    if hdr is not None :
                        seq = re.sub( r"\s+", "", seq )
                        seq = seq.upper()
                        cnt = 0
                        for c in seq :
                            if c != "X" :
                                cnt += 1
                        if cnt < tooshort :
                            logging.info( "%s: %s sequence too short: %s", bmrbid, restype, seq )
                        else :
                            for t in targets :
                                t.send( (bmrbid, restype, hdr, seq) )
                    hdr = line.strip()
# curr
#
                else : seq += line.strip()
# last
#
        if hdr is not None :
            seq = re.sub( r"\s+", "", seq )
            seq = seq.upper()
            cnt = 0
            for c in seq :
                if c != "X" :
                    cnt += 1
            if cnt < tooshort :
                logging.info( "%s: %s sequence too short: %s", bmrbid, restype, seq )
            else :
                for t in targets :
                    t.send( (bmrbid, restype, hdr, seq) )

#
#
@coroutine
def write_bmrblib( residuetype, outfile ) :
    with open( outfile, "w" ) as out :
        while True :
            (bmrbid, restype, hdr, seq) = (yield)
            if restype == residuetype :
                out.write( "%s\n" %(hdr[:80],) )
                while len( seq ) > 80 :
                    out.write( "%s\n" % (seq[:80],) )
                    seq = seq[80:]
                if len( seq ) > 0 : out.write( "%s\n" % (seq,) )

#
#
#
if __name__ == "__main__" :

    par = argparse.ArgumentParser( description = "generate BMRB FASTA libraries" )
    par.add_argument( "-v", "--verbose", default = False, action = "store_true" )
    par.add_argument( "-o", "--outdir", dest = "outdir", default = None )
    args = par.parse_args()

    logging.basicConfig(
        level = (args.verbose and logging.DEBUG or logging.ERROR),
        format = "%(asctime)s %(message)s",
        handlers = [
            logging.StreamHandler( sys.stdout )
        ]
    )

    if args.outdir is not None :
        os.chdir( os.path.realpath( outdir ) )
        os.umask( 0o002 )

# should fix suffixes on our fasta libraries: it's .lib on FTP site
#  becasue it's what FASTA script on the website expects
# should probably change to .fasta
#
    aafile = CONFIG["libs"]["aa"]["libfile"] % ("lib",)
    writeaa = write_bmrblib( "aa", aafile )
    dnafile = CONFIG["libs"]["dna"]["libfile"] % ("lib",)
    writedna = write_bmrblib( "dna", dnafile )
    rnafile = CONFIG["libs"]["rna"]["libfile"] % ("lib",)
    writerna = write_bmrblib( "rna", rnafile )
    chk = check_seq( (writeaa,writedna,writerna,), CONFIG )

    for tpl in list_files( CONFIG ) :
        chk.send( tpl )

# $#@!ing python is caching something somewhere and md5sums for the above
# come out wrong if I generate them here no matter how many flush()es and
# os.fsync()s I add where. Neither of the options below work.
# This has to run after this interpreter exited.
#
#    for i in (aafile,dnafile,rnafile,) :
#        md5fname = "%s.%s" % (i,"md5",)
#        with open( i, "rb" ) as f :
#            x = f.read()
#            chksum = hashlib.md5( x )
#            with open( md5fname, "w" ) as out :
#                out.write( "%s  %s\n" % (chksum.hexdigest(),i,) )
#
#    for i in (aafile,dnafile,rnafile,) :
#        p = subprocess.Popen( ["/bin/md5sum", i], stdout = subprocess.PIPE, stderr = subprocess.PIPE )
#        stdout, sdterr = p.communicate()
#        md5fname = "%s.%s" % (i,"md5",)
#        with open( md5fname, "w" ) as out :
#            out.write( stdout )

#
# eof
#
