#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""make BMRB sequence databases"""

import argparse
import glob
import hashlib
import logging
import os
import re
import sys


# coroutine decorator
#
def coroutine(func):
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr
    return start


#
# globals
#
CONFIG = {
    "entrydirglob": "/projects/BMRB/private/entrydirs/macromolecules/bmr*",
    "libs": {
        "aa": {
            "tooshort": 15,
            "seqfile": "%s/clean/bmr%s.prot.fasta",
            "libfile": "bmrb.prot.%s"
        },
        #
        # FIXME: how short is too short for rna & dna?
        #
        "dna": {
            "tooshort": 5,
            "seqfile": "%s/clean/bmr%s.dna.fasta",
            "libfile": "bmrb.dna.%s"
        },
        "rna": {
            "tooshort": 5,
            "seqfile": "%s/clean/bmr%s.rna.fasta",
            "libfile": "bmrb.rna.%s"
        }
    }
}

RESIDUE_TYPES = ("aa", "dna", "rna")


# filename generator
#
def list_files(config):

    pat = re.compile(r"/bmr(\d+)$")

    for i in sorted(glob.glob(config["entrydirglob"])):
        if not os.path.isdir(i):
            logging.info("Not a directory: %s", i)
            continue
        m = pat.search(i)
        if not m:
            logging.info("No BMRB ID in %s", i)
            continue
        bmrbid = m.group(1)

        found = False
        for j in RESIDUE_TYPES:
            fname = config["libs"][j]["seqfile"] % (i, bmrbid)
            if os.path.exists(fname):
                found = True
                yield (bmrbid, j, os.path.realpath(fname))

        if not found:
            logging.info("No FASTA file in %s", bmrbid)


# "targets" can be multiple to write to different destinations in one run
#
#
@coroutine
def check_seq(targets, config):

    while True:
        (bmrbid, restype, name) = (yield)
        if not os.path.exists(os.path.realpath(name)):
            continue
        tooshort = config["libs"][restype]["tooshort"]

        hdr = None
        seq = ""
        with open(os.path.realpath(name)) as f:
            for line in f:
                if line.startswith(">"):
                    _emit(targets, bmrbid, restype, hdr, seq, tooshort)
                    hdr = line.strip()
                    seq = ""
                else:
                    seq += line.strip()

        # the last sequence in the file has no ">" after it to flush it
        _emit(targets, bmrbid, restype, hdr, seq, tooshort)


def _emit(targets, bmrbid, restype, hdr, seq, tooshort):
    """Pass one sequence on to the writers, unless it is too short to be useful."""

    if hdr is None:
        return

    seq = re.sub(r"\s+", "", seq).upper()
    if len([c for c in seq if c != "X"]) < tooshort:
        logging.info("%s: %s sequence too short: %s", bmrbid, restype, seq)
        return

    for t in targets:
        t.send((bmrbid, restype, hdr, seq))


#
#
@coroutine
def write_bmrblib(residuetype, outfile):
    with open(outfile, "w") as out:
        while True:
            (bmrbid, restype, hdr, seq) = (yield)
            if restype == residuetype:
                out.write("%s\n" % (hdr[:80],))
                for i in range(0, len(seq), 80):
                    out.write("%s\n" % (seq[i:i + 80],))


#
#
#
if __name__ == "__main__":

    par = argparse.ArgumentParser(description="generate BMRB FASTA libraries")
    par.add_argument("-v", "--verbose", default=False, action="store_true")
    par.add_argument("-o", "--outdir", dest="outdir", default=None)
    args = par.parse_args()

    logging.basicConfig(
        level=(args.verbose and logging.DEBUG or logging.ERROR),
        format="%(asctime)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    if args.outdir is not None:
        os.chdir(os.path.realpath(args.outdir))
        os.umask(0o002)

    # should fix suffixes on our fasta libraries: it's .lib on the FTP site
    #  because that's what the FASTA script on the website expects
    # should probably change to .fasta
    #
    outfiles = [CONFIG["libs"][t]["libfile"] % ("lib",) for t in RESIDUE_TYPES]
    writers = [write_bmrblib(t, f) for (t, f) in zip(RESIDUE_TYPES, outfiles)]

    chk = check_seq(tuple(writers), CONFIG)
    for tpl in list_files(CONFIG):
        chk.send(tpl)

    # Closing the writers is what closes their output files: each one holds its
    # `with open(...)` across the yield, so until the generator is closed the
    # last buffered writes are still in flight.  That is why checksumming the
    # libraries in this process used to produce wrong md5s "no matter how many
    # flush()es and os.fsync()s I add" -- it was reading half-written files.
    chk.close()
    for w in writers:
        w.close()

    for i in outfiles:
        with open(i, "rb") as f:
            chksum = hashlib.md5(f.read()).hexdigest()
        with open("%s.md5" % (i,), "w") as out:
            out.write("%s  %s\n" % (chksum, i,))

#
# eof
#
