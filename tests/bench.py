#!/usr/bin/env python3
#
# Time the entry load.
#
# Reports wall time, entries/sec and MB/sec.  `--parse-only` times parsing
# alone, which is what separates "the STAR parser is slow" from "the inserts
# are slow"; `--profile` says where inside the load the time actually goes.
#
#     python tests/bench.py -c <config> -s macromolecules [-n 50]
#     python tests/bench.py -c <config> -s macromolecules --parse-only pynmrstar
#     python tests/bench.py -c <config> -s macromolecules --profile
#
# It measures loader.entries._load_entries -- schema preparation plus the parse
# and insert of every file -- so the same command times whichever loader the
# checkout currently has.
#
# Exit status: 0 on success.

import argparse
import cProfile
import os
import pstats
import sys
import time
from configparser import ConfigParser

_HERE = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.realpath(os.path.join(_HERE, "..")))

from loader import entries


def corpus(config, dbname, limit=None):
    files = entries._gen_file_list(dbname, config.get(dbname, "entrydir"))
    if limit is not None:
        files = files[:limit]
    return (files, sum(os.path.getsize(f) for f in files))


def report(label, nfiles, nbytes, seconds):
    sys.stdout.write(
        "%-26s %5d entries %7.1f MB %8.2f s %7.1f entries/s %6.2f MB/s\n"
        % (label, nfiles, nbytes / 1048576.0, seconds,
           nfiles / seconds, nbytes / 1048576.0 / seconds))
    sys.stdout.flush()


def parse_only(files, which):
    """Parse every file and throw the result away."""

    if which == "pynmrstar":
        import pynmrstar

        def parse(f):
            pynmrstar.Entry.from_file(f)
    else:
        import sas
        import starobj

        # exactly what starobj.StarParser.parse does, with handlers that drop
        # everything on the floor instead of inserting it
        class Null(sas.ContentHandler, sas.ErrorHandler):
            def comment(self, line, text): return False
            def data(self, tag, tagline, val, valline, delim, inloop): return False
            def startData(self, line, name): return False
            def endData(self, line, name): return False
            def startLoop(self, line): return False
            def endLoop(self, line): return False
            def startSaveframe(self, line, name): return False
            def endSaveframe(self, line, name): return False
            def error(self, line, msg): return True
            def warning(self, line, msg): return False
            def fatalError(self, line, msg): return True

        def parse(f):
            null = Null()
            with open(f, "r", encoding=starobj.ENCODING) as inf:
                sas.SansParser.parse(lexer=sas.StarLexer(fp=inf, bufsize=0),
                                     content_handler=null, error_handler=null)

    start = time.time()
    for f in files:
        parse(f)
    return time.time() - start


def main():
    ap = argparse.ArgumentParser(description="time the entry load")
    ap.add_argument("-c", "--config", dest="conffile", required=True)
    ap.add_argument("-s", "--schema", dest="db", default="macromolecules",
                    choices=entries.DATABASES)
    ap.add_argument("-n", "--count", dest="count", type=int, default=None,
                    help="only the first N entries")
    ap.add_argument("--parse-only", dest="parse", default=None,
                    choices=("pynmrstar", "sas"),
                    help="time parsing alone, with this parser")
    ap.add_argument("--profile", dest="profile", action="store_true", default=False)
    args = ap.parse_args()

    cp = ConfigParser()
    cp.read(os.path.realpath(args.conffile))

    (files, nbytes) = corpus(cp, args.db, args.count)

    if args.parse is not None:
        report("parse only, " + args.parse, len(files), nbytes,
               parse_only(files, args.parse))
        return

    def run():
        return entries._load_entries(cp, args.db, files, drop_tables=True)

    if args.profile:
        prof = cProfile.Profile()
        start = time.time()
        failed = prof.runcall(run)
        elapsed = time.time() - start
        pstats.Stats(prof).sort_stats("tottime").print_stats(20)
    else:
        start = time.time()
        failed = run()
        elapsed = time.time() - start

    report("load " + args.db, len(files), nbytes, elapsed)
    if failed:
        sys.stderr.write("** %d entries failed\n" % (len(failed),))
        sys.exit(1)


if __name__ == "__main__":
    main()
