#!/usr/bin/env python3
#
# Build the deployable egg.
#
# setuptools packages what is next to setup.py, so the sources are copied in
# here first (unchanged ones are skipped, to keep timestamps stable).
#
#     cd packaging && python3 setup.py bdist_egg
#
# Copy `sas` and `starobj` in here before building to make a complete package
# -- see README.md.
#
# NOTE: the modules append their own parent directory to sys.path so they can
# be run as scripts out of a checkout.  That is harmless inside the egg (the
# path does not exist), so unlike the Python 2 version of this file, nothing
# needs commenting out before building.
#
import filecmp
import glob
import os
import shutil
import sys

import setuptools

_HERE = os.path.realpath(os.path.split(__file__)[0])


def sync(srcfile, dstfile):
    """Copy into the packaging directory, unless it is already the same file."""

    if os.path.exists(dstfile) and filecmp.cmp(srcfile, dstfile, shallow=False):
        return
    sys.stdout.write("* copying %s to %s\n" % (srcfile, dstfile,))
    shutil.copy2(srcfile, dstfile)


srcdir = os.path.join(_HERE, "..", "loader")
dstdir = os.path.join(_HERE, "loader")
os.makedirs(dstdir, exist_ok=True)
for f in glob.glob(os.path.join(srcdir, "*.py")):
    sync(f, os.path.join(dstdir, os.path.split(f)[1]))

sync(os.path.join(_HERE, "..", "__main__.py"), os.path.join(_HERE, "__main__.py"))

for i in ("build", "dist", "dbloader.egg-info"):
    if os.path.isdir(os.path.join(_HERE, i)):
        shutil.rmtree(os.path.join(_HERE, i))

setuptools.setup(name="dbloader", version="2.0",
                 python_requires=">=3.6",
                 install_requires=["psycopg2"],
                 packages=setuptools.find_packages(),
                 py_modules=["__main__"])
