#!/usr/bin/python -u
#
# NOTE: if you want "installable" egg, edit the sources and comment out every
#   sys.path.append( ... __file__ ...
# before building
#
import os
import shutil
import setuptools
import sys
import hashlib
import glob

def cmpfiles( f1, f2 ) :
    h1 = hashlib.md5()
    with open( f1, "rU" ) as f :
        for line in f :
            h1.update( line )
    h2 = hashlib.md5()
    with open( f2, "rU" ) as f :
        for line in f :
            h2.update( line )
    return h1.hexdigest() == h2.hexdigest()

for i in ("build","dist","validate.egg-info") :
    if os.path.isdir( i ) :
        shutil.rmtree( i )

srcdir = os.path.realpath( os.path.join( os.path.split( __file__ )[0], "..", "loader" ) )
dstdir = os.path.realpath( os.path.join( os.path.split( __file__ )[0], "loader" ) )
for f in glob.glob( os.path.join( srcdir, "*.py" ) ) :
    dstfile = os.path.join( dstdir, os.path.split( f )[1] )
    if os.path.exists( dstfile ) and cmpfiles( f, dstfile ) :
        continue
    sys.stdout.write( "* copying %s to %s\n" % (f, dstfile,) )
    shutil.copy2( f, dstfile )

srcfile = os.path.realpath( os.path.join( os.path.split( __file__ )[0], "..", "__main__.py" ) )
dstfile = os.path.realpath( os.path.join( os.path.split( __file__ )[0], "__main__.py" ) )
if os.path.exists( dstfile ) and cmpfiles( srcfile, dstfile ) :
        pass
else :
    sys.stdout.write( "* copying %s to %s\n" % (srcfile, dstfile,) )
    shutil.copy2( srcfile, dstfile )

for i in ("build","dist","dbloader.egg-info") :
    if os.path.isdir( i ) :
        shutil.rmtree( i )

setuptools.setup( name = "dbloader", version = "1.0", 
        packages = setuptools.find_packages(),
        py_modules = ["__main__"] )
