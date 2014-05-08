from distutils.core import setup
from distutils.extension import Extension
from os import getcwd
from os.path import abspath, dirname, realpath
import os

try:
    from Cython.Build import cythonize
except ImportError:
    def cythonize(extensions, **_ignore):
        for extension in extensions:
            sources = []
            for sfile in extension.sources:
                path, ext = os.path.splitext(sfile)
                if ext in ('.pyx', '.py'):
                    if extension.language == 'c++':
                        ext = '.cpp'
                    else:
                        ext = '.c'
                    sfile = path + ext
                sources.append(sfile)
            extension.sources[:] = sources
        return extensions

extra_objs = []
libraries = ["z", "sasl2"]
library_dirs = []

if os.environ["LINKCLIB"] == "static":
    extra_objs.append("../../src/libhadoofus.a")
else:
    library_dirs.append("../../src")
    libraries.append("hadoofus")

include_dirs = [realpath(dirname(abspath(__file__)) + "/../../include")]
ext_modules = cythonize([
    Extension("hadoofus", ["hadoofus.pyx"],
              libraries=libraries,
              library_dirs=library_dirs,
              include_dirs=include_dirs,
              extra_objects=extra_objs,
              )
])

setup(
        name = "hadoofus",
        version = "0",
        description = "Python client API for HDFS",
        author = "Conrad Meyer",
        author_email = "conrad.meyer@isilon.com",
        long_description = """
This 'hadoofus' python module provides a client API for accessing instances
of the Apache Hadoop Distributed File System, as well as compatible
implementations.
""",
        url = "<none yet>",
        classifiers = [
            'Development Status :: 2 - Pre-Alpha',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: POSIX',
            'Programming Language :: Cython',
            'Topic :: Software Development :: Libraries',
            'Topic :: System :: Filesystems',
            'Topic :: System :: Networking',
            ],
        platforms = ['Posix'],
        license = 'MIT',
        ext_modules = ext_modules
)
