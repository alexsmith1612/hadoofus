from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

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
        cmdclass = {'build_ext': build_ext},
        ext_modules = [Extension("hadoofus", ["hadoofus.pyx"],
            libraries=["hadoofus", "z"])]
)
