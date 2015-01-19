hadoofus [![Build Status](https://travis-ci.org/cemeyer/hadoofus.png?branch=master)](https://travis-ci.org/cemeyer/hadoofus)
==============

#### JRE-free HDFS client library

The `hadoofus` project is an HDFS (Hadoop Distributed File System) client
library. It is implemented in C and supports RPC pipelining and out-of-order
execution. It does not require Java.

It provides a C API for directly calling Namenode RPCs and performing Datanode
block read and write operations, as well as a `libhdfs`-compatible interface
in a seperate library (`libhdfs_hadoofus.so`).

It also includes a Python wrapper module, implemented in Cython. (Cython
compiles to a C Python module.) For more information on that, see
`/wrappers/README.md`.

#### Using libhadoofus

```c

#include <hadoofus/highlevel.h>

int64_t res;
const char *err = NULL;
struct hdfs_namenode *h;
struct hdfs_object *exception = NULL;

h = hdfs_namenode_new("host.bar.com", "8020", "hdfs", &err);
if (!h)
        ... ;

res = hdfs_getProtocolVersion(h, HADOOFUS_CLIENT_PROTOCOL_STR, 61L, &exception);
if (exception) {
        // fprintf(, "...%s...", hdfs_exception_get_message(exception));
        hdfs_object_free(exception);
        ... ;
}

if (res != 61)
        ... ;

hdfs_namenode_delete(h);
```

#### Caveats

Some RPCs provided by the Hadoop `ClientProtocol` interface are not
implemented.

Note: This library currently supports the HDFS protocol as spoken by Apache
Hadoop releases 0.20.203 through 1.0.3. The protocol is expected to stay the
same for future 1.x.y releases, although Hadoop has been known to change
semantics slightly between different versions of the software. The protocol has
no spec; we do the best we can.

Hadoop-2.x speaks a slightly different protocol (but with similar
semantics). Currently, libhadoofus does not support the Hadoop-2 HDFS
protocol.

#### HDFS Semantics

HDFS attempts to be a restricted subset of a POSIX filesystem.

Files can only have one writer at a time and do not support random writes. They
can be appended but not overwritten in place.

Generally, the Namenode acts as an RPC server for querying and manipulating
file system metadata. It points clients at Datanode(s) to read/write file data.

For more information, see [wikipedia's article on Hadoop][0] or the
[HDFS Architecture Guide][1].

[0]: https://en.wikipedia.org/wiki/Apache_Hadoop#Hadoop_distributed_file_system "Hadoop distributed file system"
[1]: https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html

#### ABI Compatibility notes

We aim to preserve ABI compatibility OF THE HIGH-LEVEL C API in future versions
of this library, with some caveats.

1. Users MUST NOT access struct members directly (even though structs are in
   public headers).
2. Users MUST allow for new types of exceptions and do something appropriate
   with surprising types (i.e., abort, treat it as `IOError`, etc).
3. Additional HDFS methods may be added. They will follow the naming scheme
   used throughout this project; users should avoid using such names so that
   future changes will not cause symbol conflicts.

#### Installing

See INSTALLING.md.

#### Issues

Found a bug? Please file it on github. Thanks!

#### Contributors

* Tom Arnfeld &lt;tarnfeld@me.com&gt;
* Conrad Meyer &lt;conrad.meyer@isilon.com&gt;
* Paul Scott &lt;paul@duedil.com&gt;

#### License

Unless otherwise noted, files in this source distribution are released under
the terms of the MIT license. (Some files which are not compiled into installed
binaries or otherwise installed by this package's Makefiles come from the
Apache Hadoop sources and have different licenses. These licenses are clearly
specified at the beginning of the files.) For the full text of the MIT license,
see the file `LICENSE` included with this source distribution.
