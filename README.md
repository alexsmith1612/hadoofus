hadoofus [![Build Status (travis)](https://app.travis-ci.com/alexsmith1612/hadoofus.svg?branch=master)](https://app.travis-ci.com/github/alexsmith1612/hadoofus) [![Build Status (cirrus)](https://api.cirrus-ci.com/github/alexsmith1612/hadoofus.svg)](https://cirrus-ci.com/github/alexsmith1612/hadoofus)
============================================================================================================================

#### JRE-free multi-version HDFS client library

The `hadoofus` project is an HDFS (Hadoop Distributed File System) client
library. Like [Snakebite](https://github.com/spotify/snakebite/), it does not
require Java. Unlike Snakebite, hadoofus is implemented in C. It supports RPC
pipelining and out-of-order execution.

It provides a C API for directly calling Namenode RPCs and performing Datanode
block read and write operations.

Unlike libhdfs, Hadoofus speaks multiple versions of the HDFS protocol. At your
option, you may speak with Hadoop 0.20.203 through 1.x.y (`HDFS_NN_v1` /
`HDFS_DATANODE_AP_1_0`), Hadoop 2.0.x (`HDFS_NN_v2` / `HDFS_DATANODE_AP_2_0`),
or Hadoop 2.2.x and higher (`HDFS_NN_v2_2` / `HDFS_DATANODE_AP_2_0`).

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

Some less common RPCs provided by the Hadoop `ClientProtocol` interface in v2.x
of the protocol are not yet implemented (see Issue #29).

Note: Hadoop has been known to change semantics slightly between different
versions of the software (especially before v2 was released). The v1 protocol
has no spec; we do the best we can.

Some RPCs that exist in HDFSv1 do not exist in HDFSv2+ — e.g.
`getProtocolVersion` does not exist in v2.

The Datanode API is somewhat fragile.

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

#### Installing

See INSTALLING.md.

#### Issues

Found a bug? Please file it on github. Thanks!

#### Contributors

* Tom Arnfeld &lt;tarnfeld@me.com&gt;
* Conrad Meyer &lt;conrad.meyer@isilon.com&gt;
* Paul Scott &lt;paul@duedil.com&gt;
* Alex Smith &lt;aes7mv AT virginia.edu&gt;

#### License

Unless otherwise noted, files in this source distribution are released under the
terms of the MIT license. Some files used for CRC32C support come from elsewhere
have non-MIT, but similarly permissive licenses (namely the BSD 2-clause license
and Mark Adler's license, which he also uses for zlib), which are clearly
specified at the top of their files (Some files which are not compiled into
installed binaries or otherwise installed by this package's Makefiles come from
the Apache Hadoop sources and have different licenses. These licenses are
clearly specified at the beginning of the files.) For the full text of the MIT
license, see the file `LICENSE` included with this source distribution.
