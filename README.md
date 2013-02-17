hadoofus
========

#### JRE-free HDFS client library

The `hadoofus` project is an HDFS (Hadoop Distributed File System) client
library. It is implemented in C and supports RPC pipelining and out-of-order
execution.

It provides a C API for directly calling Namenode RPCs and performing Datanode
block read and write operations, as well as a `libhdfs`-compatible interface
(`libhdfs_hadoofus.so`).

It also includes a Python wrapper module, implemented in Cython.

Note: This library currently supports the HDFS protocol as spoken by Apache
Hadoop releases 0.20.203 through 1.0.3. The protocol is expected to stay the
same for future 1.x.y releases, although Hadoop has been known to change
semantics slightly between different versions of the software. The protocol has
no spec; we do the best we can. Hadoop-2 will speak a slightly different
protocol (but with similar semantics). Currently, libhadoofus does not support
the Hadoop-2 HDFS protocol.

#### What works

The C library and Python wrappers should work well for the implemented RPCs. We
use the python library extensively at Isilon, testing our in-house HDFS
implementation for compatability against Apache HDFS.

Caveat: some RPCs provided by the Hadoop `ClientProtocol` interface are not
implemented. We don't use these ones and haven't seen the MapReduce client code
calling them... long story short, if you need a missing RPC, file a bug! I'm
also happy to accept MIT-licensed patches.

The `libhdfs`-compat library (`libhdfs_hadoofus`) is newer and incompletely
tested. It is functional enough to run the Apache `hdfs_test.c` tests, and
the Apache HDFS `fuse_dfs` module mostly works (although it isn't hard to break
it — I don't know if this is different than `fuse_dfs` with the Java HDFS
client code).

#### Using libhadoofus (python wrappers)

Connecting:

```py
import hadoofus

cluster = hadoofus.rpc("namenode.example.com", user="hdfs")

"%r" % cluster # => rpc(address='10.7.176.238', num_calls=1, protocol=80, user='hdfs')

cluster.getProtocolVersion(61) # => 61
```

Reading from a file `/foo` to stdout:

```py
blocks = cluster.getBlockLocations("/foo", offset=0,
    length=cluster.getFileInfo("/foo").size)

for block in blocks:
  block_data = hadoofus.data(block, "<my_client_name>").read()
  sys.stdout.write(block_data)
```

Writing from stdin to `/bar`:

```py
cluster.create("/bar", 0644, "<my_client_name>")

block_sz = 64*1024*1024
buffered = b''

while True:
  input = sys.stdin.read(block_sz - len(buffered))
  buffered += input
  if input != '' and len(buffered) < block_sz:
    continue

  block = cluster.addBlock("/bar", "<my_client_name>")
  hadoofus.data(block, "<my_client_name>").write(buffered)
  buffered = b''

cluster.complete("/bar", "<my_client_name>")
```

HDFS protocol exceptions are presented as Python exceptions descended from
`hadoofus.ProtocolException`.

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

#### HDFS Semantics

HDFS attempts to be a restricted subset of a POSIX filesystem.

Files can only have one writer at a time and do not support random writes. They
can be appended but not overwritten in place.

Generally, the Namenode acts as an RPC server for querying and manipulating
file system metadata. It points clients at Datanode(s) to read/write file data.

For more information, see [wikipedia's article on Hadoop][0] or the
[HDFS Architecture Guide][1].

[0]: https://en.wikipedia.org/wiki/Apache_Hadoop#Hadoop_Distributed_File_System "Hadoop Distributed File System"
[1]: https://hadoop.apache.org/common/docs/r1.0.0/hdfs_design.html

#### Installing

See INSTALLING.md.

#### Issues

Found a bug? Please file it on github. Thanks!

#### Contributors

Conrad Meyer &lt;conrad.meyer@isilon.com&gt;

#### License

Unless otherwise noted, files in this source distribution are released under
the terms of the MIT license. (Some files which are not compiled into installed
binaries or otherwise installed by this package's Makefiles come from the
Apache Hadoop sources and have different licenses. These licenses are clearly
specified at the beginning of the files.) For the full text of the MIT license,
see the file `LICENSE` included with this source distribution.
