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
const chat *err = NULL;
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

#### Installing

See INSTALLING.md.

#### Issues

Found a bug? Please file it on github. Thanks!

#### Contributors

Conrad Meyer &lt;conrad.meyer@isilon.com&gt;

#### License

(The MIT license.) For license text, see the file `LICENSE` included with this
source distribution.
