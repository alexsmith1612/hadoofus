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

#### Contributors

Conrad Meyer &lt;conrad.meyer@isilon.com&gt;

#### License

(The MIT license.) For license text, see the file `LICENSE` included with this
source distribution.
