pydoofus and libhdfs
====================

#### Python users

While hadoofus can be used for Python 2 applications, Python developers may be
better served by [Snakebite](https://github.com/spotify/snakebite/).

#### Using the Python urllib2.urlopen handler

```py
import urllib2
import hadoofus

response = urllib2.urlopen("hdfs://user@namenode.abc.com:8020/big/data.txt")
print "Size", response.info()["Content-length"]
print response.read()
response.close()
```

If omitted, user defaults to "root." If omitted, port defaults to "8020." One
may specify "hdfs2://" or "hdfs22://" instead of "hdfs://" to connect with HDFS
versions 2.0 or 2.2+, respectively.

#### Using the Python file-like wrappers

```py
import hadoofus

myfile = hadoofus.easy.open("hdfs://namenode.mycorp.com/important/data.txt")
myfile.seek(512)
print myfile.read(200)  #=> "12345…"
myfile.close()

mywfile = hadoofus.easy.open("hdfs://namenode/new/file.txt", "w")
mywfile.write("Hello, world!")
mywfile.close()
```

#### Using the low-level Python wrappers

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

#### libhdfs\_hadoofus

`libhdfs_hadoofus.so` aims to be a drop-in replacement for Apache Hadoop's
`libhdfs.so` (JNI wrappers around Java Hadoop). Of course,
`libhdfs_hadoofus` does not require the Java Runtime.

#### Using libhadoofus to mount an HDFS volume on Linux

Because `libhdfs_hadoofus` is a drop-in replacement for `libhdfs`, it can
be used to replace `libhdfs` in applications that use it. For example, it
can be used with `fuse_dfs` to mount HDFS as a local filesystem on Linux.

#### Caveats

The `libhdfs`-compat library (`libhdfs_hadoofus`) is newer and incompletely
tested. It is functional enough to run the Apache `hdfs_test.c` tests, and
the Apache HDFS `fuse_dfs` module mostly works. (In my testing, it isn't
hard to break it — I don't know if this is different than `fuse_dfs` with
the Java HDFS client code. It seems that `fuse_dfs` is fragile and mostly a
proof-of-concept.)
