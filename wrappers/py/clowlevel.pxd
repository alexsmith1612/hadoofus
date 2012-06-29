from libc.stdint cimport int64_t
from libc.string cimport const_char
from posix.unistd cimport off_t

from cobjects cimport hdfs_object

cdef extern from "hadoofus/lowlevel.h":
    ctypedef void (*hdfs_namenode_destroy_cb)(hdfs_namenode*)
    cdef struct hdfs_namenode:
        pass
    cdef struct _hdfs_pending:
        pass
    cdef struct hdfs_datanode:
        pass
    cdef struct hdfs_rpc_response_future:
        pass
    cdef int DATANODE_AP_1_0 "HDFS_DATANODE_AP_1_0"
    cdef int DATANODE_CDH3 "HDFS_DATANODE_CDH3"
    cdef const_char *DATANODE_ERR_NO_CRCS "HDFS_DATANODE_ERR_NO_CRCS"

    void hdfs_rpc_response_future_init(hdfs_rpc_response_future *future) nogil
    void hdfs_namenode_init(hdfs_namenode *n) nogil
    const_char *hdfs_namenode_connect(hdfs_namenode *n, const_char *host, const_char *port) nogil
    const_char *hdfs_namenode_authenticate(hdfs_namenode *n, const_char *username) nogil
    int64_t hdfs_namenode_get_msgno(hdfs_namenode *n) nogil
    const_char *hdfs_namenode_invoke(hdfs_namenode *n, hdfs_object *o, hdfs_rpc_response_future *f) nogil
    void hdfs_future_get(hdfs_rpc_response_future *f, hdfs_object **o_out) nogil
    void hdfs_namenode_destroy(hdfs_namenode *n, hdfs_namenode_destroy_cb cb) nogil
    void hdfs_datanode_init(hdfs_datanode *d, int64_t blkid, int64_t size, int64_t gen, int64_t offset, const_char *client, hdfs_object *token, int proto) nogil
    const_char *hdfs_datanode_connect(hdfs_datanode *d, const_char *host, const_char *port) nogil
    const_char *hdfs_datanode_write(hdfs_datanode *d, void *buf, size_t len, bint sendcrcs) nogil
    const_char *hdfs_datanode_write_file(hdfs_datanode *d, int fd, off_t len, off_t offset, bint sendcrcs) nogil
    const_char *hdfs_datanode_read(hdfs_datanode *d, size_t off, size_t len, void *buf, bint verifycrc) nogil
    const_char *hdfs_datanode_read_file(hdfs_datanode *d, off_t bloff, off_t len, int fd, off_t fdoff, bint verifycrc) nogil
    void hdfs_datanode_destroy(hdfs_datanode *d) nogil
