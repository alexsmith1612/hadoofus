from libc.stdint cimport int64_t, intptr_t, int16_t, uint16_t, INT64_MIN, INT64_MAX
from libc.stdlib cimport malloc, free
from libc.string cimport const_char

from cpython.bytes cimport PyBytes_FromStringAndSize

from chighlevel cimport hdfs_getProtocolVersion, hdfs_getBlockLocations, hdfs_create, \
        hdfs_append, hdfs_setReplication, hdfs_setPermission, hdfs_setOwner, \
        hdfs_abandonBlock, hdfs_addBlock, hdfs_complete, hdfs_rename, hdfs_delete, \
        hdfs_mkdirs, hdfs_getListing, hdfs_renewLease, hdfs_getStats, \
        hdfs_getPreferredBlockSize, hdfs_getFileInfo, hdfs_getContentSummary, \
        hdfs_setQuota, hdfs_fsync, hdfs_setTimes, hdfs_recoverLease, \
        hdfs_datanode_new, hdfs_datanode_delete
from clowlevel cimport hdfs_namenode, hdfs_namenode_init, hdfs_namenode_destroy, \
        hdfs_namenode_destroy_cb, hdfs_namenode_connect, hdfs_namenode_authenticate, \
        hdfs_datanode, hdfs_datanode_read_file, hdfs_datanode_read, hdfs_datanode_write, \
        hdfs_datanode_write_file, hdfs_datanode_connect, hdfs_namenode_get_msgno
from cobjects cimport CLIENT_PROTOCOL, hdfs_object_type, hdfs_object, hdfs_exception, \
        H_ACCESS_CONTROL_EXCEPTION, H_PROTOCOL_EXCEPTION, H_ALREADY_BEING_CREATED_EXCEPTION, \
        H_FILE_NOT_FOUND_EXCEPTION, H_IO_EXCEPTION, H_LEASE_EXPIRED_EXCEPTION, \
        H_LOCATED_BLOCKS, H_LOCATED_BLOCK, H_BLOCK, H_DATANODE_INFO, H_DIRECTORY_LISTING, \
        H_ARRAY_LONG, H_FILE_STATUS, H_CONTENT_SUMMARY, H_ARRAY_DATANODE_INFO, \
        H_NULL, \
        hdfs_etype_to_string, hdfs_object_free, hdfs_array_datanode_info_new, \
        hdfs_array_datanode_info_append_datanode_info, hdfs_datanode_info_copy, \
        hdfs_array_long, hdfs_located_blocks, hdfs_located_block_copy, hdfs_located_block, \
        hdfs_block_new, hdfs_directory_listing, hdfs_file_status_new_ex, hdfs_null_new
cimport clowlevel

import errno
import socket
import sys
import time
import types

# Public constants from pydoofus
HADOOP_1_0 = 0x50

_WRITE_BLOCK_SIZE = 64*1024*1024
DATANODE_PROTO_AP_1_0 = clowlevel.DATANODE_AP_1_0
DATANODE_PROTO_CDH3 = clowlevel.DATANODE_CDH3
DATANODE_ERR_NO_CRCS = <char*>clowlevel.DATANODE_ERR_NO_CRCS

cdef bytes const_str_to_py(const_char* s):
    return <char*>s

# Exceptions from pydoofus
class DisconnectException(Exception):
    pass

class ProtocolException(Exception):
    def __init__(self, etype, msg):
        Exception.__init__(self, "%s: %s" % (etype, msg))
        self._etype = etype
        self._emessage = msg

class AccessControlException(ProtocolException):
    pass

class AlreadyBeingCreatedException(ProtocolException):
    pass

class FileNotFoundException(ProtocolException):
    pass

class IOException(ProtocolException):
    pass

class LeaseExpiredException(ProtocolException):
    pass

# Helper stuffs to raise a pydoofus exception from an hdfs_object exception:
cdef dict exception_types = {
        H_ACCESS_CONTROL_EXCEPTION: AccessControlException,
        H_PROTOCOL_EXCEPTION: ProtocolException,
        H_ALREADY_BEING_CREATED_EXCEPTION: AlreadyBeingCreatedException,
        H_FILE_NOT_FOUND_EXCEPTION: FileNotFoundException,
        H_IO_EXCEPTION: IOException,
        H_LEASE_EXPIRED_EXCEPTION: LeaseExpiredException,
        }

cdef raise_protocol_error(hdfs_object* ex):
    if ex.ob_type != H_PROTOCOL_EXCEPTION:
        raise AssertionError("bogus exception passed to raise_protocol_error")

    cdef hdfs_exception *h_ex = &ex._exception
    cdef bytes py_msg = h_ex._msg # copy message so we can free hdfs object
    etype = exception_types.get(h_ex._etype, ProtocolException)
    cdef bytes py_etype = const_str_to_py(hdfs_etype_to_string(h_ex._etype))

    hdfs_object_free(ex)
    raise etype(py_etype, py_msg)


cdef generic_iterable_getitem(iterable, v):
    cdef list res = []
    if type(v) is int:
        for i, e in enumerate(iterable):
            if i == v:
                return e
        raise IndexError("list index out of range")
    elif type(v) is slice:
        it = iter(xrange(v.start or 0, v.stop or sys.maxint, v.step or 1))
        try:
            nexti = next(it)
            for i, e in enumerate(iterable):
                if i == nexti:
                    res.append(e)
                    nexti = next(it)
        except StopIteration:
            pass
        return res
    else:
        raise TypeError("list indices must be integers, not %s" % type(v).__name__)


cdef object bad_types = (types.FunctionType, types.LambdaType, types.CodeType,
        types.MethodType, types.UnboundMethodType, types.BuiltinFunctionType,
        types.BuiltinMethodType)
cdef str generic_repr(object o):
    cdef list attrs = [], orig_attrs

    orig_attrs = dir(o)
    for oa in orig_attrs:
        if oa.startswith("_"):
            continue
        if isinstance(getattr(o, oa), bad_types):
            continue
        attrs.append(oa)

    cdef str res = o.__class__.__name__ + "("
    for i, at in enumerate(attrs):
        if i != 0:
            res += ", "
        res += at + "=" + repr(getattr(o, at))
    res += ")"

    return res


cdef bint generic_richcmp(object o1, object o2, int op) except *:
    if op is 0:
        return object.__lt__(o1, o2)
    elif op is 1:
        return object.__le__(o1, o2)
    elif op is 2:
        if type(o1) is not type(o2):
            return False

        dir1 = set(dir(o1))
        dir2 = set(dir(o2))

        if dir1 != dir2:
            return False

        for at in dir1:
            if type(getattr(o1, at)) is not type(getattr(o2, at)):
                return False

            if at.startswith("_"):
                continue
            if isinstance(getattr(o1, at), bad_types):
                continue

            if getattr(o1, at) != getattr(o2, at):
                return False

        return True
    elif op is 3:
        return not generic_richcmp(o1, o2, 2)
    elif op is 4:
        return object.__gt__(o1, o2)
    elif op is 5:
        return object.__ge__(o1, o2)


cdef class located_blocks:
    cdef hdfs_object* lbs

    cpdef readonly int size
    cpdef readonly bint being_written
    cpdef readonly list blocks

    def __init__(self):
        raise TypeError("This class cannot be instantiated from Python")

    def __cinit__(self):
        self.lbs = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_LOCATED_BLOCKS
        assert self.lbs is NULL

        self.lbs = o
        self.size = o._located_blocks._size
        self.being_written = o._located_blocks._being_written
        self.blocks = []
        for lb in self:
            self.blocks.append(lb)

    def __dealloc__(self):
        if self.lbs is not NULL:
            hdfs_object_free(self.lbs)

    def __iter__(self):
        cdef hdfs_located_blocks* lbs = &self.lbs._located_blocks
        cdef located_block py_lb
        cdef hdfs_object* lb

        for i in range(lbs._num_blocks):
            with nogil:
                lb = hdfs_located_block_copy(lbs._blocks[i])
            py_lb = located_block_build(lb)
            yield py_lb

    def __repr__(self):
        return generic_repr(self)

    def __getitem__(self, v):
        return generic_iterable_getitem(self, v)

    def __len__(self):
        return self.lbs._located_blocks._num_blocks

# Note: caller loses their C-level ref to the object
cdef located_blocks located_blocks_build(hdfs_object* o):
    cdef located_blocks res

    if o.ob_type is H_NULL:
        assert o._null._type is H_LOCATED_BLOCKS
        return None

    res = located_blocks.__new__(located_blocks)
    res.init(o)
    return res


cdef class located_block:
    cdef hdfs_object* lb

    cpdef readonly int64_t offset
    cpdef readonly int64_t blockid
    cpdef readonly int64_t generation
    cpdef readonly int64_t len
    cpdef readonly list locations

    def __init__(self):
        raise TypeError("This class cannot be instantiated from Python")

    def __cinit__(self):
        self.lb = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_LOCATED_BLOCK
        assert self.lb is NULL

        self.lb = o
        self.offset = o._located_block._offset
        self.blockid = o._located_block._blockid
        self.generation = o._located_block._generation
        self.len = o._located_block._len
        self.locations = []
        for loc in self:
            self.locations.append(loc)

    cdef hdfs_object* get_lb(self):
        return self.lb

    def __dealloc__(self):
        if self.lb is not NULL:
            hdfs_object_free(self.lb)

    def __iter__(self):
        cdef hdfs_located_block* lb = &self.lb._located_block
        cdef datanode_info py_di
        cdef hdfs_object* di

        for i in range(lb._num_locs):
            with nogil:
                di = hdfs_datanode_info_copy(lb._locs[i])
            py_di = datanode_info_build(di)
            yield py_di

    def __getitem__(self, v):
        return generic_iterable_getitem(self, v)

    def __len__(self):
        return self.lb._located_block._num_locs

    def __repr__(self):
        return generic_repr(self)

# Note: caller loses their C-level ref to the object
cdef located_block located_block_build(hdfs_object* o):
    cdef located_block res

    if o.ob_type is H_NULL:
        assert o._null._type is H_LOCATED_BLOCK
        return None

    res = located_block.__new__(located_block)
    res.init(o)
    return res


cdef hdfs_object* convert_located_block_to_block(located_block lb):
    assert lb is not None

    cdef hdfs_object* res = hdfs_block_new(lb.blockid, lb.len, lb.generation)
    return res


cdef class block:
    cdef hdfs_object* c_block

    cpdef readonly int64_t len
    cpdef readonly int64_t blockid
    cpdef readonly int64_t generation

    def __init__(self, copy):
        assert copy is not None
        assert type(copy) is located_block

        self.c_block = convert_located_block_to_block(copy)
        self.len = self.c_block._block._length
        self.blockid = self.c_block._block._blkid
        self.generation = self.c_block._block._generation

    def __cinit__(self):
        self.c_block = NULL

    def __dealloc__(self):
        if self.c_block is not NULL:
            hdfs_object_free(self.c_block)

    def __repr__(self):
        return generic_repr(self)


cdef class datanode_info:
    cdef hdfs_object* c_di

    cpdef readonly char* location
    cpdef readonly char* hostname
    cpdef readonly char* port
    cpdef readonly uint16_t ipc_port

    def __init__(self):
        raise TypeError("This class cannot be instantiated from Python")

    def __cinit__(self):
        self.c_di = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_DATANODE_INFO
        assert self.c_di is NULL

        self.c_di = o
        self.location = o._datanode_info._location
        self.hostname = o._datanode_info._hostname
        self.port = o._datanode_info._port
        self.ipc_port = o._datanode_info._namenodeport

    def __dealloc__(self):
        if self.c_di is not NULL:
            hdfs_object_free(self.c_di)

    def __repr__(self):
        return generic_repr(self)

# Note: caller loses their C-level ref to the object
cdef datanode_info datanode_info_build(hdfs_object* o):
    cdef datanode_info res

    if o.ob_type is H_NULL:
        assert o._null._type is H_DATANODE_INFO
        return None

    res = datanode_info.__new__(datanode_info)
    res.init(o)
    return res


cdef hdfs_object* file_status_copy(hdfs_object* fs):
    return hdfs_file_status_new_ex(
            fs._file_status._file,
            fs._file_status._size,
            fs._file_status._directory,
            fs._file_status._replication,
            fs._file_status._block_size,
            fs._file_status._mtime,
            fs._file_status._atime,
            fs._file_status._permissions,
            fs._file_status._owner,
            fs._file_status._group)


cdef class directory_listing:
    cdef hdfs_object* c_dl

    cpdef readonly list files

    def __init__(self):
        raise TypeError("This class cannot be instantiated from Python")

    def __cinit__(self):
        self.c_dl = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_DIRECTORY_LISTING
        assert self.c_dl is NULL

        self.c_dl = o
        self.files = []
        for fs in self:
            self.files.append(fs)

    def __dealloc__(self):
        if self.c_dl is not NULL:
            hdfs_object_free(self.c_dl)

    def __repr__(self):
        return generic_repr(self)

    def __str__(self):
        return "\n".join((str(fs) for fs in self))

    def __len__(self):
        return self.c_dl._directory_listing._num_files

    def __iter__(self):
        cdef hdfs_directory_listing* dl = &self.c_dl._directory_listing
        cdef file_status py_fs

        for i in range(dl._num_files):
            py_fs = file_status_build(file_status_copy(dl._files[i]))
            yield py_fs

    def __getitem__(self, v):
        return generic_iterable_getitem(self, v)

# Note: caller loses their C-level ref to the object
cdef directory_listing directory_listing_build(hdfs_object* o):
    cdef directory_listing res

    if o.ob_type is H_NULL:
        assert o._null._type is H_DIRECTORY_LISTING
        return None

    res = directory_listing.__new__(directory_listing)
    res.init(o)
    return res


cdef str format_perm_triplet(int p):
    cdef str p1 = "r", p2 = "-", p3 = "-"
    if not p & 0x4:
        p1 = "-"
    if p & 0x2:
        p2 = "w"
    if p & 0x1:
        p3 = "x"
    return p1 + p2 + p3

cdef str format_perms(int perms, bint isdir):
    dirs = "-"
    if isdir:
        dirs = "d"
    return "%s%s%s%s" % (dirs, format_perm_triplet((perms >> 6) & 0x7), \
            format_perm_triplet((perms >> 3) & 0x7), \
            format_perm_triplet(perms & 0x7))

cdef str format_date(int64_t time_ms):
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(float(time_ms)/1000))

cdef str format_repl(bint isdir, int repl):
    if isdir:
        return "-"
    return str(repl)


cdef class file_status:
    cdef hdfs_object* c_fs

    cpdef readonly int64_t size
    cpdef readonly int64_t block_size
    cpdef readonly int64_t mtime
    cpdef readonly int64_t atime
    cpdef readonly char* name
    cpdef readonly char* owner
    cpdef readonly char* group
    cpdef readonly int16_t replication
    cpdef readonly int16_t permissions
    cpdef readonly bint is_directory

    def __init__(self):
        raise TypeError("This class cannot be instantiated from Python")

    def __cinit__(self):
        self.c_fs = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_FILE_STATUS
        assert self.c_fs is NULL

        self.c_fs = o
        self.size = o._file_status._size
        self.block_size = o._file_status._block_size
        self.mtime = o._file_status._mtime
        self.atime = o._file_status._atime
        self.name = o._file_status._file
        self.owner = o._file_status._owner
        self.group = o._file_status._group
        self.replication = o._file_status._replication
        self.permissions = o._file_status._permissions
        self.is_directory = o._file_status._directory

    def __dealloc__(self):
        if self.c_fs is not NULL:
            hdfs_object_free(self.c_fs)

    def __repr__(self):
        return generic_repr(self)

    def __str__(self):
        return "%s\t%s\t%s\t%s\t%s\t%s\t%s" % (
                format_perms(self.permissions, self.is_directory),
                format_repl(self.is_directory, self.replication),
                self.owner, self.group, self.size,
                format_date(self.mtime), self.name)

    def __richcmp__(self, object o, int n):
        return generic_richcmp(self, o, n)


# Note: caller loses their C-level ref to the object
cdef file_status file_status_build(hdfs_object* o):
    cdef file_status res

    if o.ob_type is H_NULL:
        assert o._null._type is H_FILE_STATUS
        return None

    res = file_status.__new__(file_status)
    res.init(o)
    return res


cdef class content_summary:
    cdef hdfs_object* c_cs

    cpdef readonly int64_t length
    cpdef readonly int64_t files
    cpdef readonly int64_t directories
    cpdef readonly int64_t quota

    def __init__(self):
        raise TypeError("This class cannot be instantiated from Python")

    def __cinit__(self):
        self.c_cs = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_CONTENT_SUMMARY

        self.c_cs = o
        self.length = o._content_summary._length
        self.files = o._content_summary._files
        self.directories = o._content_summary._dirs
        self.quota = o._content_summary._quota

    def __dealloc__(self):
        if self.c_cs is not NULL:
            hdfs_object_free(self.c_cs)

    def __repr__(self):
        return generic_repr(self)

# Note: caller loses their C-level ref to the object
cdef content_summary content_summary_build(hdfs_object* o):
    cdef content_summary res

    if o.ob_type is H_NULL:
        assert o._null._type is H_CONTENT_SUMMARY
        return None

    res = content_summary.__new__(content_summary)
    res.init(o)
    return res


cdef hdfs_object* convert_list_to_array_datanode_info(list l_di):
    assert l_di is not None
    cdef hdfs_object* res
    cdef datanode_info di
    cdef hdfs_object* di_copy
    cdef hdfs_object* c_di

    res = hdfs_array_datanode_info_new()
    try:
        for e in l_di:
            di = e
            c_di = di.c_di
            with nogil:
                di_copy = hdfs_datanode_info_copy(c_di)
                hdfs_array_datanode_info_append_datanode_info(res, di_copy)
    except:
        hdfs_object_free(res)
        raise

    return res


cdef list convert_array_long_to_list(hdfs_object* o):
    assert o
    assert o.ob_type == H_ARRAY_LONG
    cdef list res = []
    cdef hdfs_array_long* c_arr = &o._array_long

    cdef object v
    for i in range(c_arr._len):
        v = c_arr._vals[i]
        res.append(v)

    hdfs_object_free(o)

    return res

cdef class _objects:
    cdef bint once

    cpdef readonly object DisconnectException
    cpdef readonly object DisconnectError
    cpdef readonly object ProtocolException
    cpdef readonly object AccessControlException
    cpdef readonly object AlreadyBeingCreatedException
    cpdef readonly object FileNotFoundException
    cpdef readonly object IOException
    cpdef readonly object LeaseExpiredException

    cpdef readonly object located_blocks
    cpdef readonly object located_block
    cpdef readonly object block
    cpdef readonly object datanode_info
    cpdef readonly object directory_listing
    cpdef readonly object file_status
    cpdef readonly object content_summary

    cpdef readonly int64_t INT64_MIN "i64min"
    cpdef readonly int64_t INT64_MAX "i64max"

    def __cinit__(self):
        self.once = False

    def __init__(self):
        raise TypeError("cannot construct this from python")

    cpdef init(self):
        if self.once is True:
            return

        self.once = True

        self.DisconnectError = DisconnectException
        self.DisconnectException = DisconnectException
        self.ProtocolException = ProtocolException
        self.AccessControlException = AccessControlException
        self.AlreadyBeingCreatedException = AlreadyBeingCreatedException
        self.FileNotFoundException = FileNotFoundException
        self.IOException = IOException
        self.LeaseExpiredException = LeaseExpiredException

        self.located_blocks = located_blocks
        self.located_block = located_block
        self.block = block
        self.datanode_info = datanode_info
        self.directory_listing = directory_listing
        self.file_status = file_status
        self.content_summary = content_summary

        self.INT64_MIN = INT64_MIN
        self.INT64_MAX = INT64_MAX

objects = _objects.__new__(_objects)
objects.init()


# pydoofus-esque rpc class!
cdef class rpc:
    cdef hdfs_namenode nn
    cdef bint closed

    cpdef readonly bytes _ipc_proto
    cpdef readonly str address
    cpdef readonly int protocol
    cpdef readonly str user

    def __cinit__(self):
        with nogil:
            hdfs_namenode_init(&self.nn)

    def __dealloc__(self):
        with nogil:
            hdfs_namenode_destroy(&self.nn, NULL)

    def __init__(self, addr, protocol=HADOOP_1_0, user=None):
        self._ipc_proto = const_str_to_py(CLIENT_PROTOCOL)
        self.closed = False

        if user is None:
            user = "root"

        self.address = addr
        self.protocol = protocol
        self.user = user

        cdef char* c_addr = addr
        cdef char* c_port = "8020"
        cdef const_char* err
        cdef bytes py_err

        with nogil:
            err = hdfs_namenode_connect(&self.nn, c_addr, c_port)
        if err is not NULL:
            py_err = const_str_to_py(err)
            raise socket.error(errno.ECONNREFUSED, py_err)

        cdef char* c_user = user

        with nogil:
            err = hdfs_namenode_authenticate(&self.nn, c_user)
        if err is not NULL:
            py_err = const_str_to_py(err)
            raise socket.error(errno.EPIPE, py_err)

        # pydoofus checks getProtocolVersion(), we should too.
        assert 61 == self.getProtocolVersion(61)

    def __repr__(self):
        return generic_repr(self)

    property num_calls:
        def __get__(self):
            cdef int64_t res
            with nogil:
                res = hdfs_namenode_get_msgno(&self.nn)
            return res

    # TODO plumb close() semantics through C-level api.
    # double-close() is fine, but operations must somehow raise
    # ValueError("X on closed handle") when called on a closed handle.

    cpdef int64_t getProtocolVersion(self, int64_t version, char* protostr=CLIENT_PROTOCOL) except? -1:
        cdef int64_t res
        cdef hdfs_object* ex = NULL

        with nogil:
            res = hdfs_getProtocolVersion(&self.nn, protostr, version, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef located_blocks getBlockLocations(self, char* path, int64_t offset=0, int64_t length=4LL*1024*1024*1024):
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_getBlockLocations(&self.nn, path, offset, length, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return located_blocks_build(res)

    cpdef create(self, char *path, int16_t perms, char *client, bint can_overwrite=False, int16_t replication=1, int64_t blocksize=_WRITE_BLOCK_SIZE, bint create_parent=True):
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_create(&self.nn, path, perms, client, can_overwrite, create_parent, replication, blocksize, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef located_block append(self, char *path, char *client):
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_append(&self.nn, path, client, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return located_block_build(res)

    cpdef bint setReplication(self, char *path, int16_t replication) except *:
        cdef hdfs_object* ex = NULL
        cdef bint res

        with nogil:
            res = hdfs_setReplication(&self.nn, path, replication, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef setPermission(self, char *path, int16_t perms):
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_setPermission(&self.nn, path, perms, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef setOwner(self, char *path, bytes owner_py, bytes group_py):
        cdef hdfs_object* ex = NULL
        cdef char *owner = NULL
        cdef char *group = NULL

        if owner_py is not None:
            owner = owner_py
        if group_py is not None:
            group = group_py

        with nogil:
            hdfs_setOwner(&self.nn, path, owner, group, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef abandonBlock(self, object bl, char *path, char *client):
        cdef hdfs_object* ex = NULL
        cdef block r_bl

        if type(bl) is block:
            r_bl = bl
        elif type(bl) is located_block:
            r_bl = block(bl)
        else:
            raise TypeError("Argument 'bl' has incorrect type (expected hadoofus.block or" +
                    " hadoofus.located_block, got %s)" % str(type(bl)))

        with nogil:
            hdfs_abandonBlock(&self.nn, r_bl.c_block, path, client, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef located_block addBlock(self, char *path, char *client, list excluded=None):
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res
        cdef hdfs_object* c_excl = NULL

        if excluded is not None:
            c_excl = convert_list_to_array_datanode_info(excluded)

        with nogil:
            res = hdfs_addBlock(&self.nn, path, client, c_excl, &ex)

        if c_excl is not NULL:
            hdfs_object_free(c_excl)

        if ex is not NULL:
            raise_protocol_error(ex)

        return located_block_build(res)

    cpdef bint complete(self, char *path, char *client) except *:
        cdef hdfs_object* ex = NULL
        cdef bint res = False

        while res is False:
            with nogil:
                res = hdfs_complete(&self.nn, path, client, &ex)
            if ex is not NULL:
                raise_protocol_error(ex)

        return res

    cpdef bint rename(self, char *src, char *dst) except *:
        cdef hdfs_object* ex = NULL
        cdef bint res

        with nogil:
            res = hdfs_rename(&self.nn, src, dst, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef bint delete(self, char *path, bint can_recurse=False, bint onearg=False) except *:
        cdef hdfs_object* ex = NULL
        cdef bint res

        if onearg is True:
            can_recurse = True

        with nogil:
            res = hdfs_delete(&self.nn, path, can_recurse, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef bint mkdirs(self, char *path, int16_t perms) except *:
        cdef hdfs_object* ex = NULL
        cdef bint res

        with nogil:
            res = hdfs_mkdirs(&self.nn, path, perms, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef directory_listing getListing(self, char *path):
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_getListing(&self.nn, path, NULL, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return directory_listing_build(res)

    cpdef renewLease(self, char *client):
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_renewLease(&self.nn, client, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef list getStats(self):
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_getStats(&self.nn, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return convert_array_long_to_list(res)

    cpdef int64_t getPreferredBlockSize(self, char *path) except? -1:
        cdef hdfs_object* ex = NULL
        cdef int64_t res

        with nogil:
            res = hdfs_getPreferredBlockSize(&self.nn, path, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef file_status getFileInfo(self, char *path):
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_getFileInfo(&self.nn, path, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return file_status_build(res)

    cpdef content_summary getContentSummary(self, char *path):
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_getContentSummary(&self.nn, path, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return content_summary_build(res)

    cpdef setQuota(self, char *path, int64_t nsquota, int64_t dsquota):
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_setQuota(&self.nn, path, nsquota, dsquota, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef fsync(self, char *path, char *client):
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_fsync(&self.nn, path, client, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef setTimes(self, char *path, int64_t mtime, int64_t atime):
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_setTimes(&self.nn, path, mtime, atime, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef bint recoverLease(self, char *path, char *client) except *:
        cdef hdfs_object* ex = NULL
        cdef bint res

        with nogil:
            res = hdfs_recoverLease(&self.nn, path, client, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res


rpcproto = rpc
namenode = rpc


# TODO handle the connecting ourselves so we can report:
# - num. failed datanode attempts (and list)
# - actual datanode we are connected to
cdef class data:
    cdef hdfs_datanode* dn
    cdef hdfs_object* orig_lb

    cpdef readonly int protocol
    cpdef readonly bytes client
    cpdef readonly int size

    def __cinit__(self):
        self.dn = NULL
        self.orig_lb = NULL

    def __init__(self, lb, client, proto=DATANODE_PROTO_AP_1_0):
        assert lb is not None
        assert type(lb) is located_block

        self.size = lb.len
        self.client = str(client)

        self.protocol = int(proto)
        assert self.protocol is DATANODE_PROTO_AP_1_0 or self.protocol is DATANODE_PROTO_CDH3

        cdef hdfs_object* c_lb = located_block.get_lb(lb)
        self.orig_lb = hdfs_located_block_copy(c_lb)
        self.init()

    def init(self):
        cdef hdfs_datanode* dn
        cdef int c_proto = self.protocol
        cdef char* c_client = self.client
        cdef const_char* err = NULL

        with nogil:
            dn = hdfs_datanode_new(self.orig_lb, c_client, c_proto, &err)
        if dn is NULL:
            raise ValueError("Failed to create datanode connection: %s" % \
                    const_str_to_py(err))
        self.dn = dn

    def __dealloc__(self):
        if self.dn is not NULL:
            with nogil:
                hdfs_datanode_delete(self.dn)
        if self.orig_lb is not NULL:
            hdfs_object_free(self.orig_lb)

    def __repr__(self):
        return generic_repr(self)

    def write(self, object data, int maxlen=-1, int offset=-1, bint sendcrcs=False,
            bint keepalive=False):
        cdef const_char* err
        cdef char* inp
        cdef int fd
        cdef int towrite
        cdef bytes err_s

        # TODO send keepalive packets when keepalive=true

        if type(data) is bytes:
            inp = data
            towrite = len(data)
            if maxlen is not -1 and towrite > maxlen:
                towrite = maxlen
            with nogil:
                err = hdfs_datanode_write(self.dn, inp, towrite, sendcrcs)
            if err is not NULL:
                err_s = const_str_to_py(err)
                if err_s == "EOS":
                    raise DisconnectException(err_s)
                raise Exception(err_s)
        elif type(data) is file:
            fd = data.fileno()

            if offset is -1:
                offset = data.tell()

            if maxlen is -1:
                raise ValueError("Must set maxlen to write from a file")

            with nogil:
                err = hdfs_datanode_write_file(self.dn, fd, maxlen, offset, sendcrcs)
            if err is not NULL:
                err_s = const_str_to_py(err)
                if err_s == "EOS":
                    raise DisconnectException(err_s)
                raise Exception(err_s)
        else:
            raise TypeError("data is not a file nor string; aborting write")

    def read(self, bint verifycrcs=True):
        cdef const_char* err
        cdef bytes res = PyBytes_FromStringAndSize(NULL, self.size)
        cdef char* buf = <char*>res
        cdef bytes err_s

        assert self.size > 0

        with nogil:
            err = hdfs_datanode_read(self.dn, 0, self.size, buf, verifycrcs)

        if verifycrcs and err is clowlevel.DATANODE_ERR_NO_CRCS:
            hdfs_datanode_delete(self.dn)
            self.dn = NULL
            self.init()
            err = hdfs_datanode_read(self.dn, 0, self.size, buf, False)

        if err is not NULL:
            err_s = const_str_to_py(err)
            if err_s == "EOS":
                raise DisconnectException(err_s)
            raise Exception(err_s)

        return res


dataproto = data
datanode = data
