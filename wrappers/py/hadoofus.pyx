from libc.stdint cimport int64_t, intptr_t, int16_t, uint16_t, INT64_MIN, INT64_MAX
from libc.stdlib cimport malloc, free
from libc.string cimport const_char, strdup

from cpython.bytes cimport PyBytes_FromStringAndSize

from chighlevel cimport hdfs_getProtocolVersion, hdfs_getBlockLocations, hdfs_create, \
        hdfs_append, hdfs_setReplication, hdfs_setPermission, hdfs_setOwner, \
        hdfs_abandonBlock, hdfs_addBlock, hdfs_complete, hdfs_rename, hdfs_delete, \
        hdfs_mkdirs, hdfs_getListing, hdfs_renewLease, hdfs_getStats, \
        hdfs_getPreferredBlockSize, hdfs_getFileInfo, hdfs_getContentSummary, \
        hdfs_setQuota, hdfs_fsync, hdfs_setTimes, hdfs_recoverLease, \
        hdfs_datanode_new, hdfs_datanode_delete, hdfs_concat, \
        hdfs_getDelegationToken, hdfs_cancelDelegationToken, hdfs_renewDelegationToken, \
        hdfs_setSafeMode, hdfs_getDatanodeReport, hdfs_reportBadBlocks, \
        hdfs_distributedUpgradeProgress, hdfs_finalizeUpgrade, hdfs_refreshNodes, \
        hdfs_saveNamespace, hdfs_isFileClosed, hdfs_metaSave, hdfs_setBalancerBandwidth, \
        hdfs2_getServerDefaults, hdfs2_getFileLinkInfo, hdfs2_createSymlink, \
        hdfs2_getLinkTarget
from clowlevel cimport hdfs_namenode, hdfs_namenode_init, hdfs_namenode_destroy, \
        hdfs_namenode_destroy_cb, hdfs_namenode_connect, hdfs_namenode_authenticate, \
        hdfs_datanode, hdfs_datanode_read_file, hdfs_datanode_read, hdfs_datanode_write, \
        hdfs_datanode_write_file, hdfs_datanode_connect, hdfs_namenode_get_msgno, \
        hdfs_namenode_set_version
from cobjects cimport CLIENT_PROTOCOL, hdfs_object_type, hdfs_object, hdfs_exception, \
        H_ACCESS_CONTROL_EXCEPTION, H_PROTOCOL_EXCEPTION, H_ALREADY_BEING_CREATED_EXCEPTION, \
        H_FILE_NOT_FOUND_EXCEPTION, H_IO_EXCEPTION, H_LEASE_EXPIRED_EXCEPTION, \
        H_LOCATED_BLOCKS, H_LOCATED_BLOCK, H_BLOCK, H_DATANODE_INFO, H_DIRECTORY_LISTING, \
        H_ARRAY_LONG, H_FILE_STATUS, H_CONTENT_SUMMARY, H_ARRAY_DATANODE_INFO, \
        H_NULL, H_TEXT, H_TOKEN, H_UPGRADE_STATUS_REPORT, H_FS_SERVER_DEFAULTS, \
        H_STRING, H_SECURITY_EXCEPTION, H_QUOTA_EXCEPTION, H_ILLEGAL_ARGUMENT_EXCEPTION, \
        H_INVALID_TOKEN_EXCEPTION, H_INVALID_PATH_EXCEPTION, H_FILE_ALREADY_EXISTS_EXCEPTION, \
        H_IPC_EXCEPTION, H_SASL_EXCEPTION, H_RPC_EXCEPTION, H_RPC_NO_SUCH_METHOD_EXCEPTION, \
        hdfs_etype_to_string, hdfs_object_free, hdfs_array_datanode_info_new, \
        hdfs_array_datanode_info_append_datanode_info, hdfs_datanode_info_copy, \
        hdfs_array_long, hdfs_located_blocks, hdfs_located_block_copy, hdfs_located_block, \
        hdfs_block_new, hdfs_directory_listing, hdfs_file_status_new_ex, hdfs_null_new, \
        hdfs_array_string_new, hdfs_array_string_add, hdfs_located_blocks_new, \
        hdfs_located_block_new, hdfs_datanode_info_new, hdfs_file_status_new_ex, \
        hdfs_content_summary_new, hdfs_text_new, hdfs_token_new_empty, hdfs_array_datanode_info, \
        hdfs_array_locatedblock_append_located_block, hdfs_array_locatedblock_new
cimport clowlevel
cimport cobjects
cimport csasl2

import errno
import os
import random
import socket
import sys
import time
import types
import urlparse

# Public constants from pydoofus
HADOOP_1_0 = clowlevel.HDFS_NN_v1
HADOOP_2_0 = clowlevel.HDFS_NN_v2
HADOOP_2_2 = clowlevel.HDFS_NN_v2_2

_WRITE_BLOCK_SIZE = 64*1024*1024
DATANODE_PROTO_AP_1_0 = clowlevel.DATANODE_AP_1_0
DATANODE_PROTO_CDH3 = clowlevel.DATANODE_CDH3
DATANODE_ERR_NO_CRCS = <char*>clowlevel.DATANODE_ERR_NO_CRCS

NO_KERB = clowlevel.HDFS_NO_KERB
TRY_KERB = clowlevel.HDFS_TRY_KERB
REQUIRE_KERB = clowlevel.HDFS_REQUIRE_KERB

SAFEMODE_ENTER = "SAFEMODE_ENTER"
SAFEMODE_LEAVE = "SAFEMODE_LEAVE"
SAFEMODE_GET = "SAFEMODE_GET"

DNREPORT_ALL = "ALL"
DNREPORT_LIVE = "LIVE"
DNREPORT_DEAD = "DEAD"

UPGRADEACTION_GET = "GET_STATUS"
UPGRADEACTION_DETAIL = "DETAILED_STATUS"
UPGRADEACTION_FORCE = "FORCE_PROCEED"

# These are on-wire values:
CSUM_NULL = 0
CSUM_CRC32 = 1
CSUM_CRC32C = 2
_VALID_CSUMS = frozenset([0, 1, 2])

def sasl_init():
    """
    sasl_init() should be called once before creating kerberized HDFS
    connections.
    """
    cdef int r, sok

    sok = csasl2.SASL_OK

    r = csasl2.sasl_client_init(NULL)
    if r != sok:
        raise Exception("Could not initialize SASL2: %d" % r)

def sasl_done():
    """
    sasl_done() should be called when an application is done with kerberized
    HDFS connections.
    """
    csasl2.sasl_done()

cdef bytes const_str_to_py(const_char* s):
    return <char*>s

cdef char* xstrdup(char* s):
    cdef char* r
    r = strdup(s)
    if r == NULL:
        raise AssertionError("OOM")
    return r

cdef char* pydup(object o):
    cdef bytes py_s
    py_s = bytes(str(o))
    return xstrdup(py_s)

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

class SecurityException(ProtocolException):
    pass

class DSQuotaExceededException(ProtocolException):
    pass

class IllegalArgumentException(ProtocolException):
    pass

class InvalidToken(ProtocolException):
    pass

class InvalidPathException(ProtocolException):
    pass

class FileAlreadyExistsException(ProtocolException):
    pass

class IpcException(ProtocolException):
    pass

class SaslException(ProtocolException):
    pass

class RpcServerException(ProtocolException):
    pass

class RpcNoSuchMethodException(ProtocolException):
    pass

# Helper stuffs to raise a pydoofus exception from an hdfs_object exception:
cdef dict exception_types = {
        H_ACCESS_CONTROL_EXCEPTION: AccessControlException,
        H_PROTOCOL_EXCEPTION: ProtocolException,
        H_ALREADY_BEING_CREATED_EXCEPTION: AlreadyBeingCreatedException,
        H_FILE_NOT_FOUND_EXCEPTION: FileNotFoundException,
        H_IO_EXCEPTION: IOException,
        H_LEASE_EXPIRED_EXCEPTION: LeaseExpiredException,
        H_SECURITY_EXCEPTION: SecurityException,
        H_QUOTA_EXCEPTION: DSQuotaExceededException,
        H_ILLEGAL_ARGUMENT_EXCEPTION: IllegalArgumentException,
        H_INVALID_TOKEN_EXCEPTION: InvalidToken,
        H_INVALID_PATH_EXCEPTION: InvalidPathException,
        H_FILE_ALREADY_EXISTS_EXCEPTION: FileAlreadyExistsException,
        H_IPC_EXCEPTION: IpcException,
        H_SASL_EXCEPTION: SaslException,
        H_RPC_EXCEPTION: RpcServerException,
        H_RPC_NO_SUCH_METHOD_EXCEPTION: RpcNoSuchMethodException,
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

    cpdef readonly list blocks

    def __init__(self, being_created=False, size=0):
        self.init(hdfs_located_blocks_new(being_created, size))

    def __cinit__(self):
        self.lbs = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_LOCATED_BLOCKS
        assert self.lbs is NULL

        self.lbs = o
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
            lb = hdfs_located_block_copy(lbs._blocks[i])
            py_lb = located_block_build(lb)
            yield py_lb

    def __repr__(self):
        return generic_repr(self)

    def __getitem__(self, v):
        return generic_iterable_getitem(self, v)

    def __len__(self):
        return self.lbs._located_blocks._num_blocks

    property being_created:
        "If this file has been created, but not yet complete()"

        def __get__(self):
            return bool(self.lbs._located_blocks._being_written)

        def __set__(self, being_created):
            cdef bint bc = bool(being_created)
            self.lbs._located_blocks._being_written = bc

    property size:
        "Total size of the file described by this LocatedBlocks object"

        def __get__(self):
            return self.lbs._located_blocks._size

        def __set__(self, size):
            cdef int64_t csize = int(size)
            self.lbs._located_blocks._size = csize


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

    cpdef readonly list locations

    def __init__(self, blkid=0, len_=0, generation=0, offset=0):
        self.init(hdfs_located_block_new(int(blkid), int(len_),
            int(generation), int(offset)))

    def __cinit__(self):
        self.lb = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_LOCATED_BLOCK
        assert self.lb is NULL

        self.lb = o
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

    property offset:
        def __get__(self):
            return self.lb._located_block._offset

        def __set__(self, offset):
            self.lb._located_block._offset = int(offset)

    property blockid:
        def __get__(self):
            return self.lb._located_block._blockid

        def __set__(self, blockid):
            self.lb._located_block._blockid = int(blockid)

    property generation:
        def __get__(self):
            return self.lb._located_block._generation

        def __set__(self, generation):
            self.lb._located_block._generation = generation

    property len:
        def __get__(self):
            return self.lb._located_block._len

        def __set__(self, len):
            self.lb._located_block._len = int(len)


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

    def __init__(self, lb_or_len=None, blockid=0, generation=0):
        if type(lb_or_len) not in (type(None), int):
            assert type(lb_or_len) is located_block
            self.c_block = convert_located_block_to_block(lb_or_len)
        else:
            if lb_or_len is None:
                lb_or_len = 0
            self.c_block = hdfs_block_new(int(blockid), int(lb_or_len),
                    int(generation))

    def __cinit__(self):
        self.c_block = NULL

    def __dealloc__(self):
        if self.c_block is not NULL:
            hdfs_object_free(self.c_block)

    def __repr__(self):
        return generic_repr(self)

    property len:
        def __get__(self):
            return self.c_block._block._length

        def __set__(self, len_):
            self.c_block._block._length = int(len_)

    property blockid:
        def __get__(self):
            return self.c_block._block._blkid

        def __set__(self, blkid):
            self.c_block._block._blkid = int(blkid)

    property generation:
        def __get__(self):
            return self.c_block._block._generation

        def __set__(self, generation):
            self.c_block._block._generation = int(generation)


cdef class datanode_info:
    cdef hdfs_object* c_di

    def __init__(self, hostname, port, location, ipc_port):
        host_s = str(hostname)
        port_s = str(port)
        loc_s = str(location)
        self.c_di = hdfs_datanode_info_new(host_s, port_s, loc_s,
                int(ipc_port))

    def __cinit__(self):
        self.c_di = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_DATANODE_INFO
        assert self.c_di is NULL

        self.c_di = o

    def __dealloc__(self):
        if self.c_di is not NULL:
            hdfs_object_free(self.c_di)

    def __repr__(self):
        return generic_repr(self)

    property location:
        def __get__(self):
            return self.c_di._datanode_info._location

        def __set__(self, location):
            cdef char* cp = pydup(location)
            free(self.c_di._datanode_info._location)
            self.c_di._datanode_info._location = cp

    property hostname:
        def __get__(self):
            return self.c_di._datanode_info._hostname

        def __set__(self, hostname):
            cdef char* cp = pydup(hostname)
            free(self.c_di._datanode_info._hostname)
            self.c_di._datanode_info._hostname = cp

    property port:
        def __get__(self):
            return self.c_di._datanode_info._port

        def __set__(self, port):
            cdef char* cp = pydup(port)
            free(self.c_di._datanode_info._port)
            self.c_di._datanode_info._port = cp

    property ipc_port:
        def __get__(self):
            return self.c_di._datanode_info._namenodeport

        def __set__(self, port):
            self.c_di._datanode_info._namenodeport = int(port)

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

    def __init__(self):
        self.c_fs = hdfs_file_status_new_ex("", 0, False, 0, 0, 0, 0, 0, "",
                "")

    def __cinit__(self):
        self.c_fs = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_FILE_STATUS
        assert self.c_fs is NULL

        self.c_fs = o

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

    property size:
        def __get__(self):
            return self.c_fs._file_status._size

        def __set__(self, sz):
            self.c_fs._file_status._size = int(sz)

    property block_size:
        def __get__(self):
            return self.c_fs._file_status._block_size

        def __set__(self, sz):
            self.c_fs._file_status._block_size = int(sz)

    property mtime:
        def __get__(self):
            return self.c_fs._file_status._mtime

        def __set__(self, mtime):
            self.c_fs._file_status._mtime = int(mtime)

    property atime:
        def __get__(self):
            return self.c_fs._file_status._atime

        def __set__(self, atime):
            self.c_fs._file_status._atime = int(atime)

    property name:
        def __get__(self):
            return self.c_fs._file_status._file

        def __set__(self, name):
            cdef char* cp = pydup(name)
            free(self.c_fs._file_status._file)
            self.c_fs._file_status._file = cp

    property owner:
        def __get__(self):
            return self.c_fs._file_status._owner

        def __set__(self, owner):
            cdef char* cp = pydup(owner)
            free(self.c_fs._file_status._owner)
            self.c_fs._file_status._owner = cp

    property group:
        def __get__(self):
            return self.c_fs._file_status._group

        def __set__(self, group):
            cdef char* cp = pydup(group)
            free(self.c_fs._file_status._group)
            self.c_fs._file_status._group = cp

    property replication:
        def __get__(self):
            return self.c_fs._file_status._replication

        def __set__(self, replication):
            self.c_fs._file_status._replication = int(replication)

    property permissions:
        def __get__(self):
            return self.c_fs._file_status._permissions

        def __set__(self, permissions):
            self.c_fs._file_status._permissions = int(permissions)

    property is_directory:
        def __get__(self):
            return self.c_fs._file_status._directory

        def __set__(self, isdir):
            self.c_fs._file_status._directory = bool(isdir)


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

    def __init__(self, length=0, files=0, dirs=0, quota=0):
        self.c_cs = hdfs_content_summary_new(int(length), int(files),
                int(dirs), int(quota))

    def __cinit__(self):
        self.c_cs = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_CONTENT_SUMMARY

        self.c_cs = o

    def __dealloc__(self):
        if self.c_cs is not NULL:
            hdfs_object_free(self.c_cs)

    def __repr__(self):
        return generic_repr(self)

    property length:
        def __get__(self):
            return self.c_cs._content_summary._length

        def __set__(self, length):
            self.c_cs._content_summary._length = int(length)

    property files:
        def __get__(self):
            return self.c_cs._content_summary._files

        def __set__(self, files):
            self.c_cs._content_summary._files = int(files)

    property directories:
        def __get__(self):
            return self.c_cs._content_summary._dirs

        def __set__(self, dirs):
            self.c_cs._content_summary._dirs = int(dirs)

    property quota:
        def __get__(self):
            return self.c_cs._content_summary._quota

        def __set__(self, quota):
            self.c_cs._content_summary._quota = int(quota)

# Note: caller loses their C-level ref to the object
cdef content_summary content_summary_build(hdfs_object* o):
    cdef content_summary res

    if o.ob_type is H_NULL:
        assert o._null._type is H_CONTENT_SUMMARY
        return None

    res = content_summary.__new__(content_summary)
    res.init(o)
    return res


cdef class token:
    cdef hdfs_object* c_tok

    def __init__(self):
        self.c_tok = hdfs_token_new_empty()

    def __cinit__(self):
        self.c_tok = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_TOKEN

        self.c_tok = o

    def __dealloc__(self):
        if self.c_tok is not NULL:
            hdfs_object_free(self.c_tok)

    def __repr__(self):
        return generic_repr(self)


cdef token token_build(hdfs_object* o):
    cdef token res

    if o.ob_type is H_NULL:
        assert o._null._type is H_TOKEN
        return None

    res = token.__new__(token)
    res.init(o)
    return res


cdef class upgrade_status_report:
    cdef hdfs_object* c_ob

    def __init__(self):
        raise TypeError("This class cannot be instatiated from Python")

    def __cinit__(self):
        self.c_ob = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_UPGRADE_STATUS_REPORT

        self.c_ob = o

    def __dealloc__(self):
        if self.c_ob is not NULL:
            hdfs_object_free(self.c_ob)

    def __repr__(self):
        return generic_repr(self)

    property version:
        def __get__(self):
            return self.c_ob._upgrade_status._version

        def __set__(self, val):
            self.c_ob._upgrade_status._version = int(val)

    property status:
        def __get__(self):
            return self.c_ob._upgrade_status._status

        def __set__(self, val):
            self.c_ob._upgrade_status._status = int(val)


cdef upgrade_status_report upgrade_status_report_build(hdfs_object* o):
    cdef upgrade_status_report res

    if o.ob_type is H_NULL:
        assert o._null._type is H_UPGRADE_STATUS_REPORT
        return None

    res = upgrade_status_report.__new__(upgrade_status_report)
    res.init(o)
    return res


cdef class server_defaults:
    cdef hdfs_object* c_ob

    def __init__(self):
        raise TypeError("This class cannot be instatiated from Python")

    def __cinit__(self):
        self.c_ob = NULL

    cdef init(self, hdfs_object *o):
        assert o is not NULL
        assert o.ob_type == H_FS_SERVER_DEFAULTS

        self.c_ob = o

    def __dealloc__(self):
        if self.c_ob is not NULL:
            hdfs_object_free(self.c_ob)

    def __repr__(self):
        return generic_repr(self)

    property blocksize:
        def __get__(self):
            return self.c_ob._server_defaults._blocksize

        def __set__(self, val):
            self.c_ob._server_defaults._blocksize = int(val)

    property bytes_per_checksum:
        def __get__(self):
            return self.c_ob._server_defaults._bytes_per_checksum

        def __set__(self, val):
            self.c_ob._server_defaults._bytes_per_checksum = int(val)

    property write_packet_size:
        def __get__(self):
            return self.c_ob._server_defaults._write_packet_size

        def __set__(self, val):
            self.c_ob._server_defaults._write_packet_size = int(val)

    property replication:
        def __get__(self):
            return self.c_ob._server_defaults._replication

        def __set__(self, val):
            self.c_ob._server_defaults._replication = int(val)

    property filebuffersize:
        def __get__(self):
            return self.c_ob._server_defaults._filebuffersize

        def __set__(self, val):
            self.c_ob._server_defaults._filebuffersize = int(val)

    property encrypt_data_transfer:
        def __get__(self):
            return bool(self.c_ob._server_defaults._encrypt_data_transfer)

        def __set__(self, val):
            cdef bint bv = bool(val)
            self.c_ob._server_defaults._encrypt_data_transfer = bv

    property trashinterval:
        def __get__(self):
            return self.c_ob._server_defaults._trashinterval

        def __set__(self, val):
            self.c_ob._server_defaults._trashinterval = int(val)

    property checksumtype:
        def __get__(self):
            return self.c_ob._server_defaults._checksumtype

        def __set__(self, val):
            assert int(val) in _VALID_CSUMS
            self.c_ob._server_defaults._checksumtype = int(val)


cdef server_defaults server_defaults_build(hdfs_object* o):
    cdef server_defaults res

    if o.ob_type is H_NULL:
        assert o._null._type is H_FS_SERVER_DEFAULTS
        return None

    res = server_defaults.__new__(server_defaults)
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


cdef class rpc:
    """
    Represents a connection to an HDFS Namenode (HDFS RPC server)

    Example usage:
        clientname = "client_1234"
        path = "/a/b/c.txt"

        h = hadoofus.namenode("127.0.0.1", user='foo')
        h.create(path, 0644, clientname)
        block = h.addBlock(path, clientname)
        hadoofus.datanode(block).write("Hello, world!", clientname)
        h.complete(path, clientname)

        file_blocks = h.getBlockLocations(path)
        for block in file_blocks:
            print hadoofus.datanode(block).read(clientname)
    """

    cdef hdfs_namenode nn
    cdef bint closed

    cpdef readonly bytes _ipc_proto
    cpdef readonly str address
    cpdef readonly int protocol
    cpdef readonly str user

    def __cinit__(self):
        with nogil:
            hdfs_namenode_init(&self.nn, clowlevel.HDFS_NO_KERB)

    def __dealloc__(self):
        with nogil:
            hdfs_namenode_destroy(&self.nn, NULL)

    def __init__(self, addr, protocol=HADOOP_1_0, user=None, port=None, kerb=NO_KERB, **kwargs):
        """
        Create a connection to an HDFS namenode

        addr:
            a string hostname or ip address, e.g., '127.0.0.1'.
        protocol (optional):
            only hadoofus.HADOOP_1_0 for now.
        user (optional):
            a string username to present to the server for authorization
            purposes. defaults to "root".
        """
        self._ipc_proto = const_str_to_py(CLIENT_PROTOCOL)
        self.closed = False

        cdef clowlevel.hdfs_kerb kp = kerb
        if kerb != NO_KERB:
            hdfs_namenode_init(&self.nn, kp)

        if user is None:
            user = "root"

        self.address = addr
        self.protocol = protocol
        self.user = user

        cdef char* c_addr = addr
        cdef char* c_port = "8020"
        cdef const_char* err
        cdef bytes py_err

        if port is not None:
            port = str(port)
            c_port = port

        cdef clowlevel.hdfs_namenode_proto c_pr = protocol
        hdfs_namenode_set_version(&self.nn, c_pr)
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
        pv = kwargs.get('protocol_version', 61)
        assert pv == self.getProtocolVersion(pv)

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
        """
        Returns remote protocol version

        This RPC is HDFSv1-only.

        version:
            an integer; apache hadoop 0.20.203 through 1.0.x all speak
            protocol 61.
        protostr (optional):
            a string; all recent hadoop versions use "...ClientProtocol",
            defined in hadoofus.rpc._ipc_proto, for this value.
        """
        cdef int64_t res
        cdef hdfs_object* ex = NULL

        assert self.protocol < HADOOP_2_0

        with nogil:
            res = hdfs_getProtocolVersion(&self.nn, protostr, version, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef located_blocks getBlockLocations(self, char* path, int64_t offset=0, int64_t length=4LL*1024*1024*1024):
        """
        Return a LocatedBlocks object, which contains file length as well as a
        list of LocatedBlock objects. (LocatedBlock objects contain a block's
        unique id number, length, and a list of "locations", ip:port pairs
        pointing the caller at datanodes where the block can be retrieved.)
        Clients must connect to one of these datanodes to fetch the data.

        path:
            An absolute path in the HDFS namespace, e.g. "/a/b/c.txt"
        offset (optional):
            Beginning of desired read range in this file (integer). Note: the
            range is truncated/rounded up to block offsets, s.t. all blocks
            containing part of the desired range are returned.

            Defaults to 0.
        length (optional):
            Length of the desired range, starting at 'offset'.

            Defaults to 4GB.

        raises IOException:
            if an error occurs (the error message may be revealing)
        """
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_getBlockLocations(&self.nn, path, offset, length, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return located_blocks_build(res)

    cpdef create(self, char *path, int16_t perms, char *client, bint can_overwrite=False, int16_t replication=1, int64_t blocksize=_WRITE_BLOCK_SIZE, bint create_parent=True):
        """
        Creates a new empty file entry in the HDFS namespace

        path:
            Absolute path of the file being created, e.g., "/a/b.txt"
        perms:
            Unix permission to create the file with, e.g., 0644
        client:
            Any string; must be consistent across addBlock(), writes, and
            complete() for a given file
        can_overwrite (optional):
            bool; if True, overwrite the file at 'path' (if it exists).

            defaults to false.
        replication (optional):
            Replication factor. This library can only really write files with
            replication=1 at this time.

            If you're using this library and want to write higher replication
            files to an HDFS cluster, you could create() / write with
            replication=1, then later use setReplication() to have the cluster
            copy the file to many nodes.

            Defaults to 1
        blocksize (optional):
            Maximum block size for this file. Java default is 64 MiB.

            Defaults to _WRITE_BLOCK_SIZE (64MB)
        create_parent (optional):
            If None or True, automatically create parent directory if missing.

            Defaults to True

        raises AccessControlException:
            if permission is denied
        raises IOException:
            for other errors
        """
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_create(&self.nn, path, perms, client, can_overwrite, create_parent, replication, blocksize, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef located_block append(self, char *path, char *client):
        """
        Returns the last block in the file if it is partially filled, otherwise
        null.

        path:
            Absolute path of the file to append
        clientname:
            Just a string. See rpc.create() for usage.

        raises IOException:
            if an error occurs
        """
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_append(&self.nn, path, client, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return located_block_build(res)

    cpdef bint setReplication(self, char *path, int16_t replication) except *:
        """
        Change the replication factor of an existing file. Actual block
        population / deletion will be done asynchronously. Returns 'false'
        if path is a directory or doesn't exist.

        path:
            Absolute path to the file
        replication:
            An integer (typically 3-10)

        raises IOException:
            if an error occurs
        """
        cdef hdfs_object* ex = NULL
        cdef bint res

        with nogil:
            res = hdfs_setReplication(&self.nn, path, replication, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef setPermission(self, char *path, int16_t perms):
        """
        Basically, chmod

        path:
            Absolute path to chmod
        perms:
            Unix permissions, e.g., 0644. Note: HDFS discards mode bits outside
            of 0777 for directories or 0666 for files.
        """
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_setPermission(&self.nn, path, perms, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef setOwner(self, char *path, bytes owner_py, bytes group_py):
        """
        chown

        path:
            Absolute path to chown
        owner:
            New owner of the file; if None, leave unchanged
        group:
            New group for the file; if None, leave unchanged

        Note: Apache HDFS' concept of users and groups are simply string tags.
        So, calling setOwner() with an owner or group which doesn't yet exist
        will create it.
        """
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
        """
        Clients can abandon blocks; partial writes to this block are discarded.
        Clients may then obtain a new block, or complete the file.

        block:
            An hdfs Block or LocatedBlock object
        path:
            Path to the file for which the block is being abandoned
        clientname:
            Just a string. See rpc.create() for usage.
        """
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
        """
        Add a block to a file (which must already be opened for writing by the
        client). Returns a LocatedBlock object, which is a block id and
        a list of IPs to connect to for writing the actual block data.

        path:
            Absolute path to the file
        clientname:
            A string; see rpc.create() for usage
        excluded (optional):
            If non-None, excluded is a list of DatanodeInfo objects which this
            client has been given before by addBlock() but failed to connect
            to. This is a mechanism for reporting Datanode outages to the
            Namenode.
        """
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
        """
        A client has finished writing to a file. Releases exclusive ownership
        of the file.

        According to Java HDFS, a call to complete() won't return true until
        all of the file's blocks have been fully replicated. At the RPC layer,
        if complete() returns false, the caller should try again. However,
        python callers do not have to do this because this we wrap complete()
        for them.

        path:
            Absolute path of the file to complete. Must be open for writing by
            the client
        clientname:
            Just a string. See rpc.create() for usage.
        """
        cdef hdfs_object* ex = NULL
        cdef bint res = False

        while res is False:
            with nogil:
                res = hdfs_complete(&self.nn, path, client, &ex)
            if ex is not NULL:
                raise_protocol_error(ex)

        return res

    cpdef bint rename(self, char *src, char *dst) except *:
        """
        Rename an entity in the HDFS namespace. Returns False if the src name
        doesn't exist or if the new name already exists in the namespace.

        src:
            Existing file or directory (absolute path string)
        dest:
            Destination path, or destination directory (if the destination
            already exists, and is a directory, the file is moved into the
            directory; the file keeps the same basename).

        raises IOException:
            on error
        """
        cdef hdfs_object* ex = NULL
        cdef bint res

        with nogil:
            res = hdfs_rename(&self.nn, src, dst, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef bint delete(self, char *path, bint can_recurse=False, bint onearg=False) except *:
        """
        Delete an entity from the HDFS namespace. Returns False if the file or
        directory was not removed.

        path:
            Absolute path to remove
        can_recurse (optional):
            If True, recursive delete

            Defaults to False
        """
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
        """
        Recursively make directories with permissions 'perms'. Returns False if
        the operation fails.

        (Similar to Unix 'mkdir -p'.)

        path:
            The directory to create, e.g., /a/b/c/d. Will create all needed
            parent directories.
        perms:
            Unix permissions for created directories, e.g., 0644
        """
        cdef hdfs_object* ex = NULL
        cdef bint res

        with nogil:
            res = hdfs_mkdirs(&self.nn, path, perms, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef directory_listing getListing(self, char *path):
        """
        Returns a DirectoryListing object (mostly just a list of FileStatus
        objects)

        path:
            Absolute path of the directory to list
        """
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_getListing(&self.nn, path, NULL, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return directory_listing_build(res)

    cpdef renewLease(self, char *client):
        """
        Renews all leases (locks that will expire automatically if clients
        fail to renew them) held by a client

        clientname:
            String name of client, e.g., 'client-123'
        """
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_renewLease(&self.nn, client, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef list getStats(self):
        """
        Gets a set of filesystem statistics as a list of integers (in bytes
        unless otherwise noted):
            [0]: total storage capacity of the system
            [1]: total used space
            [2]: available storage space
            [3]: number of "under-replicated" blocks
            [4]: number of blocks with a corrupt replica
            [5]: number of blocks with no uncorrupted replica
        """
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_getStats(&self.nn, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return convert_array_long_to_list(res)

    cpdef int64_t getPreferredBlockSize(self, char *path) except? -1:
        """
        Returns the Namenode's preferred block size (integer) for a given file

        path:
            Absolute path of the file

        raises IOException:
            if it feels like it
        """
        cdef hdfs_object* ex = NULL
        cdef int64_t res

        with nogil:
            res = hdfs_getPreferredBlockSize(&self.nn, path, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef file_status getFileInfo(self, char *path):
        """
        Returns an HdfsFileStatus for a file. This contains information such as
        if the file is currently open for writing or not; whether the file is
        a directory; the length, replication factor, block size, mtime, atime,
        permissions, owner, and group.

        Returns None if no such file exists.

        path:
            Absolute path to query

        raises IOException:
            if permission is denied
        """
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_getFileInfo(&self.nn, path, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return file_status_build(res)

    cpdef content_summary getContentSummary(self, char *path):
        """
        Returns a summary of the files rooted at a path; basically the list:
            [ total data length,
              number of files,
              number of directories,
              quota,
              space consumed,
              space quota ]

        Units are bytes or files or directories.

        path:
            Absolute path to summarize
        """
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        with nogil:
            res = hdfs_getContentSummary(&self.nn, path, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return content_summary_build(res)

    cpdef setQuota(self, char *path, int64_t nsquota, int64_t dsquota):
        """
        Set a quota on a directory

        path:
            Absolute path of directory
        ns_quota:
            Integer representing 'namenode space' limit; QUOTA_DONT_SET implies
            no change, and QUOTA_RESET destroys a quota.
        ds_quota:
            Integer representing a 'datanode space' limit; takes the same
            special values as ns_quota.

        raises FileNotFoundException:
            if the path is not a directory or doesn't exist
        raises QuotaExceededException:
            if the directory size is too large for the given quota
        """
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_setQuota(&self.nn, path, nsquota, dsquota, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef fsync(self, char *path, char *client):
        """
        Writes all metadata for a file onto persistent storage. The file must
        be open for writing.

        path:
            Absolute path of the file
        clientname:
            Client who has the file open for writing. See rpc.create() for
            details.
        """
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_fsync(&self.nn, path, client, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef setTimes(self, char *path, int64_t mtime, int64_t atime):
        """
        Set a file's modification and access time

        Note that HDFS has no concept of time zones; these values are absolute
        GMT times.

        path:
            Absolute path of file or directory
        mtime:
            Integer representing the new modification time (in java-style
            milliseconds since unix epoch); -1 means 'no change'.
        atime:
            Integer representing the new access time (takes the same values as
            mtime).
        """
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_setTimes(&self.nn, path, mtime, atime, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef bint recoverLease(self, char *path, char *client) except *:
        """
        Attempts to recover (destroy) another client's lease on a file. If that
        file didn't have a lease before recoverLease(), returns True.

        path:
            Absolute path to the file
        client:
            An HDFS client name. See rpc.create().

        raises IOException:
            if 'path' is not a valid HDFS file name
        raises FileNotFoundException:
            if 'path' doesn't exist
        raises AccessControlException:
            if permission is denied
        """
        cdef hdfs_object* ex = NULL
        cdef bint res

        with nogil:
            res = hdfs_recoverLease(&self.nn, path, client, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef concat(self, char *target, object srcs):
        """
        Concatenates several files together into 'target'.

        targ:
            Absolute path of the target file; should exist
        srcs:
            Array of paths of files to concatenate to target

        raises AccessControlException:
            if permission is denied
        raises IOException:
            for other errors
        """

        cdef hdfs_object* ex = NULL
        cdef hdfs_object* c_srcs = NULL

        cdef bytes py_s_src
        cdef char* c_src

        if srcs is None:
            pass
        elif isinstance(srcs, list):
            c_srcs = hdfs_array_string_new(0, NULL)
            for src in srcs:
                py_s_src = bytes(src)
                c_src = py_s_src

                hdfs_array_string_add(c_srcs, c_src)
        else:
            raise TypeError("srcs should be a list or None")

        with nogil:
            hdfs_concat(&self.nn, target, c_srcs, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef token getDelegationToken(self, char *renewer):
        """
        Get a valid Delegation Token.

        renewer:
            The designated renewer for the token

        raises IOException
        """

        cdef hdfs_object* res
        cdef hdfs_object* ex = NULL

        with nogil:
            res = hdfs_getDelegationToken(&self.nn, renewer, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return token_build(res)

    cpdef int64_t renewDelegationToken(self, object token_) except? -1:
        """
        Renew an existing delegation token.

        token:
            The token to renew

        raises IOException
        """

        cdef hdfs_object* ex
        cdef token r_tok
        cdef int64_t res

        if type(token_) is token:
            r_tok = token_
        else:
            raise TypeError("Argument 'token' has incorrect type (expected hadoofus.token, got %s)" % \
                    str(type(token_)))

        with nogil:
            res = hdfs_renewDelegationToken(&self.nn, r_tok.c_tok, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)
        return res

    cpdef cancelDelegationToken(self, object token_):
        """
        Cancel an existing delegation token.

        token:
            The token to cancel

        raises IOException
        """

        cdef hdfs_object* ex
        cdef token r_tok

        if type(token_) is token:
            r_tok = token_
        else:
            raise TypeError("Argument 'token' has incorrect type (expected hadoofus.token, got %s)" % \
                    str(type(token_)))

        with nogil:
            hdfs_cancelDelegationToken(&self.nn, r_tok.c_tok, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef bint setSafeMode(self, char *mode) except *:
        """
        Enter, leave, or get safe mode.

        mode:
            One of SAFEMODE_ENTER, _LEAVE, or _GET.

        raises IOException
        """
        cdef hdfs_object* ex = NULL
        cdef bint res = False

        with nogil:
            res = hdfs_setSafeMode(&self.nn, mode, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef getDatanodeReport(self, char *dnreporttype):
        """
        Get a report on the system's datanodes. Returns an array of
        datanode_infos, one per datanode matching the query.

        dnreporttype:
            LIVE, DEAD, or ALL

        raises IOException
        """

        cdef hdfs_object* res
        cdef hdfs_object* ex = NULL

        with nogil:
            res = hdfs_getDatanodeReport(&self.nn, dnreporttype, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        assert res.ob_type == H_ARRAY_DATANODE_INFO
        py_res = []

        cdef hdfs_array_datanode_info *adi
        cdef hdfs_object *di
        cdef datanode_info py_di

        adi = &res._array_datanode_info
        for i in range(adi._len):
            with nogil:
                di = hdfs_datanode_info_copy(adi._values[i])
            py_di = datanode_info_build(di)
            py_res.append(py_di)

        return py_res

    cpdef reportBadBlocks(self, object blocks):
        """
        Report corrupted blocks to the namenode.

        blocks:
            Array (list) of located_blocks to report
        """

        cdef hdfs_object* ex = NULL
        cdef hdfs_object* c_blks
        cdef hdfs_object* c_lb

        c_blks = hdfs_array_locatedblock_new()
        if blocks is not None and isinstance(blocks, list):
            for blk in blocks:
                assert type(blk) is located_block

                c_lb = located_block.get_lb(blk)
                hdfs_array_locatedblock_append_located_block(c_blks,
                        hdfs_located_block_copy(c_lb))

        elif blocks is not None:
            raise TypeError("blocks should be a list of located_block or None")

        with nogil:
            hdfs_reportBadBlocks(&self.nn, c_blks, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef upgrade_status_report distributedUpgradeProgress(self, char *upgradeaction):
        """
        Report distributed upgrade progress or force current upgrade to
        proceed. Returns upgrade_status_report or None if no upgrades are in
        progress.

        upgradeaction:
            GET_STATUS, DETAILED_STATUS, or FORCE_PROCEED.

        raises IOException
        """

        cdef hdfs_object* res
        cdef hdfs_object* ex = NULL

        with nogil:
            res = hdfs_distributedUpgradeProgress(&self.nn, upgradeaction, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return upgrade_status_report_build(res)

    cpdef finalizeUpgrade(self):
        """
        Finalize the previous upgrade. Remove file system state saved during
        upgrade. The upgrade will become irreversible.

        raises IOException
        """

        cdef hdfs_object* ex
        with nogil:
            hdfs_finalizeUpgrade(&self.nn, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef refreshNodes(self):
        """
        Tells the namenode to reread the hosts and exclude files.

        raises IOException
        """

        cdef hdfs_object* ex
        with nogil:
            hdfs_refreshNodes(&self.nn, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef saveNamespace(self):
        """
        Saves current namespace into storage directories and resets edits log.
        Requires superuser privileges and safe mode.

        raises AccessControlException:
            if permission is denied
        raises IOException:
            if image creation fails
        """

        cdef hdfs_object* ex
        with nogil:
            hdfs_saveNamespace(&self.nn, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef bint isFileClosed(self, char *path) except *:
        """
        Get the 'close status' of a file. Returns true if file is closed.

        path:
            Absolute path of the file.

        raises AccessControlException:
            if permission is denied
        raises FileNotFoundException:
            if the file does not exist
        raises IOException:
            if some other error occurs
        """
        cdef hdfs_object* ex = NULL
        cdef bint res = False

        with nogil:
            res = hdfs_isFileClosed(&self.nn, path, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return res

    cpdef metaSave(self, char *path):
        """
        Dumps namenode data structures into specified file. If file already
        exists, then append.

        path:
            Absolute path of the file.

        raises IOException
        """
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_metaSave(&self.nn, path, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef setBalancerBandwidth(self, int64_t bw):
        """
        Tell all datanodes to use a new, non-persistent bandwidth value for
        'dfs.balance.bandwidthPerSec.'

        bw:
            Balancer bandwidth in bytes per second.

        raises IOException
        """
        cdef hdfs_object* ex = NULL

        with nogil:
            hdfs_setBalancerBandwidth(&self.nn, bw, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

    cpdef server_defaults getServerDefaults(self):
        """
        Get server default values.

        This RPC is new in HDFSv2.

        raises IOException
        """
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        assert self.protocol >= HADOOP_2_0

        with nogil:
            res = hdfs2_getServerDefaults(&self.nn, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return server_defaults_build(res)

    cpdef file_status getFileLinkInfo(self, char *path):
        """
        Get the file info for a specific file or directory. If the path is a
        symlink, then the FileStatus of the symlink is returned.

        Returns None if no such file exists.

        path:
            Absolute path to query

        raises AccessControlException:
            if permission is denied
        raises UnresolvedLinkException:
            if 'path' contains a symlink
        raises IOException:
            if some other error occurs
        """
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res

        assert self.protocol >= HADOOP_2_0

        with nogil:
            res = hdfs2_getFileLinkInfo(&self.nn, path, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        return file_status_build(res)

    cpdef char* getLinkTarget(self, char *path) except NULL:
        """
        Return the target of the given symlink. If there is an intermediate
        symlink in the path, the given path is returned with this symlink
        resolved.

        Returns the path after resolving the *first* symbolic link in the path.

        path:
            Absolute path to query

        raises AccessControlException:
            if permission is denied
        raises FileNotFoundException:
            if 'path' does not exist
        raises IOException:
            if the given path does not refer to a symlink or some other error
            occurs
        """
        cdef hdfs_object* ex = NULL
        cdef hdfs_object* res
        cdef char* c_res = NULL

        assert self.protocol >= HADOOP_2_0

        with nogil:
            res = hdfs2_getLinkTarget(&self.nn, path, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)

        assert res.ob_type == H_STRING
        c_res = xstrdup(res._string._val)
        return c_res

    cpdef createSymlink(self, char *target, char *link, int16_t dirperms, bint createparent):
        """
        Create symlink to a file or directory.

        target:
            Absolute path of link destination.
        link:
            Absolute path of the link being created.
        dirperms:
            Permissions to use when creating parent directories.
        createparent:
            If true, create missing parent directories.

        raises AccessControlException:
            if permission is denied
        raises FileAlreadyExistsException:
            if 'link' already exists
        raises FileNotFoundException:
            if parent of 'link' does not exist and 'createparent' is false
        raises ParentNotDirectoryException:
            if parent of 'link' is not a directory
        raises UnresolvedLinkException:
            if 'link' contains a symlink
        raises SnapshotAccessControlException:
            if 'link' is a RO snapshot
        raises IOException:
            if some other error occurs
        """
        cdef hdfs_object* ex = NULL

        assert self.protocol >= HADOOP_2_0

        with nogil:
            hdfs2_createSymlink(&self.nn, target, link, dirperms, createparent, &ex)
        if ex is not NULL:
            raise_protocol_error(ex)


rpcproto = rpc
namenode = rpc


# TODO handle the connecting ourselves so we can report:
# - num. failed datanode attempts (and list)
# - actual datanode we are connected to
cdef class data:
    """
    Represents a connection to an HDFS Datanode (HDFS block server)

    Example use:
        client = "client_123"
        block = namenode_connection.addBlock(...)

        datanode_connection = hadoofus.data(block, client)
        datanode_connection.write("abcdefg...")
    """
    cdef hdfs_datanode* dn
    cdef hdfs_object* orig_lb

    cpdef readonly int protocol
    cpdef readonly bytes client
    cpdef readonly int size

    def __cinit__(self):
        self.dn = NULL
        self.orig_lb = NULL

    def __init__(self, lb, client, proto=DATANODE_PROTO_AP_1_0):
        """
        Creates a datanode connection object

        lb:
            a LocatedBlock, from getBlockLocations() or addBlock()
        client:
            just a client string. See rpc.create()'s docstring.
        proto (optional):
            Currently only supports DATANODE_PROTO_AP_1_0. (Defaults to this
            value.)
        """
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

    def write(self, object data, int maxlen=-1, int offset=-1, bint sendcrcs=True,
            bint keepalive=False):
        """
        Writes data into the block represented by this datanode connection

        Calling write() has the side effect of closing the datanode connection.

        data:
            Either a byte string or file-like object
        maxlen (optional):
            Maximum length to write to this block; required if data is a
            file-like object. Defaults to -1 (unlimited).
        offset (optional):
            If data is a file-like object, represents the offset in data to
            begin reading from. If data is a byte string, offset is ignored.
            Defaults to -1 (current offset in data, i.e., data.tell()).
        sendcrcs (optional):
            HDFS provides an additional data integrity facility on top of TCP
            and IP; if False, this facility is disabled (this may improve
            performance).

            The additional verification is simply a crc32 for every 512 bytes
            of data, not a cryptographically strong hash function.

            Defaults to True.

        Ex: dn.write("AAAAAAAAAAAAAAAAAAAAAAAAAAAAA...")
            dn.write(filelikeobject, maxlen=160)

        Caveat: If the data to be written is longer than the block size, the
                write may fail! This wrapper doesn't check for that. When data
                is a string and maxlen != -1, writes min(len(data), maxlen).
        Caveat: If you pass a file object for data, you must give us
                maxlen so we know where the last packet is!
        """
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
        """
        Reads out the entire block and returns the contents as a byte string.
        Raises an exception if an error occurs before the read is complete.

        verifycrcs (optional):
            If True, raises an exception IF the server sends CRC data AND that
            CRC data indicates that the block is corrupt. If the server doesn't
            send CRC data, no validation is performed and the operation will
            silently succeed. Apache HDFS sends CRC data by default.

            Defaults to True.

        Calling read() has the side effect of closing the datanode connection.

        Ex: dn.read() -> "AAAAAAAAAAAAa..."
        """
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


def _easy_connect(path, user):
    pr = urlparse.urlparse(path, scheme='hdfs', allow_fragments=False)
    port = "8020"
    if pr.port is not None:
        port = str(pr.port)
    if pr.hostname is None:
        raise ValueError("HDFS URL '%s' lacks a hostname!" % path)
    if pr.query != '':
        raise ValueError("Query components are invalid in HDFS URLs")

    return (rpc(pr.hostname, user=user, port=port), pr.path)


class hdfs_file(object):
    """
    File-like objects with basic reading / writing for HDFS.
    """

    def __init__(self, conn, fn, perms):
        if perms not in ("r", "w"):
            raise ValueError("perms not 'r' nor 'w'")

        self._conn = conn
        self._fn = fn
        self._perms = perms
        self._buf = ''
        self._client = None
        self._tell = 0
        self._eof = None

    def _openfile(self):
        if self._client is None:
            self._client = "hadoofus_py_" + str(random.randrange(10000))
            if self._perms == 'w':
                self._conn.create(self._fn, 0644, self._client, can_overwrite=True, replication=3)

    def _get_eof(self):
        if self._eof is None:
            fs = self._conn.getFileInfo(self._fn)
            self._eof = fs.size
        return self._eof

    def close(self):
        if self._conn is not None and self._perms == 'w':
            self.flush()
            self._conn.complete(self._fn, self._client)
        self._conn = None

    def flush(self):
        if self._conn is None:
            raise ValueError("closed")

        if self._perms == 'r':
            return

        self._openfile()
        if len(self._buf) > 0:
            lb = self._conn.addBlock(self._fn, self._client)
            dn = data(lb, self._client)
            dn.write(self._buf)

            self._buf = ''

    def write(self, s):
        if self._conn is None:
            raise ValueError("closed")

        if self._perms == 'r':
            raise ValueError("read-only file")

        self._tell += len(s)

        while len(self._buf) + len(s) >= _WRITE_BLOCK_SIZE:
            aux = s[:_WRITE_BLOCK_SIZE - len(self._buf)]
            s = s[_WRITE_BLOCK_SIZE - len(self._buf):]
            self._buf += aux

            self.flush()

        self._buf = s

    def tell(self):
        if self._conn is None:
            raise ValueError("closed")

        return self._tell

    def seek(self, offset, whence=os.SEEK_SET):
        if self._conn is None:
            raise ValueError("closed")

        if self._perms == 'w':
            raise ValueError("can't seek files for writing")

        if whence not in (os.SEEK_SET, os.SEEK_CUR, os.SEEK_END):
            raise ValueError("bad whence value")

        self._buf = ''

        if whence == os.SEEK_SET:
            self._tell = offset
        elif whence == os.SEEK_CUR:
            self._tell += offset
        elif whence == os.SEEK_END:
            self._tell = offset + self._get_eof()

    def read(self, size=None):
        if self._conn is None:
            raise ValueError("closed")

        if self._perms == 'w':
            raise ValueError("can't read files open for writing")

        if size is None:
            size = 9999999999

        res = ''

        if len(self._buf) > 0:
            l = min(len(self._buf), size)

            res += self._buf[:l]
            self._buf = self._buf[l:]

            self._tell += l
            size -= l

            if size == 0:
                return res

        if self._tell >= self._get_eof():
            return ''

        self._openfile()

        while size > 0 and self._tell < self._get_eof():
            blks = self._conn.getBlockLocations(self._fn, offset=self._tell,
                    length=size)
            for blk in blks:
                blk_data = data(blk, self._client).read()
                if self._tell > blk.offset:
                    blk_data = blk_data[self._tell - blk.offset:]
                self._buf += blk_data

            l = min(len(self._buf), size)

            res += self._buf[:l]
            self._buf = self._buf[l:]

            self._tell += l
            size -= l

        return res


class easy(object):
    @staticmethod
    def open(path, perms="r", user="hadoop"):
        """
        hadoofus.easy.open('hdfs://my_namenode/a/b/c.txt', 'r') opens a file-
        like object for reading from 'c.txt'.

        Similarly, easy.open(..., 'w') opens a file-like object for writing.
        """

        conn, fn = _easy_connect(path, user)
        return hdfs_file(conn, fn, perms)

    # TODO other useful operations (see: os.utimes(), etc)
