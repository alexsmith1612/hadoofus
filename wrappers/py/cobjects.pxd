from libc.stdint cimport int64_t, int16_t, int32_t, uint16_t, int8_t, uint16_t
from libc.string cimport const_char

cdef extern from "hadoofus/objects.h":
    ctypedef int64_t const_int64_t "const int64_t"
    cdef struct stat:
        pass
    ctypedef stat const_stat "const struct stat"
    cdef const_char *CLIENT_PROTOCOL "HADOOFUS_CLIENT_PROTOCOL_STR"
    cdef enum hdfs_object_type:
        H_VOID = 0x20,
        H_NULL,
        H_BOOLEAN,
        H_INT,
        H_LONG,
        H_ARRAY_LONG,
        H_LOCATED_BLOCK,
        H_LOCATED_BLOCKS,
        H_DIRECTORY_LISTING,
        H_DATANODE_INFO,
        H_ARRAY_DATANODE_INFO,
        H_FILE_STATUS,
        H_CONTENT_SUMMARY,
        H_LOCATED_DIRECTORY_LISTING,
        H_UPGRADE_STATUS_REPORT,
        H_BLOCK,
        H_ARRAY_BYTE,
        H_RPC_INVOCATION,
        H_AUTHHEADER,
        H_TOKEN,
        H_STRING,
        H_FSPERMS,
        H_SHORT,
        H_ARRAY_STRING,
        H_TEXT,

        H_PROTOCOL_EXCEPTION = 0x100,
        H_ACCESS_CONTROL_EXCEPTION,
        H_ALREADY_BEING_CREATED_EXCEPTION,
        H_FILE_NOT_FOUND_EXCEPTION,
        H_IO_EXCEPTION,
        H_LEASE_EXPIRED_EXCEPTION,
    cdef struct hdfs_void:
        pass
    cdef struct hdfs_boolean:
        pass
    cdef struct hdfs_short:
        pass
    cdef struct hdfs_int:
        pass
    cdef struct hdfs_long:
        pass
    cdef struct hdfs_array_byte:
        pass
    cdef struct hdfs_rpc_invocation:
        pass
    cdef struct hdfs_authheader:
        pass
    cdef struct hdfs_token:
        pass
    cdef struct hdfs_string:
        pass
    cdef struct hdfs_fsperms:
        pass
    cdef struct hdfs_array_string:
        pass

    cdef struct hdfs_null:
        hdfs_object_type _type

    cdef struct hdfs_content_summary:
        int64_t _length
        int64_t _files
        int64_t _dirs
        int64_t _quota

    cdef struct hdfs_file_status:
        int64_t _size
        int64_t _block_size
        int64_t _mtime
        int64_t _atime
        char* _file
        char* _owner
        char* _group
        int16_t _replication
        int16_t _permissions
        bint _directory

    cdef struct hdfs_directory_listing:
        hdfs_object** _files
        int _num_files

    cdef struct hdfs_datanode_info:
        char* _location
        char* _hostname
        char* _port
        uint16_t _namenodeport

    cdef struct hdfs_array_datanode_info:
        int _len
        hdfs_object** _values

    cdef struct hdfs_block:
        int64_t _blkid
        int64_t _length
        int64_t _generation

    cdef struct hdfs_located_block:
        int64_t _offset
        int64_t _blockid
        int64_t _generation
        int64_t _len

        hdfs_object** _locs
        int _num_locs

    cdef struct hdfs_located_blocks:
        int64_t _size
        hdfs_object** _blocks
        int _num_blocks
        char _being_written

    cdef struct hdfs_array_long:
        int64_t* _vals
        int _len

    cdef struct hdfs_exception:
        hdfs_object_type _etype
        char* _msg

    cdef struct hdfs_object:
        hdfs_void _void "ob_val._void"
        hdfs_null _null "ob_val._null"
        hdfs_boolean _boolean "ob_val._boolean"
        hdfs_short _short "ob_val._short"
        hdfs_int _int "ob_val._int"
        hdfs_long _long "ob_val._long"
        hdfs_array_long _array_long "ob_val._array_long"
        hdfs_located_block _located_block "ob_val._located_block"
        hdfs_located_blocks _located_blocks "ob_val._located_blocks"
        hdfs_directory_listing _directory_listing "ob_val._directory_listing"
        hdfs_datanode_info _datanode_info "ob_val._datanode_info"
        hdfs_array_datanode_info _array_datanode_info "ob_val._array_datanode_info"
        hdfs_file_status _file_status "ob_val._file_status"
        hdfs_content_summary _content_summary "ob_val._content_summary"
        hdfs_block _block "ob_val._block"
        hdfs_array_byte _array_byte "ob_val._array_byte"
        hdfs_rpc_invocation _rpc_invocation "ob_val._rpc_invocation"
        hdfs_authheader _authheader "ob_val._authheader"
        hdfs_exception _exception "ob_val._exception"
        hdfs_token _token "ob_val._token"
        hdfs_string _string "ob_val._string"
        hdfs_fsperms _fsperms "ob_val._fsperms"
        hdfs_array_string _array_string "ob_val._array_string"
        hdfs_object_type ob_type

    cdef struct hdfs_heap_buf:
        pass

    const_char *hdfs_etype_to_string(hdfs_object_type e)

    hdfs_object *hdfs_void_new()
    hdfs_object *hdfs_null_new(hdfs_object_type type)
    hdfs_object *hdfs_boolean_new(bint val)
    hdfs_object *hdfs_short_new(int16_t val)
    hdfs_object *hdfs_int_new(int32_t val)
    hdfs_object *hdfs_long_new(int64_t val)

    hdfs_object *hdfs_array_long_new(int len, const_int64_t *values)

    hdfs_object *hdfs_block_new(int64_t blkid, int64_t len, int64_t generation)
    hdfs_object *hdfs_block_copy(hdfs_object *)
    hdfs_object *hdfs_block_from_located_block(hdfs_object *)
    hdfs_object *hdfs_located_block_new(int64_t blkid, int64_t len, int64_t generation, int64_t offset)
    hdfs_object *hdfs_located_block_copy(hdfs_object*) nogil
    hdfs_object *hdfs_located_blocks_new(bint beingcreated, int64_t size)
    hdfs_object *hdfs_directory_listing_new()
    hdfs_object *hdfs_located_directory_listing_new()
    hdfs_object *hdfs_datanode_info_new(const_char *host, const_char *port, const_char *rack, uint16_t namenodeport)
    hdfs_object *hdfs_datanode_info_copy(hdfs_object *) nogil
    hdfs_object *hdfs_array_datanode_info_new()
    hdfs_object *hdfs_array_datanode_info_copy(hdfs_object *) nogil
    hdfs_object *hdfs_file_status_new(const_char *logical_name, const_stat *st, const_char *owner, const_char *group)
    hdfs_object *hdfs_file_status_new_ex(const_char *logical_name, int64_t size, bint directory, int replication, int64_t block_size, int64_t mtime_ms, int64_t atime_ms, int perms, const_char *owner, const_char *group)
    hdfs_object *hdfs_content_summary_new(int64_t length, int64_t files, int64_t dirs, int64_t quota)
    hdfs_object *hdfs_array_byte_new(int32_t len, int8_t *bytes)
    hdfs_object *hdfs_array_byte_copy(hdfs_object *)
    hdfs_object *hdfs_rpc_invocation_new(const_char *name, ...)
    hdfs_object *hdfs_authheader_new(const_char *user)
    hdfs_object *hdfs_protocol_exception_new(hdfs_object_type, const_char *)
    hdfs_object *hdfs_token_new(const_char *, const_char *, const_char *, const_char *)
    hdfs_object *hdfs_token_new_empty()
    hdfs_object *hdfs_token_copy(hdfs_object *)
    hdfs_object *hdfs_string_new(const_char *)
    hdfs_object *hdfs_fsperms_new(int16_t)
    hdfs_object *hdfs_array_string_new(int32_t len, const_char **values)
    void hdfs_array_string_add(hdfs_object *, const_char *value)
    hdfs_object *hdfs_text_new(const_char *)
    hdfs_object *hdfs_array_locatedblock_new()

    void hdfs_located_block_append_datanode_info(hdfs_object *located_block, hdfs_object *datanode_info)
    void hdfs_located_blocks_append_located_block(hdfs_object *located_blocks, hdfs_object *located_block)
    void hdfs_directory_listing_append_file_status(hdfs_object *directory_listing, hdfs_object *file_status, hdfs_object *located_blocks)
    void hdfs_array_datanode_info_append_datanode_info(hdfs_object *array, hdfs_object *datanode_info) nogil
    void hdfs_array_locatedblock_append_located_block(hdfs_object *arr_located_block, hdfs_object *lb)

    void hdfs_object_serialize(hdfs_heap_buf *hbuf, hdfs_object *obj)

    hdfs_object *hdfs_object_slurp(hdfs_heap_buf *rbuf, hdfs_object_type realtype)

    void hdfs_object_free(hdfs_object *obj) nogil
