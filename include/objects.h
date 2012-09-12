#ifndef HADOOFUS_OBJECTS_H
#define HADOOFUS_OBJECTS_H

#include <stdbool.h>

#include <sys/stat.h>
#include <sys/types.h>

#define HADOOFUS_CLIENT_PROTOCOL_STR "org.apache.hadoop.hdfs.protocol.ClientProtocol"

enum hdfs_object_type {
	_H_START = 0x20,
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
	H_PROTOCOL_EXCEPTION,
	H_ACCESS_CONTROL_EXCEPTION,
	H_ALREADY_BEING_CREATED_EXCEPTION,
	H_FILE_NOT_FOUND_EXCEPTION,
	H_IO_EXCEPTION,
	H_LEASE_EXPIRED_EXCEPTION,
	_H_END,
	_H_INVALID = _H_END,
};

const char *	hdfs_etype_to_string(enum hdfs_object_type e);

struct hdfs_object;

struct hdfs_void {
};

struct hdfs_null {
	enum hdfs_object_type _type;
};

struct hdfs_boolean {
	bool _val;
};

struct hdfs_short {
	int16_t _val;
};

struct hdfs_int {
	int32_t _val;
};

struct hdfs_long {
	int64_t _val;
};

struct hdfs_array_long {
	int64_t *_vals;
	int _len;
};

struct hdfs_located_block {
	int64_t _offset;

	int64_t _blockid;
	int64_t _generation;
	int64_t _len;

	struct hdfs_object **_locs/* type: hdfs_datanode_info[] */;
	int _num_locs;
};

struct hdfs_located_blocks {
	int64_t _size;
	struct hdfs_object **_blocks/* type: hdfs_located_block[] */;
	int _num_blocks;
	bool _being_written;
};

struct hdfs_directory_listing {
	struct hdfs_object **_files/* type: hdfs_file_status[] */;
	struct hdfs_object **_located_blocks/* hdfs_located_blocks[] or NULL */;
	int _num_files;
	bool _has_locations;
};

struct hdfs_datanode_info {
	char *_location/* rack */;
	char *_hostname,
	     *_port;
	uint16_t _namenodeport;
	/* "name" is hostname:port, "hostname" is just hostname */
};

struct hdfs_array_datanode_info {
	struct hdfs_object **_values/* type: hdfs_datanode_info[] */;
	int _len;
};

struct hdfs_file_status {
	int64_t _size;
	int64_t _block_size,
		_mtime,
		_atime/* in ms since epoch */;
	char *_file,
	     *_owner,
	     *_group;
	int16_t _replication,
		_permissions;
	bool _directory;
};

struct hdfs_content_summary {
	int64_t _length/* space in bytes */,
		_files/* number of files */,
		_dirs/* number of directories */,
		_quota/* hard quota */;
};

struct hdfs_block {
	int64_t _blkid,
		_length,
		_generation;
};

struct hdfs_array_byte {
	int8_t *_bytes;
	int _len;
};

struct hdfs_rpc_invocation {
	struct hdfs_object *_args[8];
	char *_method;
	int _nargs,
	    _msgno;
};

struct hdfs_authheader {
	char *_username;
};

struct hdfs_token {
	char *_strings[4];
};

struct hdfs_string {
	char *_val;
};

struct hdfs_array_string {
	int32_t _len;
	char **_val;
};

struct hdfs_fsperms {
	int16_t _perms;
};

struct hdfs_exception {
	char *_msg;
	enum hdfs_object_type _etype;
};

struct hdfs_object {
	union {
		struct hdfs_void _void;
		struct hdfs_null _null;
		struct hdfs_boolean _boolean;
		struct hdfs_short _short;
		struct hdfs_int _int;
		struct hdfs_long _long;
		struct hdfs_array_long _array_long;
		struct hdfs_located_block _located_block;
		struct hdfs_located_blocks _located_blocks;
		struct hdfs_directory_listing _directory_listing;
		struct hdfs_datanode_info _datanode_info;
		struct hdfs_array_datanode_info _array_datanode_info;
		struct hdfs_file_status _file_status;
		struct hdfs_content_summary _content_summary;
		struct hdfs_block _block;
		struct hdfs_array_byte _array_byte;
		struct hdfs_rpc_invocation _rpc_invocation;
		struct hdfs_authheader _authheader;
		struct hdfs_exception _exception;
		struct hdfs_token _token;
		struct hdfs_string _string;
		struct hdfs_fsperms _fsperms;
		struct hdfs_array_string _array_string;
	} ob_val;
	enum hdfs_object_type ob_type;
};

// These functions copy user-supplied values.
struct hdfs_object *	hdfs_void_new(void);
struct hdfs_object *	hdfs_null_new(enum hdfs_object_type type);
struct hdfs_object *	hdfs_boolean_new(bool val);
struct hdfs_object *	hdfs_short_new(int16_t val);
struct hdfs_object *	hdfs_int_new(int32_t val);
struct hdfs_object *	hdfs_long_new(int64_t val);

struct hdfs_object *	hdfs_array_long_new(int len, const int64_t *values);

struct hdfs_object *	hdfs_block_new(int64_t blkid, int64_t len, int64_t generation);
struct hdfs_object *	hdfs_block_copy(struct hdfs_object *);
struct hdfs_object *	hdfs_block_from_located_block(struct hdfs_object *);
struct hdfs_object *	hdfs_located_block_new(int64_t blkid, int64_t len, int64_t generation, int64_t offset);
struct hdfs_object *	hdfs_located_block_copy(struct hdfs_object *);
struct hdfs_object *	hdfs_located_blocks_new(bool beingcreated, int64_t size);
struct hdfs_object *	hdfs_directory_listing_new(void);
struct hdfs_object *	hdfs_located_directory_listing_new(void);
struct hdfs_object *	hdfs_datanode_info_new(const char *host, const char *port,
			const char *rack, uint16_t namenodeport);
struct hdfs_object *	hdfs_datanode_info_copy(struct hdfs_object *);
struct hdfs_object *	hdfs_array_datanode_info_new(void);
struct hdfs_object *	hdfs_array_datanode_info_copy(struct hdfs_object *);
struct hdfs_object *	hdfs_file_status_new(const char *logical_name, const struct stat *st,
			const char *owner, const char *group);
struct hdfs_object *	hdfs_file_status_new_ex(const char *logical_name, int64_t size,
    			bool directory, int replication, int64_t block_size, int64_t mtime_ms,
			int64_t atime_ms, int perms, const char *owner, const char *group);
struct hdfs_object *	hdfs_content_summary_new(int64_t length, int64_t files, int64_t dirs,
			int64_t quota);
struct hdfs_object *	hdfs_array_byte_new(int32_t len, int8_t *bytes);
struct hdfs_object *	hdfs_array_byte_copy(struct hdfs_object *);
struct hdfs_object *	hdfs_array_string_new(int32_t len, const char **strings); /* copies */
void			hdfs_array_string_add(struct hdfs_object *, const char *); /* copies */
struct hdfs_object *	hdfs_rpc_invocation_new(const char *name, ...);
struct hdfs_object *	hdfs_authheader_new(const char *user);
struct hdfs_object *	hdfs_protocol_exception_new(enum hdfs_object_type, const char *);
struct hdfs_object *	hdfs_token_new(const char *, const char *, const char *, const char *);
struct hdfs_object *	hdfs_token_new_empty(void);
struct hdfs_object *	hdfs_token_copy(struct hdfs_object *);
struct hdfs_object *	hdfs_string_new(const char *);
struct hdfs_object *	hdfs_fsperms_new(int16_t);

// Caller loses references to objects that are being appended into arrays.
void	hdfs_located_block_append_datanode_info(
	struct hdfs_object *located_block, struct hdfs_object *datanode_info);
void	hdfs_located_blocks_append_located_block(
	struct hdfs_object *located_blocks, struct hdfs_object *located_block);
void	hdfs_directory_listing_append_file_status(
	struct hdfs_object *directory_listing, struct hdfs_object *file_status,
	struct hdfs_object *located_blocks);
void	hdfs_array_datanode_info_append_datanode_info(
	struct hdfs_object *array, struct hdfs_object *datanode_info);

// Serialize an object, allocating memory on the heap. Initialize the struct
// pointed to by hbuf to all zeros.
struct hdfs_heap_buf {
	char *buf;
	int used,
	    size;
};
void	hdfs_object_serialize(struct hdfs_heap_buf *hbuf, struct hdfs_object *obj);

// Returns NULL if an object cannot be decoded. rbuf->used is set to -1 if we
// just hit EOS, or -2 if we encountered something invalid.
// Otherwise, returns a decoded object and sets rbuf->used to the size of the
// serialized object.
struct hdfs_object *	hdfs_object_slurp(struct hdfs_heap_buf *rbuf,
			enum hdfs_object_type realtype);

// Recursively frees an object.
void	hdfs_object_free(struct hdfs_object *obj);

#endif
