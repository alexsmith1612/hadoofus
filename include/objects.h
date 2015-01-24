#ifndef HADOOFUS_OBJECTS_H
#define HADOOFUS_OBJECTS_H

#include <stdbool.h>

#include <sys/stat.h>
#include <sys/types.h>

#define HADOOFUS_CLIENT_PROTOCOL_STR "org.apache.hadoop.hdfs.protocol.ClientProtocol"
#define _HDFS_CLIENT_ID_LEN 16

enum hdfs_kerb {
	HDFS_NO_KERB,      // plaintext username (default)
	HDFS_TRY_KERB,     // attempt kerb, but allow fallback to plaintext
	HDFS_REQUIRE_KERB, // fail if server attempts to fallback to plaintext
};

enum hdfs_namenode_proto {
	HDFS_NN_v1,
	HDFS_NN_v2,
	HDFS_NN_v2_2,
	_HDFS_NN_vLATEST = HDFS_NN_v2_2,
};

enum hdfs_checksum_type {
	/* Chosen to match wire values in HDFS2 */
	HDFS_CSUM_NULL = 0,
	HDFS_CSUM_CRC32 = 1,
	HDFS_CSUM_CRC32C = 2,
};

enum hdfs_file_type {
	/* Chosen to match wire values in HDFS2 */
	HDFS_FT_DIR = 1,
	HDFS_FT_FILE = 2,
	HDFS_FT_SYMLINK = 3,
};

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
	H_TEXT,
	H_SAFEMODEACTION,
	H_DNREPORTTYPE,
	H_ARRAY_LOCATEDBLOCK,
	H_UPGRADE_ACTION,

	/* Room in ABI in case HDFSv1 ever adds new types: */
	_H_PLACEHOLDER_1,
	_H_PLACEHOLDER_2,
	_H_PLACEHOLDER_3,
	_H_PLACEHOLDER_4,
	_H_PLACEHOLDER_5,
	_H_PLACEHOLDER_6,
	_H_PLACEHOLDER_7,
	_H_PLACEHOLDER_8,
	_H_PLACEHOLDER_9,
	_H_PLACEHOLDER_10,

	/* v2+ types */
	H_FS_SERVER_DEFAULTS,

	/* Leaving room for new types */

	H_PROTOCOL_EXCEPTION = 0x100,
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

/*
 * Users should not use these structs directly, or even access them directly
 * out of an hdfs_object! Instead, only helper functions and generic
 * hdfs_objects should be used.
 */
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

	struct hdfs_object *_token;

	/* v2+ */
	char *_pool_id;
	bool _corrupt;
};

struct hdfs_located_blocks {
	int64_t _size;  /* total size of the file */
	struct hdfs_object **_blocks/* type: hdfs_located_block[] */;
	int _num_blocks;
	bool _being_written;

	/* v2+ */
	struct hdfs_object *_last_block;
	bool _last_block_complete;
};

struct hdfs_directory_listing {
	struct hdfs_object **_files/* type: hdfs_file_status[] */;
	struct hdfs_object **_located_blocks/* hdfs_located_blocks[] or NULL */;
	int _num_files;
	bool _has_locations;

	uint32_t _remaining_entries;
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

	/* v2 fields */
	enum hdfs_file_type _type;
	char *_symlink_target;		/* NULL iff not a symlink */
	uint64_t _fileid;
	int32_t _num_children;
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

	enum hdfs_namenode_proto _proto;
	uint8_t *_client_id;
};

struct hdfs_authheader {
	/*
	 * Username is used for authentication. It is sometimes referred to as
	 * "effective user" in Hadoop docs.
	 *
	 * "Real user" is used for FS access authorization, iff present AND
	 * "effective uesr" is allowed to impersonate "real user."
	 */
	char *_username,
	     *_real_username;
	enum hdfs_namenode_proto _proto;
	enum hdfs_kerb _kerberized;
	uint8_t *_client_id;
};

struct hdfs_token {
	/*
	 * Token is really:
	 *     (Representation:)   (Logical member:)
	 *   - len[0], strings[0]: id (byte[])
	 *   - len[1], strings[1]: password (byte[])
	 *   - strings[2]:         kind (Text)
	 *   - strings[3]:         service (Text)
	 */
	char *_strings[4];
	int32_t _lens[2];
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

struct hdfs_upgrade_status_report {
	int32_t _version;
	int16_t _status;
};

struct hdfs_fsserverdefaults {
	uint64_t _blocksize;
	uint32_t _bytes_per_checksum;
	uint32_t _write_packet_size;

	uint32_t _replication;
	uint32_t _filebuffersize;

	bool _encrypt_data_transfer;
	uint64_t _trashinterval;
	enum hdfs_checksum_type _checksumtype;
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
		struct hdfs_upgrade_status_report _upgrade_status;
		struct hdfs_fsserverdefaults _server_defaults;
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
struct hdfs_object *	hdfs_array_string_copy(struct hdfs_object *);
struct hdfs_object *	hdfs_rpc_invocation_new(const char *name, ...);
struct hdfs_object *	hdfs_authheader_new(const char *user);
struct hdfs_object *	hdfs_authheader_new_ext(enum hdfs_namenode_proto,
			const char * /*user*/, const char * /*real user*/,
			enum hdfs_kerb);
struct hdfs_object *	hdfs_protocol_exception_new(enum hdfs_object_type, const char *);
struct hdfs_object *	hdfs_token_new(const char *, const char *, const char *, const char *);
struct hdfs_object *	hdfs_token_new_empty(void);
struct hdfs_object *	hdfs_token_new_nulsafe(const char *id, size_t idlen,
			const char *pw, size_t pwlen, const char *kind,
			const char *service);
struct hdfs_object *	hdfs_token_copy(struct hdfs_object *);
struct hdfs_object *	hdfs_string_new(const char *);
struct hdfs_object *	hdfs_text_new(const char *);
struct hdfs_object *	hdfs_fsperms_new(int16_t);
struct hdfs_object *	hdfs_safemodeaction_new(const char *);
struct hdfs_object *	hdfs_dnreporttype_new(const char *);
struct hdfs_object *	hdfs_array_locatedblock_new(void);
struct hdfs_object *	hdfs_array_locatedblock_copy(struct hdfs_object *);
struct hdfs_object *	hdfs_upgradeaction_new(const char *);
struct hdfs_object *	hdfs_upgrade_status_report_new(int32_t, int16_t);

// Caller loses references to objects that are being appended into arrays.
void	hdfs_located_block_append_datanode_info(
	struct hdfs_object *located_block, struct hdfs_object *datanode_info);
void	hdfs_located_blocks_append_located_block(
	struct hdfs_object *located_blocks, struct hdfs_object *located_block);
void	hdfs_array_locatedblock_append_located_block(
	struct hdfs_object *arr_located_block, struct hdfs_object *located_block);
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
