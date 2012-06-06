#ifndef HADOOFUS_OBJECTS_H
#define HADOOFUS_OBJECTS_H

#include <sys/stat.h>
#include <sys/types.h>

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
	_H_END,
};

struct hdfs_object;

struct hdfs_void {
};

struct hdfs_null {
	enum hdfs_object_type _type;
};

struct hdfs_boolean {
	bool _val;
};

struct hdfs_int {
	int32_t _val;
};

struct hdfs_long {
	int64_t _val;
};

struct hdfs_array_long {
	int _len;
	int64_t *_vals;
};

struct hdfs_located_block {
	int64_t _blockid;
	int64_t _generation;

	int64_t _len;

	int _num_locs;
	struct hdfs_object **_locs/* type: hdfs_datanode_info[] */;
};

struct hdfs_located_blocks {
	bool _being_written;
	int64_t _size;

	int _num_blocks;
	struct hdfs_object **_blocks/* type: hdfs_located_block[] */;
};

struct hdfs_directory_listing {
	bool _has_locations;

	int _num_files;
	struct hdfs_object **_files/* type: hdfs_file_status[] */;
	struct hdfs_object **_located_blocks/* hdfs_located_blocks[] or NULL */;
};

struct hdfs_datanode_info {
	char *_location/* rack */;
	char *_hostname,
	     *_port;
	/* "name" is hostname:port, "hostname" is just hostname */
};

struct hdfs_array_datanode_info {
	int _len;
	struct hdfs_object **_values/* type: hdfs_datanode_info[] */;
};

struct hdfs_file_status {
	char *_file;
	int64_t _size;
	bool _directory;
	int16_t _replication;
	int64_t _block_size, _mtime, _atime/* in ms since epoch */;
	int16_t _permissions;
	char *_owner, *_group;
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
	int _len;
	int8_t *_bytes;
};

struct hdfs_rpc_invocation {
};

struct hdfs_authheader {
	char *username;
};

struct hdfs_object {
	enum hdfs_object_type ob_type;
	union {
		struct hdfs_void _void;
		struct hdfs_null _null;
		struct hdfs_boolean _boolean;
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
	} ob_val;
};

// These functions copy user-supplied values.
struct hdfs_object *	hdfs_void_new(void);
struct hdfs_object *	hdfs_null_new(enum hdfs_object_type type);
struct hdfs_object *	hdfs_boolean_new(bool val);
struct hdfs_object *	hdfs_int_new(int32_t val);
struct hdfs_object *	hdfs_long_new(int64_t val);

struct hdfs_object *	hdfs_array_long_new(int len, const int64_t *values);

struct hdfs_object *	hdfs_block_new(int64_t blkid, int64_t len, int64_t generation);
struct hdfs_object *	hdfs_located_block_new(int64_t blkid, int64_t generation, int64_t len);
struct hdfs_object *	hdfs_located_blocks_new(bool beingcreated, int64_t size);
struct hdfs_object *	hdfs_directory_listing_new(void);
struct hdfs_object *	hdfs_located_directory_listing_new(void);
struct hdfs_object *	hdfs_datanode_info_new(const char *host, const char *port, const char *rack);
struct hdfs_object *	hdfs_array_datanode_info_new(void);
struct hdfs_object *	hdfs_file_status_new(const char *logical_name, const struct stat *st,
			const char *owner, const char *group);
struct hdfs_object *	hdfs_content_summary_new(int64_t length, int64_t files, int64_t dirs,
			int64_t quota);
struct hdfs_object *	hdfs_array_byte_new(int32_t len, int8_t *bytes);
struct hdfs_object *	hdfs_authheader_new(const char *user);

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

// Serialize an object, allocating memory on the heap.
void	hdfs_object_serialize(struct hdfs_object *obj, char **buf, int *buflen);

// Recursively frees an object.
void	hdfs_object_free(struct hdfs_object *obj);

#endif
