#ifndef HADOOFUS_OBJECTS_H
#define HADOOFUS_OBJECTS_H

#include <stdbool.h>
#include <stdint.h>

#include <sys/stat.h>
#include <sys/types.h>

#define HADOOFUS_CLIENT_PROTOCOL_STR "org.apache.hadoop.hdfs.protocol.ClientProtocol"
#define _HDFS_CLIENT_ID_LEN 16

enum hdfs_error_kind {
	he_errno,
	he_gaierr,
	he_saslerr,
	he_hdfserr,
};
#define _he_num_kinds (he_hdfserr + 1)

#define _HDFS_ERR_MINIMUM HDFS_ERR_AGAIN
enum hdfs_error_numeric {
	// Non-blocking API indicates that we are waiting on some events
	// or resources to proceed
	// XXX Perhaps rename? HDFS_ERR_INPROGRESS?
	HDFS_ERR_AGAIN = 1,
	// I.e., remote end closed the TCP connection or middleman injected
	// RST.
	HDFS_ERR_END_OF_STREAM,
	// Input file shorter than expected, or unexpected zero write(2)ing
	// output.
	HDFS_ERR_END_OF_FILE,

	// Programmer misuse of API
	HDFS_ERR_NAMENODE_UNCONNECTED,
	HDFS_ERR_NAMENODE_UNAUTHENTICATED,

	// Error returned on reads if the user requested CRC validation but the
	// server did not transmit CRCs.
	HDFS_ERR_DATANODE_NO_CRCS,
	// Need at least one datanode to connect, obviously.
	HDFS_ERR_ZERO_DATANODES,
	// Datanode is providing a checksum type we do no support
	HDFS_ERR_DATANODE_UNSUPPORTED_CHECKSUM,

// Direct translations of Datanode STATUS codes.  More information may be
// available in the hdfs_datanode_opresult_message (optionally provided by HDFS
// datanode).  If no message was provided, the pointer will be NULL.
	HDFS_ERR_DN_ERROR,
	HDFS_ERR_DN_ERROR_CHECKSUM,
	HDFS_ERR_DN_ERROR_INVALID,
	HDFS_ERR_DN_ERROR_EXISTS,
	HDFS_ERR_DN_ERROR_ACCESS_TOKEN,
	// Or STATUS code not recognized by libhadoofus; check
	// hdfs_datanode_unknown_status to determine value.
	HDFS_ERR_UNRECOGNIZED_DN_ERROR,
	// STATUS code was recognized, but not valid in the scenario.  The
	// value can be checked with hdfs_datanode_unknown_status.
	HDFS_ERR_INVALID_DN_ERROR,

// Protocol encoding errors
	// HDFS wire messages are prefixed with a length.  That length is
	// itself variable in length.  This error means the encoding of that
	// length prefix was erroneous.
	HDFS_ERR_INVALID_VLINT,
	// Protobuf decode failures:
	HDFS_ERR_INVALID_BLOCKOPRESPONSEPROTO,
	HDFS_ERR_INVALID_PACKETHEADERPROTO,
	HDFS_ERR_INVALID_PIPELINEACKPROTO,
	// v1 DN wire protocol errors
	HDFS_ERR_V1_DATANODE_PROTOCOL,

	// Namenode gave unexpected msgno
	HDFS_ERR_NAMENODE_BAD_MSGNO,
	// Other namenode protocol response parsing failures
	HDFS_ERR_NAMENODE_PROTOCOL,

// Other protocol errors
	// A SUCCESS OpRes had a message attached; as usual, the message can be
	// found in hdfs_datanode_opresult_message.
	HDFS_ERR_INVALID_DN_OPRESP_MSG,
	HDFS_ERR_DATANODE_PACKET_SIZE,
	// The packet's expressed CRCs length doesn't match the data length
	HDFS_ERR_DATANODE_CRC_LEN,
	// We got a non-zero CRCs length when we didn't expect CRCs
	HDFS_ERR_DATANODE_UNEXPECTED_CRC_LEN,
	HDFS_ERR_DATANODE_UNEXPECTED_READ_OFFSET,
	HDFS_ERR_DATANODE_BAD_CHECKSUM,
	HDFS_ERR_DATANODE_BAD_SEQNO,
	// Packet ACK count did not match the number of datanodes in the
	// pipeline
	HDFS_ERR_DATANODE_BAD_ACK_COUNT,
	// Last packet flag set when we expected more packets
	HDFS_ERR_DATANODE_BAD_LASTPACKET,
	// Kerberos downgrade attempt when client requested enforcing mode;
	// possible MITM attack.
	HDFS_ERR_KERBEROS_DOWNGRADE,
	// Unexpected and unhandled error negotiating kerberos
	HDFS_ERR_KERBEROS_NEGOTIATION,
	// A located block object has storage ids but not the same number as
	// datanode_infos/locations
	HDFS_ERR_LOCATED_BLOCK_BAD_STORAGE_IDS,
	// A located block object has storage types but not the same number as
	// datanode_infos/locations
	HDFS_ERR_LOCATED_BLOCK_BAD_STORAGE_TYPES,
	_HDFS_ERR_END
};
#define _HDFS_ERR_MAXIMUM (_HDFS_ERR_END - 1)

#define _HE_KIND_BITS 2
#define _HE_NUM_BITS (32 - _HE_KIND_BITS)
struct hdfs_error {
	enum hdfs_error_kind her_kind : _HE_KIND_BITS;
	int her_num : _HE_NUM_BITS;
};
_Static_assert((1 << _HE_KIND_BITS) >= _he_num_kinds,
    "if we grow more kinds of error, we need to expand the width of her_kind");
_Static_assert(sizeof(struct hdfs_error) == sizeof(uint32_t),
    "for now, attempt to return a 32-bit value for 32-bit platforms where a "
    "64-bit return is slightly more expenisve");

#define HDFS_SUCCESS	(struct hdfs_error) { 0 }
// XXX Perhaps rename? AGAIN to INPROGRESS?
#define HDFS_AGAIN      (struct hdfs_error) { .her_kind = he_hdfserr, .her_num = HDFS_ERR_AGAIN }

static inline bool
hdfs_is_again(struct hdfs_error herr)
{
	return (herr.her_kind == he_hdfserr && herr.her_num == HDFS_ERR_AGAIN);
}

// Return true if the struct represents an unsucessful result.
// XXX Do we want this to indicate HDFS_AGAIN as failure or success?
// The current implementation assumes that hdfs_is_error() returns
// true for HDFS_AGAIN. Perhaps add hdfs_is_fatal() or something
// like that that returns false for HDFS_SUCCESS and HDFS_AGAIN
static inline bool
hdfs_is_error(struct hdfs_error herr)
{
	return (herr.her_kind != 0 || herr.her_num != 0);
}

// Return a string representation of hdfs_error::her_kind.
const char *hdfs_error_str_kind(struct hdfs_error);

// Return a string representation of hdfs_error::her_num.
const char *hdfs_error_str(struct hdfs_error);

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
	// Chosen to match wire values in HDFS2. Note that additional values may
	// be received if the hdfs protocol is updated.
	HDFS_CSUM_NULL = 0,
	HDFS_CSUM_CRC32 = 1,
	HDFS_CSUM_CRC32C = 2,
};

enum hdfs_file_type {
	// Chosen to match wire values in HDFS2. Note that additional values may
	// be received if the hdfs protocol is updated.
	HDFS_FT_DIR = 1,
	HDFS_FT_FILE = 2,
	HDFS_FT_SYMLINK = 3,
};

enum hdfs_storage_type {
	// Chosen to match wire values in HDFS2. Note that additional values may
	// be received if the hdfs protocol is updated.
	HDFS_STORAGE_DISK = 1,
	HDFS_STORAGE_SSD = 2,
	HDFS_STORAGE_ARCHIVE = 3,
	HDFS_STORAGE_RAM_DISK = 4,
	HDFS_STORAGE_PROVIDED = 5,
};

enum hdfs_datanode_report_type {
	// Chosen to match wire values in HDFS2. Note that additional values may
	// be received if the hdfs protocol is updated.
	HDFS_DNREPORT_ALL = 1,
	HDFS_DNREPORT_LIVE = 2,
	HDFS_DNREPORT_DEAD = 3,
	HDFS_DNREPORT_DECOMMISSIONING = 4,
	HDFS_DNREPORT_ENTERING_MAINTENANCE = 5,
	HDFS_DNREPORT_IN_MAINTENANCE = 6,
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

	/* v2+ types */
	H_FS_SERVER_DEFAULTS,
	_H_V2_START = H_FS_SERVER_DEFAULTS,

	/* end of valid non-exception type range */
	_H_END,
	_H_INVALID = _H_END,

	H_PROTOCOL_EXCEPTION = 0x80,
};

enum hdfs_exception_type {
	_H_EXCEPTION_START = 0x100,
	H_BASE_PROTOCOL_EXCEPTION = 0x100,
	H_ACCESS_CONTROL_EXCEPTION,
	H_ALREADY_BEING_CREATED_EXCEPTION,
	H_FILE_NOT_FOUND_EXCEPTION,
	H_IO_EXCEPTION,
	H_LEASE_EXPIRED_EXCEPTION,
	H_SECURITY_EXCEPTION,
	H_QUOTA_EXCEPTION,
	H_ILLEGAL_ARGUMENT_EXCEPTION,
	H_INVALID_TOKEN_EXCEPTION,
	H_INVALID_PATH_EXCEPTION,
	H_FILE_ALREADY_EXISTS_EXCEPTION,
	H_IPC_EXCEPTION,
	H_SASL_EXCEPTION,
	H_RPC_EXCEPTION,
	H_RPC_NO_SUCH_METHOD_EXCEPTION,

	_H_EXCEPTION_END,
};

const char *	hdfs_etype_to_string(enum hdfs_exception_type e);

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

	struct hdfs_object *_token;

	/* v2+ */
	char *_pool_id;
	bool _corrupt;

	char **_storage_ids;
	int _num_storage_ids;

	enum hdfs_storage_type *_storage_types;
	int _num_storage_types;
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
	char *_ipaddr,
	     *_hostname,
	     *_port,
	     *_uuid;
	uint16_t _namenodeport,
		 _infoport;
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

	/* v2 */
	char *_pool_id;
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

struct hdfs_dnreporttype {
	enum hdfs_datanode_report_type _type;
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
	enum hdfs_exception_type _etype;
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
		struct hdfs_dnreporttype _dnreporttype;
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
struct hdfs_object *	hdfs_datanode_info_new(const char *ipaddr, const char *host, const char *port,
			const char *rack, const char *uuid, uint16_t namenodeport, uint16_t infoport);
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
struct hdfs_object *	hdfs_protocol_exception_new(enum hdfs_exception_type, const char *);
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
struct hdfs_object *	hdfs_dnreporttype_new(enum hdfs_datanode_report_type type);
struct hdfs_object *	hdfs_array_locatedblock_new(void);
struct hdfs_object *	hdfs_array_locatedblock_copy(struct hdfs_object *);
struct hdfs_object *	hdfs_upgradeaction_new(const char *);
struct hdfs_object *	hdfs_upgrade_status_report_new(int32_t, int16_t);

// Caller loses references to objects that are being appended into arrays.
void	hdfs_located_block_append_datanode_info(
	struct hdfs_object *located_block, struct hdfs_object *datanode_info);
void	hdfs_located_block_append_storage_id(
	struct hdfs_object *located_block, char *storage_id);
void	hdfs_located_block_append_storage_type(
	struct hdfs_object *located_block, enum hdfs_storage_type type);
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
	int pos,
	    used,
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
