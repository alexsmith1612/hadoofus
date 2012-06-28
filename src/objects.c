#ifdef NDEBUG
# undef NDEBUG
#endif

#include <assert.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include "heapbuf.h"
#include "heapbufobjs.h"
#include "objects.h"

static struct _hdfs_result _HDFS_INVALID_PROTO_OBJ;
struct _hdfs_result *_HDFS_INVALID_PROTO = &_HDFS_INVALID_PROTO_OBJ;

static struct {
	const char *type;
	bool objtype;
	struct hdfs_object *(*slurper)(struct hdfs_heap_buf *);
} object_types[] = {
	[H_VOID - _H_START] = { .type = VOID_TYPE, .objtype = false,
		.slurper = _oslurp_null, },
	[H_NULL - _H_START] = { .type = NULL_TYPE1, .objtype = false,
		.slurper = _oslurp_null, },
	[H_BOOLEAN - _H_START] = { .type = BOOLEAN_TYPE, .objtype = false,
		.slurper = _oslurp_boolean, },
	[H_INT - _H_START] = { .type = INT_TYPE, .objtype = false,
		.slurper = _oslurp_int, },
	[H_LONG - _H_START] = { .type = LONG_TYPE, .objtype = false,
		.slurper = _oslurp_long, },
	[H_ARRAY_LONG - _H_START] = { .type = ARRAYLONG_TYPE, .objtype = false,
		.slurper = _oslurp_array_long, },
	[H_LOCATED_BLOCK - _H_START] = { .type = LOCATEDBLOCK_TYPE, .objtype = true,
		.slurper = _oslurp_located_block, },
	[H_LOCATED_BLOCKS - _H_START] = { .type = LOCATEDBLOCKS_TYPE, .objtype = true,
		.slurper = _oslurp_located_blocks, },
	[H_DIRECTORY_LISTING - _H_START] = { .type = DIRECTORYLISTING_TYPE, .objtype = true,
		.slurper = _oslurp_directory_listing, },
	[H_DATANODE_INFO - _H_START] = { .type = DATANODEINFO_TYPE, .objtype = true,
		.slurper = _oslurp_datanode_info, },
	[H_ARRAY_DATANODE_INFO - _H_START] = { .type = ARRAYDATANODEINFO_TYPE, .objtype = false,
		.slurper = _oslurp_array_datanode_info, },
	[H_FILE_STATUS - _H_START] = { .type = FILESTATUS_TYPE, .objtype = true,
		.slurper = _oslurp_file_status, },
	[H_CONTENT_SUMMARY - _H_START] = { .type = CONTENTSUMMARY_TYPE, .objtype = true,
		.slurper = _oslurp_content_summary, },
	[H_LOCATED_DIRECTORY_LISTING - _H_START] = { .type = LOCATEDDIRECTORYLISTING_TYPE,
		.objtype = true, .slurper = /*_oslurp_located_directory_listing*/NULL, },
	[H_UPGRADE_STATUS_REPORT - _H_START] = { .type = UPGRADESTATUSREPORT_TYPE, .objtype = true,
		.slurper = /*_oslurp_upgrade_status_report*/NULL, },
	[H_BLOCK - _H_START] = { .type = BLOCK_TYPE, .objtype = true,
		.slurper = _oslurp_block, },
	[H_ARRAY_BYTE - _H_START] = { .type = ARRAYBYTE_TYPE, .objtype = false,
		.slurper = /*_oslurp_array_byte*/NULL, },
	[H_RPC_INVOCATION - _H_START] = { .type = NULL, .objtype = false },
	[H_AUTHHEADER - _H_START] = { .type = NULL, .objtype = false },
	[H_STRING - _H_START] = { .type = STRING_TYPE, .objtype = false },
	[H_FSPERMS - _H_START] = { .type = FSPERMS_TYPE, .objtype = true,
		.slurper = _oslurp_fsperms, },
	[H_SHORT - _H_START] = { .type = SHORT_TYPE, .objtype = false,
		.slurper = _oslurp_short, },
};

static struct {
	const char *type;
} exception_types[] = {
	[0] = { .type = NULL, },
	[H_ACCESS_CONTROL_EXCEPTION - H_PROTOCOL_EXCEPTION] = {
		.type = ACCESS_EXCEPTION_STR, },
	[H_ALREADY_BEING_CREATED_EXCEPTION - H_PROTOCOL_EXCEPTION] = {
		.type = ALREADY_BEING_EXCEPTION_STR, },
	[H_FILE_NOT_FOUND_EXCEPTION - H_PROTOCOL_EXCEPTION] = {
		.type = NOT_FOUND_EXCEPTION_STR, },
	[H_IO_EXCEPTION - H_PROTOCOL_EXCEPTION] = {
		.type = IO_EXCEPTION_STR, },
	[H_LEASE_EXPIRED_EXCEPTION - H_PROTOCOL_EXCEPTION] = {
		.type = LEASE_EXCEPTION_STR, },
};

enum hdfs_object_type
_string_to_type(const char *otype)
{
	for (int i = 0; i < nelem(object_types); i++)
		if (streq(otype, object_types[i].type))
			return _H_START + i;

	return _H_INVALID;
}

static enum hdfs_object_type
_string_to_etype(const char *etype)
{
	for (int i = 1/*skip proto exception; never matches*/;
	    i < nelem(exception_types); i++)
		if (streq(etype, exception_types[i].type))
			return H_PROTOCOL_EXCEPTION + i;

	return H_PROTOCOL_EXCEPTION;
}

const char *
hdfs_etype_to_string(enum hdfs_object_type e)
{
	const char *res = exception_types[e - H_PROTOCOL_EXCEPTION].type;
	if (!res)
		return "ProtocolException";
	return res;
}

static struct hdfs_object *
_object_exception(const char *etype, const char *emsg)
{
	enum hdfs_object_type realtype;
	realtype = _string_to_etype(etype);
	return hdfs_protocol_exception_new(realtype, emsg);
}

static inline int64_t
_timespec_to_ms(struct timespec ts)
{
	return ((int64_t)ts.tv_sec * 1000UL) +
	    (((int64_t)ts.tv_nsec / 1000000ULL) % 1000UL);
}

static inline void *
_objmalloc(void)
{
	void *r = malloc(sizeof(struct hdfs_object));
	assert(r);
	memset(r, 0, sizeof(struct hdfs_object));
	return r;
}

struct hdfs_object *
hdfs_void_new()
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_VOID;
	return r;
}

struct hdfs_object *
hdfs_null_new(enum hdfs_object_type type)
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_NULL;
	r->ob_val._null._type = type;
	return r;
}

struct hdfs_object *
hdfs_boolean_new(bool val)
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_BOOLEAN;
	r->ob_val._boolean._val = val;
	return r;
}

struct hdfs_object *
hdfs_short_new(int16_t val)
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_SHORT;
	r->ob_val._short._val = val;
	return r;
}

struct hdfs_object *
hdfs_int_new(int32_t val)
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_INT;
	r->ob_val._int._val = val;
	return r;
}

struct hdfs_object *
hdfs_long_new(int64_t val)
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_LONG;
	r->ob_val._long._val = val;
	return r;
}

struct hdfs_object *
hdfs_token_new(const char *s1, const char *s2, const char *s3, const char *s4)
{
	struct hdfs_object *r = _objmalloc();
	char *copy_strs[4];

	copy_strs[0] = strdup(s1);
	copy_strs[1] = strdup(s2);
	copy_strs[2] = strdup(s3);
	copy_strs[3] = strdup(s4);

	r->ob_type = H_TOKEN;
	for (int i = 0; i < nelem(copy_strs); i++) {
		assert(copy_strs[i]);
		r->ob_val._token._strings[i] = copy_strs[i];
	}

	return r;
}

struct hdfs_object *
hdfs_token_new_empty()
{
	struct hdfs_object *r = _objmalloc();

	r->ob_type = H_TOKEN;
	for (int i = 0; i < 4; i++) {
		char *s = strdup("");
		assert(s);
		r->ob_val._token._strings[i] = s;
	}

	return r;
}

struct hdfs_object *
hdfs_token_copy(struct hdfs_object *src)
{
	struct hdfs_object *r = _objmalloc();

	assert(src);
	assert(src->ob_type == H_TOKEN);

	r->ob_type = H_TOKEN;
	for (int i = 0; i < 4; i++) {
		char *s = strdup(src->ob_val._token._strings[i]);
		assert(s);
		r->ob_val._token._strings[i] = s;
	}

	return r;
}

struct hdfs_object *
hdfs_array_long_new(int len, const int64_t *values)
{
	struct hdfs_object *r = _objmalloc();
	int64_t *values_copied = malloc(len * sizeof(int64_t));
	assert(values_copied);
	memcpy(values_copied, values, len * sizeof(int64_t));

	r->ob_type = H_ARRAY_LONG;
	r->ob_val._array_long = (struct hdfs_array_long) {
		._len = len,
		._vals = values_copied,
	};
	return r;
}

struct hdfs_object *
hdfs_located_block_new(int64_t blkid, int64_t len, int64_t generation, int64_t offset)
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_LOCATED_BLOCK;
	r->ob_val._located_block = (struct hdfs_located_block) {
		._blockid = blkid,
		._len = len,
		._generation = generation,
		._offset = offset,
	};
	return r;
}

struct hdfs_object *
hdfs_located_block_copy(struct hdfs_object *src)
{
	int nlocs;
	struct hdfs_object *r = _objmalloc(),
			   **arr_locs = NULL;

	assert(src);
	assert(src->ob_type == H_LOCATED_BLOCK);

	nlocs = src->ob_val._located_block._num_locs;

	if (nlocs > 0) {
		arr_locs = malloc(nlocs * sizeof *arr_locs);
		assert(arr_locs);


		for (int i = 0; i < nlocs; i++)
			arr_locs[i] = hdfs_datanode_info_copy(
			    src->ob_val._located_block._locs[i]);
	}

	r->ob_type = H_LOCATED_BLOCK;
	r->ob_val._located_block = (struct hdfs_located_block) {
		._blockid = src->ob_val._located_block._blockid,
		._len = src->ob_val._located_block._len,
		._generation = src->ob_val._located_block._generation,
		._num_locs = nlocs,
		._locs = arr_locs,
		._offset = src->ob_val._located_block._offset,
	};
	return r;
}

struct hdfs_object *
hdfs_located_blocks_new(bool beingcreated, int64_t size)
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_LOCATED_BLOCKS;
	r->ob_val._located_blocks = (struct hdfs_located_blocks) {
		._being_written = beingcreated,
		._size = size,
	};
	return r;
}

static struct hdfs_object *
_hdfs_directory_listing_new(bool has_locations)
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_DIRECTORY_LISTING;
	r->ob_val._directory_listing._has_locations = has_locations;
	return r;
}

struct hdfs_object *
hdfs_located_directory_listing_new()
{
	return _hdfs_directory_listing_new(true/*locations*/);
}

struct hdfs_object *
hdfs_directory_listing_new()
{
	return _hdfs_directory_listing_new(false/*locations*/);
}

struct hdfs_object *
hdfs_datanode_info_new(const char *host, const char *port, const char *rack,
	uint16_t namenodeport) // "/default-rack"
{
	char *rack_copy = strdup(rack),
	     *host_copy = strdup(host),
	     *port_copy = strdup(port);
	struct hdfs_object *r = _objmalloc();

	assert(rack_copy);
	assert(host_copy);
	assert(port_copy);

	r->ob_type = H_DATANODE_INFO;
	r->ob_val._datanode_info = (struct hdfs_datanode_info) {
		._location = rack_copy,
		._hostname = host_copy,
		._port = port_copy,
		._namenodeport = namenodeport,
	};
	return r;
}

struct hdfs_object *
hdfs_datanode_info_copy(struct hdfs_object *src)
{
	struct hdfs_object *r = _objmalloc();
	char *rack_copy,
	     *host_copy,
	     *port_copy;
	uint16_t namenodeport;

	assert(src);
	assert(src->ob_type == H_DATANODE_INFO);

	rack_copy = strdup(src->ob_val._datanode_info._location);
	host_copy = strdup(src->ob_val._datanode_info._hostname);
	port_copy = strdup(src->ob_val._datanode_info._port);
	namenodeport = src->ob_val._datanode_info._namenodeport;

	assert(rack_copy);
	assert(host_copy);
	assert(port_copy);

	r->ob_type = H_DATANODE_INFO;
	r->ob_val._datanode_info = (struct hdfs_datanode_info) {
		._location = rack_copy,
		._hostname = host_copy,
		._port = port_copy,
		._namenodeport = namenodeport,
	};
	return r;
}

struct hdfs_object *
hdfs_array_datanode_info_new()
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_ARRAY_DATANODE_INFO;
	return r;
}

struct hdfs_object *
hdfs_array_datanode_info_copy(struct hdfs_object *src)
{
	struct hdfs_object *r;
	int n;

	if (!src)
		return hdfs_null_new(H_ARRAY_DATANODE_INFO);
	if (src->ob_type == H_NULL) {
		assert(src->ob_val._null._type == H_ARRAY_DATANODE_INFO);
		return hdfs_null_new(H_ARRAY_DATANODE_INFO);
	}

	r = _objmalloc();

	assert(src->ob_type == H_ARRAY_DATANODE_INFO);
	n = src->ob_val._array_datanode_info._len;

	r->ob_type = H_ARRAY_DATANODE_INFO;
	r->ob_val._array_datanode_info._len = n;
	if (n > 0) {
		r->ob_val._array_datanode_info._values =
		    malloc(n * sizeof(struct hdfs_object *));
		assert(r->ob_val._array_datanode_info._values);

		for (int i = 0; i < n; i++)
			r->ob_val._array_datanode_info._values[i] =
			    hdfs_datanode_info_copy(src->ob_val._array_datanode_info._values[i]);
	}

	return r;
}

struct hdfs_object *
hdfs_file_status_new(const char *logical_name, const struct stat *sb,
    const char *owner, const char *group)
{
	struct hdfs_object *r = _objmalloc();
	char *name_copy = strdup(logical_name),
	     *owner_copy = strdup(owner),
	     *group_copy = strdup(group);
	int mode = (S_ISDIR(sb->st_mode))? (sb->st_mode & 0777) :
	    (sb->st_mode & 0666);

#ifndef __ISILON__
# define st_mtimespec st_mtim
# define st_atimespec st_atim
#endif

	assert(name_copy);
	assert(owner_copy);
	assert(group_copy);

	r->ob_type = H_FILE_STATUS;
	r->ob_val._file_status = (struct hdfs_file_status) {
		._file = name_copy,
		._size = sb->st_size,
		._directory = S_ISDIR(sb->st_mode),
		._replication = !S_ISDIR(sb->st_mode), // 1 for files, 0 for dir
		._block_size = 64*1024*1024,
		._mtime = _timespec_to_ms(sb->st_mtimespec),
		._atime = _timespec_to_ms(sb->st_atimespec),
		._permissions = mode,
		._owner = owner_copy,
		._group = group_copy,
	};
	return r;

#ifndef __ISILON__
# undef st_mtimespec
# undef st_atimespec
#endif
}

struct hdfs_object *
hdfs_file_status_new_ex(const char *logical_name, int64_t size, bool directory,
	int replication, int64_t block_size, int64_t mtime_ms, int64_t atime_ms,
	int perms, const char *owner, const char *group)
{
	struct hdfs_object *r = _objmalloc();
	char *name_copy = strdup(logical_name),
	     *owner_copy = strdup(owner),
	     *group_copy = strdup(group);

	assert(name_copy);
	assert(owner_copy);
	assert(group_copy);

	r->ob_type = H_FILE_STATUS;
	r->ob_val._file_status = (struct hdfs_file_status) {
		._file = name_copy,
		._size = size,
		._directory = directory,
		._replication = replication,
		._block_size = block_size,
		._mtime = mtime_ms,
		._atime = atime_ms,
		._permissions = perms,
		._owner = owner_copy,
		._group = group_copy,
	};
	return r;
}

struct hdfs_object *
hdfs_content_summary_new(int64_t length, int64_t files, int64_t dirs, int64_t quota)
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_CONTENT_SUMMARY;
	r->ob_val._content_summary = (struct hdfs_content_summary) {
		._length = length,
		._files = files,
		._dirs = dirs,
		._quota = quota,
	};
	return r;
}

struct hdfs_object *
hdfs_block_new(int64_t blkid, int64_t len, int64_t generation)
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_BLOCK;
	r->ob_val._block = (struct hdfs_block) {
		._blkid = blkid,
		._length = len,
		._generation = generation,
	};
	return r;
}

struct hdfs_object *
hdfs_block_copy(struct hdfs_object *src)
{
	struct hdfs_object *r;

	if (!src)
		return hdfs_null_new(H_BLOCK);

	r = _objmalloc();

	assert(src->ob_type == H_BLOCK);

	r->ob_type = H_BLOCK;
	r->ob_val._block = src->ob_val._block;
	return r;
}

struct hdfs_object *
hdfs_block_from_located_block(struct hdfs_object *src)
{
	struct hdfs_object *r = _objmalloc();

	assert(src);
	assert(src->ob_type == H_LOCATED_BLOCK);

	r->ob_type = H_BLOCK;
	r->ob_val._block = (struct hdfs_block) {
		._blkid = src->ob_val._located_block._blockid,
		._length = src->ob_val._located_block._len,
		._generation = src->ob_val._located_block._generation,
	};
	return r;
}

struct hdfs_object *
hdfs_array_byte_new(int len, int8_t *bytes)
{
	int8_t *bytes_copy = NULL;
	struct hdfs_object *r = _objmalloc();

	if (len) {
		bytes_copy = malloc(len);
		assert(bytes_copy);
		memcpy(bytes_copy, bytes, len);
	}

	r->ob_type = H_ARRAY_BYTE;
	r->ob_val._array_byte = (struct hdfs_array_byte) {
		._len = len,
		._bytes = bytes_copy,
	};
	return r;
}

struct hdfs_object *
hdfs_array_byte_copy(struct hdfs_object *src)
{
	struct hdfs_object *r;
	int32_t len;

	if (!src)
		return hdfs_null_new(H_ARRAY_BYTE);

	assert(src->ob_type == H_ARRAY_BYTE);

	r = _objmalloc();
	len = src->ob_val._array_byte._len;

	r->ob_type = H_ARRAY_BYTE;
	r->ob_val._array_byte._len = len;
	if (len) {
		int8_t *bytes_copy = malloc(len);
		assert(bytes_copy);
		memcpy(bytes_copy, src->ob_val._array_byte._bytes, len);
		r->ob_val._array_byte._bytes = bytes_copy;
	}
	return r;
}

struct hdfs_object *
hdfs_rpc_invocation_new(const char *name, ...)
{
	int i;
	char *meth_copy = strdup(name);
	struct hdfs_object *r = _objmalloc();
	va_list ap;
	struct hdfs_object *arg;

	assert(meth_copy);

	r->ob_type = H_RPC_INVOCATION;
	r->ob_val._rpc_invocation = (struct hdfs_rpc_invocation) {
		._method = meth_copy,
	};

	va_start(ap, name);
	i = 0;
	while (true) {
		arg = va_arg(ap, struct hdfs_object *);
		if (!arg)
			break;
		r->ob_val._rpc_invocation._args[i] = arg;
		i++;

		assert(i < nelem(r->ob_val._rpc_invocation._args));
	}
	r->ob_val._rpc_invocation._nargs = i;
	va_end(ap);

	return r;
}

void
_rpc_invocation_set_msgno(struct hdfs_object *rpc, int32_t msgno)
{
	assert(rpc);
	assert(rpc->ob_type == H_RPC_INVOCATION);

	rpc->ob_val._rpc_invocation._msgno = msgno;
}

struct hdfs_object *
hdfs_authheader_new(const char *user)
{
	char *user_copy = strdup(user);
	struct hdfs_object *r = _objmalloc();

	assert(user_copy);

	r->ob_type = H_AUTHHEADER;
	r->ob_val._authheader = (struct hdfs_authheader) {
		._username = user_copy,
	};
	return r;
}

struct hdfs_object *
hdfs_string_new(const char *s)
{
	char *str_copy;
	struct hdfs_object *r;

	if (!s) {
		return hdfs_null_new(H_STRING);
	}

	str_copy = strdup(s);
	r = _objmalloc();

	assert(str_copy);

	r->ob_type = H_STRING;
	r->ob_val._string = (struct hdfs_string) {
		._val = str_copy,
	};
	return r;
}

struct hdfs_object *
hdfs_fsperms_new(int16_t perms)
{
	struct hdfs_object *r = _objmalloc();
	r->ob_type = H_FSPERMS;
	r->ob_val._fsperms._perms = perms;
	return r;
}

struct hdfs_object *
hdfs_protocol_exception_new(enum hdfs_object_type etype, const char *msg)
{
	char *msg_copy = strdup(msg);
	struct hdfs_object *r = _objmalloc();
	assert(msg_copy);
	r->ob_type = H_PROTOCOL_EXCEPTION;
	r->ob_val._exception = (struct hdfs_exception) {
		._etype = etype,
		._msg = msg_copy,
	};
	return r;
}

// Caller loses references to objects that are being appended into other
// objects.
#define H_ARRAY_RESIZE 8
#define H_ARRAY_APPEND(array, array_len, obj) do { \
	if (array_len % H_ARRAY_RESIZE == 0) { \
		array = realloc(array, (array_len+H_ARRAY_RESIZE) * sizeof(struct hdfs_object *)); \
		assert(array); \
	} \
	array[array_len] = obj; \
	array_len += 1; \
} while (0)

void
hdfs_located_block_append_datanode_info(struct hdfs_object *located_block,
	struct hdfs_object *datanode_info)
{
	assert(located_block->ob_type == H_LOCATED_BLOCK);
	assert(datanode_info->ob_type == H_DATANODE_INFO);

	H_ARRAY_APPEND(located_block->ob_val._located_block._locs,
	    located_block->ob_val._located_block._num_locs,
	    datanode_info);
}

void
hdfs_located_blocks_append_located_block(struct hdfs_object *located_blocks,
	struct hdfs_object *located_block)
{
	assert(located_blocks->ob_type == H_LOCATED_BLOCKS);
	assert(located_block->ob_type == H_LOCATED_BLOCK);

	H_ARRAY_APPEND(located_blocks->ob_val._located_blocks._blocks,
	    located_blocks->ob_val._located_blocks._num_blocks, located_block);
}

void
hdfs_directory_listing_append_file_status(struct hdfs_object *directory_listing,
	struct hdfs_object *file_status, struct hdfs_object *located_blocks)
{
	assert(directory_listing);
	assert(directory_listing->ob_type == H_DIRECTORY_LISTING);
	assert(file_status);
	assert(file_status->ob_type == H_FILE_STATUS);

	H_ARRAY_APPEND(directory_listing->ob_val._directory_listing._files,
	    directory_listing->ob_val._directory_listing._num_files, file_status);

	if (directory_listing->ob_val._directory_listing._has_locations) {
		assert(located_blocks);
		assert(located_blocks->ob_type == H_LOCATED_BLOCKS);

		directory_listing->ob_val._directory_listing._num_files -= 1;
		H_ARRAY_APPEND(directory_listing->ob_val._directory_listing._located_blocks,
		    directory_listing->ob_val._directory_listing._num_files, located_blocks);
	} else {
		assert(located_blocks == NULL);
	}
}

void
hdfs_array_datanode_info_append_datanode_info(struct hdfs_object *array, struct hdfs_object *datanode_info)
{
	assert(array->ob_type == H_ARRAY_DATANODE_INFO);
	assert(datanode_info->ob_type == H_DATANODE_INFO);

	H_ARRAY_APPEND(array->ob_val._array_datanode_info._values,
	    array->ob_val._array_datanode_info._len,
	    datanode_info);
}

#define FREE_H_ARRAY_ELMS(array, array_len) do { \
	for (int i = 0; i < array_len; i++) { \
		hdfs_object_free(array[i]); \
	} \
} while (0)

#define FREE_H_ARRAY(array, array_len) do { \
	FREE_H_ARRAY_ELMS(array, array_len); \
	free(array); \
} while (0)

// Recursively frees an object:
void
hdfs_object_free(struct hdfs_object *obj)
{
	switch (obj->ob_type) {
	case H_VOID: break; // NOOP
	case H_NULL: break;
	case H_BOOLEAN: break;
	case H_SHORT: break;
	case H_FSPERMS: break;
	case H_INT: break;
	case H_LONG: break;
	case H_ARRAY_LONG:
		free(obj->ob_val._array_long._vals);
		break;
	case H_LOCATED_BLOCK:
		FREE_H_ARRAY(obj->ob_val._located_block._locs,
		    obj->ob_val._located_block._num_locs);
		break;
	case H_LOCATED_BLOCKS:
		FREE_H_ARRAY(obj->ob_val._located_blocks._blocks,
		    obj->ob_val._located_blocks._num_blocks);
		break;
	case H_LOCATED_DIRECTORY_LISTING: // FALLTHROUGH
	case H_DIRECTORY_LISTING:
		FREE_H_ARRAY(obj->ob_val._directory_listing._files,
		    obj->ob_val._directory_listing._num_files);
		if (obj->ob_val._directory_listing._has_locations) {
			FREE_H_ARRAY(obj->ob_val._directory_listing._located_blocks,
			    obj->ob_val._directory_listing._num_files);
		}
		break;
	case H_DATANODE_INFO:
		free(obj->ob_val._datanode_info._location);
		break;
	case H_ARRAY_DATANODE_INFO:
		FREE_H_ARRAY(obj->ob_val._array_datanode_info._values,
		    obj->ob_val._array_datanode_info._len);
		break;
	case H_FILE_STATUS:
		free(obj->ob_val._file_status._file);
		free(obj->ob_val._file_status._owner);
		free(obj->ob_val._file_status._group);
		break;
	case H_CONTENT_SUMMARY: break;
	case H_BLOCK: break;
	case H_ARRAY_BYTE:
		if (obj->ob_val._array_byte._bytes)
			free(obj->ob_val._array_byte._bytes);
		break;
	case H_RPC_INVOCATION:
		free(obj->ob_val._rpc_invocation._method);
		FREE_H_ARRAY_ELMS(obj->ob_val._rpc_invocation._args,
		    obj->ob_val._rpc_invocation._nargs);
		break;
	case H_AUTHHEADER:
		free(obj->ob_val._authheader._username);
		break;
	case H_TOKEN:
		for (int i = 0; i < nelem(obj->ob_val._token._strings); i++)
			free(obj->ob_val._token._strings[i]);
		break;
	case H_PROTOCOL_EXCEPTION:
		free(obj->ob_val._exception._msg);
		break;
	case H_STRING:
		free(obj->ob_val._string._val);
		break;
	case H_UPGRADE_STATUS_REPORT: // FALLTHROUGH
	default:
		assert(false);
	}
	free(obj);
}

static const char *
_rawtypestring(enum hdfs_object_type t)
{
	return object_types[t - _H_START].type;
}

static const char *
_typestring(struct hdfs_object *obj)
{
	return object_types[obj->ob_type - _H_START].type;
}

static bool
_is_object_objtype(struct hdfs_object *obj)
{
	return object_types[obj->ob_type - _H_START].objtype;
}

static bool
_is_type_objtype(enum hdfs_object_type t)
{
	return object_types[t - _H_START].objtype;
}

// Serializes an hdfs_object into a buffer. dest must be non-NULL and
// initialized to zero.
void
hdfs_object_serialize(struct hdfs_heap_buf *dest, struct hdfs_object *obj)
{
	switch (obj->ob_type) {
	case H_VOID:
		_bappend_string(dest, "void");
		break;
	case H_NULL:
		_bappend_string(dest, _rawtypestring(obj->ob_val._null._type));
		break;
	case H_BOOLEAN:
		_bappend_s8(dest, obj->ob_val._boolean._val);
		break;
	case H_SHORT:
		_bappend_s16(dest, obj->ob_val._short._val);
		break;
	case H_INT:
		_bappend_s32(dest, obj->ob_val._int._val);
		break;
	case H_LONG:
		_bappend_s64(dest, obj->ob_val._long._val);
		break;

	case H_ARRAY_LONG:
		{
		int len = obj->ob_val._array_long._len;
		_bappend_s32(dest, len);
		for (int i = 0; i < len; i++) {
			_bappend_string(dest, _rawtypestring(H_LONG));
			_bappend_s64(dest, obj->ob_val._array_long._vals[i]);
		}
		}
		break;
	case H_ARRAY_DATANODE_INFO:
		{
		int len = obj->ob_val._array_datanode_info._len;
		_bappend_s32(dest, len);
		for (int i = 0; i < len; i++) {
			_bappend_string(dest, _rawtypestring(H_DATANODE_INFO));
			_bappend_string(dest, _rawtypestring(H_DATANODE_INFO));
			hdfs_object_serialize(dest,
			    obj->ob_val._array_datanode_info._values[i]);
		}
		}
		break;
	case H_ARRAY_BYTE:
		{
		int len = obj->ob_val._array_byte._len;
		_bappend_s32(dest, len);
		_bappend_mem(dest, len, obj->ob_val._array_byte._bytes);
		}
		break;

	case H_LOCATED_BLOCK:
		{
		int len;
		// Token:
		_bappend_text(dest, "");
		_bappend_text(dest, "");
		_bappend_text(dest, "");
		_bappend_text(dest, "");

		_bappend_s8(dest, 0);
		_bappend_s64(dest, obj->ob_val._located_block._offset);
		_bappend_s64(dest, obj->ob_val._located_block._blockid);
		_bappend_s64(dest, obj->ob_val._located_block._len);
		_bappend_s64(dest, obj->ob_val._located_block._generation);
		len = obj->ob_val._located_block._num_locs;
		_bappend_s32(dest, len);
		for (int i = 0; i < len; i++)
			hdfs_object_serialize(dest,
			    obj->ob_val._located_block._locs[i]);
		}
		break;
	case H_LOCATED_BLOCKS:
		{
		int len;
		_bappend_s64(dest, obj->ob_val._located_blocks._size);
		_bappend_s8(dest, obj->ob_val._located_blocks._being_written);
		len = obj->ob_val._located_blocks._num_blocks;
		_bappend_s32(dest, len);
		for (int i = 0; i < len; i++)
			hdfs_object_serialize(dest,
			    obj->ob_val._located_blocks._blocks[i]);
		}
		break;
	case H_LOCATED_DIRECTORY_LISTING: // FALLTHROUGH
	case H_DIRECTORY_LISTING:
		{
		int len = obj->ob_val._directory_listing._num_files;
		_bappend_s32(dest, len);
		for (int i = 0; i < len; i++)
			hdfs_object_serialize(dest,
			    obj->ob_val._directory_listing._files[i]);
		if (obj->ob_val._directory_listing._has_locations)
			for (int i = 0; i < len; i++)
				hdfs_object_serialize(dest,
				    obj->ob_val._directory_listing._located_blocks[i]);
		_bappend_s32(dest, 0/*remaining*/);
		}
		break;
	case H_DATANODE_INFO:
		{
		char *hostport = malloc(strlen(obj->ob_val._datanode_info._hostname) +
		    1 + strlen(obj->ob_val._datanode_info._port) + 1);
		assert(hostport);
		strcpy(hostport, obj->ob_val._datanode_info._hostname);
		strcat(hostport, ":");
		strcat(hostport, obj->ob_val._datanode_info._port);

		_bappend_string(dest, hostport);
		_bappend_string(dest, hostport);
		_bappend_u16(dest, 31337/*arbitrary*/);
		_bappend_u16(dest, obj->ob_val._datanode_info._namenodeport/*ipc port*/);
		_bappend_s64(dest, 0/*capacity*/);
		_bappend_s64(dest, 0/*space used*/);
		_bappend_s64(dest, 0/*remaining*/);
		_bappend_s64(dest, 0/*last update*/);
		_bappend_s32(dest, 0/*xceivercount*/);
		_bappend_text(dest, obj->ob_val._datanode_info._location);
		_bappend_text(dest, obj->ob_val._datanode_info._hostname);
		_bappend_text(dest, "NORMAL");

		free(hostport);
		}
		break;
	case H_FILE_STATUS:
		{
		int len = strlen(obj->ob_val._file_status._file);
		mode_t perms = obj->ob_val._file_status._permissions;
		_bappend_s32(dest, len);
		_bappend_mem(dest, len, obj->ob_val._file_status._file);
		_bappend_s64(dest, obj->ob_val._file_status._size);
		_bappend_s8(dest, obj->ob_val._file_status._directory);
		_bappend_u16(dest, obj->ob_val._file_status._replication);
		_bappend_s64(dest, obj->ob_val._file_status._block_size);
		_bappend_s64(dest, obj->ob_val._file_status._mtime);
		_bappend_s64(dest, obj->ob_val._file_status._atime);
		if (obj->ob_val._file_status._directory)
			perms &= 0777;
		else
			perms &= 0666;
		_bappend_u16(dest, perms);
		_bappend_text(dest, obj->ob_val._file_status._owner);
		_bappend_text(dest, obj->ob_val._file_status._group);
		}
		break;
	case H_CONTENT_SUMMARY:
		_bappend_s64(dest, obj->ob_val._content_summary._length);
		_bappend_s64(dest, obj->ob_val._content_summary._files);
		_bappend_s64(dest, obj->ob_val._content_summary._dirs);
		_bappend_s64(dest, -1);
		_bappend_s64(dest, obj->ob_val._content_summary._length);
		_bappend_s64(dest, obj->ob_val._content_summary._quota);
		break;
	case H_RPC_INVOCATION:
		{
		struct hdfs_heap_buf rbuf = { 0 };
		_bappend_s32(&rbuf, obj->ob_val._rpc_invocation._msgno);
		_bappend_string(&rbuf, obj->ob_val._rpc_invocation._method);
		_bappend_s32(&rbuf, obj->ob_val._rpc_invocation._nargs);
		for (int i = 0; i < obj->ob_val._rpc_invocation._nargs; i++) {
			struct hdfs_object *aobj = obj->ob_val._rpc_invocation._args[i];

			_bappend_string(&rbuf, _typestring(aobj));
			if (_is_object_objtype(aobj))
				_bappend_string(&rbuf, _typestring(aobj));
			if (aobj->ob_type == H_NULL || aobj->ob_type == H_VOID)
				_bappend_string(&rbuf, NULL_TYPE2);
			hdfs_object_serialize(&rbuf, aobj);
		}

		_bappend_s32(dest, rbuf.used);
		_bappend_mem(dest, rbuf.used, rbuf.buf);
		free(rbuf.buf);
		}
		break;
	case H_AUTHHEADER:
		{
		struct hdfs_heap_buf abuf = { 0 };
		_bappend_text(&abuf, CLIENT_PROTOCOL);
		_bappend_s8(&abuf, 1);
		_bappend_string(&abuf, obj->ob_val._authheader._username);
		_bappend_s8(&abuf, 0);

		_bappend_s32(dest, abuf.used);
		_bappend_mem(dest, abuf.used, abuf.buf);
		free(abuf.buf);
		}
		break;
	case H_STRING:
		_bappend_string(dest, obj->ob_val._string._val);
		break;
	case H_FSPERMS:
		_bappend_s16(dest, obj->ob_val._fsperms._perms);
		break;
	case H_BLOCK:
		_bappend_s64(dest, obj->ob_val._block._blkid);
		_bappend_s64(dest, obj->ob_val._block._length);
		_bappend_s64(dest, obj->ob_val._block._generation);
		break;
	case H_TOKEN:
		for (int i = 0; i < 4; i++)
			_bappend_text(dest, obj->ob_val._token._strings[i]);
		break;
	case H_UPGRADE_STATUS_REPORT: // FALLTHROUGH
	default:
		assert(false);
	}
}

void
_hdfs_result_free(struct _hdfs_result *r)
{
	if (r->rs_obj)
		hdfs_object_free(r->rs_obj);
	free(r);
}

struct _hdfs_result *
_hdfs_result_deserialize(char *buf, int buflen, int *obj_size)
{
	struct _hdfs_result *r = NULL;
	struct hdfs_object *o = NULL;
	struct hdfs_heap_buf rbuf = {
		.buf = buf,
		.used = 0,
		.size = buflen,
	};

	int32_t msgno, status;
	char *etype = NULL, *emsg = NULL;
	char *otype = NULL, *ttype = NULL;
	enum hdfs_object_type realtype;

	msgno = _bslurp_s32(&rbuf);
	if (rbuf.used < 0)
		goto out;
	status = _bslurp_s32(&rbuf);
	if (rbuf.used < 0)
		goto out;

	// Parse exceptions
	if (status != 0) {
		etype = _bslurp_string32(&rbuf);
		if (rbuf.used < 0)
			goto out;
		emsg = _bslurp_string32(&rbuf);
		if (rbuf.used < 0)
			goto out;

		r = malloc(sizeof *r);
		assert(r);
		r->rs_msgno = msgno;
		r->rs_obj = _object_exception(etype, emsg);
		goto out;
	}

	// If we got this far we're reading a normal object
	otype = _bslurp_string(&rbuf);
	if (rbuf.used < 0)
		goto out;

	realtype = _string_to_type(otype);
	if (realtype == _H_INVALID) {
		rbuf.used = -2;
		goto out;
	}

	// "object" types have their type twice (I think the idea is that the
	// first type is that of the API and the second could be a child class
	// or implementing class of an interface, but in practice it's always
	// the same)
	if (_is_type_objtype(realtype)) {
		ttype = _bslurp_string(&rbuf);
		if (rbuf.used < 0)
			goto out;
		assert(streq(ttype, otype));
	} else if (realtype == H_VOID) {
		realtype = H_NULL;
	}

	if (realtype == H_NULL) {
		// ttype for null values is NOT the same as its otype; it's another string.
		ttype = _bslurp_string(&rbuf);
		if (rbuf.used < 0)
			goto out;
		assert(streq(ttype, NULL_TYPE2));
	}

	o = hdfs_object_slurp(&rbuf, realtype);
	if (rbuf.used < 0)
		goto out;

	r = malloc(sizeof *r);
	assert(r);
	r->rs_msgno = msgno;
	r->rs_obj = o;

out:
	if (otype)
		free(otype);
	if (ttype)
		free(ttype);
	if (etype)
		free(etype);
	if (emsg)
		free(emsg);

	if (r) {
		*obj_size = rbuf.used;
	} else {
		if (o)
			hdfs_object_free(o);
		if (rbuf.used == -2)
			r = _HDFS_INVALID_PROTO;
	}
	return r;
}

struct hdfs_object *
hdfs_object_slurp(struct hdfs_heap_buf *rbuf, enum hdfs_object_type realtype)
{
	struct hdfs_object *(*slurper)(struct hdfs_heap_buf *);

	slurper = object_types[realtype - _H_START].slurper;
	assert(slurper);

	return slurper(rbuf);
}
