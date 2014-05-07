#include <stdlib.h>

#include "heapbuf.h"
#include "heapbufobjs.h"
#include "objects-internal.h"
#include "util.h"

struct hdfs_object *
_oslurp_null(struct hdfs_heap_buf *b)
{
	char *t;
	enum hdfs_object_type rt;
	struct hdfs_object *res = NULL;

	t = _bslurp_string(b);
	if (b->used < 0)
		return res;

	if (streq(t, "void")) {
		res = hdfs_void_new();
	} else {
		rt = _string_to_type(t);
		if (rt == _H_INVALID)
			b->used = -2;
		else
			res = hdfs_null_new(rt);
	}
	free(t);
	return res;
}

struct hdfs_object *
_oslurp_boolean(struct hdfs_heap_buf *b)
{
	bool v;

	v = _bslurp_s8(b);
	if (b->used < 0)
		return NULL;

	return hdfs_boolean_new(v);
}

struct hdfs_object *
_oslurp_short(struct hdfs_heap_buf *b)
{
	int16_t v;

	v = _bslurp_s16(b);
	if (b->used < 0)
		return NULL;

	return hdfs_short_new(v);
}

struct hdfs_object *
_oslurp_int(struct hdfs_heap_buf *b)
{
	int32_t v;

	v = _bslurp_s32(b);
	if (b->used < 0)
		return NULL;

	return hdfs_int_new(v);
}

struct hdfs_object *
_oslurp_long(struct hdfs_heap_buf *b)
{
	int64_t v;

	v = _bslurp_s64(b);
	if (b->used < 0)
		return NULL;

	return hdfs_long_new(v);
}

struct hdfs_object *
_oslurp_array_long(struct hdfs_heap_buf *b)
{
	struct hdfs_object *res = NULL;
	int64_t *longs = NULL;
	int32_t n;

	n = _bslurp_s32(b);
	if (b->used < 0)
		return NULL;
	if (n < 0) {
		b->used = -2;
		return NULL;
	}

	if (n > 0) {
		longs = malloc(n * sizeof *longs);
		ASSERT(longs);

		for (int32_t i = 0; i < n; i++) {
			bool eq;
			char *s = _bslurp_string(b);
			if (b->used < 0)
				goto out;
			eq = streq(s, LONG_TYPE);
			free(s);
			if (!eq) {
				b->used = -2;
				goto out;
			}

			longs[i] = _bslurp_s64(b);
			if (b->used < 0)
				goto out;
		}
	}

	res = hdfs_array_long_new(n, longs);

out:
	if (longs)
		free(longs);
	return res;
}

struct hdfs_object *
_oslurp_token(struct hdfs_heap_buf *b)
{
	char *s[4] = { 0 };
	struct hdfs_object *res = NULL;

	for (unsigned i = 0; i < nelem(s); i++) {
		s[i] = _bslurp_text(b);
		if (b->used < 0)
			goto out;
	}

	res = hdfs_token_new(s[0], s[1], s[2], s[3]);

out:
	for (unsigned i = 0; i < nelem(s); i++)
		if (s[i])
			free(s[i]);
	return res;
}

struct hdfs_object *
_oslurp_block(struct hdfs_heap_buf *b)
{
	int64_t values[3]; // blkid, len, generation

	for (unsigned i = 0; i < nelem(values); i++) {
		values[i] = _bslurp_s64(b);
		if (b->used < 0)
			return NULL;
	}

	return hdfs_block_new(values[0], values[1], values[2]);
}

struct hdfs_object *
_oslurp_located_block(struct hdfs_heap_buf *b)
{
	struct hdfs_object *token = NULL, *block = NULL, *di = NULL,
			   *res = NULL;
	bool corrupt;
	int64_t offset,
		blkid, len, generation;
	int32_t n_datanodes;

	token = _oslurp_token(b);
	if (b->used < 0)
		return NULL;

	// TODO keep token info around
	hdfs_object_free(token);
	token = NULL;

	corrupt = _bslurp_s8(b);
	if (b->used < 0)
		return NULL;
	ASSERT(!corrupt); // TODO keep corrupt info around

	offset = _bslurp_s64(b);
	if (b->used < 0)
		return NULL;
	if (offset < 0) {
		b->used = -2;
		return NULL;
	}

	block = _oslurp_block(b);
	if (b->used < 0)
		return NULL;

	blkid = block->ob_val._block._blkid;
	len = block->ob_val._block._length;
	generation = block->ob_val._block._generation;

	hdfs_object_free(block);
	block = NULL;

	n_datanodes = _bslurp_s32(b);
	if (b->used < 0)
		return NULL;
	if (n_datanodes < 0) {
		b->used = -2;
		return NULL;
	}

	res = hdfs_located_block_new(blkid, len, generation, offset);
	for (int i = 0; i < n_datanodes; i++) {
		di = _oslurp_datanode_info(b);
		if (b->used < 0) {
			hdfs_object_free(res);
			return NULL;
		}
		hdfs_located_block_append_datanode_info(res, di);
	}

	return res;
}

struct hdfs_object *
_oslurp_located_blocks(struct hdfs_heap_buf *b)
{
	int64_t size;
	bool under_construction;
	int32_t nblocks;
	struct hdfs_object *res = NULL;

	size = _bslurp_s64(b);
	if (b->used < 0)
		return NULL;
	under_construction = _bslurp_s8(b);
	if (b->used < 0)
		return NULL;

	// raw located_block[]
	nblocks = _bslurp_s32(b);
	if (b->used < 0)
		return NULL;

	res = hdfs_located_blocks_new(under_construction, size);
	for (int i = 0; i < nblocks; i++) {
		struct hdfs_object *located_block;

		located_block = _oslurp_located_block(b);
		if (b->used < 0) {
			hdfs_object_free(res);
			return NULL;
		}
		hdfs_located_blocks_append_located_block(res, located_block);
	}

	return res;
}

struct hdfs_object *
_oslurp_datanode_info(struct hdfs_heap_buf *b)
{
	struct hdfs_object *res = NULL;
	const char *port;
	char *name = NULL,
	     *sid = NULL,
	     *location = NULL,
	     *hostname = NULL,
	     *adminstate = NULL;
	uint16_t rpcport;

	/* datanodeId {{{ */
	name = _bslurp_string(b);
	if (b->used < 0)
		goto out;
	sid = _bslurp_string(b);
	if (b->used < 0)
		goto out;
	/*infoport = (uint16_t)*/_bslurp_s16(b);
	if (b->used < 0)
		goto out;
	/* }}} */

	rpcport = (uint16_t)_bslurp_s16(b);
	if (b->used < 0)
		goto out;

	/*capacity = */_bslurp_s64(b);
	if (b->used < 0)
		goto out;
	/*dfsused = */_bslurp_s64(b);
	if (b->used < 0)
		goto out;
	/*remaining = */_bslurp_s64(b);
	if (b->used < 0)
		goto out;
	/*lastupdate = */_bslurp_s64(b);
	if (b->used < 0)
		goto out;

	/*xceivercount = */_bslurp_s32(b);
	if (b->used < 0)
		goto out;

	location = _bslurp_text(b);
	if (b->used < 0)
		goto out;
	hostname = _bslurp_text(b);
	if (b->used < 0)
		goto out;

	// most enums are "string"s on the wire; adminstate is a "text" for
	// some reason.
	adminstate = _bslurp_text(b);
	if (b->used < 0)
		goto out;

	port = strchr(name, ':');
	if (!port) {
		b->used = -2;
		goto out;
	}
	port++;
	res = hdfs_datanode_info_new(hostname, port, location, rpcport);

out:
	if (name)
		free(name);
	if (sid)
		free(sid);
	if (location)
		free(location);
	if (hostname)
		free(hostname);
	if (adminstate)
		free(adminstate);
	return res;
}

struct hdfs_object *
_oslurp_directory_listing(struct hdfs_heap_buf *b)
{
	struct hdfs_object *res;
	int32_t len, remaining;

	len = _bslurp_s32(b);
	if (b->used < 0)
		return NULL;
	if (len < 0) {
		b->used = -2;
		return NULL;
	}

	res = hdfs_directory_listing_new();
	for (int i = 0; i < len; i++) {
		struct hdfs_object *fs;
		fs = _oslurp_file_status(b);
		if (b->used < 0) {
			hdfs_object_free(res);
			return NULL;
		}

		hdfs_directory_listing_append_file_status(res, fs, NULL);
	}

	remaining = _bslurp_s32(b);
	if (b->used < 0) {
		hdfs_object_free(res);
		return NULL;
	}
	ASSERT(remaining == 0);

	return res;
}

struct hdfs_object *
_oslurp_file_status(struct hdfs_heap_buf *b)
{
	struct hdfs_object *res = NULL;
	char *path = NULL,
	     *owner = NULL,
	     *group = NULL;
	int64_t size, blocksize, mtime, atime;
	int16_t replication, perms;
	bool isdir;

	path = _bslurp_string32(b);
	if (b->used < 0)
		goto out;
	size = _bslurp_s64(b);
	if (b->used < 0)
		goto out;
	isdir = _bslurp_s8(b);
	if (b->used < 0)
		goto out;
	replication = _bslurp_s16(b);
	if (b->used < 0)
		goto out;
	blocksize = _bslurp_s64(b);
	if (b->used < 0)
		goto out;
	mtime = _bslurp_s64(b);
	if (b->used < 0)
		goto out;
	atime = _bslurp_s64(b);
	if (b->used < 0)
		goto out;
	perms = _bslurp_s16(b);
	if (b->used < 0)
		goto out;
	if (perms < 0) {
		b->used = -2;
		goto out;
	}
	owner = _bslurp_text(b);
	if (b->used < 0)
		goto out;
	group = _bslurp_text(b);
	if (b->used < 0)
		goto out;

	res = hdfs_file_status_new_ex(path, size, isdir, replication,
	    blocksize, mtime, atime, perms, owner, group);

out:
	if (path)
		free(path);
	if (owner)
		free(owner);
	if (group)
		free(group);
	return res;
}

struct hdfs_object *
_oslurp_array_datanode_info(struct hdfs_heap_buf *b)
{
	struct hdfs_object *res = NULL;
	int32_t len;
	char *tstring = NULL;

	len = _bslurp_s32(b);
	if (b->used < 0)
		return NULL;
	if (len < 0) {
		b->used = -2;
		return NULL;
	}

	res = hdfs_array_datanode_info_new();
	for (int i = 0; i < len; i++) {
		// DatanodeInfo is an "object" type, thus the type string is
		// encoded twice... theoretically there could be a null here
		// but I'm pretty sure the Apache API doesn't produce/expect it.
		struct hdfs_object *di;
		for (int j = 0; j < 2; j++) {
			tstring = _bslurp_string(b);
			if (b->used < 0)
				goto err;
			if (!streq(tstring, DATANODEINFO_TYPE))
				goto err;
			free(tstring);
			tstring = NULL;
		}
		di = _oslurp_datanode_info(b);
		if (b->used < 0)
			goto err;
		hdfs_array_datanode_info_append_datanode_info(res, di);
	}
	return res;

err:
	if (tstring)
		free(tstring);
	if (res)
		hdfs_object_free(res);
	return NULL;
}

struct hdfs_object *
_oslurp_content_summary(struct hdfs_heap_buf *b)
{
	int64_t v[6];

	for (unsigned i = 0; i < nelem(v); i++) {
		v[i] = _bslurp_s64(b);
		if (b->used < 0)
			return NULL;
	}

	return hdfs_content_summary_new(v[0]/*length*/, v[1]/*files*/,
	    v[2]/*dirs*/, v[5]/*quota*/);
}

struct hdfs_object *
_oslurp_fsperms(struct hdfs_heap_buf *b)
{
	int16_t perms;

	perms = _bslurp_s16(b);
	if (b->used < 0)
		return NULL;

	return hdfs_fsperms_new(perms);
}
