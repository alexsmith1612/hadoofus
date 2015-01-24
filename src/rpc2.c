#include <stdint.h>
#include <stdio.h>

#include <hadoofus/objects.h>

#include "heapbuf.h"
#include "objects-internal.h"
#include "rpc2-internal.h"
#include "util.h"

#include "ClientNamenodeProtocol.pb-c.h"

/* Support logic for v2+ Namenode RPC (requests) */

#define ENCODE_PREAMBLE(lowerCamel, CamelCase, UPPER_CASE)		\
static void								\
_rpc2_encode_ ## lowerCamel (struct hdfs_heap_buf *dest,		\
	struct hdfs_rpc_invocation *rpc)				\
{									\
	CamelCase ## RequestProto req =					\
	    UPPER_CASE ## _REQUEST_PROTO__INIT;				\
	size_t sz;

	/*
	 * Additional validation of input object and population of req
	 * structure go here.
	 */

#define ENCODE_POSTSCRIPT(lower_case)					\
	sz = lower_case ## _request_proto__get_packed_size(&req);	\
	_hbuf_reserve(dest, sz);					\
	lower_case ## _request_proto__pack(&req,			\
	    (void *)&dest->buf[dest->used]);				\
	dest->used += sz;						\
}

/* New in v2 methods */
ENCODE_PREAMBLE(getServerDefaults, GetServerDefaults, GET_SERVER_DEFAULTS)
{
	ASSERT(rpc->_nargs == 0);
}
ENCODE_POSTSCRIPT(get_server_defaults)


/* Compat for v1 methods */
ENCODE_PREAMBLE(getListing, GetListing, GET_LISTING)
{
	ASSERT(rpc->_nargs == 2);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_ARRAY_BYTE);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.startafter.len = rpc->_args[1]->ob_val._array_byte._len;
	req.startafter.data = (void *)rpc->_args[1]->ob_val._array_byte._bytes;

	/* TODO Add hdfs2-only method for specifying needlocation */
	req.needlocation = false;
}
ENCODE_POSTSCRIPT(get_listing)

ENCODE_PREAMBLE(getBlockLocations, GetBlockLocations, GET_BLOCK_LOCATIONS)
{
	ASSERT(rpc->_nargs == 3);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_LONG);
	ASSERT(rpc->_args[2]->ob_type == H_LONG);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.offset = rpc->_args[1]->ob_val._long._val;
	req.length = rpc->_args[2]->ob_val._long._val;
}
ENCODE_POSTSCRIPT(get_block_locations)

ENCODE_PREAMBLE(create, Create, CREATE)
	FsPermissionProto perms = FS_PERMISSION_PROTO__INIT;
{
	ASSERT(rpc->_nargs == 7);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_FSPERMS);
	ASSERT(rpc->_args[2]->ob_type == H_STRING);
	ASSERT(rpc->_args[3]->ob_type == H_BOOLEAN);
	ASSERT(rpc->_args[4]->ob_type == H_BOOLEAN);
	ASSERT(rpc->_args[5]->ob_type == H_SHORT);
	ASSERT(rpc->_args[6]->ob_type == H_LONG);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.masked = &perms;
	perms.perm = rpc->_args[1]->ob_val._fsperms._perms;
	req.clientname = rpc->_args[2]->ob_val._string._val;
	req.createflag = CREATE_FLAG_PROTO__CREATE |
	    (rpc->_args[3]->ob_val._boolean._val?
	     CREATE_FLAG_PROTO__OVERWRITE : 0);
	req.createparent = rpc->_args[4]->ob_val._boolean._val;
	req.replication = rpc->_args[5]->ob_val._short._val;
	req.blocksize = rpc->_args[6]->ob_val._long._val;
}
ENCODE_POSTSCRIPT(create)

ENCODE_PREAMBLE(delete, Delete, DELETE)
{
	ASSERT(rpc->_nargs == 2);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_BOOLEAN);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.recursive = rpc->_args[1]->ob_val._boolean._val;
}
ENCODE_POSTSCRIPT(delete)

ENCODE_PREAMBLE(append, Append, APPEND)
{
	ASSERT(rpc->_nargs == 2);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_STRING);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.clientname = rpc->_args[1]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(append)

ENCODE_PREAMBLE(setReplication, SetReplication, SET_REPLICATION)
{
	ASSERT(rpc->_nargs == 2);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_SHORT);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.replication = rpc->_args[1]->ob_val._short._val;
}
ENCODE_POSTSCRIPT(set_replication)

ENCODE_PREAMBLE(setPermission, SetPermission, SET_PERMISSION)
	FsPermissionProto perms = FS_PERMISSION_PROTO__INIT;
{
	ASSERT(rpc->_nargs == 2);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_FSPERMS);

	req.src = rpc->_args[0]->ob_val._string._val;
	perms.perm = rpc->_args[1]->ob_val._fsperms._perms;
	req.permission = &perms;
}
ENCODE_POSTSCRIPT(set_permission)

ENCODE_PREAMBLE(setOwner, SetOwner, SET_OWNER)
{
	ASSERT(rpc->_nargs == 3);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_STRING);
	ASSERT(rpc->_args[2]->ob_type == H_STRING);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.username = rpc->_args[1]->ob_val._string._val;
	req.groupname = rpc->_args[2]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(set_owner)

ENCODE_PREAMBLE(complete, Complete, COMPLETE)
{
	ASSERT(rpc->_nargs == 2);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_STRING);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.clientname = rpc->_args[1]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(complete)

typedef void (*_rpc2_encoder)(struct hdfs_heap_buf *, struct hdfs_rpc_invocation *);
static struct _rpc2_enc_lut {
	const char *	re_method;
	_rpc2_encoder	re_encoder;
} rpc2_encoders[] = {
#define _RENC(method)	{ #method , _rpc2_encode_ ## method , }
	_RENC(getServerDefaults),
	_RENC(getListing),
	_RENC(getBlockLocations),
	_RENC(create),
	_RENC(delete),
	_RENC(append),
	_RENC(setReplication),
	_RENC(setPermission),
	_RENC(setOwner),
	_RENC(complete),
	{ NULL, NULL, },
#undef _RENC
};

void
_rpc2_request_serialize(struct hdfs_heap_buf *dest,
	struct hdfs_rpc_invocation *rpc)
{
	/* XXXPERF Could be a hash/rt/sorted table */
	unsigned i;

	for (i = 0; rpc2_encoders[i].re_method; i++) {
		if (streq(rpc->_method, rpc2_encoders[i].re_method)) {
			rpc2_encoders[i].re_encoder(dest, rpc);
			return;
		}
	}

	/* The method does not exist in HDFSv2, or it is not yet implemented. */
	ASSERT(false);
}

/* Support logic for Namenode RPC response parsing */

/*
 * These slurpers expect exactly one protobuf in the heapbuf passed in, and
 * advance the cursor (->used) to the end (->size) after a successful parse.
 */
#define DECODE_PB_EX(lowerCamel, CamelCase, lower_case, objbuilder_ex)	\
static struct hdfs_object *						\
_oslurp_ ## lowerCamel (struct hdfs_heap_buf *buf)			\
{									\
	CamelCase ## ResponseProto *resp;				\
	struct hdfs_object *result;					\
									\
	result = NULL;							\
									\
	resp = lower_case ## _response_proto__unpack(NULL,		\
	    buf->size - buf->used, (void *)&buf->buf[buf->used]);	\
	buf->used = buf->size;						\
	if (resp == NULL) {						\
		buf->used = -2;						\
		return NULL;						\
	}								\
									\
	objbuilder_ex;							\
									\
	lower_case ## _response_proto__free_unpacked(resp, NULL);	\
	return result;							\
}

#define DECODE_PB(lowerCamel, CamelCase, lower_case, objbuilder, respfield)	\
	DECODE_PB_EX(lowerCamel, CamelCase, lower_case,				\
	    result = _hdfs_ ## objbuilder ## _new_proto(resp->respfield))

#define DECODE_PB_VOID(lowerCamel, CamelCase, lower_case)		\
	DECODE_PB_EX(lowerCamel, CamelCase, lower_case,			\
	    result = hdfs_void_new())


DECODE_PB(getServerDefaults, GetServerDefaults, get_server_defaults, fsserverdefaults, serverdefaults)
DECODE_PB(getListing, GetListing, get_listing, directory_listing, dirlist)
DECODE_PB(getBlockLocations, GetBlockLocations, get_block_locations, located_blocks, locations)
#if 0
DECODE_PB_EX(create, Create, create,
	/* HDFSv2.2+ returns a FileStatus, while 2.0.x returns void. */
	if (resp->fs)
		result = _hdfs_file_status_new_proto(resp->fs);
	else
		result = hdfs_void_new())
#else
/*
 * The HDFSv1 routine returns void, so we can't return a FileStatus even though
 * v2.2+ provide it.
 */
DECODE_PB_VOID(create, Create, create)
#endif
DECODE_PB(delete, Delete, delete, boolean, result)
DECODE_PB_EX(append, Append, append,
	if (resp->block)
		result = _hdfs_located_block_new_proto(resp->block);
	else
		result = hdfs_null_new(H_LOCATED_BLOCK))
DECODE_PB(setReplication, SetReplication, set_replication, boolean, result)
DECODE_PB_VOID(setPermission, SetPermission, set_permission)
DECODE_PB_VOID(setOwner, SetOwner, set_owner)
DECODE_PB(complete, Complete, complete, boolean, result)

static struct _rpc2_dec_lut {
	const char *		rd_method;
	hdfs_object_slurper	rd_decoder;
} rpc2_decoders[] = {
#define _RDEC(method)	{ #method , _oslurp_ ## method , }
	_RDEC(getServerDefaults),
	_RDEC(getListing),
	_RDEC(getBlockLocations),
	_RDEC(create),
	_RDEC(delete),
	_RDEC(append),
	_RDEC(setReplication),
	_RDEC(setPermission),
	_RDEC(setOwner),
	_RDEC(complete),
	{ NULL, NULL, },
#undef _RDEC
};

hdfs_object_slurper
_rpc2_slurper_for_rpc(struct hdfs_object *rpc)
{
	/* XXXPERF s.a. */
	unsigned i;

	ASSERT(rpc->ob_type == H_RPC_INVOCATION);

	for (i = 0; rpc2_decoders[i].rd_method; i++)
		if (streq(rpc->ob_val._rpc_invocation._method,
		    rpc2_decoders[i].rd_method))
			return rpc2_decoders[i].rd_decoder;

	/*
	 * The method does not exist in HDFSv2 (this function can be called for
	 * HDFSv1 methods) or it is not yet implemented.
	 */
	return NULL;
}
