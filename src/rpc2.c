#include <stdint.h>
#include <stdio.h>

#include <hadoofus/lowlevel.h>
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
	Hadoop__Hdfs__ ## CamelCase ## RequestProto req =		\
	    HADOOP__HDFS__ ## UPPER_CASE ## _REQUEST_PROTO__INIT;	\
	size_t sz;

	/*
	 * Additional validation of input object and population of req
	 * structure go here.
	 */

#define ENCODE_POSTSCRIPT(lower_case)							\
	sz = hadoop__hdfs__ ## lower_case ## _request_proto__get_packed_size(&req);	\
	_hbuf_reserve(dest, sz);							\
	hadoop__hdfs__ ## lower_case ## _request_proto__pack(&req,			\
	    (void *)&dest->buf[dest->used]);						\
	dest->used += sz;								\
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
	Hadoop__Hdfs__FsPermissionProto perms = HADOOP__HDFS__FS_PERMISSION_PROTO__INIT;
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
	req.createflag = HADOOP__HDFS__CREATE_FLAG_PROTO__CREATE |
	    (rpc->_args[3]->ob_val._boolean._val?
	     HADOOP__HDFS__CREATE_FLAG_PROTO__OVERWRITE : 0);
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
	Hadoop__Hdfs__FsPermissionProto perms = HADOOP__HDFS__FS_PERMISSION_PROTO__INIT;
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

ENCODE_PREAMBLE(abandonBlock, AbandonBlock, ABANDON_BLOCK)
	Hadoop__Hdfs__ExtendedBlockProto eb = HADOOP__HDFS__EXTENDED_BLOCK_PROTO__INIT;
{
	ASSERT(rpc->_nargs == 3);
	ASSERT(rpc->_args[0]->ob_type == H_BLOCK);
	ASSERT(rpc->_args[0]->ob_val._block._pool_id);
	ASSERT(rpc->_args[1]->ob_type == H_STRING);
	ASSERT(rpc->_args[2]->ob_type == H_STRING);

	eb.poolid = rpc->_args[0]->ob_val._block._pool_id;
	eb.blockid = rpc->_args[0]->ob_val._block._blkid;
	eb.generationstamp = rpc->_args[0]->ob_val._block._generation;
	if (rpc->_args[0]->ob_val._block._length) {
		eb.has_numbytes = true;
		eb.numbytes = rpc->_args[0]->ob_val._block._length;
	}
	req.b = &eb;

	req.src = rpc->_args[1]->ob_val._string._val;
	req.holder = rpc->_args[2]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(abandon_block)

ENCODE_PREAMBLE(addBlock, AddBlock, ADD_BLOCK)
{
	ASSERT(rpc->_nargs == 3);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_STRING);
	ASSERT(rpc->_args[2]->ob_type == H_ARRAY_DATANODE_INFO ||
	    (rpc->_args[2]->ob_type == H_NULL &&
	     rpc->_args[2]->ob_val._null._type == H_ARRAY_DATANODE_INFO));

	req.src = rpc->_args[0]->ob_val._string._val;
	req.clientname = rpc->_args[1]->ob_val._string._val;
	req.previous = NULL;

	if (rpc->_args[2]->ob_type != H_NULL) {
		/* XXX not yet implemented, but it could be */
		ASSERT(rpc->_args[2]->ob_val._array_datanode_info._len == 0);
	}
	req.n_excludenodes = 0;
	req.n_favorednodes = 0;
}
ENCODE_POSTSCRIPT(add_block)

ENCODE_PREAMBLE(rename, Rename, RENAME)
{
	ASSERT(rpc->_nargs == 2);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_STRING);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.dst = rpc->_args[1]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(rename)

ENCODE_PREAMBLE(mkdirs, Mkdirs, MKDIRS)
	Hadoop__Hdfs__FsPermissionProto perms = HADOOP__HDFS__FS_PERMISSION_PROTO__INIT;
{
	ASSERT(rpc->_nargs == 2);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_FSPERMS);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.masked = &perms;
	perms.perm = rpc->_args[1]->ob_val._fsperms._perms;

	/* XXX Could have a seperate v2 RPC to specify */
	req.createparent = true;
}
ENCODE_POSTSCRIPT(mkdirs)

ENCODE_PREAMBLE(renewLease, RenewLease, RENEW_LEASE)
{
	ASSERT(rpc->_nargs == 1);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);

	req.clientname = rpc->_args[0]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(renew_lease)

ENCODE_PREAMBLE(recoverLease, RecoverLease, RECOVER_LEASE)
{
	ASSERT(rpc->_nargs == 2);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_STRING);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.clientname = rpc->_args[1]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(recover_lease)

ENCODE_PREAMBLE(getContentSummary, GetContentSummary, GET_CONTENT_SUMMARY)
{
	ASSERT(rpc->_nargs == 1);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);

	req.path = rpc->_args[0]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(get_content_summary)

ENCODE_PREAMBLE(setQuota, SetQuota, SET_QUOTA)
{
	ASSERT(rpc->_nargs == 3);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_LONG);
	ASSERT(rpc->_args[2]->ob_type == H_LONG);

	req.path = rpc->_args[0]->ob_val._string._val;
	req.namespacequota = rpc->_args[1]->ob_val._long._val;
	req.storagespacequota = rpc->_args[2]->ob_val._long._val;
}
ENCODE_POSTSCRIPT(set_quota)

ENCODE_PREAMBLE(fsync, Fsync, FSYNC)
{
	ASSERT(rpc->_nargs == 2);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_STRING);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.client = rpc->_args[1]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(fsync)

ENCODE_PREAMBLE(setTimes, SetTimes, SET_TIMES)
{
	ASSERT(rpc->_nargs == 3);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_LONG);
	ASSERT(rpc->_args[2]->ob_type == H_LONG);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.mtime = rpc->_args[1]->ob_val._long._val;
	req.atime = rpc->_args[2]->ob_val._long._val;
}
ENCODE_POSTSCRIPT(set_times)

ENCODE_PREAMBLE(getFileInfo, GetFileInfo, GET_FILE_INFO)
{
	ASSERT(rpc->_nargs == 1);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);

	req.src = rpc->_args[0]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(get_file_info)

ENCODE_PREAMBLE(getFileLinkInfo, GetFileLinkInfo, GET_FILE_LINK_INFO)
{
	ASSERT(rpc->_nargs == 1);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);

	req.src = rpc->_args[0]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(get_file_link_info)

ENCODE_PREAMBLE(createSymlink, CreateSymlink, CREATE_SYMLINK)
	Hadoop__Hdfs__FsPermissionProto perms = HADOOP__HDFS__FS_PERMISSION_PROTO__INIT;
{
	ASSERT(rpc->_nargs == 4);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_STRING);
	ASSERT(rpc->_args[2]->ob_type == H_FSPERMS);
	ASSERT(rpc->_args[3]->ob_type == H_BOOLEAN);

	req.target = rpc->_args[0]->ob_val._string._val;
	req.link = rpc->_args[1]->ob_val._string._val;
	perms.perm = rpc->_args[2]->ob_val._fsperms._perms;
	req.dirperm = &perms;
	req.createparent = rpc->_args[3]->ob_val._boolean._val;
}
ENCODE_POSTSCRIPT(create_symlink)

ENCODE_PREAMBLE(getLinkTarget, GetLinkTarget, GET_LINK_TARGET)
{
	ASSERT(rpc->_nargs == 1);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);

	req.path = rpc->_args[0]->ob_val._string._val;
}
ENCODE_POSTSCRIPT(get_link_target)

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
	_RENC(abandonBlock),
	_RENC(addBlock),
	_RENC(rename),
	_RENC(mkdirs),
	_RENC(renewLease),
	_RENC(recoverLease),
	_RENC(getContentSummary),
	_RENC(setQuota),
	_RENC(fsync),
	_RENC(setTimes),
	_RENC(getFileInfo),
	_RENC(getFileLinkInfo),
	_RENC(createSymlink),
	_RENC(getLinkTarget),
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
#define DECODE_PB_EX(lowerCamel, CamelCase, lower_case, objbuilder_ex)			\
static struct hdfs_object *								\
_oslurp_ ## lowerCamel (struct hdfs_heap_buf *buf)					\
{											\
	Hadoop__Hdfs__ ## CamelCase ## ResponseProto *resp;				\
	struct hdfs_object *result;							\
											\
	result = NULL;									\
											\
	resp = hadoop__hdfs__ ## lower_case ## _response_proto__unpack(NULL,		\
	    buf->size - buf->used, (void *)&buf->buf[buf->used]);			\
	buf->used = buf->size;								\
	if (resp == NULL) {								\
		buf->used = _H_PARSE_ERROR;						\
		return NULL;								\
	}										\
											\
	objbuilder_ex;									\
											\
	hadoop__hdfs__ ## lower_case ## _response_proto__free_unpacked(resp, NULL);	\
	return result;									\
}

#define DECODE_PB(lowerCamel, CamelCase, lower_case, objbuilder, respfield)	\
	DECODE_PB_EX(lowerCamel, CamelCase, lower_case,				\
	    result = _hdfs_ ## objbuilder ## _new_proto(resp->respfield))

#define _DECODE_PB_NULLABLE(lowerCamel, CamelCase, lower_case, objbuilder, respfield, nullctor)	\
	DECODE_PB_EX(lowerCamel, CamelCase, lower_case,					\
		if (resp->respfield != NULL) {						\
			result = _hdfs_ ## objbuilder ## _new_proto(resp->respfield);	\
		} else {								\
			result = nullctor;					\
		}									\
	)

#define DECODE_PB_NULLABLE(lowerCamel, CamelCase, lower_case, objbuilder, respfield, htype)	\
	_DECODE_PB_NULLABLE(lowerCamel, CamelCase, lower_case, objbuilder, respfield,	\
	    hdfs_null_new(htype))

#define DECODE_PB_VOID(lowerCamel, CamelCase, lower_case)		\
	DECODE_PB_EX(lowerCamel, CamelCase, lower_case,			\
	    result = hdfs_void_new())


DECODE_PB(getServerDefaults, GetServerDefaults, get_server_defaults, fsserverdefaults, serverdefaults)
DECODE_PB_NULLABLE(getListing, GetListing, get_listing, directory_listing, dirlist, H_DIRECTORY_LISTING)
DECODE_PB_NULLABLE(getBlockLocations, GetBlockLocations, get_block_locations, located_blocks, locations, H_LOCATED_BLOCKS)
/*
 * HDFSv2.2+ returns a FileStatus, while 2.0.x and 1.x return void.
 *
 * This is represented in the shared protobuf as a NULL 'fs'.  Also, the same
 * high-level wrapper is used across all versions, so we represent void in
 * earlier versions as a return value of (FileStatus *)NULL.
 */
DECODE_PB_NULLABLE(create, Create, create, file_status, fs, H_FILE_STATUS)
DECODE_PB(delete, Delete, delete, boolean, result)
DECODE_PB_NULLABLE(append, Append, append, located_block, block,
    H_LOCATED_BLOCK);
DECODE_PB(setReplication, SetReplication, set_replication, boolean, result)
DECODE_PB_VOID(setPermission, SetPermission, set_permission)
DECODE_PB_VOID(setOwner, SetOwner, set_owner)
DECODE_PB(complete, Complete, complete, boolean, result)
DECODE_PB_VOID(abandonBlock, AbandonBlock, abandon_block)
DECODE_PB(addBlock, AddBlock, add_block, located_block, block)
DECODE_PB(rename, Rename, rename, boolean, result)
DECODE_PB(mkdirs, Mkdirs, mkdirs, boolean, result)
DECODE_PB_VOID(renewLease, RenewLease, renew_lease)
DECODE_PB(recoverLease, RecoverLease, recover_lease, boolean, result)
DECODE_PB(getContentSummary, GetContentSummary, get_content_summary, content_summary, summary)
DECODE_PB_VOID(setQuota, SetQuota, set_quota)
DECODE_PB_VOID(fsync, Fsync, fsync)
DECODE_PB_VOID(setTimes, SetTimes, set_times)
DECODE_PB_NULLABLE(getFileInfo, GetFileInfo, get_file_info, file_status, fs, H_FILE_STATUS)
DECODE_PB_NULLABLE(getFileLinkInfo, GetFileLinkInfo, get_file_link_info, file_status, fs, H_FILE_STATUS)
DECODE_PB_VOID(createSymlink, CreateSymlink, create_symlink)
DECODE_PB_EX(getLinkTarget, GetLinkTarget, get_link_target,
	result = hdfs_string_new(resp->targetpath))

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
	_RDEC(abandonBlock),
	_RDEC(addBlock),
	_RDEC(rename),
	_RDEC(mkdirs),
	_RDEC(renewLease),
	_RDEC(recoverLease),
	_RDEC(getContentSummary),
	_RDEC(setQuota),
	_RDEC(fsync),
	_RDEC(setTimes),
	_RDEC(getFileInfo),
	_RDEC(getFileLinkInfo),
	_RDEC(createSymlink),
	_RDEC(getLinkTarget),
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
