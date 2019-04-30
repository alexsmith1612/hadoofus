#include <stdio.h>
#include <stdlib.h>

#include <hadoofus/highlevel.h>

#include "util.h"

#define BAIL_ON_ERR(error) do {						\
	if (__predict_true(!hdfs_is_error(error)))			\
		break;							\
	assert_fail("The high-level interface cannot handle errors\n"	\
	    "below the protocol level.  Got unexpected error %s:%s\n"	\
	    "in %s (%s:%u)\n", hdfs_error_str_kind(error),		\
	    hdfs_error_str(error), __func__, __FILE__, __LINE__);	\
} while (false)

// XXX TODO rename to hdfs_namenode_new() since there is not default version
// highlevel API anymore (although perhaps leave this as is and add an
// hdfs_namenode_new() that defaults to v2.2+)
EXPORT_SYM struct hdfs_namenode *
hdfs_namenode_new_version(const char *host, const char *port,
	const char *username, enum hdfs_kerb kerb_pref,
	enum hdfs_namenode_proto vers, struct hdfs_error *error_out)
{
	struct hdfs_error error;
	struct hdfs_namenode *h;

	h = hdfs_namenode_allocate();
	hdfs_namenode_init_ver(h, kerb_pref, vers);

	error = hdfs_namenode_connect(h, host, port);
	if (hdfs_is_error(error))
		goto out;

	error = hdfs_namenode_authenticate(h, username);
	if (hdfs_is_error(error))
		goto out;

out:
	if (hdfs_is_error(error)) {
		hdfs_namenode_delete(h);
		h = NULL;
		*error_out = error;
	}
	return h;
}

EXPORT_SYM void
hdfs_namenode_delete(struct hdfs_namenode *h)
{
	hdfs_namenode_destroy(h);
	free(h);
}

// RPC implementations

// XXX this _HDFS_RPC_CASE() macro is not ideal in that it expands to a compound
// statement instead of a single regular statement, and we cannot use the traditional
// `do { } while (0)` construct due to the break. Be careful about where and how this
// macro is used, and if possible change the implementation to avoid such a compound
// statement macro
#define _HDFS_RPC_CASE(name, args...)			\
	rpc = hdfs_rpc_invocation_new(			\
	    #name,					\
	    ##args,					\
	    NULL);					\
	break

#define _HDFS_RPC_NB_DECL(name, args...) \
EXPORT_SYM struct hdfs_error \
hdfs_ ## name ## _nb(struct hdfs_namenode *h, ##args, struct hdfs_rpc_response_future *future)

#define _HDFS_RPC_NB_BODY(v1_case, v2_case, v2_2_case) \
{ \
	struct hdfs_error error; \
	struct hdfs_object *rpc = NULL; \
\
	switch (h->nn_proto) { \
	case HDFS_NN_v1: \
		v1_case; \
	case HDFS_NN_v2: \
		v2_case; \
	case HDFS_NN_v2_2: \
		v2_2_case; \
	default: \
		ASSERT(false); \
	}; \
	ASSERT(rpc); \
\
	error = hdfs_namenode_invoke(h, rpc, future); \
	hdfs_object_free(rpc); \
	return error; \
}

#define _HDFS_RPC_DECL(type, name, args...) \
EXPORT_SYM type \
hdfs_ ## name (struct hdfs_namenode *h, ##args, struct hdfs_object **exception_out)

#define _HDFS_RPC_BODY_EX(htype, static_asserts, ret_ex, v1_case, v2_case, v2_2_case) \
{ \
	struct hdfs_rpc_response_future future; \
	struct hdfs_object *rpc = NULL, *object; \
	struct hdfs_error error; \
\
	_Static_assert(htype >= _H_START && htype < _H_END, \
	    "htype must be a valid type"); \
	static_asserts; \
\
	future.fu_inited = false; \
	hdfs_rpc_response_future_init(&future); \
\
	switch (h->nn_proto) { \
	case HDFS_NN_v1: \
		v1_case; \
	case HDFS_NN_v2: \
		v2_case; \
	case HDFS_NN_v2_2: \
		v2_2_case; \
	default: \
		ASSERT(false); \
	}; \
	ASSERT(rpc); \
\
	error = hdfs_namenode_invoke(h, rpc, &future); \
	BAIL_ON_ERR(error); \
\
	hdfs_object_free(rpc); \
\
	hdfs_future_get(&future, &object); \
\
	ret_ex; \
}

#define _HDFS_PRIM_RPC_RET(htype, result, retval, dflt) \
do { \
	ASSERT(object->ob_type == htype || \
	    object->ob_type == H_PROTOCOL_EXCEPTION); \
\
	if (object->ob_type == H_PROTOCOL_EXCEPTION) { \
		*exception_out = object; \
		return dflt ; \
	} \
\
	*exception_out = NULL; \
\
	result; \
	hdfs_object_free(object); \
	return retval; \
} while (false)

#define _HDFS_PRIM_RPC_DECL(type, name, args...) \
	_HDFS_RPC_DECL(type, name, ##args)

#define _HDFS_PRIM_RPC_BODY(htype, result, retval, dflt, v1_case, v2_case, v2_2_case) \
	_HDFS_RPC_BODY_EX(htype, \
		/*static_asserts*/, \
		_HDFS_PRIM_RPC_RET(htype, result, retval, dflt), \
		v1_case, \
		v2_case, \
		v2_2_case \
	)

#define _HDFS_OBJ_RPC_RET(void_ok, htype) \
do { \
	ASSERT(object->ob_type == htype || \
	    (object->ob_type == H_NULL && object->ob_val._null._type == htype) || \
	    (void_ok && object->ob_type == H_VOID) || \
	    object->ob_type == H_PROTOCOL_EXCEPTION); \
\
	if (object->ob_type == H_PROTOCOL_EXCEPTION) { \
		*exception_out = object; \
		return NULL; \
	} \
\
	*exception_out = NULL; \
\
	if (object->ob_type == H_NULL || \
	    (void_ok && object->ob_type == H_VOID)) { \
		hdfs_object_free(object); \
		object = NULL; \
	} \
\
	return object; \
} while (false)

#define _HDFS_OBJ_RPC_DECL(name, args...) \
	_HDFS_RPC_DECL(struct hdfs_object *, name, ##args)

#define _HDFS_OBJ_RPC_BODY_IMPL(void_ok, htype, v1_case, v2_case, v2_2_case) \
	_HDFS_RPC_BODY_EX(htype, \
		_Static_assert(void_ok == true || void_ok == false, \
		    "void_ok must be bool"), \
		_HDFS_OBJ_RPC_RET(void_ok, htype), \
		v1_case, \
		v2_case, \
		v2_2_case \
	)

#define _HDFS_OBJ_RPC_BODY(htype, v1_case, v2_case, v2_2_case) _HDFS_OBJ_RPC_BODY_IMPL(false, htype, v1_case, v2_case, v2_2_case)
#define _HDFS_OBJ_RPC_BODY_VOID_OK(htype, v1_case, v2_case, v2_2_case) _HDFS_OBJ_RPC_BODY_IMPL(true, htype, v1_case, v2_case, v2_2_case)

// XXX TODO change the v1-only functions to hdfs1_*() (or something like that)

// XXX v1 only
_HDFS_RPC_NB_DECL(getProtocolVersion,
	const char *protocol, int64_t client_version)
_HDFS_RPC_NB_BODY(
	 _HDFS_RPC_CASE(getProtocolVersion,
		hdfs_string_new(protocol),
		hdfs_long_new(client_version)
	),
	/*fall through to v2_2*/,
	/*fall through to default*/
)

_HDFS_PRIM_RPC_DECL(int64_t, getProtocolVersion,
	const char *protocol, int64_t client_version)
_HDFS_PRIM_RPC_BODY(H_LONG,
	int64_t res = object->ob_val._long._val,
	res,
	0,
	 _HDFS_RPC_CASE(getProtocolVersion,
		hdfs_string_new(protocol),
		hdfs_long_new(client_version)
	),
	/*fall through to v2_2*/,
	/*fall through to default*/
)

// XXX returned LocatedBlocksProto is optional (probably for the empty file case), make sure the deserializer handles this
_HDFS_RPC_NB_DECL(getBlockLocations,
	const char *path, int64_t offset, int64_t length)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getBlockLocations,
		hdfs_string_new(path),
		hdfs_long_new(offset),
		hdfs_long_new(length)
	)
)

_HDFS_OBJ_RPC_DECL(getBlockLocations,
	const char *path, int64_t offset, int64_t length)
_HDFS_OBJ_RPC_BODY(H_LOCATED_BLOCKS,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getBlockLocations,
		hdfs_string_new(path),
		hdfs_long_new(offset),
		hdfs_long_new(length)
	)
)

// XXX more createFlags
// XXX CryptoProtovolVersionProto
// XXX unmasked perms
// XXX ecPolicyName
// XXX make sure accepts NULL file status return
_HDFS_RPC_NB_DECL(create,
	const char *path, uint16_t perms, const char *clientname,
	bool overwrite, bool create_parent, int16_t replication,
	int64_t blocksize)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(create,
		hdfs_string_new(path),
		hdfs_fsperms_new(perms),
		hdfs_string_new(clientname),
		hdfs_boolean_new(overwrite),
		hdfs_boolean_new(create_parent),
		hdfs_short_new(replication),
		hdfs_long_new(blocksize)
	)
)

_HDFS_OBJ_RPC_DECL(create,
	const char *path, uint16_t perms, const char *clientname,
	bool overwrite, bool create_parent, int16_t replication,
	int64_t blocksize)
_HDFS_OBJ_RPC_BODY_VOID_OK(H_FILE_STATUS,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(create,
		hdfs_string_new(path),
		hdfs_fsperms_new(perms),
		hdfs_string_new(clientname),
		hdfs_boolean_new(overwrite),
		hdfs_boolean_new(create_parent),
		hdfs_short_new(replication),
		hdfs_long_new(blocksize)
	)
)

// XXX As of v2.7.0 AppendResponseProto includes an HdfsFileStatusProto in
// addition to the LocatedBlockProto so that one does not need to subsequently
// call getFileInfo in order to get the block size, etc. TODO update append to
// handle this (perhaps make an H_LOCATED_BLOCK_WITH_STATUS object?)
// XXX flags (bit values from CreateFlagProto)
// XXX return LocatedBlockProto and HdfsFileStatusProto (perhaps both may be NULL?)
_HDFS_RPC_NB_DECL(append,
	const char *path, const char *client)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(append,
		hdfs_string_new(path),
		hdfs_string_new(client)
	)
)

_HDFS_OBJ_RPC_DECL(append,
	const char *path, const char *client)
_HDFS_OBJ_RPC_BODY(H_LOCATED_BLOCK,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(append,
		hdfs_string_new(path),
		hdfs_string_new(client)
	)
)

_HDFS_RPC_NB_DECL(setReplication,
	const char *path, int16_t replication)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setReplication,
		hdfs_string_new(path),
		hdfs_short_new(replication)
	)
)

_HDFS_PRIM_RPC_DECL(bool, setReplication,
	const char *path, int16_t replication)
_HDFS_PRIM_RPC_BODY(H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setReplication,
		hdfs_string_new(path),
		hdfs_short_new(replication)
	)
)

_HDFS_RPC_NB_DECL(setPermission,
	const char *path, int16_t perms)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setPermission,
		hdfs_string_new(path),
		hdfs_fsperms_new(perms)
	)
)

_HDFS_PRIM_RPC_DECL(void, setPermission,
	const char *path, int16_t perms)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setPermission,
		hdfs_string_new(path),
		hdfs_fsperms_new(perms)
	)
)

_HDFS_RPC_NB_DECL(setOwner,
	const char *path, const char *owner, const char *group)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setOwner,
		hdfs_string_new(path),
		hdfs_string_new(owner),
		hdfs_string_new(group)
	)
)

_HDFS_PRIM_RPC_DECL(void, setOwner,
	const char *path, const char *owner, const char *group)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setOwner,
		hdfs_string_new(path),
		hdfs_string_new(owner),
		hdfs_string_new(group)
	)
)

// XXX fileid arg
_HDFS_RPC_NB_DECL(abandonBlock,
	struct hdfs_object *block, const char *path, const char *client)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(abandonBlock,
		hdfs_block_copy(block),
		hdfs_string_new(path),
		hdfs_string_new(client)
	)
)

_HDFS_PRIM_RPC_DECL(void, abandonBlock,
	struct hdfs_object *block, const char *path, const char *client)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(abandonBlock,
		hdfs_block_copy(block),
		hdfs_string_new(path),
		hdfs_string_new(client)
	)
)

// XXX favoredNodes
// XXX AddBlockFlagProto
_HDFS_RPC_NB_DECL(addBlock,
	const char *path, const char *client, struct hdfs_object *excluded,
	struct hdfs_object *previous_block, int64_t fileid)
_HDFS_RPC_NB_BODY(
	_HDFS_RPC_CASE(addBlock,
		hdfs_string_new(path),
		hdfs_string_new(client),
		hdfs_array_datanode_info_copy(excluded)
	),
	/*fall through to v2.2*/, // XXX consider having _HDFS_RPC_CASE() without fileid for v2.0
	_HDFS_RPC_CASE(addBlock,
		hdfs_string_new(path),
		hdfs_string_new(client),
		hdfs_array_datanode_info_copy(excluded),
		hdfs_block_copy(previous_block),
		hdfs_long_new(fileid)
	)
)

_HDFS_OBJ_RPC_DECL(addBlock,
	const char *path, const char *client, struct hdfs_object *excluded,
	struct hdfs_object *previous_block, int64_t fileid)
_HDFS_OBJ_RPC_BODY(H_LOCATED_BLOCK,
	_HDFS_RPC_CASE(addBlock,
		hdfs_string_new(path),
		hdfs_string_new(client),
		hdfs_array_datanode_info_copy(excluded)
	),
	/*fall through to v2.2*/, // XXX consider having _HDFS_RPC_CASE() without fileid for v2.0
	_HDFS_RPC_CASE(addBlock,
		hdfs_string_new(path),
		hdfs_string_new(client),
		hdfs_array_datanode_info_copy(excluded),
		hdfs_block_copy(previous_block),
		hdfs_long_new(fileid)
	)
)

_HDFS_RPC_NB_DECL(complete,
	const char *path, const char *client, struct hdfs_object *last_block,
	int64_t fileid /* new in v2.2; zero prior */)
_HDFS_RPC_NB_BODY(
	_HDFS_RPC_CASE(complete,
		hdfs_string_new(path),
		hdfs_string_new(client)
	),
	/*fall through to v2.2*/, // XXX consider having _HDFS_RPC_CASE() without fileid for v2.0
	_HDFS_RPC_CASE(complete,
		hdfs_string_new(path),
		hdfs_string_new(client),
		hdfs_block_copy(last_block),
		hdfs_long_new(fileid)
	)
)

_HDFS_PRIM_RPC_DECL(bool, complete,
	const char *path, const char *client, struct hdfs_object *last_block,
	int64_t fileid /* new in v2.2; zero prior */)
_HDFS_PRIM_RPC_BODY(H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	_HDFS_RPC_CASE(complete,
		hdfs_string_new(path),
		hdfs_string_new(client)
	),
	/*fall through to v2.2*/, // XXX consider having _HDFS_RPC_CASE() without fileid for v2.0
	_HDFS_RPC_CASE(complete,
		hdfs_string_new(path),
		hdfs_string_new(client),
		hdfs_block_copy(last_block),
		hdfs_long_new(fileid)
	)
)

_HDFS_RPC_NB_DECL(rename,
	const char *src, const char *dst)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(rename,
		hdfs_string_new(src),
		hdfs_string_new(dst)
	)
)

_HDFS_PRIM_RPC_DECL(bool, rename,
	const char *src, const char *dst)
_HDFS_PRIM_RPC_BODY(H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(rename,
		hdfs_string_new(src),
		hdfs_string_new(dst)
	)
)

_HDFS_RPC_NB_DECL(delete,
	const char *path, bool can_recurse)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(delete,
		hdfs_string_new(path),
		hdfs_boolean_new(can_recurse)
	)
)

_HDFS_PRIM_RPC_DECL(bool, delete,
	const char *path, bool can_recurse)
_HDFS_PRIM_RPC_BODY(H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(delete,
		hdfs_string_new(path),
		hdfs_boolean_new(can_recurse)
	)
)

// XXX createParent
// XXX unmasked perms
_HDFS_RPC_NB_DECL(mkdirs,
	const char *path, int16_t perms)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(mkdirs,
		hdfs_string_new(path),
		hdfs_fsperms_new(perms)
	)
)

_HDFS_PRIM_RPC_DECL(bool, mkdirs,
	const char *path, int16_t perms)
_HDFS_PRIM_RPC_BODY(H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(mkdirs,
		hdfs_string_new(path),
		hdfs_fsperms_new(perms)
	)
)

// XXX needLocation
// XXX make return nullable
_HDFS_RPC_NB_DECL(getListing,
	const char *path, struct hdfs_object *begin)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getListing,
		hdfs_string_new(path),
		(begin ? hdfs_array_byte_copy(begin) : hdfs_array_byte_new(0, NULL))
	)
)

_HDFS_OBJ_RPC_DECL(getListing,
	const char *path, struct hdfs_object *begin)
_HDFS_OBJ_RPC_BODY(H_DIRECTORY_LISTING,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getListing,
		hdfs_string_new(path),
		(begin ? hdfs_array_byte_copy(begin) : hdfs_array_byte_new(0, NULL))
	)
)

_HDFS_RPC_NB_DECL(renewLease,
	const char *client)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(renewLease,
		hdfs_string_new(client)
	)
)

_HDFS_PRIM_RPC_DECL(void, renewLease,
	const char *client)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(renewLease,
		hdfs_string_new(client)
	)
)

// XXX v1 only
_HDFS_RPC_NB_DECL(getStats)
_HDFS_RPC_NB_BODY(
	_HDFS_RPC_CASE(getStats),
	/*fall through to v2.2*/,
	/*fall through to default*/
)

_HDFS_OBJ_RPC_DECL(getStats)
_HDFS_OBJ_RPC_BODY(H_ARRAY_LONG,
	_HDFS_RPC_CASE(getStats),
	/*fall through to v2.2*/,
	/*fall through to default*/
)

// XXX uint64_t vs int64_t?
// XXX TODO implement for v2
_HDFS_RPC_NB_DECL(getPreferredBlockSize,
	const char *path)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getPreferredBlockSize,
		hdfs_string_new(path)
	)
)

_HDFS_PRIM_RPC_DECL(int64_t, getPreferredBlockSize,
	const char *path)
_HDFS_PRIM_RPC_BODY(H_LONG,
	int64_t res = object->ob_val._long._val,
	res,
	0,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getPreferredBlockSize,
		hdfs_string_new(path)
	)
)

// XXX make return nullable
_HDFS_RPC_NB_DECL(getFileInfo,
	const char *path)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getFileInfo,
		hdfs_string_new(path)
	)
)

_HDFS_OBJ_RPC_DECL(getFileInfo,
	const char *path)
_HDFS_OBJ_RPC_BODY(H_FILE_STATUS,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getFileInfo,
		hdfs_string_new(path)
	)
)

_HDFS_RPC_NB_DECL(getContentSummary,
	const char *path)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getContentSummary,
		hdfs_string_new(path)
	)
)

_HDFS_OBJ_RPC_DECL(getContentSummary,
	const char *path)
_HDFS_OBJ_RPC_BODY(H_CONTENT_SUMMARY,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getContentSummary,
		hdfs_string_new(path)
	)
)

// XXX StorageTypeProto
_HDFS_RPC_NB_DECL(setQuota,
	const char *path, int64_t ns_quota, int64_t ss_quota)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setQuota,
		hdfs_string_new(path),
		hdfs_long_new(ns_quota),
		hdfs_long_new(ss_quota)
	)
)

_HDFS_PRIM_RPC_DECL(void, setQuota,
	const char *path, int64_t ns_quota, int64_t ss_quota)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setQuota,
		hdfs_string_new(path),
		hdfs_long_new(ns_quota),
		hdfs_long_new(ss_quota)
	)
)

// XXX lastBlockLength
// XXX fileid
_HDFS_RPC_NB_DECL(fsync,
	const char *path, const char *client)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(fsync,
		hdfs_string_new(path),
		hdfs_string_new(client)
	)
)

_HDFS_PRIM_RPC_DECL(void, fsync,
	const char *path, const char *client)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(fsync,
		hdfs_string_new(path),
		hdfs_string_new(client)
	)
)

_HDFS_RPC_NB_DECL(setTimes,
	const char *path, int64_t mtime, int64_t atime)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setTimes,
		hdfs_string_new(path),
		hdfs_long_new(mtime),
		hdfs_long_new(atime)
	)
)

_HDFS_PRIM_RPC_DECL(void, setTimes,
	const char *path, int64_t mtime, int64_t atime)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setTimes,
		hdfs_string_new(path),
		hdfs_long_new(mtime),
		hdfs_long_new(atime)
	)
)

_HDFS_RPC_NB_DECL(recoverLease,
	const char *path, const char *client)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(recoverLease,
		hdfs_string_new(path),
		hdfs_string_new(client)
	)
)

_HDFS_PRIM_RPC_DECL(bool, recoverLease,
	const char *path, const char *client)
_HDFS_PRIM_RPC_BODY(H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(recoverLease,
		hdfs_string_new(path),
		hdfs_string_new(client)
	)
)

// XXX TODO v2 implementation
_HDFS_RPC_NB_DECL(concat,
	const char *target, struct hdfs_object *srcs)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(concat,
		hdfs_string_new(target),
		(srcs ? hdfs_array_string_copy(srcs) : hdfs_array_string_new(0, NULL))
	)
)

_HDFS_PRIM_RPC_DECL(void, concat,
	const char *target, struct hdfs_object *srcs)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(concat,
		hdfs_string_new(target),
		(srcs ? hdfs_array_string_copy(srcs) : hdfs_array_string_new(0, NULL))
	)
)

// XXX TODO v2 implementation
_HDFS_RPC_NB_DECL(getDelegationToken,
	const char *renewer)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getDelegationToken,
		hdfs_text_new(renewer)
	)
)

_HDFS_OBJ_RPC_DECL(getDelegationToken,
	const char *renewer)
_HDFS_OBJ_RPC_BODY(H_TOKEN,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getDelegationToken,
		hdfs_text_new(renewer)
	)
)

// XXX TODO v2 implementation
_HDFS_RPC_NB_DECL(cancelDelegationToken,
	struct hdfs_object *token)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(cancelDelegationToken,
		(token ? hdfs_token_copy(token) : hdfs_token_new_empty())
	)
)

_HDFS_PRIM_RPC_DECL(void, cancelDelegationToken,
	struct hdfs_object *token)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(cancelDelegationToken,
		(token ? hdfs_token_copy(token) : hdfs_token_new_empty())
	)
)

// XXX TODO v2 implementation
_HDFS_RPC_NB_DECL(renewDelegationToken,
	struct hdfs_object *token)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(renewDelegationToken,
		(token ? hdfs_token_copy(token) : hdfs_token_new_empty())
	)
)

_HDFS_PRIM_RPC_DECL(int64_t, renewDelegationToken,
	struct hdfs_object *token)
_HDFS_PRIM_RPC_BODY(H_LONG,
	int64_t res = object->ob_val._long._val,
	res,
	0,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(renewDelegationToken,
		(token ? hdfs_token_copy(token) : hdfs_token_new_empty())
	)
)

// XXX TODO implement for v2
_HDFS_RPC_NB_DECL(setSafeMode,
	const char *mode)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setSafeMode,
		hdfs_safemodeaction_new(mode)
	)
)

_HDFS_PRIM_RPC_DECL(bool, setSafeMode,
	const char *mode)
_HDFS_PRIM_RPC_BODY(H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setSafeMode,
		hdfs_safemodeaction_new(mode)
	)
)

// XXX TODO implement for v2
_HDFS_RPC_NB_DECL(getDatanodeReport,
	const char *mode)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getDatanodeReport,
		hdfs_dnreporttype_new(mode)
	)
)

_HDFS_OBJ_RPC_DECL(getDatanodeReport,
	const char *mode)
_HDFS_OBJ_RPC_BODY(H_ARRAY_DATANODE_INFO,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getDatanodeReport,
		hdfs_dnreporttype_new(mode)
	)
)

// XXX TODO implement for v2
_HDFS_RPC_NB_DECL(reportBadBlocks,
	struct hdfs_object *blocks)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(reportBadBlocks,
		(blocks ? hdfs_array_locatedblock_copy(blocks) : hdfs_array_locatedblock_new())
	)
)

_HDFS_PRIM_RPC_DECL(void, reportBadBlocks,
	struct hdfs_object *blocks)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(reportBadBlocks,
		(blocks ? hdfs_array_locatedblock_copy(blocks) : hdfs_array_locatedblock_new())
	)
)

// XXX v1 only
_HDFS_RPC_NB_DECL(distributedUpgradeProgress,
	const char *act)
_HDFS_RPC_NB_BODY(
	_HDFS_RPC_CASE(distributedUpgradeProgress,
		hdfs_upgradeaction_new(act)
	),
	/*fall through to v2.2*/,
	/*fall through to default*/
)

_HDFS_OBJ_RPC_DECL(distributedUpgradeProgress,
	const char *act)
_HDFS_OBJ_RPC_BODY(H_UPGRADE_STATUS_REPORT,
	_HDFS_RPC_CASE(distributedUpgradeProgress,
		hdfs_upgradeaction_new(act)
	),
	/*fall through to v2.2*/,
	/*fall through to default*/
)

// XXX TODO implement for v2
_HDFS_RPC_NB_DECL(finalizeUpgrade)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(finalizeUpgrade)
)

_HDFS_PRIM_RPC_DECL(void, finalizeUpgrade)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(finalizeUpgrade)
)

// XXX TODO implement for v2
_HDFS_RPC_NB_DECL(refreshNodes)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(refreshNodes)
)

_HDFS_PRIM_RPC_DECL(void, refreshNodes)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(refreshNodes)
)

// XXX timeWindow
// XXX txGap
// XXX returns optional bool (maybe? --- .proto still says void response in comment)
// XXX TODO implement for v2
_HDFS_RPC_NB_DECL(saveNamespace)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(saveNamespace)
)

_HDFS_PRIM_RPC_DECL(void, saveNamespace)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(saveNamespace)
)

// XXX TODO implement for v2
_HDFS_RPC_NB_DECL(metaSave,
	const char *filename)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(metaSave,
		hdfs_string_new(filename)
	)
)

_HDFS_PRIM_RPC_DECL(void, metaSave,
	const char *filename)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(metaSave,
		hdfs_string_new(filename)
	)
)

// XXX TODO implement for v2
_HDFS_RPC_NB_DECL(setBalancerBandwidth,
	int64_t bw)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setBalancerBandwidth,
		hdfs_long_new(bw)
	)
)

_HDFS_PRIM_RPC_DECL(void, setBalancerBandwidth,
	int64_t bw)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(setBalancerBandwidth,
		hdfs_long_new(bw)
	)
)

// XXX TODO implement for v2
_HDFS_RPC_NB_DECL(isFileClosed,
	const char *src)
_HDFS_RPC_NB_BODY(
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(isFileClosed,
		hdfs_string_new(src)
	)
)

_HDFS_PRIM_RPC_DECL(bool, isFileClosed,
	const char *src)
_HDFS_PRIM_RPC_BODY(H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	/*fall through to v2*/,
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(isFileClosed,
		hdfs_string_new(src)
	)
)

// XXX TODO merge all hdfs2_*() functions into hdfs_*() functions
#define _HDFS2_RPC_NB_DECL(name, args...) \
EXPORT_SYM struct hdfs_error \
hdfs2_ ## name ## _nb(struct hdfs_namenode *h, ##args, struct hdfs_rpc_response_future *future)

#define _HDFS2_PRIM_RPC_DECL(type, name, args...) \
EXPORT_SYM type \
hdfs2_ ## name (struct hdfs_namenode *h, ##args, struct hdfs_object **exception_out)

#define _HDFS2_OBJ_RPC_DECL(name, args...) \
EXPORT_SYM struct hdfs_object * \
hdfs2_ ## name (struct hdfs_namenode *h, ##args, struct hdfs_object **exception_out)

 // XXX perhaps change the below ASSERT(false)'s to ASSERT(h->nn_proto != HDFS_NN_v1)
 // to make the assertion print a more obvious?
_HDFS2_RPC_NB_DECL(getServerDefaults)
_HDFS_RPC_NB_BODY(
	ASSERT(false),
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getServerDefaults)
)

_HDFS2_OBJ_RPC_DECL(getServerDefaults)
_HDFS_OBJ_RPC_BODY(H_FS_SERVER_DEFAULTS,
	ASSERT(false),
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getServerDefaults)
)

_HDFS2_RPC_NB_DECL(getFileLinkInfo, const char *src)
_HDFS_RPC_NB_BODY(
	ASSERT(false),
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getFileLinkInfo,
		hdfs_string_new(src)
	)
)

_HDFS2_OBJ_RPC_DECL(getFileLinkInfo, const char *src)
_HDFS_OBJ_RPC_BODY(H_FILE_STATUS,
	ASSERT(false),
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getFileLinkInfo,
		hdfs_string_new(src)
	);
)

_HDFS2_RPC_NB_DECL(createSymlink,
	const char *target, const char *link, int16_t dirperm,
	bool createparent)
_HDFS_RPC_NB_BODY(
	ASSERT(false),
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(createSymlink,
		hdfs_string_new(target),
		hdfs_string_new(link),
		hdfs_fsperms_new(dirperm),
		hdfs_boolean_new(createparent)
	)
)

_HDFS2_PRIM_RPC_DECL(void, createSymlink,
	const char *target, const char *link, int16_t dirperm,
	bool createparent)
_HDFS_PRIM_RPC_BODY(H_VOID,
	,
	,
	,
	ASSERT(false),
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(createSymlink,
		hdfs_string_new(target),
		hdfs_string_new(link),
		hdfs_fsperms_new(dirperm),
		hdfs_boolean_new(createparent)
	)
)

_HDFS2_RPC_NB_DECL(getLinkTarget, const char *path)
_HDFS_RPC_NB_BODY(
	ASSERT(false),
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getLinkTarget,
		hdfs_string_new(path)
	)
)

_HDFS2_OBJ_RPC_DECL(getLinkTarget, const char *path)
_HDFS_OBJ_RPC_BODY(H_STRING,
	ASSERT(false),
	/*fall through to v2.2*/,
	_HDFS_RPC_CASE(getLinkTarget,
		hdfs_string_new(path)
	)
)
