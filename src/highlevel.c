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

EXPORT_SYM struct hdfs_namenode *
hdfs_namenode_new_version(const char *host, const char *port,
	const char *username, enum hdfs_kerb kerb_pref,
	enum hdfs_namenode_proto vers, struct hdfs_error *error_out)
{
	struct hdfs_error error;
	struct hdfs_namenode *h;

	h = hdfs_namenode_allocate();
	hdfs_namenode_init(h, kerb_pref);

	hdfs_namenode_set_version(h, vers);

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
	hdfs_namenode_destroy(h, (hdfs_namenode_destroy_cb)free);
}

// RPC implementations

#define _HDFS_PRIM_RPC_DECL(type, name, args...) \
EXPORT_SYM type \
hdfs_ ## name (struct hdfs_namenode *h, ##args, struct hdfs_object **exception_out)

#define _HDFS_PRIM_RPC_BODY(name, htype, result, retval, dflt, args...) \
{ \
	struct hdfs_rpc_response_future future; \
	struct hdfs_object *rpc, *object; \
	struct hdfs_error error; \
\
	_Static_assert(htype >= _H_START && htype < _H_V2_MAX, \
	    "htype must be a valid type"); \
\
	future.fu_inited = false; \
	hdfs_rpc_response_future_init(&future); \
	rpc = hdfs_rpc_invocation_new( \
	    #name, \
	    ##args, \
	    NULL); \
	error = hdfs_namenode_invoke(h, rpc, &future); \
	BAIL_ON_ERR(error); \
\
	hdfs_object_free(rpc); \
\
	hdfs_future_get(&future, &object); \
\
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
}

_HDFS_PRIM_RPC_DECL(int64_t, getProtocolVersion,
	const char *protocol, int64_t client_version)
_HDFS_PRIM_RPC_BODY(getProtocolVersion,
	H_LONG,
	int64_t res = object->ob_val._long._val,
	res,
	0,
	hdfs_string_new(protocol),
	hdfs_long_new(client_version)
)

#define _HDFS_OBJ_RPC_DECL(name, args...) \
EXPORT_SYM struct hdfs_object * \
hdfs_ ## name (struct hdfs_namenode *h, ##args, struct hdfs_object **exception_out)

#define _HDFS_OBJ_RPC_BODY_IMPL(name, void_ok, htype, args...) \
{ \
	struct hdfs_rpc_response_future future; \
	struct hdfs_object *rpc, *object; \
	struct hdfs_error error; \
\
	_Static_assert(void_ok == true || void_ok == false, \
	    "void_ok must be bool"); \
	_Static_assert(htype >= _H_START && htype < _H_V2_MAX, \
	    "htype must be a valid type"); \
\
	future.fu_inited = false; \
	hdfs_rpc_response_future_init(&future); \
	rpc = hdfs_rpc_invocation_new( \
	    #name, \
	    ##args, \
	    NULL); \
	error = hdfs_namenode_invoke(h, rpc, &future); \
	BAIL_ON_ERR(error); \
\
	hdfs_object_free(rpc); \
\
	hdfs_future_get(&future, &object); \
\
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
}

#define _HDFS_OBJ_RPC_BODY(name, htype, args...) _HDFS_OBJ_RPC_BODY_IMPL(name, false, htype, ## args)

_HDFS_OBJ_RPC_DECL(getBlockLocations,
	const char *path, int64_t offset, int64_t length)
_HDFS_OBJ_RPC_BODY(getBlockLocations,
	H_LOCATED_BLOCKS,
	hdfs_string_new(path),
	hdfs_long_new(offset),
	hdfs_long_new(length)
)

_HDFS_OBJ_RPC_DECL(create,
	const char *path, uint16_t perms, const char *clientname,
	bool overwrite, bool create_parent, int16_t replication,
	int64_t blocksize)
_HDFS_OBJ_RPC_BODY_IMPL(create,
	true,
	H_FILE_STATUS,
	hdfs_string_new(path),
	hdfs_fsperms_new(perms),
	hdfs_string_new(clientname),
	hdfs_boolean_new(overwrite),
	hdfs_boolean_new(create_parent),
	hdfs_short_new(replication),
	hdfs_long_new(blocksize)
)

_HDFS_OBJ_RPC_DECL(append,
	const char *path, const char *client)
_HDFS_OBJ_RPC_BODY(append,
	H_LOCATED_BLOCK,
	hdfs_string_new(path),
	hdfs_string_new(client)
)

_HDFS_PRIM_RPC_DECL(bool, setReplication,
	const char *path, int16_t replication)
_HDFS_PRIM_RPC_BODY(setReplication,
	H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	hdfs_string_new(path),
	hdfs_short_new(replication)
)

_HDFS_PRIM_RPC_DECL(void, setPermission,
	const char *path, int16_t perms)
_HDFS_PRIM_RPC_BODY(setPermission,
	H_VOID,
	,
	,
	,
	hdfs_string_new(path),
	hdfs_fsperms_new(perms)
)

_HDFS_PRIM_RPC_DECL(void, setOwner,
	const char *path, const char *owner, const char *group)
_HDFS_PRIM_RPC_BODY(setOwner,
	H_VOID,
	,
	,
	,
	hdfs_string_new(path),
	hdfs_string_new(owner),
	hdfs_string_new(group)
)

_HDFS_PRIM_RPC_DECL(void, abandonBlock,
	struct hdfs_object *block, const char *path, const char *client)
_HDFS_PRIM_RPC_BODY(abandonBlock,
	H_VOID,
	,
	,
	,
	hdfs_block_copy(block),
	hdfs_string_new(path),
	hdfs_string_new(client)
)

_HDFS_OBJ_RPC_DECL(addBlock,
	const char *path, const char *client, struct hdfs_object *excluded)
_HDFS_OBJ_RPC_BODY(addBlock,
	H_LOCATED_BLOCK,
	hdfs_string_new(path),
	hdfs_string_new(client),
	hdfs_array_datanode_info_copy(excluded)
)

_HDFS_PRIM_RPC_DECL(bool, complete,
	const char *path, const char *client)
_HDFS_PRIM_RPC_BODY(complete,
	H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	hdfs_string_new(path),
	hdfs_string_new(client)
)

_HDFS_PRIM_RPC_DECL(bool, rename,
	const char *src, const char *dst)
_HDFS_PRIM_RPC_BODY(rename,
	H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	hdfs_string_new(src),
	hdfs_string_new(dst)
)

_HDFS_PRIM_RPC_DECL(bool, delete,
	const char *path, bool can_recurse)
_HDFS_PRIM_RPC_BODY(delete,
	H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	hdfs_string_new(path),
	hdfs_boolean_new(can_recurse)
)

_HDFS_PRIM_RPC_DECL(bool, mkdirs,
	const char *path, int16_t perms)
_HDFS_PRIM_RPC_BODY(mkdirs,
	H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	hdfs_string_new(path),
	hdfs_fsperms_new(perms)
)

_HDFS_OBJ_RPC_DECL(getListing,
	const char *path, struct hdfs_object *begin)
_HDFS_OBJ_RPC_BODY(getListing,
	H_DIRECTORY_LISTING,
	hdfs_string_new(path),
	(begin? hdfs_array_byte_copy(begin) : hdfs_array_byte_new(0, NULL))
)

_HDFS_PRIM_RPC_DECL(void, renewLease,
	const char *client)
_HDFS_PRIM_RPC_BODY(renewLease,
	H_VOID,
	,
	,
	,
	hdfs_string_new(client)
)

_HDFS_OBJ_RPC_DECL(getStats)
_HDFS_OBJ_RPC_BODY(getStats,
	H_ARRAY_LONG
)

_HDFS_PRIM_RPC_DECL(int64_t, getPreferredBlockSize,
	const char *path)
_HDFS_PRIM_RPC_BODY(getPreferredBlockSize,
	H_LONG,
	int64_t res = object->ob_val._long._val,
	res,
	0,
	hdfs_string_new(path)
)

_HDFS_OBJ_RPC_DECL(getFileInfo,
	const char *path)
_HDFS_OBJ_RPC_BODY(getFileInfo,
	H_FILE_STATUS,
	hdfs_string_new(path)
)

_HDFS_OBJ_RPC_DECL(getContentSummary,
	const char *path)
_HDFS_OBJ_RPC_BODY(getContentSummary,
	H_CONTENT_SUMMARY,
	hdfs_string_new(path)
)

_HDFS_PRIM_RPC_DECL(void, setQuota,
	const char *path, int64_t ns_quota, int64_t ds_quota)
_HDFS_PRIM_RPC_BODY(setQuota,
	H_VOID,
	,
	,
	,
	hdfs_string_new(path),
	hdfs_long_new(ns_quota),
	hdfs_long_new(ds_quota)
)

_HDFS_PRIM_RPC_DECL(void, fsync,
	const char *path, const char *client)
_HDFS_PRIM_RPC_BODY(fsync,
	H_VOID,
	,
	,
	,
	hdfs_string_new(path),
	hdfs_string_new(client)
)

_HDFS_PRIM_RPC_DECL(void, setTimes,
	const char *path, int64_t mtime, int64_t atime)
_HDFS_PRIM_RPC_BODY(setTimes,
	H_VOID,
	,
	,
	,
	hdfs_string_new(path),
	hdfs_long_new(mtime),
	hdfs_long_new(atime)
)

_HDFS_PRIM_RPC_DECL(bool, recoverLease,
	const char *path, const char *client)
_HDFS_PRIM_RPC_BODY(recoverLease,
	H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	hdfs_string_new(path),
	hdfs_string_new(client)
)

_HDFS_PRIM_RPC_DECL(void, concat,
	const char *target, struct hdfs_object *srcs)
_HDFS_PRIM_RPC_BODY(concat,
	H_VOID,
	,
	,
	,
	hdfs_string_new(target),
	(srcs? hdfs_array_string_copy(srcs) : hdfs_array_string_new(0, NULL))
)

_HDFS_OBJ_RPC_DECL(getDelegationToken,
	const char *renewer)
_HDFS_OBJ_RPC_BODY(getDelegationToken,
	H_TOKEN,
	hdfs_text_new(renewer)
)

_HDFS_PRIM_RPC_DECL(void, cancelDelegationToken,
	struct hdfs_object *token)
_HDFS_PRIM_RPC_BODY(cancelDelegationToken,
	H_VOID,
	,
	,
	,
	(token? hdfs_token_copy(token) : hdfs_token_new_empty())
)

_HDFS_PRIM_RPC_DECL(int64_t, renewDelegationToken,
	struct hdfs_object *token)
_HDFS_PRIM_RPC_BODY(renewDelegationToken,
	H_LONG,
	int64_t res = object->ob_val._long._val,
	res,
	0,
	(token? hdfs_token_copy(token) : hdfs_token_new_empty())
)

_HDFS_PRIM_RPC_DECL(bool, setSafeMode,
	const char *mode)
_HDFS_PRIM_RPC_BODY(setSafeMode,
	H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	hdfs_safemodeaction_new(mode)
)

_HDFS_OBJ_RPC_DECL(getDatanodeReport,
	const char *mode)
_HDFS_OBJ_RPC_BODY(getDatanodeReport,
	H_ARRAY_DATANODE_INFO,
	hdfs_dnreporttype_new(mode)
)

_HDFS_PRIM_RPC_DECL(void, reportBadBlocks,
	struct hdfs_object *blocks)
_HDFS_PRIM_RPC_BODY(reportBadBlocks,
	H_VOID,
	,
	,
	,
	(blocks? hdfs_array_locatedblock_copy(blocks) :
	 hdfs_array_locatedblock_new())
)

_HDFS_OBJ_RPC_DECL(distributedUpgradeProgress,
	const char *act)
_HDFS_OBJ_RPC_BODY(distributedUpgradeProgress,
	H_UPGRADE_STATUS_REPORT,
	hdfs_upgradeaction_new(act)
)

_HDFS_PRIM_RPC_DECL(void, finalizeUpgrade)
_HDFS_PRIM_RPC_BODY(finalizeUpgrade,
	H_VOID,
	,
	,
	,
	NULL /* No args */
)

_HDFS_PRIM_RPC_DECL(void, refreshNodes)
_HDFS_PRIM_RPC_BODY(refreshNodes,
	H_VOID,
	,
	,
	,
	NULL /* No args */
)

_HDFS_PRIM_RPC_DECL(void, saveNamespace)
_HDFS_PRIM_RPC_BODY(saveNamespace,
	H_VOID,
	,
	,
	,
	NULL /* No args */
)

_HDFS_PRIM_RPC_DECL(void, metaSave,
	const char *filename)
_HDFS_PRIM_RPC_BODY(metaSave,
	H_VOID,
	,
	,
	,
	hdfs_string_new(filename)
)

_HDFS_PRIM_RPC_DECL(void, setBalancerBandwidth,
	int64_t bw)
_HDFS_PRIM_RPC_BODY(setBalancerBandwidth,
	H_VOID,
	,
	,
	,
	hdfs_long_new(bw)
)

_HDFS_PRIM_RPC_DECL(bool, isFileClosed,
	const char *src)
_HDFS_PRIM_RPC_BODY(isFileClosed,
	H_BOOLEAN,
	bool res = object->ob_val._boolean._val,
	res,
	false,
	hdfs_string_new(src)
)

#define _HDFS2_PRIM_RPC_DECL(type, name, args...) \
EXPORT_SYM type \
hdfs2_ ## name (struct hdfs_namenode *h, ##args, struct hdfs_object **exception_out)

#define _HDFS2_OBJ_RPC_DECL(name, args...) \
EXPORT_SYM struct hdfs_object * \
hdfs2_ ## name (struct hdfs_namenode *h, ##args, struct hdfs_object **exception_out)

_HDFS2_OBJ_RPC_DECL(getServerDefaults)
_HDFS_OBJ_RPC_BODY(getServerDefaults,
	H_FS_SERVER_DEFAULTS,
	NULL
)

_HDFS2_OBJ_RPC_DECL(getFileLinkInfo, const char *src)
_HDFS_OBJ_RPC_BODY(getFileLinkInfo,
	H_FILE_STATUS,
	hdfs_string_new(src)
)

_HDFS2_PRIM_RPC_DECL(void, createSymlink,
	const char *target, const char *link, int16_t dirperm,
	bool createparent)
_HDFS_PRIM_RPC_BODY(createSymlink,
	H_VOID,
	,
	,
	,
	hdfs_string_new(target),
	hdfs_string_new(link),
	hdfs_fsperms_new(dirperm),
	hdfs_boolean_new(createparent)
)

_HDFS2_OBJ_RPC_DECL(getLinkTarget, const char *path)
_HDFS_OBJ_RPC_BODY(getLinkTarget,
	H_STRING,
	hdfs_string_new(path)
)
