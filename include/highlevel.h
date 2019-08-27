#ifndef HADOOFUS_HIGHLEVEL_H
#define HADOOFUS_HIGHLEVEL_H

#include <assert.h>
#include <stdbool.h>

#include <hadoofus/lowlevel.h>

//
// This is the high-level HDFS API.
//

// Creates a new namenode connection. On error, returns NULL and sets
// *error_out to an error value.
//
// Kerb setting one of:
//   HDFS_NO_KERB      -- "Authenticate" with plaintext username (hadoop default)
//   HDFS_TRY_KERB     -- attempt kerb, but allow fallback to plaintext
//   HDFS_REQUIRE_KERB -- fail if server attempts to fallback to plaintext
//
// N.B.: TRY_KERB or REQUIRE_KERB mean the caller has already initialized SASL,
// using sasl_client_init().
//
// Protocol version setting one of:
//   HDFS_NN_v1
//   HDFS_NN_v2
//   HDFS_NN_v2_2
struct hdfs_namenode *	hdfs_namenode_new_version(const char *host,
				const char *port, const char *username,
				enum hdfs_kerb, enum hdfs_namenode_proto,
				struct hdfs_error *error_out);

// Tears down the connection and frees memory.
void			hdfs_namenode_delete(struct hdfs_namenode *);

static inline bool
hdfs_object_is_null(struct hdfs_object *o)
{
	assert(o);
	return o->ob_type == H_NULL;
}

static inline enum hdfs_object_type
hdfs_null_type(struct hdfs_object *o)
{
	assert(o);
	assert(o->ob_type == H_NULL);

	return o->ob_val._null._type;
}

static inline bool
hdfs_object_is_exception(struct hdfs_object *o)
{
	assert(o);
	return o->ob_type == H_PROTOCOL_EXCEPTION;
}

static inline enum hdfs_exception_type
hdfs_exception_get_type(struct hdfs_object *o)
{
	assert(o);
	assert(o->ob_type == H_PROTOCOL_EXCEPTION);

	return o->ob_val._exception._etype;
}

static inline const char *
hdfs_exception_get_message(struct hdfs_object *o)
{
	assert(o);
	assert(o->ob_type == H_PROTOCOL_EXCEPTION);

	return o->ob_val._exception._msg;
}

//
// Hadoop ClientProtocol API:
//
// TODO: borrow individual routine documentation from pyhdfs
// (or from Apache's hadoop-hdfs-project/hadoop-hdfs-client/.../ClientProtocol.java)
//
// Each ClientProtocol RPC has a blocking API and a non-blocking API (the same
// function name suffixed with _nb).
//

//
// Blocking API:
//
// These routines set *exception_out on exception; callers should check this
// first. When an RPC sets *exception_out, the return value is undefined.
// (Callers are responsible for freeing the exception object.)
//
// When these routines return an hdfs_object, the caller is responsible for
// freeing it. The hdfs_object will be the expected type for the given RPC.
//
// Some routines may return NULL and set *exception_out to NULL. This indicates
// a successful RPC invocation, with NULL or no result.
//
// N.B. These functions abort(3) on errors below the ClientProtocol level (such
// as socket errors)
//
// N.B. These functions assume that there are no pending RPCs when they are
// called, that is, the blocking API may not be used while any non-blocking API
// RPC is waiting for its response.
//

//
// Non-blocking API (suffixed with _nb):
//
// XXX consider moving the non-blocking functions to lowlevel.h (but it makes
// sense to have them here, paired with their blocking counterparts, in order to
// minimize the risk of discrepancies appearing between the two during
// development)
//
// These routines assume that *msgno is a valid pointer to an int64_t. They
// invoke the given RPC and set *msgno to the given msgno for this RPC
// invocation, which the user must then use to correlate objects received from
// hdfs_namenode_recv() with their given RPC invocation (see lowlevel.h).
//
// userdata is a pointer that will be passed back to the caller when the result
// is returned by hdfs_namenode_recv() (this has the same semantics as
// hdfs_namenode_invoke()---see lowlevel.h)
//
// Their return values match that of hdfs_namenode_invoke() and should be
// handled equivalently
//

struct hdfs_error	hdfs_getProtocolVersion_nb(struct hdfs_namenode *,
			const char *protocol, int64_t client_version,
			int64_t *msgno, void *userdata);

int64_t			hdfs_getProtocolVersion(struct hdfs_namenode *,
			const char *protocol, int64_t client_version,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs_getBlockLocations_nb(struct hdfs_namenode *, const char *path,
			int64_t offset, int64_t length, int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs_getBlockLocations(struct hdfs_namenode *, const char *path,
			int64_t offset, int64_t length, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_create_nb(struct hdfs_namenode *, const char *path,
			uint16_t perms, const char *clientname, bool overwrite,
			bool create_parent, int16_t replication, int64_t blocksize,
			int64_t *msgno, void *userdata);

// 'void' prior to v2.2, and result will always be NULL for those versions.
struct hdfs_object *	hdfs_create(struct hdfs_namenode *, const char *path,
			uint16_t perms, const char *clientname, bool overwrite,
			bool create_parent, int16_t replication, int64_t blocksize,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs_append_nb(struct hdfs_namenode *, const char *path,
			const char *client, int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs_append(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_setReplication_nb(struct hdfs_namenode *, const char *path,
			int16_t replication, int64_t *msgno, void *userdata);

bool			hdfs_setReplication(struct hdfs_namenode *, const char *path,
			int16_t replication, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_setPermission_nb(struct hdfs_namenode *, const char *path,
			int16_t perms, int64_t *msgno, void *userdata);

void			hdfs_setPermission(struct hdfs_namenode *, const char *path,
			int16_t perms, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_setOwner_nb(struct hdfs_namenode *, const char *path,
			const char *owner, const char *group, int64_t *msgno, void *userdata);

void			hdfs_setOwner(struct hdfs_namenode *, const char *path,
			const char *owner, const char *group, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_abandonBlock_nb(struct hdfs_namenode *, struct hdfs_object *block,
			const char *path, const char *client, int64_t *msgno, void *userdata);

void			hdfs_abandonBlock(struct hdfs_namenode *, struct hdfs_object *block,
			const char *path, const char *client, struct hdfs_object **exception_out);

/*
 * 'previous_block' is an H_BLOCK hdfs_block object. One can be obtained from a
 * H_LOCATED_BLOCK with hdfs_block_from_located_block(). It should be NULL if and
 * only if there is no previous block (i.e. this is adding the first block to the
 * file). If previous_block is not NULL, then its _length member must must be set
 * to the block's correct current size (after any datanode writes that may have
 * been performed).
 *
 * In v1 the arguments 'previous_block' and 'fileid' are ignored (i.e. just pass
 * NULL and 0 for them, respectively)
 *
 * FileId is new in 2.2; pass zero for 2.0 and earlier versions.
 */
struct hdfs_error	hdfs_addBlock_nb(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object *excluded,
			struct hdfs_object *previous_block, int64_t fileid,
			int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs_addBlock(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object *excluded,
			struct hdfs_object *previous_block, int64_t fileid,
			struct hdfs_object **exception_out);

/*
 * 'last_block' is an H_BLOCK hdfs_block object. One can be obtained from a
 * H_LOCATED_BLOCK with hdfs_block_from_located_block(). It should be NULL if and
 * only if there is no last block (i.e. this is completing an empty file that has
 * had no blocks added to it). If is not NULL, then its _length member must must
 * be set to the block's correct current size (after any datanode writes that may
 * have been performed).
 *
 * In v1 the arguments 'last_block' and 'fileid' are ignored (i.e. just pass NULL
 * and 0 for them, respectively)
 *
 * FileId is new in 2.2; pass zero for 2.0 and earlier versions.
 */
struct hdfs_error	hdfs_complete_nb(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object *last_block,
			int64_t fileid, int64_t *msgno, void *userdata);

bool			hdfs_complete(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object *last_block,
			int64_t fileid, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_rename_nb(struct hdfs_namenode *, const char *src,
			const char *dst, int64_t *msgno, void *userdata);

bool			hdfs_rename(struct hdfs_namenode *, const char *src,
			const char *dst, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_delete_nb(struct hdfs_namenode *, const char *path,
			bool can_recurse, int64_t *msgno, void *userdata);

bool			hdfs_delete(struct hdfs_namenode *, const char *path,
			bool can_recurse, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_mkdirs_nb(struct hdfs_namenode *, const char *path,
			int16_t perms, int64_t *msgno, void *userdata);

bool			hdfs_mkdirs(struct hdfs_namenode *, const char *path,
			int16_t perms, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_getListing_nb(struct hdfs_namenode *, const char *path,
			struct hdfs_object *begin, int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs_getListing(struct hdfs_namenode *, const char *path,
			struct hdfs_object *begin, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_renewLease_nb(struct hdfs_namenode *, const char *client,
			int64_t *msgno, void *userdata);

void			hdfs_renewLease(struct hdfs_namenode *, const char *client,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs_getStats_nb(struct hdfs_namenode *,
			int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs_getStats(struct hdfs_namenode *,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs_getPreferredBlockSize_nb(struct hdfs_namenode *,
			const char *path, int64_t *msgno, void *userdata);

int64_t			hdfs_getPreferredBlockSize(struct hdfs_namenode *,
			const char *path, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_getFileInfo_nb(struct hdfs_namenode *,
			const char *path, int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs_getFileInfo(struct hdfs_namenode *,
			const char *path, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_getContentSummary_nb(struct hdfs_namenode *,
			const char *path, int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs_getContentSummary(struct hdfs_namenode *,
			const char *path, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_setQuota_nb(struct hdfs_namenode *, const char *path,
			int64_t ns_quota, int64_t ss_quota, int64_t *msgno, void *userdata);

void			hdfs_setQuota(struct hdfs_namenode *, const char *path,
			int64_t ns_quota, int64_t ss_quota, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_fsync_nb(struct hdfs_namenode *, const char *path,
			const char *client, int64_t *msgno, void *userdata);

void			hdfs_fsync(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_setTimes_nb(struct hdfs_namenode *, const char *path,
			int64_t mtime, int64_t atime, int64_t *msgno, void *userdata);

void			hdfs_setTimes(struct hdfs_namenode *, const char *path,
			int64_t mtime, int64_t atime, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_recoverLease_nb(struct hdfs_namenode *, const char *path,
			const char *client, int64_t *msgno, void *userdata);

bool			hdfs_recoverLease(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_concat_nb(struct hdfs_namenode *, const char *target,
			struct hdfs_object *srcs, int64_t *msgno, void *userdata);

void			hdfs_concat(struct hdfs_namenode *, const char *target,
			struct hdfs_object *srcs, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_getDelegationToken_nb(struct hdfs_namenode *, const char *renewer,
			int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs_getDelegationToken(struct hdfs_namenode *, const char *renewer,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs_cancelDelegationToken_nb(struct hdfs_namenode *, struct hdfs_object *,
			int64_t *msgno, void *userdata);

void			hdfs_cancelDelegationToken(struct hdfs_namenode *, struct hdfs_object *,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs_renewDelegationToken_nb(struct hdfs_namenode *, struct hdfs_object *,
			int64_t *msgno, void *userdata);

int64_t			hdfs_renewDelegationToken(struct hdfs_namenode *, struct hdfs_object *,
			struct hdfs_object **exception_out);

#define			HDFS_SAFEMODE_ENTER	"SAFEMODE_ENTER"
#define			HDFS_SAFEMODE_LEAVE	"SAFEMODE_LEAVE"
#define			HDFS_SAFEMODE_GET	"SAFEMODE_GET"
struct hdfs_error	hdfs_setSafeMode_nb(struct hdfs_namenode *, const char *safemodeaction,
			int64_t *msgno, void *userdata);

bool			hdfs_setSafeMode(struct hdfs_namenode *, const char *safemodeaction,
			struct hdfs_object **exception_out);

#define			HDFS_DNREPORT_ALL	"ALL"
#define			HDFS_DNREPORT_LIVE	"LIVE"
#define			HDFS_DNREPORT_DEAD	"DEAD"
struct hdfs_error	hdfs_getDatanodeReport_nb(struct hdfs_namenode *, const char *dnreporttype,
			int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs_getDatanodeReport(struct hdfs_namenode *, const char *dnreporttype,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs_reportBadBlocks_nb(struct hdfs_namenode *, struct hdfs_object *blocks,
			int64_t *msgno, void *userdata);

void			hdfs_reportBadBlocks(struct hdfs_namenode *, struct hdfs_object *blocks,
			struct hdfs_object **exception_out);

#define			HDFS_UPGRADEACTION_STATUS		"GET_STATUS"
#define			HDFS_UPGRADEACTION_DETAILED		"DETAILED_STATUS"
#define			HDFS_UPGRADEACTION_FORCE_PROCEED	"FORCE_PROCEED"
struct hdfs_error	hdfs_distributedUpgradeProgress_nb(struct hdfs_namenode *, const char *,
			int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs_distributedUpgradeProgress(struct hdfs_namenode *, const char *,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs_finalizeUpgrade_nb(struct hdfs_namenode *, int64_t *msgno, void *userdata);

void			hdfs_finalizeUpgrade(struct hdfs_namenode *, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_refreshNodes_nb(struct hdfs_namenode *, int64_t *msgno, void *userdata);

void			hdfs_refreshNodes(struct hdfs_namenode *, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_saveNamespace_nb(struct hdfs_namenode *, int64_t *msgno, void *userdata);

void			hdfs_saveNamespace(struct hdfs_namenode *, struct hdfs_object **exception_out);

struct hdfs_error	hdfs_isFileClosed_nb(struct hdfs_namenode *, const char *,
			int64_t *msgno, void *userdata);

bool			hdfs_isFileClosed(struct hdfs_namenode *, const char *,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs_metaSave_nb(struct hdfs_namenode *, const char *,
			int64_t *msgno, void *userdata);

void			hdfs_metaSave(struct hdfs_namenode *, const char *,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs_setBalancerBandwidth_nb(struct hdfs_namenode *, int64_t,
			int64_t *msgno, void *userdata);

void			hdfs_setBalancerBandwidth(struct hdfs_namenode *, int64_t,
			struct hdfs_object **exception_out);

// HDFSv2+

struct hdfs_error	hdfs2_getServerDefaults_nb(struct hdfs_namenode *,
			int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs2_getServerDefaults(struct hdfs_namenode *,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs2_getFileLinkInfo_nb(struct hdfs_namenode *, const char *,
			int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs2_getFileLinkInfo(struct hdfs_namenode *, const char *,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs2_createSymlink_nb(struct hdfs_namenode *, const char * /*target*/,
			const char * /*link*/, int16_t /*dirperms*/, bool /*createparent*/,
			int64_t *msgno, void *userdata);

void			hdfs2_createSymlink(struct hdfs_namenode *, const char * /*target*/,
			const char * /*link*/, int16_t /*dirperms*/, bool /*createparent*/,
			struct hdfs_object **exception_out);

struct hdfs_error	hdfs2_getLinkTarget_nb(struct hdfs_namenode *, const char *,
			int64_t *msgno, void *userdata);

struct hdfs_object *	hdfs2_getLinkTarget(struct hdfs_namenode *, const char *,
			struct hdfs_object **exception_out);

//
// High-level Datanode API
//

// Creates a new datanode connection (blocking). On error, returns NULL and sets
// *error_out to an error value.
struct hdfs_datanode *	hdfs_datanode_new(struct hdfs_object *located_block,
			const char *client, int proto, bool crcs,
			struct hdfs_error *error_out);

// Destroys the connection and frees memory.
void			hdfs_datanode_delete(struct hdfs_datanode *);

#endif
