#ifndef HADOOFUS_HIGHLEVEL_H
#define HADOOFUS_HIGHLEVEL_H

#include <assert.h>
#include <stdbool.h>

#include <hadoofus/lowlevel.h>

//
// This is the high-level HDFS API.
//

// Creates a new namenode connection. On error, returns NULL and sets
// *error_out to a description of the problem.
struct hdfs_namenode *	hdfs_namenode_new(const char *host, const char *port,
				const char *username, const char **error_out);

// Tears down the connection and frees memory.
void			hdfs_namenode_delete(struct hdfs_namenode *);

static inline bool
hdfs_object_is_exception(struct hdfs_object *o)
{
	assert(o);
	return o->ob_type == H_PROTOCOL_EXCEPTION;
}

static inline enum hdfs_object_type
hdfs_exception_get_type(struct hdfs_object *o)
{
	assert(o);
	assert(o->ob_type == H_PROTOCOL_EXCEPTION);

	return o->ob_val._exception._etype;
}

//
// Hadoop ClientProtocol API:
//
// TODO: borrow individual routine documentation from pyhdfs
//
// These routines set exception_out on exception; callers should check this
// first. When an RPC sets exception_out, the return value is undefined.
// (Callers are responsible for freeing the exception object.)
//
// When these routines return an hdfs_object, the caller is responsible for
// freeing it. The hdfs_object will be an H_NULL value or the expected type for
// the given RPC.
//

int64_t			hdfs_getProtocolVersion(struct hdfs_namenode *,
			const char *protocol, int64_t client_version,
			struct hdfs_object **exception_out);

struct hdfs_object *	hdfs_getBlockLocations(struct hdfs_namenode *, const char *path,
			int64_t offset, int64_t length, struct hdfs_object **exception_out);

void			hdfs_create(struct hdfs_namenode *, const char *path,
			uint16_t perms, const char *clientname, bool overwrite,
			bool create_parent, int16_t replication, int64_t blocksize,
			struct hdfs_object **exception_out);

struct hdfs_object *	hdfs_append(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object **exception_out);

bool			hdfs_setReplication(struct hdfs_namenode *, const char *path,
			int16_t replication, struct hdfs_object **exception_out);

void			hdfs_setPermission(struct hdfs_namenode *, const char *path,
			int16_t perms, struct hdfs_object **exception_out);

void			hdfs_setOwner(struct hdfs_namenode *, const char *path,
			const char *owner, const char *group, struct hdfs_object **exception_out);

void			hdfs_abandonBlock(struct hdfs_namenode *, struct hdfs_object *block,
			const char *path, const char *client, struct hdfs_object **exception_out);

struct hdfs_object *	hdfs_addBlock(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object *excluded,
			struct hdfs_object **exception_out);

bool			hdfs_complete(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object **exception_out);

bool			hdfs_rename(struct hdfs_namenode *, const char *src,
			const char *dst, struct hdfs_object **exception_out);

bool			hdfs_delete(struct hdfs_namenode *, const char *path,
			bool can_recurse, struct hdfs_object **exception_out);

bool			hdfs_mkdirs(struct hdfs_namenode *, const char *path,
			int16_t perms, struct hdfs_object **exception_out);

struct hdfs_object *	hdfs_getListing(struct hdfs_namenode *, const char *path,
			struct hdfs_object *begin, struct hdfs_object **exception_out);

void			hdfs_renewLease(struct hdfs_namenode *, const char *client,
			struct hdfs_object **exception_out);

struct hdfs_object *	hdfs_getStats(struct hdfs_namenode *,
			struct hdfs_object **exception_out);

int64_t			hdfs_getPreferredBlockSize(struct hdfs_namenode *,
			const char *path, struct hdfs_object **exception_out);

struct hdfs_object *	hdfs_getFileInfo(struct hdfs_namenode *,
			const char *path, struct hdfs_object **exception_out);

struct hdfs_object *	hdfs_getContentSummary(struct hdfs_namenode *,
			const char *path, struct hdfs_object **exception_out);

void			hdfs_setQuota(struct hdfs_namenode *, const char *path,
			int64_t ns_quota, int64_t ds_quota, struct hdfs_object **exception_out);

void			hdfs_fsync(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object **exception_out);

void			hdfs_setTimes(struct hdfs_namenode *, const char *path,
			int64_t mtime, int64_t atime, struct hdfs_object **exception_out);

bool			hdfs_recoverLease(struct hdfs_namenode *, const char *path,
			const char *client, struct hdfs_object **exception_out);

#endif
