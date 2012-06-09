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

int64_t			hdfs_namenode_getProtocolVersion(struct hdfs_namenode *,
			const char *protocol, int64_t client_version,
			struct hdfs_object **exception_out);

struct hdfs_object *	hdfs_namenode_getBlockLocations(struct hdfs_namenode *, const char *path,
			int64_t offset, int64_t length, struct hdfs_object **exception_out);

void			hdfs_namenode_create(struct hdfs_namenode *, const char *path,
			uint16_t perms, const char *clientname, bool overwrite,
			bool create_parent, int16_t replication, int64_t blocksize,
			struct hdfs_object **exception_out);

// TODO add the rest

#endif
