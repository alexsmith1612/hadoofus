#ifndef _HADOOFUS_OBJECTS_H
#define _HADOOFUS_OBJECTS_H

#include <string.h>

#include <hadoofus/objects.h>

#define NULL_TYPE1 "org.apache.hadoop.io.Writable"
#define NULL_TYPE2 "org.apache.hadoop.io.ObjectWritable$NullInstance"
#define VOID_TYPE NULL_TYPE1
#define BOOLEAN_TYPE "boolean"
#define SHORT_TYPE "short"
#define INT_TYPE "int"
#define LONG_TYPE "long"
#define ARRAYLONG_TYPE "[J"
#define DATANODEINFO_TYPE "org.apache.hadoop.hdfs.protocol.DatanodeInfo"
#define ARRAYDATANODEINFO_TYPE "[Lorg.apache.hadoop.hdfs.protocol.DatanodeInfo;"
#define LOCATEDBLOCK_TYPE "org.apache.hadoop.hdfs.protocol.LocatedBlock"
#define LOCATEDBLOCKS_TYPE "org.apache.hadoop.hdfs.protocol.LocatedBlocks"
#define DIRECTORYLISTING_TYPE "org.apache.hadoop.hdfs.protocol.DirectoryListing"
#define LOCATEDDIRECTORYLISTING_TYPE "org.apache.hadoop.hdfs.protocol.LocatedDirectoryListing"
#define FILESTATUS_TYPE "org.apache.hadoop.hdfs.protocol.HdfsFileStatus"
#define CONTENTSUMMARY_TYPE "org.apache.hadoop.fs.ContentSummary"
#define UPGRADESTATUSREPORT_TYPE "org.apache.hadoop.hdfs.server.common.UpgradeStatusReport"
#define BLOCK_TYPE "org.apache.hadoop.hdfs.protocol.Block"
#define ARRAYBYTE_TYPE "[B"
#define TOKEN_TYPE "org.apache.hadoop.security.token.Token"
#define STRING_TYPE "java.lang.String"
#define FSPERMS_TYPE "org.apache.hadoop.fs.permission.FsPermission"
#define ARRAYSTRING_TYPE "[Ljava.lang.String;"
#define TEXT_TYPE "org.apache.hadoop.io.Text"

#define CLIENT_PROTOCOL HADOOFUS_CLIENT_PROTOCOL_STR

#define ACCESS_EXCEPTION_STR "org.apache.hadoop.security.AccessControlException"
#define ALREADY_BEING_EXCEPTION_STR "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException"
#define NOT_FOUND_EXCEPTION_STR "java.io.FileNotFoundException"
#define IO_EXCEPTION_STR "java.io.IOException"
#define LEASE_EXCEPTION_STR "org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException"

struct _hdfs_result {
	int64_t rs_msgno;
	struct hdfs_object *rs_obj;
};

void			_rpc_invocation_set_msgno(struct hdfs_object *, int32_t);

// Returns _HDFS_INVALID_PROTO if the buffer contains invalid protocol data.
// Returns NULL if we can't decode a response from the available buffer.
// Otherwise, returns a result object.
struct _hdfs_result *	_hdfs_result_deserialize(char *buf, int buflen, int *obj_size);
void			_hdfs_result_free(struct _hdfs_result *);

enum hdfs_object_type	_string_to_type(const char *);

static inline bool
streq(const char *a, const char *b)
{
	return strcmp(a, b) == 0;
}

extern struct _hdfs_result *_HDFS_INVALID_PROTO;

#endif
