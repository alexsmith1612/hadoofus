#ifndef _HADOOFUS_OBJECTS_H
#define _HADOOFUS_OBJECTS_H

#include <string.h>

#include <hadoofus/objects.h>

#include "hdfs.pb-c.h"

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
#define SAFEMODEACTION_TYPE "org.apache.hadoop.hdfs.protocol.FSConstants$SafeModeAction"
#define DNREPORTTYPE_TYPE "org.apache.hadoop.hdfs.protocol.FSConstants$DatanodeReportType"
#define ARRAYLOCATEDBLOCK_TYPE "[L" LOCATEDBLOCK_TYPE ";"
#define UPGRADEACTION_TYPE "org.apache.hadoop.hdfs.protocol.FSConstants$UpgradeAction"

#define CLIENT_PROTOCOL HADOOFUS_CLIENT_PROTOCOL_STR

#define ACCESS_EXCEPTION_STR "org.apache.hadoop.security.AccessControlException"
#define ALREADY_BEING_EXCEPTION_STR "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException"
#define NOT_FOUND_EXCEPTION_STR "java.io.FileNotFoundException"
#define IO_EXCEPTION_STR "java.io.IOException"
#define LEASE_EXCEPTION_STR "org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException"

#define SECURITY_EXCEPTION_STR "java.lang.SecurityException"
#define QUOTA_EXCEPTION_STR "org.apache.hadoop.hdfs.protocol.DSQuotaExceededException"
#define ILLARG_EXCEPTION_STR "java.lang.IllegalArgumentException"
#define INVTOK_EXCEPTION_STR "org.apache.hadoop.security.token.SecretManager$InvalidToken"
#define INVPATH_EXCEPTION_STR "org.apache.hadoop.fs.InvalidPathException"
#define EEXIST_EXCEPTION_STR "org.apache.hadoop.fs.FileAlreadyExistsException"
#define IPC_EXCEPTION_STR "org.apache.hadoop.ipc.IpcException"
#define SASL_EXCEPTION_STR "javax.security.sasl.SaslException"
#define RPC_EXCEPTION_STR "org.apache.hadoop.ipc.RpcServerException"
#define RPC_ENOENT_EXCEPTION_STR "org.apache.hadoop.ipc.RpcNoSuchMethodException"

struct _hdfs_result {
	int64_t rs_msgno;
	struct hdfs_object *rs_obj;
	int rs_size;
};

struct _hdfs_pending {
	int64_t pd_msgno;
	struct hdfs_object *(*pd_slurper)(struct hdfs_heap_buf *);
	void *pd_userdata;
};

void			_rpc_invocation_set_msgno(struct hdfs_object *, int32_t);
void			_rpc_invocation_set_proto(struct hdfs_object *,
			enum hdfs_namenode_proto pr);
void			_rpc_invocation_set_clientid(struct hdfs_object *, uint8_t *);

void			_authheader_set_clientid(struct hdfs_object *, uint8_t *);

// Returns HDFS_SUCCESS and populates *res on success.
// Returns HDFS_AGAIN if we can't decode a response from the available buffer.
// Returns other error code otherwise.
struct hdfs_error	_hdfs_result_deserialize(char *buf, int buflen, struct _hdfs_result *res);
struct hdfs_error	_hdfs_result_deserialize_v2(char *buf, int buflen, struct _hdfs_result *res,
			struct _hdfs_pending *pend, int npend);
struct hdfs_error	_hdfs_result_deserialize_v2_2(char *buf, int buflen, struct _hdfs_result *res,
			struct _hdfs_pending *pend, int npend);

enum hdfs_object_type	_string_to_type(const char *);

static inline bool
streq(const char *a, const char *b)
{
	return strcmp(a, b) == 0;
}

// HDFSv2+ protobuf-to-hdfs_object converters
enum hdfs_checksum_type	_hdfs_csum_from_proto(Hadoop__Hdfs__ChecksumTypeProto);
enum hdfs_file_type	_hdfs_file_type_from_proto(Hadoop__Hdfs__HdfsFileStatusProto__FileType);

struct hdfs_object *	_hdfs_fsserverdefaults_new_proto(Hadoop__Hdfs__FsServerDefaultsProto *);
struct hdfs_object *	_hdfs_directory_listing_new_proto(Hadoop__Hdfs__DirectoryListingProto *);
struct hdfs_object *	_hdfs_file_status_new_proto(Hadoop__Hdfs__HdfsFileStatusProto *);
struct hdfs_object *	_hdfs_located_blocks_new_proto(Hadoop__Hdfs__LocatedBlocksProto *);
struct hdfs_object *	_hdfs_located_block_new_proto(Hadoop__Hdfs__LocatedBlockProto *);
struct hdfs_object *	_hdfs_boolean_new_proto(protobuf_c_boolean);
struct hdfs_object *	_hdfs_token_new_proto(Hadoop__Common__TokenProto *);
struct hdfs_object *	_hdfs_datanode_info_new_proto(Hadoop__Hdfs__DatanodeInfoProto *);
struct hdfs_object *	_hdfs_content_summary_new_proto(Hadoop__Hdfs__ContentSummaryProto *);

#endif
