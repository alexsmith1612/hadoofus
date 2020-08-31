#ifndef _HADOOFUS_OBJECTS_H
#define _HADOOFUS_OBJECTS_H

#include <string.h>

#include <hadoofus/objects.h>

#include "ClientNamenodeProtocol.pb-c.h"
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
#define DSQUOTA_EXCEPTION_STR "org.apache.hadoop.hdfs.protocol.DSQuotaExceededException"
#define ILLARG_EXCEPTION_STR "java.lang.IllegalArgumentException"
#define INVTOK_EXCEPTION_STR "org.apache.hadoop.security.token.SecretManager$InvalidToken"
#define INVPATH_EXCEPTION_STR "org.apache.hadoop.fs.InvalidPathException"
#define EEXIST_EXCEPTION_STR "org.apache.hadoop.fs.FileAlreadyExistsException"
#define IPC_EXCEPTION_STR "org.apache.hadoop.ipc.IpcException"
#define SASL_EXCEPTION_STR "javax.security.sasl.SaslException"
#define RPC_EXCEPTION_STR "org.apache.hadoop.ipc.RpcServerException"
#define RPC_ENOENT_EXCEPTION_STR "org.apache.hadoop.ipc.RpcNoSuchMethodException"

#define HADOOP_ILLARG_EXCEPTION_STR "org.apache.hadoop.HadoopIllegalArgumentException"
#define SAFEMODE_EXCEPTION_STR "org.apache.hadoop.hdfs.server.namenode.SafeModeException"
#define PARENT_NOTDIR_EXCEPTION_STR "org.apache.hadoop.fs.ParentNotDirectoryException"
#define QUOTA_EXCEPTION_STR "org.apache.hadoop.hdfs.protocol.QuotaExceededException"
#define NSQUOTA_EXCEPTION_STR "org.apache.hadoop.hdfs.protocol.NSQuotaExceededException"
#define ENOTSUP_EXCEPTION_STR "java.lang.UnsupportedOperationException"
#define NOT_REPLICATED_EXCEPTION_STR "org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException"
#define ENOTDIR_EXCEPTION_STR "org.apache.hadoop.fs.PathIsNotDirectoryException"
#define ENOTEMPTY_EXCEPTION_STR "org.apache.hadoop.fs.PathIsNotEmptyDirectoryException"
#define UNRESLINK_EXCEPTION_STR "org.apache.hadoop.fs.UnresolvedLinkException"
#define REPLICA_NOTFOUND_EXCEPTION_STR "org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException"
#define STANDBY_EXCEPTION_STR "org.apache.hadoop.ipc.StandbyException"
#define RECOVERY_INPROGRESS_EXCEPTION_STR "org.apache.hadoop.hdfs.protocol.RecoveryInProgressException"


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

void			_hdfs_rpc_invocation_set_msgno(struct hdfs_object *, int32_t);
void			_hdfs_rpc_invocation_set_proto(struct hdfs_object *,
			enum hdfs_namenode_proto pr);
void			_hdfs_rpc_invocation_set_clientid(struct hdfs_object *, uint8_t *);

void			_hdfs_authheader_set_clientid(struct hdfs_object *, uint8_t *);

// Returns HDFS_SUCCESS and populates *res on success.
// Returns HDFS_AGAIN if we can't decode a response from the available buffer.
// Returns other error code otherwise.
struct hdfs_error	_hdfs_result_deserialize(char *buf, int buflen, struct _hdfs_result *res);
struct hdfs_error	_hdfs_result_deserialize_v2(char *buf, int buflen, struct _hdfs_result *res,
			struct _hdfs_pending *pend, int npend);
struct hdfs_error	_hdfs_result_deserialize_v2_2(char *buf, int buflen, struct _hdfs_result *res,
			struct _hdfs_pending *pend, int npend);

enum hdfs_object_type	_hdfs_string_to_type(const char *);


struct hdfs_transfer_targets *	_hdfs_transfer_targets_copy(struct hdfs_transfer_targets *src);

// Helper function for compat from v1 to v2.2+. Caller loses their reference to
// the located block argument
struct hdfs_object *	_hdfs_located_block_with_status_from_located_block(struct hdfs_object *lb);

// HDFSv2+ protobuf-to-hdfs_object converters
struct hdfs_object *	_hdfs_fsserverdefaults_new_proto(Hadoop__Hdfs__FsServerDefaultsProto *);
struct hdfs_object *	_hdfs_directory_listing_new_proto(Hadoop__Hdfs__DirectoryListingProto *);
struct hdfs_object *	_hdfs_file_status_new_proto(Hadoop__Hdfs__HdfsFileStatusProto *);
struct hdfs_object *	_hdfs_located_blocks_new_proto(Hadoop__Hdfs__LocatedBlocksProto *);
struct hdfs_object *	_hdfs_located_block_new_proto(Hadoop__Hdfs__LocatedBlockProto *);
struct hdfs_object *	_hdfs_located_block_with_status_new_proto(Hadoop__Hdfs__LocatedBlockProto *,
			Hadoop__Hdfs__HdfsFileStatusProto *);
struct hdfs_object *	_hdfs_boolean_new_proto(protobuf_c_boolean);
struct hdfs_object *	_hdfs_token_new_proto(Hadoop__Common__TokenProto *);
struct hdfs_object *	_hdfs_datanode_info_new_proto(Hadoop__Hdfs__DatanodeInfoProto *);
struct hdfs_object *	_hdfs_array_datanode_info_new_proto(Hadoop__Hdfs__DatanodeInfoProto **, size_t);
struct hdfs_object *	_hdfs_content_summary_new_proto(Hadoop__Hdfs__ContentSummaryProto *);

// HDFSv2+ proto-buf-to-hadoofus enum conversion assertions and functions to
// placate compiler warnings while maintaining type safety

_Static_assert((unsigned)HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_NULL == HDFS_CSUM_NULL,
    "Protobufs NULL checksum enum does not match our NULL checksum enum");
_Static_assert((unsigned)HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_CRC32 == HDFS_CSUM_CRC32,
    "Protobufs CRC32 enum does not match our CRC32 enum");
_Static_assert((unsigned)HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_CRC32C == HDFS_CSUM_CRC32C,
    "Protobufs CRC32C enum does not match our CRC32C enum");
static inline enum hdfs_checksum_type
_hdfs_csum_from_proto(Hadoop__Hdfs__ChecksumTypeProto pr)
{
	return (unsigned)pr;
}

static inline Hadoop__Hdfs__ChecksumTypeProto
_hdfs_csum_to_proto(enum hdfs_checksum_type csum)
{
	return (unsigned)csum;
}

_Static_assert((unsigned)HADOOP__HDFS__HDFS_FILE_STATUS_PROTO__FILE_TYPE__IS_DIR == HDFS_FT_DIR,
    "Protobufs DIR file type enum does not match our DIR file type enum");
_Static_assert((unsigned)HADOOP__HDFS__HDFS_FILE_STATUS_PROTO__FILE_TYPE__IS_FILE == HDFS_FT_FILE,
    "Protobufs FILE file type enum does not match our FILE file type enum");
_Static_assert((unsigned)HADOOP__HDFS__HDFS_FILE_STATUS_PROTO__FILE_TYPE__IS_SYMLINK == HDFS_FT_SYMLINK,
    "Protobufs SYMLINK file type enum does not match our SYMLINK file type enum");
static inline enum hdfs_file_type
_hdfs_file_type_from_proto(Hadoop__Hdfs__HdfsFileStatusProto__FileType pr)
{
	return (unsigned)pr;
}

_Static_assert((unsigned)HADOOP__HDFS__STORAGE_TYPE_PROTO__DISK == HDFS_STORAGE_DISK,
    "Protobufs DISK storage type enum does not match our DISK storage type enum");
_Static_assert((unsigned)HADOOP__HDFS__STORAGE_TYPE_PROTO__SSD == HDFS_STORAGE_SSD,
    "Protobufs SSD storage type enum does not match our SSD storage type enum");
_Static_assert((unsigned)HADOOP__HDFS__STORAGE_TYPE_PROTO__ARCHIVE == HDFS_STORAGE_ARCHIVE,
    "Protobufs ARCHIVE storage type enum does not match our ARCHIVE storage type enum");
_Static_assert((unsigned)HADOOP__HDFS__STORAGE_TYPE_PROTO__RAM_DISK == HDFS_STORAGE_RAM_DISK,
    "Protobufs RAM_DISK storage type enum does not match our RAM_DISK storage type enum");
_Static_assert((unsigned)HADOOP__HDFS__STORAGE_TYPE_PROTO__PROVIDED == HDFS_STORAGE_PROVIDED,
    "Protobufs PROVIDED storage type enum does not match our PROVIDED storage type enum");
static inline enum hdfs_storage_type
_hdfs_storage_type_from_proto(Hadoop__Hdfs__StorageTypeProto pr)
{
	return (unsigned)pr;
}

static inline Hadoop__Hdfs__StorageTypeProto
_hdfs_storage_type_to_proto(enum hdfs_storage_type stype)
{
	return (unsigned)stype;
}

_Static_assert(sizeof(Hadoop__Hdfs__StorageTypeProto) == sizeof(enum hdfs_storage_type),
    "sizeof protobufs storage type enum does not match sizeof out storage type enum");
// XXX somewhat hackish, but the enums have the same size as enforced by the above
// static assertion, and no changes get made to the input pointer, so this technical
// aliasing violation shouldn't lead to any adverse effects
static inline Hadoop__Hdfs__StorageTypeProto *
_hdfs_storage_type_ptr_to_proto(enum hdfs_storage_type *ptr)
{
	union {
		enum hdfs_storage_type *our_ptr;
		Hadoop__Hdfs__StorageTypeProto *pr_ptr;
	} cast = { .our_ptr = ptr };

	return cast.pr_ptr;
}

_Static_assert((unsigned)HADOOP__HDFS__DATANODE_REPORT_TYPE_PROTO__ALL == HDFS_DNREPORT_ALL,
    "Protobufs ALL datanode report type enum does not match our ALL datanode report type enum");
_Static_assert((unsigned)HADOOP__HDFS__DATANODE_REPORT_TYPE_PROTO__LIVE == HDFS_DNREPORT_LIVE,
    "Protobufs LIVE datanode report type enum does not match our LIVE datanode report type enum");
_Static_assert((unsigned)HADOOP__HDFS__DATANODE_REPORT_TYPE_PROTO__DEAD == HDFS_DNREPORT_DEAD,
    "Protobufs DEAD datanode report type enum does not match our DEAD datanode report type enum");
_Static_assert((unsigned)HADOOP__HDFS__DATANODE_REPORT_TYPE_PROTO__DECOMMISSIONING == HDFS_DNREPORT_DECOMMISSIONING,
    "Protobufs DECOMMISSIONING datanode report type enum does not match our DECOMMISSIONING datanode report type enum");
_Static_assert((unsigned)HADOOP__HDFS__DATANODE_REPORT_TYPE_PROTO__ENTERING_MAINTENANCE == HDFS_DNREPORT_ENTERING_MAINTENANCE,
    "Protobufs ENTERING_MAINTENANCE datanode report type enum does not match our ENTERING_MAINTENANCE datanode report type enum");
_Static_assert((unsigned)HADOOP__HDFS__DATANODE_REPORT_TYPE_PROTO__IN_MAINTENANCE == HDFS_DNREPORT_IN_MAINTENANCE,
    "Protobufs IN_MAINTENANCE datanode report type enum does not match our IN_MAINTENANCE datanode report type enum");
static inline Hadoop__Hdfs__DatanodeReportTypeProto
_hdfs_datanode_report_type_to_proto(enum hdfs_datanode_report_type dnr)
{
	return (unsigned)dnr;
}

_Static_assert((unsigned)HADOOP__HDFS__CIPHER_SUITE_PROTO__UNKNOWN == HDFS_CS_UNKNOWN,
    "Protobufs UNKNOWN cipher suite enum does not match our UNKNOWN cipher suite enum");
_Static_assert((unsigned)HADOOP__HDFS__CIPHER_SUITE_PROTO__AES_CTR_NOPADDING == HDFS_CS_AES_CTR_NOPADDING,
    "Protobufs AES_CTR_NOPADDING cipher suite enum does not match our AES_CTR_NOPADDING cipher suite enum");
static inline enum hdfs_cipher_suite
_hdfs_cipher_suite_from_proto(Hadoop__Hdfs__CipherSuiteProto pr)
{
	return (unsigned)pr;
}

_Static_assert((unsigned)HADOOP__HDFS__CRYPTO_PROTOCOL_VERSION_PROTO__UNKNOWN_PROTOCOL_VERSION == HDFS_CPV_UNKNOWN,
    "Protobufs UNKNOWN crypto protocol version enum does not match our UNKNOWN crypto protocol enum");
_Static_assert((unsigned)HADOOP__HDFS__CRYPTO_PROTOCOL_VERSION_PROTO__ENCRYPTION_ZONES == HDFS_CPV_ENCRYPTION_ZONES,
    "Protobufs ENCRYPTION_ZONES crypto protocol version enum does not match our ENCRYPTION_ZONES crypto protocol enum");
static inline enum hdfs_crypto_proto_version
_hdfs_crypto_proto_version_from_proto(Hadoop__Hdfs__CryptoProtocolVersionProto pr)
{
	return (unsigned)pr;
}

#endif
