from libc.stdint cimport int64_t, uint16_t, int16_t
from libc.string cimport const_char

from clowlevel cimport hdfs_namenode, hdfs_datanode, const_char, hdfs_kerb
from cobjects cimport hdfs_object, hdfs_object_type

cdef extern from "hadoofus/highlevel.h":
    hdfs_namenode *hdfs_namenode_new(const_char *host, const_char *port, const_char *username, hdfs_kerb, const_char **error_out)
    void hdfs_namenode_delete(hdfs_namenode *n)
    bint hdfs_object_is_null(hdfs_object *o)
    hdfs_object_type hdfs_null_type(hdfs_object *o)
    bint hdfs_object_is_exception(hdfs_object *o)
    hdfs_object_type hdfs_exception_get_type(hdfs_object *o)
    const_char *hdfs_exception_get_message(hdfs_object *o)

    int64_t hdfs_getProtocolVersion(hdfs_namenode *n, const_char *protocol, int64_t client_version, hdfs_object **exception_out) nogil
    hdfs_object *hdfs_getBlockLocations(hdfs_namenode *n, const_char *path, int64_t offset, int64_t length, hdfs_object **exception_out) nogil
    void hdfs_create(hdfs_namenode *n, const_char *path, uint16_t perms, const_char *clientname, bint overwrite, bint create_parent, int16_t replication, int64_t blocksize, hdfs_object **exception_out) nogil
    hdfs_object *hdfs_append(hdfs_namenode *n, const_char *path, const_char *client, hdfs_object **exception_out) nogil
    bint hdfs_setReplication(hdfs_namenode *n, const_char *path, int16_t replication, hdfs_object **exception_out) nogil
    void hdfs_setPermission(hdfs_namenode *n, const_char *path, int16_t perms, hdfs_object **exception_out) nogil
    void hdfs_setOwner(hdfs_namenode *, const_char *path, const_char *owner, const_char *group, hdfs_object **exception_out) nogil
    void hdfs_abandonBlock(hdfs_namenode *, hdfs_object *block, const_char *path, const_char *client, hdfs_object **exception_out) nogil
    hdfs_object *hdfs_addBlock(hdfs_namenode *n, const_char *path, const_char *client, hdfs_object *excluded, hdfs_object **exception_out) nogil
    bint hdfs_complete(hdfs_namenode *n, const_char *path, const_char *client, hdfs_object **exception_out) nogil
    bint hdfs_rename(hdfs_namenode *n, const_char *src, const_char *dst, hdfs_object **exception_out) nogil
    bint hdfs_delete(hdfs_namenode *n, const_char *path, bint can_recurse, hdfs_object **exception_out) nogil
    bint hdfs_mkdirs(hdfs_namenode *n, const_char *path, int16_t perms, hdfs_object **exception_out) nogil
    hdfs_object *hdfs_getListing(hdfs_namenode *n, const_char *path, hdfs_object *begin, hdfs_object **exception_out) nogil
    void hdfs_renewLease(hdfs_namenode *n, const_char *client, hdfs_object **exception_out) nogil
    hdfs_object *hdfs_getStats(hdfs_namenode *n, hdfs_object **exception_out) nogil
    int64_t hdfs_getPreferredBlockSize(hdfs_namenode *n, const_char *path, hdfs_object **exception_out) nogil
    hdfs_object *hdfs_getFileInfo(hdfs_namenode *n, const_char *path, hdfs_object **exception_out) nogil
    hdfs_object *hdfs_getContentSummary(hdfs_namenode *n, const_char *path, hdfs_object **exception_out) nogil
    void hdfs_setQuota(hdfs_namenode *n, const_char *path, int64_t ns_quota, int64_t ds_quota, hdfs_object **exception_out) nogil
    void hdfs_fsync(hdfs_namenode *n, const_char *path, const_char *client, hdfs_object **exception_out) nogil
    void hdfs_setTimes(hdfs_namenode *n, const_char *path, int64_t mtime, int64_t atime, hdfs_object **exception_out) nogil
    bint hdfs_recoverLease(hdfs_namenode *n, const_char *path, const_char *client, hdfs_object **exception_out) nogil
    void hdfs_concat(hdfs_namenode *n, const_char *target, hdfs_object *srcs, hdfs_object **exception_out) nogil
    hdfs_object *hdfs_getDelegationToken(hdfs_namenode *n, const_char *renewer, hdfs_object **exception_out) nogil
    void hdfs_cancelDelegationToken(hdfs_namenode *n, hdfs_object *dt, hdfs_object **exception_out) nogil
    int64_t hdfs_renewDelegationToken(hdfs_namenode *n, hdfs_object *dt, hdfs_object **exception_out) nogil
    bint hdfs_setSafeMode(hdfs_namenode *n, const_char *mode, hdfs_object **exception_out) nogil
    hdfs_object *hdfs_getDatanodeReport(hdfs_namenode *n, const_char *dnreporttype, hdfs_object **exception_out) nogil
    void hdfs_reportBadBlocks(hdfs_namenode *n, hdfs_object *dt, hdfs_object **exception_out) nogil
    hdfs_object *hdfs_distributedUpgradeProgress(hdfs_namenode *n, const_char *upgradeaction, hdfs_object **exception_out) nogil
    void hdfs_finalizeUpgrade(hdfs_namenode *n, hdfs_object **exception_out) nogil
    void hdfs_refreshNodes(hdfs_namenode *n, hdfs_object **exception_out) nogil
    void hdfs_saveNamespace(hdfs_namenode *n, hdfs_object **exception_out) nogil
    bint hdfs_isFileClosed(hdfs_namenode *n, const_char *path, hdfs_object **exception_out) nogil
    void hdfs_metaSave(hdfs_namenode *n, const_char *path, hdfs_object **exception_out) nogil
    void hdfs_setBalancerBandwidth(hdfs_namenode *n, int64_t bw, hdfs_object **exception_out) nogil

    hdfs_object *hdfs2_getServerDefaults(hdfs_namenode *n, hdfs_object **exception_out) nogil
    hdfs_object *hdfs2_getFileLinkInfo(hdfs_namenode *n, const_char *path, hdfs_object **exception_out) nogil
    void hdfs2_createSymlink(hdfs_namenode *n, const_char *target, const_char *link, int16_t dirperms, bint createparent, hdfs_object **exception_out) nogil
    hdfs_object *hdfs2_getLinkTarget(hdfs_namenode *n, const_char *path, hdfs_object **exception_out) nogil

    hdfs_datanode *hdfs_datanode_new(hdfs_object *located_block, const_char *client, int proto, const_char **error_out) nogil
    void hdfs_datanode_delete(hdfs_datanode *) nogil
