#ifndef _HADOOFUS_RPC2_H
#define _HADOOFUS_RPC2_H

typedef struct hdfs_object *(*hdfs_object_slurper)(struct hdfs_heap_buf *);

void	_rpc2_request_serialize(struct hdfs_heap_buf *,
	struct hdfs_rpc_invocation *);

hdfs_object_slurper	_rpc2_slurper_for_rpc(struct hdfs_object *);

#endif
