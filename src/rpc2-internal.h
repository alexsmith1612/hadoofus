#ifndef _HADOOFUS_RPC2_H
#define _HADOOFUS_RPC2_H

#include <stddef.h>

#include <hadoofus/objects.h>

typedef struct hdfs_object *(*hdfs_object_slurper)(struct hdfs_heap_buf *);

void	_hdfs_rpc2_request_serialize(struct hdfs_heap_buf *,
	struct hdfs_rpc_invocation *);
size_t	_hdfs_rpc2_request_get_size(struct hdfs_rpc_invocation *);


hdfs_object_slurper	_hdfs_rpc2_slurper_for_rpc(struct hdfs_object *);

#endif // _HADOOFUS_RPC2_H
