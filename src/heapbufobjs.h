#ifndef _HADOOFUS_HEAPBUFOBJS_H
#define _HADOOFUS_HEAPBUFOBJS_H

#include <stdint.h>

#include <hadoofus/objects.h>

// Slurp functions read data from the passed buf. 'size' represents the size of
// the buf, 'used' represents the amount already read by other slurpers. On
// EOS, slurp functions set 'used' to -1. On invalid protocol data, slurp
// functions set 'used' to -2.
struct hdfs_object *	_oslurp_void(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_null(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_boolean(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_short(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_int(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_long(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_array_long(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_token(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_located_block(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_located_blocks(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_directory_listing(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_datanode_info(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_array_datanode_info(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_file_status(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_content_summary(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_block(struct hdfs_heap_buf *);
struct hdfs_object *	_oslurp_fsperms(struct hdfs_heap_buf *);

#endif
