#ifndef _HADOOFUS_HEAPBUFOBJS_H
#define _HADOOFUS_HEAPBUFOBJS_H

#include <stdint.h>

#include <hadoofus/objects.h>

// Slurp functions read data from the passed buf. 'size' represents the size of
// the buf, 'used' represents the amount already read by other slurpers. On
// EOS, slurp functions set 'used' to _H_PARSE_EOF. On invalid protocol data,
// slurp functions set 'used' to _H_PARSE_ERROR.
struct hdfs_object *	_hdfs_oslurp_void(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_null(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_boolean(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_short(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_int(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_long(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_array_long(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_token(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_located_block(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_located_blocks(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_directory_listing(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_datanode_info(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_array_datanode_info(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_file_status(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_content_summary(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_block(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_fsperms(struct hdfs_heap_buf *);
struct hdfs_object *	_hdfs_oslurp_upgrade_status_report(struct hdfs_heap_buf *);

#endif
