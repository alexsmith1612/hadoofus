#ifndef _HADOOFUS_HEAPBUF_H
#define _HADOOFUS_HEAPBUF_H

#include <stdint.h>

#include <hadoofus/objects.h>

void	_bappend_s8(struct hdfs_heap_buf *, int8_t);
void	_bappend_u16(struct hdfs_heap_buf *, uint16_t);
void	_bappend_s32(struct hdfs_heap_buf *, int32_t);
void	_bappend_u32(struct hdfs_heap_buf *, uint32_t);
void	_bappend_s64(struct hdfs_heap_buf *, int64_t);
void	_bappend_u64(struct hdfs_heap_buf *, uint64_t);
void	_bappend_string(struct hdfs_heap_buf *, const char *);
void	_bappend_text(struct hdfs_heap_buf *, const char *);
void	_bappend_mem(struct hdfs_heap_buf *, int, const void *);

#endif
