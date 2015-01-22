#ifndef _HADOOFUS_HEAPBUF_H
#define _HADOOFUS_HEAPBUF_H

#include <stdint.h>

#include <sasl/sasl.h>

#include <hadoofus/objects.h>

/* Allocate enough space to store size bytes at ->buf[->used]. */
void	_hbuf_reserve(struct hdfs_heap_buf *, size_t);

// Append serialized data to the passed buf. Resizes the underlying (malloc'd)
// buf as needed; 'size' is kept current (and is the size of the underlying
// buf), 'used' is the number of bytes of valid data.
void	_bappend_s8(struct hdfs_heap_buf *, int8_t);
void	_bappend_s16(struct hdfs_heap_buf *, int16_t);
void	_bappend_u16(struct hdfs_heap_buf *, uint16_t);
void	_bappend_s32(struct hdfs_heap_buf *, int32_t);
void	_bappend_s64(struct hdfs_heap_buf *, int64_t);
void	_bappend_vlint(struct hdfs_heap_buf *, int64_t);
void	_bappend_string(struct hdfs_heap_buf *, const char *);
void	_bappend_text(struct hdfs_heap_buf *, const char *);
void	_bappend_mem(struct hdfs_heap_buf *, size_t, const void *);

// Slurp functions read data from the passed buf. 'size' represents the size of
// the buf, 'used' represents the amount already read by other slurpers. On
// EOS, slurp functions set 'used' to -1. On invalid protocol data, slurp
// functions set 'used' to -2.
int8_t		_bslurp_s8(struct hdfs_heap_buf *);
int16_t		_bslurp_s16(struct hdfs_heap_buf *);
int32_t		_bslurp_s32(struct hdfs_heap_buf *);
int64_t		_bslurp_s64(struct hdfs_heap_buf *);
int64_t		_bslurp_vlint(struct hdfs_heap_buf *);
char *		_bslurp_string(struct hdfs_heap_buf *);
char *		_bslurp_string32(struct hdfs_heap_buf *);
char *		_bslurp_text(struct hdfs_heap_buf *);
// Helper for string slurpers. For their purposes, allocates an extra byte at
// the end of the returned buf.
void		_bslurp_mem1(struct hdfs_heap_buf *, size_t, char **);

void	_sasl_encode_inplace(sasl_conn_t *, struct hdfs_heap_buf *);
int	_sasl_decode_at_offset(sasl_conn_t *, char **bufp, size_t offset, int r, int *remain);

#endif
