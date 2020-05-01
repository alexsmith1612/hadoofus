#ifndef _HADOOFUS_HEAPBUF_H
#define _HADOOFUS_HEAPBUF_H

#include <stdint.h>

#include <sasl/sasl.h>

#include <hadoofus/objects.h>

// If fewer than resize_at bytes can be stored at _hbuf_writeptr(h),
// increase the buffer size by resize_by (0 upsizes enough to store
// resize_at bytes)
void	_hdfs_hbuf_resize(struct hdfs_heap_buf *h, size_t resize_at, size_t resize_by);
// Allocate enough space to store size bytes at _hbuf_writeptr(h).
#define	_hdfs_hbuf_reserve(h, size) _hdfs_hbuf_resize((h), (size), 0)

static inline char *	_hdfs_hbuf_writeptr(struct hdfs_heap_buf *h) { return h->buf + h->used; }
static inline int	_hdfs_hbuf_remsize(struct hdfs_heap_buf *h) { return h->size - h->used; }
static inline void	_hdfs_hbuf_append(struct hdfs_heap_buf *h, size_t num) { h->used += num; }
static inline char *	_hdfs_hbuf_readptr(struct hdfs_heap_buf *h) { return h->buf + h->pos; }
static inline int	_hdfs_hbuf_readlen(struct hdfs_heap_buf *h) { return h->used - h->pos; }
static inline void	_hdfs_hbuf_consume(struct hdfs_heap_buf *h, size_t num) { h->pos += num; }
static inline void	_hdfs_hbuf_reset(struct hdfs_heap_buf *h) { h->pos = h->used = 0; }

// Return the number of bytes needed to encode the input as a vlint
// Note, this is in heapbuf.{c,h} to keep it with _bappend_vlint()
int	_hdfs_get_vlint_encoding_size(int64_t);

// Append serialized data to the passed buf. Resizes the underlying (malloc'd)
// buf as needed; 'size' is kept current (and is the size of the underlying
// buf), 'used' is the number of bytes of valid data.
void	_hdfs_bappend_s8(struct hdfs_heap_buf *, int8_t);
void	_hdfs_bappend_s16(struct hdfs_heap_buf *, int16_t);
void	_hdfs_bappend_u16(struct hdfs_heap_buf *, uint16_t);
void	_hdfs_bappend_s32(struct hdfs_heap_buf *, int32_t);
void	_hdfs_bappend_s64(struct hdfs_heap_buf *, int64_t);
void	_hdfs_bappend_vlint(struct hdfs_heap_buf *, int64_t);
void	_hdfs_bappend_string(struct hdfs_heap_buf *, const char *);
void	_hdfs_bappend_text(struct hdfs_heap_buf *, const char *);
void	_hdfs_bappend_mem(struct hdfs_heap_buf *, size_t, const void *);

// Slurp functions read data from the passed buf. 'size' represents the size of
// the buf, 'used' represents the amount already read by other slurpers. On
// EOS, slurp functions set 'used' to _H_PARSE_EOF. On invalid protocol data,
// slurp functions set 'used' to _H_PARSE_ERROR.
#define		_H_PARSE_EOF	(-1)
#define		_H_PARSE_ERROR	(-2)
int8_t		_hdfs_bslurp_s8(struct hdfs_heap_buf *);
int16_t		_hdfs_bslurp_s16(struct hdfs_heap_buf *);
int32_t		_hdfs_bslurp_s32(struct hdfs_heap_buf *);
int64_t		_hdfs_bslurp_s64(struct hdfs_heap_buf *);
int64_t		_hdfs_bslurp_vlint(struct hdfs_heap_buf *);
char *		_hdfs_bslurp_string(struct hdfs_heap_buf *);
char *		_hdfs_bslurp_string32(struct hdfs_heap_buf *);
char *		_hdfs_bslurp_text(struct hdfs_heap_buf *);
// Helper for string slurpers. For their purposes, allocates an extra byte at
// the end of the returned buf.
void		_hdfs_bslurp_mem1(struct hdfs_heap_buf *, size_t, char **);

struct hdfs_error	_hdfs_sasl_encode_at_offset(sasl_conn_t *ctx, struct hdfs_heap_buf *h, int offset);

#endif // _HADOOFUS_HEAPBUF_H
