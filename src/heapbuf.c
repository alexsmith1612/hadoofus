#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "heapbuf.h"

#define _MIN(a, b) (((a) > (b))? (b) : (a))

static void
_hbuf_reserve(struct hdfs_heap_buf *h, size_t space)
{
	int remain, toalloc;

	remain = h->size - h->used;
	if (remain >= space)
		return;

	toalloc = _MIN(32, space - remain + 16);

	h->buf = realloc(h->buf, h->size + toalloc);
	assert(h->buf);
	h->size += toalloc;
}

void
_bappend_s8(struct hdfs_heap_buf *h, int8_t arg)
{
	_hbuf_reserve(h, sizeof(arg));
	((int8_t *)h->buf)[h->used++] = arg;
}

void
_bappend_u16(struct hdfs_heap_buf *h, uint16_t arg)
{
	_hbuf_reserve(h, sizeof(arg));
	((uint8_t *)h->buf)[h->used++] = arg >> 8;
	((uint8_t *)h->buf)[h->used++] = 0xff & arg;
}

void
_bappend_s32(struct hdfs_heap_buf *h, int32_t sarg)
{
	uint32_t arg = (uint32_t)sarg;
	_hbuf_reserve(h, sizeof(arg));
	((uint8_t *)h->buf)[h->used++] = arg >> 24;
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 16);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 8);
	((uint8_t *)h->buf)[h->used++] = 0xff & arg;
}

void
_bappend_u32(struct hdfs_heap_buf *h, uint32_t arg)
{
	_hbuf_reserve(h, sizeof(arg));
	((uint8_t *)h->buf)[h->used++] = arg >> 24;
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 16);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 8);
	((uint8_t *)h->buf)[h->used++] = 0xff & arg;
}

void
_bappend_s64(struct hdfs_heap_buf *h, int64_t sarg)
{
	uint64_t arg = (uint64_t)sarg;
	_hbuf_reserve(h, sizeof(arg));
	((uint8_t *)h->buf)[h->used++] = arg >> 56;
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 48);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 40);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 32);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 24);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 16);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 8);
	((uint8_t *)h->buf)[h->used++] = 0xff & arg;
}

void
_bappend_u64(struct hdfs_heap_buf *h, uint64_t arg)
{
	_hbuf_reserve(h, sizeof(arg));
	((uint8_t *)h->buf)[h->used++] = arg >> 56;
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 48);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 40);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 32);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 24);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 16);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 8);
	((uint8_t *)h->buf)[h->used++] = 0xff & arg;
}

void
_bappend_string(struct hdfs_heap_buf *h, const char *arg)
{
	int len = strlen(arg);

	assert(len >= 0);
	assert(len <= INT16_MAX);

	_bappend_u16(h, (uint16_t)len);
	_bappend_mem(h, len, arg);
}

void
_bappend_text(struct hdfs_heap_buf *h, const char *arg)
{
	int len = strlen(arg);

	// a limited form of the variable length int format, but usually we
	// don't need more for "text" fields:
	assert(len >= 0);
	assert(len < 127);

	_bappend_s8(h, (int8_t)len);
	_bappend_mem(h, len, arg);
}

void
_bappend_mem(struct hdfs_heap_buf *h, int arglen, const void *arg)
{
	assert(arglen >= 0);

	_hbuf_reserve(h, arglen);
	memcpy(h->buf + h->used, arg, arglen);
	h->used += arglen;
}
