#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "heapbuf.h"

#define _MAX(a, b) (((a) > (b))? (a) : (b))

static void
_hbuf_reserve(struct hdfs_heap_buf *h, size_t space)
{
	int remain, toalloc;

	remain = h->size - h->used;
	if (remain >= space)
		return;

	toalloc = _MAX(32, space - remain + 16);

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

// Buffer parsing functionality:

static bool
_eos(struct hdfs_heap_buf *b, size_t needed)
{
	if (b->size - b->used < needed) {
		b->used = -1;
		return true;
	}
	return false;
}

int8_t	
_bslurp_s8(struct hdfs_heap_buf *b)
{
	uint8_t res;
	if (_eos(b, sizeof(res)))
		return 0;
	res = ((uint8_t *)b->buf)[b->used++];
	return (int8_t)res;
}

int16_t	
_bslurp_s16(struct hdfs_heap_buf *b)
{
	uint16_t res;
	if (_eos(b, sizeof(res)))
		return 0;
	res = ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	return (int16_t)res;
}

int32_t	
_bslurp_s32(struct hdfs_heap_buf *b)
{
	uint32_t res;
	if (_eos(b, sizeof(res)))
		return 0;
	res = ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	return (int32_t)res;
}

int64_t	
_bslurp_s64(struct hdfs_heap_buf *b)
{
	uint64_t res;
	if (_eos(b, sizeof(res)))
		return 0;
	res = ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	return (int64_t)res;
}

void
_bslurp_mem1(struct hdfs_heap_buf *b, int len, char **obuf)
{
	char *res;

	assert(len >= 0);

	if (_eos(b, len))
		return;

	res = malloc(len + 1);
	assert(res);
	memcpy(res, b->buf + b->used, len);
	b->used += len;
	*obuf = res;
}

char *
_bslurp_string(struct hdfs_heap_buf *b)
{
	int16_t slen;
	char *res;

	slen = _bslurp_s16(b);
	if (b->used < 0)
		return NULL;

	if (slen < 0) {
		b->used = -2;
		return NULL;
	}

	_bslurp_mem1(b, slen, &res);
	if (b->used < 0)
		return NULL;

	res[slen] = '\0';
	return res;
}

char *
_bslurp_string32(struct hdfs_heap_buf *b)
{
	int32_t slen;
	char *res;

	slen = _bslurp_s32(b);
	if (b->used < 0)
		return NULL;

	if (slen < 0 || slen > 10*1024*1024/*sanity*/) {
		b->used = -2;
		return NULL;
	}

	_bslurp_mem1(b, slen, &res);
	if (b->used < 0)
		return NULL;

	res[slen] = '\0';
	return res;
}

char *
_bslurp_text(struct hdfs_heap_buf *b)
{
	int8_t len;
	char *res;

	len = _bslurp_s8(b);
	if (b->used < 0)
		return NULL;

	// we don't handle "text" objects longer than 127 bytes right now
	assert(len >= 0);

	_bslurp_mem1(b, len, &res);
	if (b->used < 0)
		return NULL;

	res[len] = '\0';
	return res;
}
