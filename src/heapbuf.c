#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "heapbuf.h"
#include "util.h"

#define _MAX(a, b) (((a) > (b))? (a) : (b))

static void
_hbuf_reserve(struct hdfs_heap_buf *h, size_t space)
{
	int remain, toalloc;

	remain = h->size - h->used;
	if ((size_t)remain >= space)
		return;

	toalloc = _MAX(32, space - remain + 16);

	h->buf = realloc(h->buf, h->size + toalloc);
	ASSERT(h->buf);
	h->size += toalloc;
}

void
_bappend_s8(struct hdfs_heap_buf *h, int8_t arg)
{
	_hbuf_reserve(h, sizeof(arg));
	((int8_t *)h->buf)[h->used++] = arg;
}

void
_bappend_s16(struct hdfs_heap_buf *h, int16_t sarg)
{
	uint16_t arg = (uint16_t)sarg;
	_hbuf_reserve(h, sizeof(arg));
	((uint8_t *)h->buf)[h->used++] = arg >> 8;
	((uint8_t *)h->buf)[h->used++] = 0xff & arg;
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
_bappend_string(struct hdfs_heap_buf *h, const char *arg)
{
	int len = strlen(arg);

	ASSERT(len >= 0);
	ASSERT(len <= INT16_MAX);

	_bappend_u16(h, (uint16_t)len);
	_bappend_mem(h, len, arg);
}

void
_bappend_text(struct hdfs_heap_buf *h, const char *arg)
{
	int len = strlen(arg);

	// a limited form of the variable length int format, but usually we
	// don't need more for "text" fields:
	ASSERT(len >= 0);
	ASSERT(len < 127);

	_bappend_s8(h, (int8_t)len);
	_bappend_mem(h, len, arg);
}

void
_bappend_mem(struct hdfs_heap_buf *h, int arglen, const void *arg)
{
	ASSERT(arglen >= 0);

	_hbuf_reserve(h, arglen);
	memcpy(h->buf + h->used, arg, arglen);
	h->used += arglen;
}

// Buffer parsing functionality:

static bool
_eos(struct hdfs_heap_buf *b, size_t needed)
{
	if ((size_t)(b->size - b->used) < needed) {
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

	ASSERT(len >= 0);

	if (_eos(b, len))
		return;

	res = malloc(len + 1);
	ASSERT(res);
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
	ASSERT(len >= 0);

	_bslurp_mem1(b, len, &res);
	if (b->used < 0)
		return NULL;

	res[len] = '\0';
	return res;
}

void
_sasl_encode_inplace(sasl_conn_t *ctx, struct hdfs_heap_buf *b)
{
	const char *out;
	unsigned outlen;
	int r;

	r = sasl_encode(ctx, b->buf, b->used, &out, &outlen);
	if (r != SASL_OK) {
		fprintf(stderr, "sasl_encode: %s\n",
		    sasl_errstring(r, NULL, NULL));
		abort();
	}

	free(b->buf);
	b->buf = malloc(4 + outlen);
	ASSERT(b->buf);

	// copy in length-prefixed encoded bits
	_be32enc(b->buf, outlen);
	memcpy(b->buf + 4, out, outlen);
	b->used = b->size = 4 + outlen;
}

// Returns decoded size; sets remain; resizes bufp/remain if needed.
// On error, returns SASL error code (negative)
int
_sasl_decode_at_offset(sasl_conn_t *ctx, char **bufp, size_t offset, int r, int *remain)
{
	const char *out;
	unsigned outlen;
	int s;
	uint32_t sasl_len;

	// this is pretty fragile. at the moment we expect to slurp exactly one
	// kerb reply in a given read(). fixing it will require adding another
	// buffer to the read code when the connection is sasl'd, which is more
	// work for now (it's not clear that hadoop supports ssf > 0 anyway).
	if (r < 4)
		abort();

	sasl_len = _be32dec(*bufp + offset);
	if ((unsigned)r - 4 != sasl_len)
		abort();

	s = sasl_decode(ctx, *bufp + offset + 4, sasl_len, &out, &outlen);
	if (s != SASL_OK)
		return s;

	if (outlen > (unsigned)*remain) {
		*remain = outlen + 4*1024;
		*bufp = realloc(*bufp, offset + *remain);
		ASSERT(*bufp);
	}

	memcpy(*bufp + offset, out, outlen);
	return outlen;
}
