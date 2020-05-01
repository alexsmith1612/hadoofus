#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hadoofus/lowlevel.h>

#include "heapbuf.h"
#include "util.h"

#define _MAX(a, b) (((a) > (b))? (a) : (b))

void
_hdfs_hbuf_resize(struct hdfs_heap_buf *h, size_t resize_at, size_t resize_by)
{
	size_t toalloc;

	// No significant data left in the buffer, so reset
	if (_hdfs_hbuf_readlen(h) == 0)
		_hdfs_hbuf_reset(h);

	// Enough space at the end
	if ((size_t)_hdfs_hbuf_remsize(h) >= resize_at)
		return;

	// Enough space after compacting
	if ((size_t)(_hdfs_hbuf_remsize(h) + h->pos) >= resize_at) {
		memmove(h->buf, _hdfs_hbuf_readptr(h), _hdfs_hbuf_readlen(h));
		h->used -= h->pos;
		h->pos = 0;
		return;
	}

	// Otherwise we need to allocate more space
	if (resize_by == 0) // use default
		toalloc = h->size + _MAX(32, resize_at - _hdfs_hbuf_remsize(h) + 16);
	else
		toalloc = h->size + resize_by;

	// If we don't care about any data currently in the buffer,
	// free()/malloc(), otherwise realloc
	if (h->used == 0) {
		free(h->buf);
		h->buf = malloc(toalloc);
	} else
		h->buf = realloc(h->buf, toalloc);
	ASSERT(h->buf);
	h->size = toalloc;
}

void
_hdfs_bappend_s8(struct hdfs_heap_buf *h, int8_t arg)
{
	_hdfs_hbuf_reserve(h, sizeof(arg));
	((int8_t *)h->buf)[h->used++] = arg;
}

void
_hdfs_bappend_s16(struct hdfs_heap_buf *h, int16_t sarg)
{
	uint16_t arg = (uint16_t)sarg;
	_hdfs_hbuf_reserve(h, sizeof(arg));
	((uint8_t *)h->buf)[h->used++] = arg >> 8;
	((uint8_t *)h->buf)[h->used++] = 0xff & arg;
}

void
_hdfs_bappend_u16(struct hdfs_heap_buf *h, uint16_t arg)
{
	_hdfs_hbuf_reserve(h, sizeof(arg));
	((uint8_t *)h->buf)[h->used++] = arg >> 8;
	((uint8_t *)h->buf)[h->used++] = 0xff & arg;
}

void
_hdfs_bappend_s32(struct hdfs_heap_buf *h, int32_t sarg)
{
	uint32_t arg = (uint32_t)sarg;
	_hdfs_hbuf_reserve(h, sizeof(arg));
	((uint8_t *)h->buf)[h->used++] = arg >> 24;
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 16);
	((uint8_t *)h->buf)[h->used++] = 0xff & (arg >> 8);
	((uint8_t *)h->buf)[h->used++] = 0xff & arg;
}

void
_hdfs_bappend_s64(struct hdfs_heap_buf *h, int64_t sarg)
{
	uint64_t arg = (uint64_t)sarg;
	_hdfs_hbuf_reserve(h, sizeof(arg));
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
_hdfs_bappend_string(struct hdfs_heap_buf *h, const char *arg)
{
	int len = strlen(arg);

	ASSERT(len >= 0);
	ASSERT(len <= INT16_MAX);

	_hdfs_bappend_u16(h, (uint16_t)len);
	_hdfs_bappend_mem(h, len, arg);
}

void
_hdfs_bappend_vlint(struct hdfs_heap_buf *h, int64_t i64)
{
	uint64_t val;
	uint8_t lb;

	val = (uint64_t)i64;

	do {
		lb = val & 0x7f;
		val >>= 7;

		if (val)
			lb |= 0x80;
		_hdfs_bappend_s8(h, (int8_t)lb);
	} while (val);
}

int
_hdfs_get_vlint_encoding_size(int64_t i64)
{
	uint64_t val;
	int ret = 1;

	val = (uint64_t)i64;

	while (val >>= 7) { ret++; }

	return ret;
}

void
_hdfs_bappend_text(struct hdfs_heap_buf *h, const char *arg)
{
	int len = strlen(arg);

	_hdfs_bappend_vlint(h, len);
	_hdfs_bappend_mem(h, len, arg);
}

void
_hdfs_bappend_mem(struct hdfs_heap_buf *h, size_t arglen, const void *arg)
{

	_hdfs_hbuf_reserve(h, arglen);
	memcpy(h->buf + h->used, arg, arglen);
	h->used += arglen;
}

// Buffer parsing functionality:

static bool
_eos(struct hdfs_heap_buf *b, size_t needed)
{
	if ((size_t)(b->size - b->used) < needed) {
		b->used = _H_PARSE_EOF;
		return true;
	}
	return false;
}

int8_t	
_hdfs_bslurp_s8(struct hdfs_heap_buf *b)
{
	uint8_t res;
	if (_eos(b, sizeof(res)))
		return 0;
	res = ((uint8_t *)b->buf)[b->used++];
	return (int8_t)res;
}

int16_t	
_hdfs_bslurp_s16(struct hdfs_heap_buf *b)
{
	uint16_t res;
	if (_eos(b, sizeof(res)))
		return 0;
	res = ((uint8_t *)b->buf)[b->used++];
	res = (res << 8) | ((uint8_t *)b->buf)[b->used++];
	return (int16_t)res;
}

int32_t	
_hdfs_bslurp_s32(struct hdfs_heap_buf *b)
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
_hdfs_bslurp_s64(struct hdfs_heap_buf *b)
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

/*
 * (Negative) signed numbers are encoded as a 64-bit extended negative in PB
 * (even int32).
 *
 * Not 100% sure if vlint encoding is the same between HDFSv1 and PB.
 * TODO: verify.
 */
int64_t
_hdfs_bslurp_vlint(struct hdfs_heap_buf *hb)
{
	uint64_t res, shift, b;

	res = 0;
	for (shift = 0; shift < 64; shift += 7) {
		b = (uint8_t)_hdfs_bslurp_s8(hb);
		if (hb->used < 0)
			return -1;

		res |= ((b & 0x7f) << shift);
		if ((b & 0x80) == 0)
			break;
	}
	if (shift >= 64) {
		/* Invalid varint >64-bits */
		hb->used = _H_PARSE_ERROR;
		return -1;
	}
	return (int64_t)res;
}

void
_hdfs_bslurp_mem1(struct hdfs_heap_buf *b, size_t len, char **obuf)
{
	char *res;

	if (_eos(b, len))
		return;

	res = malloc(len + 1);
	ASSERT(res);
	memcpy(res, b->buf + b->used, len);
	b->used += len;
	*obuf = res;
}

char *
_hdfs_bslurp_string(struct hdfs_heap_buf *b)
{
	int16_t slen;
	char *res;

	slen = _hdfs_bslurp_s16(b);
	if (b->used < 0)
		return NULL;

	if (slen < 0) {
		b->used = _H_PARSE_ERROR;
		return NULL;
	}

	_hdfs_bslurp_mem1(b, slen, &res);
	if (b->used < 0)
		return NULL;

	res[slen] = '\0';
	return res;
}

char *
_hdfs_bslurp_string32(struct hdfs_heap_buf *b)
{
	int32_t slen;
	char *res;

	slen = _hdfs_bslurp_s32(b);
	if (b->used < 0)
		return NULL;

	if (slen < 0 || slen > 10*1024*1024/*sanity*/) {
		b->used = _H_PARSE_ERROR;
		return NULL;
	}

	_hdfs_bslurp_mem1(b, slen, &res);
	if (b->used < 0)
		return NULL;

	res[slen] = '\0';
	return res;
}

char *
_hdfs_bslurp_text(struct hdfs_heap_buf *b)
{
	int8_t len;
	char *res;

	len = _hdfs_bslurp_vlint(b);
	if (b->used < 0)
		return NULL;

	ASSERT(len >= 0);

	_hdfs_bslurp_mem1(b, len, &res);
	if (b->used < 0)
		return NULL;

	res[len] = '\0';
	return res;
}

struct hdfs_error
_hdfs_sasl_encode_at_offset(sasl_conn_t *ctx, struct hdfs_heap_buf *h, int offset)
{
	const char *out;
	unsigned outlen;
	int r, toencode;

	ASSERT(offset >= 0);
	ASSERT(offset < h->used);
	ASSERT(offset >= h->pos);

	toencode = h->used - offset;
	r = sasl_encode(ctx, h->buf + offset, toencode, &out, &outlen);
	if (r != SASL_OK)
		return error_from_sasl(r);

	h->used = offset; // "remove" the plaintext data from the buffer
	_hdfs_hbuf_reserve(h, outlen + 4); // allocate enough space for the length prefix and encoded data

	// copy in length-prefixed encoded bits
	_be32enc(_hdfs_hbuf_writeptr(h), outlen);
	memcpy(_hdfs_hbuf_writeptr(h) + 4, out, outlen);
	_hdfs_hbuf_append(h, 4 + outlen);

	return HDFS_SUCCESS;
}
