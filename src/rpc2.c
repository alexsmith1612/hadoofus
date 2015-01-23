#include <stdint.h>
#include <stdio.h>

#include <hadoofus/objects.h>

#include "heapbuf.h"
#include "objects-internal.h"
#include "rpc2-internal.h"
#include "util.h"

#include "ClientNamenodeProtocol.pb-c.h"

/* Support logic for v2+ Namenode RPC (requests) */

#define ENCODE_PREAMBLE(lowerCamel, CamelCase, UPPER_CASE)		\
static void								\
_rpc2_encode_ ## lowerCamel (struct hdfs_heap_buf *dest,		\
	struct hdfs_rpc_invocation *rpc)				\
{									\
	CamelCase ## RequestProto req =					\
	    UPPER_CASE ## _REQUEST_PROTO__INIT;				\
	size_t sz;

	/*
	 * Additional validation of input object and population of req
	 * structure go here.
	 */

#define ENCODE_POSTSCRIPT(lower_case)					\
	sz = lower_case ## _request_proto__get_packed_size(&req);	\
	_hbuf_reserve(dest, sz);					\
	lower_case ## _request_proto__pack(&req,			\
	    (void *)&dest->buf[dest->used]);				\
	dest->used += sz;						\
}

ENCODE_PREAMBLE(getServerDefaults, GetServerDefaults, GET_SERVER_DEFAULTS)
{
	ASSERT(rpc->_nargs == 0);
}
ENCODE_POSTSCRIPT(get_server_defaults)


/* Compat for v1 method hdfs_getListing(). */
ENCODE_PREAMBLE(getListing, GetListing, GET_LISTING)
{
	ASSERT(rpc->_nargs == 2);
	ASSERT(rpc->_args[0]->ob_type == H_STRING);
	ASSERT(rpc->_args[1]->ob_type == H_ARRAY_BYTE);

	req.src = rpc->_args[0]->ob_val._string._val;
	req.startafter.len = rpc->_args[1]->ob_val._array_byte._len;
	req.startafter.data = (void *)rpc->_args[1]->ob_val._array_byte._bytes;

	/* TODO Add hdfs2-only method for specifying needlocation */
	req.needlocation = false;
}
ENCODE_POSTSCRIPT(get_listing)

typedef void (*_rpc2_encoder)(struct hdfs_heap_buf *, struct hdfs_rpc_invocation *);
static struct _rpc2_enc_lut {
	const char *	re_method;
	_rpc2_encoder	re_encoder;
} rpc2_encoders[] = {
#define _RENC(method)	{ #method , _rpc2_encode_ ## method , }
	_RENC(getServerDefaults),
	_RENC(getListing),
	{ NULL, NULL, },
#undef _RENC
};

void
_rpc2_request_serialize(struct hdfs_heap_buf *dest,
	struct hdfs_rpc_invocation *rpc)
{
	/* XXXPERF Could be a hash/rt/sorted table */
	unsigned i;

	for (i = 0; rpc2_encoders[i].re_method; i++) {
		if (streq(rpc->_method, rpc2_encoders[i].re_method)) {
			rpc2_encoders[i].re_encoder(dest, rpc);
			return;
		}
	}

	/* The method does not exist in HDFSv2, or it is not yet implemented. */
	ASSERT(false);
}

/* Support logic for Namenode RPC response parsing */

/*
 * These slurpers expect exactly one protobuf in the heapbuf passed in, and
 * advance the cursor (->used) to the end (->size) after a successful parse.
 */
#define DECODE_PB(lowerCamel, CamelCase, lower_case, objbuilder, respfield)	\
static struct hdfs_object *						\
_oslurp_ ## lowerCamel (struct hdfs_heap_buf *buf)			\
{									\
	CamelCase ## ResponseProto *resp;				\
	struct hdfs_object *result;					\
									\
	result = NULL;							\
									\
	resp = lower_case ## _response_proto__unpack(NULL,		\
	    buf->size - buf->used, (void *)&buf->buf[buf->used]);	\
	buf->used = buf->size;						\
	if (resp == NULL) {						\
		buf->used = -2;						\
		return NULL;						\
	}								\
									\
	result = _hdfs_ ## objbuilder ## _new_proto(resp->respfield);	\
									\
	lower_case ## _response_proto__free_unpacked(resp, NULL);	\
	return result;							\
}

DECODE_PB(getServerDefaults, GetServerDefaults, get_server_defaults, fsserverdefaults, serverdefaults)
DECODE_PB(getListing, GetListing, get_listing, directory_listing, dirlist)

static struct _rpc2_dec_lut {
	const char *		rd_method;
	hdfs_object_slurper	rd_decoder;
} rpc2_decoders[] = {
#define _RDEC(method)	{ #method , _oslurp_ ## method , }
	_RDEC(getServerDefaults),
	_RDEC(getListing),
	{ NULL, NULL, },
#undef _RDEC
};

hdfs_object_slurper
_rpc2_slurper_for_rpc(struct hdfs_object *rpc)
{
	/* XXXPERF s.a. */
	unsigned i;

	ASSERT(rpc->ob_type == H_RPC_INVOCATION);

	for (i = 0; rpc2_decoders[i].rd_method; i++)
		if (streq(rpc->ob_val._rpc_invocation._method,
		    rpc2_decoders[i].rd_method))
			return rpc2_decoders[i].rd_decoder;

	/*
	 * The method does not exist in HDFSv2 (this function can be called for
	 * HDFSv1 methods) or it is not yet implemented.
	 */
	return NULL;
}
