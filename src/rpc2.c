#include <stdint.h>
#include <stdio.h>

#include <hadoofus/objects.h>

#include "heapbuf.h"
#include "objects-internal.h"
#include "rpc2-internal.h"
#include "util.h"

#include "ClientNamenodeProtocol.pb-c.h"

/* Support logic for v2+ Namenode RPC (requests) */

static void
_rpc2_encode_getServerDefaults(struct hdfs_heap_buf *dest,
	struct hdfs_rpc_invocation *rpc)
{
	GetServerDefaultsRequestProto req =
	    GET_SERVER_DEFAULTS_REQUEST_PROTO__INIT;
	size_t sz;

	ASSERT(streq(rpc->_method, "getServerDefaults"));
	ASSERT(rpc->_nargs == 0);

	sz = get_server_defaults_request_proto__get_packed_size(&req);
	_hbuf_reserve(dest, sz);
	get_server_defaults_request_proto__pack(&req,
	    (void *)&dest->buf[dest->used]);
	dest->used += sz;
}

void
_rpc2_request_serialize(struct hdfs_heap_buf *dest,
	struct hdfs_rpc_invocation *rpc)
{
	/* XXX Lookup table */

	_rpc2_encode_getServerDefaults(dest, rpc);
}

/* Support logic for Namenode RPC response parsing */

/*
 * These slurpers expect exactly one protobuf in the heapbuf passed in, and
 * advance the cursor (->used) to the end (->size) after a successful parse.
 */
static struct hdfs_object *
_oslurp_getServerDefaults(struct hdfs_heap_buf *buf)
{
	GetServerDefaultsResponseProto *resp;
	struct hdfs_object *result;

	result = NULL;

	resp = get_server_defaults_response_proto__unpack(NULL,
	    buf->size - buf->used, (void *)&buf->buf[buf->used]);
	buf->used = buf->size;
	if (resp == NULL) {
		buf->used = -2;
		return NULL;
	}

	result = _hdfs_fsserverdefaults_new_proto(resp->serverdefaults);

	get_server_defaults_response_proto__free_unpacked(resp, NULL);
	return result;
}

hdfs_object_slurper
_rpc2_slurper_for_rpc(struct hdfs_object *rpc)
{
	/* XXX Lookup table */

	ASSERT(rpc->ob_type == H_RPC_INVOCATION);
	if (streq(rpc->ob_val._rpc_invocation._method, "getServerDefaults"))
		return _oslurp_getServerDefaults;

	return NULL;
}
