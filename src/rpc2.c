#include <stdint.h>
#include <stdio.h>

#include <hadoofus/objects.h>

#include "heapbuf.h"
#include "objects-internal.h"
#include "rpc2-internal.h"
#include "util.h"

#include "ClientNamenodeProtocol.pb-c.h"

/* Support logic for v2+ Namenode RPC */

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
