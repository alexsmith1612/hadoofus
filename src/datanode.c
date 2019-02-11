#include <netinet/in.h>
#include <netinet/tcp.h>

#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <zlib.h>

#include <hadoofus/highlevel.h>

#include "heapbuf.h"
#include "net.h"
#include "objects-internal.h"
#include "pthread_wrappers.h"
#include "util.h"

#include "datatransfer.pb-c.h"

/*
 * These are HDFS wire values.
 */
#define OP_WRITE 0x50
#define OP_READ 0x51

_Thread_local int hdfs_datanode_unknown_status EXPORT_SYM;
_Thread_local const char *hdfs_datanode_opresult_message EXPORT_SYM;

static char DN_CHECKSUM_OK[2] = {
	(char)(STATUS__CHECKSUM_OK >> 8),
	(char)(STATUS__CHECKSUM_OK & 0xff),
};
static char DN_ERROR_CHECKSUM[2] = {
	(char)(STATUS__ERROR_CHECKSUM >> 8),
	(char)(STATUS__ERROR_CHECKSUM & 0xff),
};

static const int MAX_UNACKED_PACKETS = 80 /*same as apache*/,
	     CHUNK_SIZE = 512,
	     PACKET_SIZE = 64 * 1024;

struct _packet_state {
	int64_t seqno,
		first_unacked,
		offset;
	off_t remains,
	      fdoffset;
	void *buf;
	struct hdfs_heap_buf *recvbuf;
	int sock,
	    unacked_packets,
	    proto,
	    fd;
	bool sendcrcs;
};

struct _read_state {
	int64_t client_offset,
		server_offset;
	int32_t chunk_size;
	bool has_crcs;
};

static struct hdfs_error	error_from_datanode(int);

static struct hdfs_error	_datanode_read(struct hdfs_datanode *, off_t bloff, off_t len,
				int fd, off_t fdoff, void *buf, bool verify);
static struct hdfs_error	_datanode_write(struct hdfs_datanode *, const void *buf, int fd, off_t len,
				off_t offset, bool sendcrcs);
static struct hdfs_error	_read_read_status(struct hdfs_datanode *, struct hdfs_heap_buf *,
				struct _read_state *);
static struct hdfs_error	_read_read_status2(struct hdfs_datanode *, struct hdfs_heap_buf *,
				struct _read_state *);
static struct hdfs_error	_read_write_status(struct hdfs_datanode *, struct hdfs_heap_buf *);
static struct hdfs_error	_read_write_status2(struct hdfs_datanode *, struct hdfs_heap_buf *);
static struct hdfs_error	_recv_packet(struct _packet_state *, struct _read_state *);
static struct hdfs_error	_process_recv_packet(struct _packet_state *, struct _read_state *,
				ssize_t /*hdr_len*/, ssize_t /*plen*/, ssize_t /*dlen*/,
				int64_t /*offset*/, bool /*lastpacket*/);
static struct hdfs_error	_send_packet(struct _packet_state *);
static void			_set_opres_msg(const char *);
static struct hdfs_error	_verify_crcdata(void *crcdata, int32_t chunksize,
				int32_t crcdlen, int32_t dlen);
static struct hdfs_error	_wait_ack(struct _packet_state *ps);
static struct hdfs_error	_wait_ack2(struct _packet_state *ps);

//
// high-level api
//

EXPORT_SYM struct hdfs_datanode *
hdfs_datanode_new(struct hdfs_object *located_block, const char *client,
	int proto, struct hdfs_error *error_out)
{
	struct hdfs_datanode *d;
	struct hdfs_error error;
	int32_t n;

	ASSERT(located_block);
	ASSERT(located_block->ob_type == H_LOCATED_BLOCK);

	/* Bail early if the LB is non-actionable */
	if (__predict_false(located_block->ob_val._located_block._num_locs == 0)) {
		*error_out = error_from_hdfs(HDFS_ERR_ZERO_DATANODES);
		return NULL;
	}

	d = malloc(sizeof(*d));
	ASSERT(d);

	hdfs_datanode_init(d,
	    located_block->ob_val._located_block._blockid,
	    located_block->ob_val._located_block._len,
	    located_block->ob_val._located_block._generation,
	    located_block->ob_val._located_block._offset,
	    client,
	    located_block->ob_val._located_block._token,
	    proto);

	if (proto >= HDFS_DATANODE_AP_2_0)
		hdfs_datanode_set_pool_id(d,
		    located_block->ob_val._located_block._pool_id);

	// Try each datanode in the LocatedBlock until one successfully
	// connects
	n = located_block->ob_val._located_block._num_locs;
	for (int32_t i = 0; i < n; i++) {
		struct hdfs_object *di =
		    located_block->ob_val._located_block._locs[i];

		error = hdfs_datanode_connect(d,
		    di->ob_val._datanode_info._ipaddr,
		    di->ob_val._datanode_info._port);
		if (!hdfs_is_error(error))
			return d;
	}

	hdfs_datanode_destroy(d);
	free(d);
	*error_out = error;
	return NULL;
}

EXPORT_SYM void
hdfs_datanode_delete(struct hdfs_datanode *d)
{
	hdfs_datanode_destroy(d);
	free(d);
}

//
// low-level api
//

EXPORT_SYM void
hdfs_datanode_init(struct hdfs_datanode *d,
	int64_t blkid, int64_t size, int64_t gen, /* block */
	int64_t offset, const char *client, struct hdfs_object *token,
	int proto)
{
	ASSERT(d);
	ASSERT(proto == HDFS_DATANODE_AP_1_0 || proto == HDFS_DATANODE_CDH3 ||
	    proto == HDFS_DATANODE_AP_2_0);

	_mtx_init(&d->dn_lock);
	d->dn_sock = -1;
	d->dn_used = false;

	d->dn_blkid = blkid;
	d->dn_size = size;
	d->dn_gen = gen;

	d->dn_offset = offset;
	if (token)
		d->dn_token = hdfs_token_copy(token);
	else
		d->dn_token = hdfs_token_new_empty();

	ASSERT(client);
	d->dn_client = strdup(client);
	ASSERT(d->dn_client);

	d->dn_proto = proto;
	d->dn_pool_id = NULL;
}

EXPORT_SYM void
hdfs_datanode_set_pool_id(struct hdfs_datanode *d, const char *pool_id)
{
	char *pool_copy;

	ASSERT(pool_id);
	pool_copy = strdup(pool_id);
	ASSERT(pool_copy);

	d->dn_pool_id = pool_copy;
}

EXPORT_SYM void
hdfs_datanode_destroy(struct hdfs_datanode *d)
{
	ASSERT(d);

	_lock(&d->dn_lock);
	if (d->dn_sock != -1)
		close(d->dn_sock);
	hdfs_object_free(d->dn_token);
	free(d->dn_client);
	free(d->dn_pool_id);
	_unlock(&d->dn_lock);

	_mtx_destroy(&d->dn_lock);
	memset(d, 0, sizeof *d);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_connect(struct hdfs_datanode *d, const char *host, const char *port)
{
	struct hdfs_error error;

	ASSERT(d);

	_lock(&d->dn_lock);

	ASSERT(d->dn_sock == -1);
	error = _connect(&d->dn_sock, host, port);

	_unlock(&d->dn_lock);

	return error;
}

// Datanode write operations

EXPORT_SYM struct hdfs_error
hdfs_datanode_write(struct hdfs_datanode *d, const void *buf, size_t len, bool sendcrcs)
{
	ASSERT(buf);

	return _datanode_write(d, buf, -1, len, -1, sendcrcs);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_write_file(struct hdfs_datanode *d, int fd, off_t len, off_t offset,
	bool sendcrcs)
{
	ASSERT(offset >= 0);
	ASSERT(fd >= 0);

	return _datanode_write(d, NULL, fd, len, offset, sendcrcs);
}

// Datanode read operations

EXPORT_SYM struct hdfs_error
hdfs_datanode_read(struct hdfs_datanode *d, size_t off, size_t len, void *buf,
	bool verifycrc)
{
	ASSERT(buf);

	return _datanode_read(d, off, len, -1/*fd*/, -1/*fdoff*/, buf,
	    verifycrc);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_read_file(struct hdfs_datanode *d, off_t bloff, off_t len,
	int fd, off_t fdoff, bool verifycrc)
{
	ASSERT(bloff >= 0);
	ASSERT(fdoff >= 0);
	ASSERT(fd >= 0);

	return _datanode_read(d, bloff, len, fd, fdoff, NULL/*buf*/, verifycrc);
}

static struct hdfs_error
error_from_datanode(int dnstatus)
{
	enum hdfs_error_numeric ecode;

	ASSERT(dnstatus != STATUS__SUCCESS);

	switch (dnstatus) {
	case STATUS__ERROR:
		ecode = HDFS_ERR_DN_ERROR;
		break;
	case STATUS__ERROR_CHECKSUM:
		ecode = HDFS_ERR_DN_ERROR_CHECKSUM;
		break;
	case STATUS__ERROR_INVALID:
		ecode = HDFS_ERR_DN_ERROR_INVALID;
		break;
	case STATUS__ERROR_EXISTS:
		ecode = HDFS_ERR_DN_ERROR_EXISTS;
		break;
	case STATUS__ERROR_ACCESS_TOKEN:
		ecode = HDFS_ERR_DN_ERROR_ACCESS_TOKEN;
		break;
	case STATUS__CHECKSUM_OK:
		ecode = HDFS_ERR_INVALID_DN_ERROR;
		hdfs_datanode_unknown_status = dnstatus;
		break;
	default:
		ecode = HDFS_ERR_UNRECOGNIZED_DN_ERROR;
		hdfs_datanode_unknown_status = dnstatus;
		break;
	}
	return error_from_hdfs(ecode);
}

static void
_compose_read_header(struct hdfs_heap_buf *h, struct hdfs_datanode *d,
	off_t offset, off_t len, bool crcs)
{
	_bappend_s16(h, d->dn_proto);

	_bappend_s8(h, OP_READ);

	if (d->dn_proto >= HDFS_DATANODE_AP_2_0) {
		BlockTokenIdentifierProto token =
		    BLOCK_TOKEN_IDENTIFIER_PROTO__INIT;
		ExtendedBlockProto ebp = EXTENDED_BLOCK_PROTO__INIT;
		BaseHeaderProto bhdr = BASE_HEADER_PROTO__INIT;
		ClientOperationHeaderProto hdr =
		    CLIENT_OPERATION_HEADER_PROTO__INIT;
		OpReadBlockProto opread = OP_READ_BLOCK_PROTO__INIT;

		struct hdfs_token *h_token;
		size_t sz;

		h_token = &d->dn_token->ob_val._token;

		ASSERT(d->dn_pool_id);
		ebp.poolid = d->dn_pool_id;
		ebp.blockid = d->dn_blkid;
		ebp.generationstamp = d->dn_gen;

		token.identifier.len = h_token->_lens[0];
		token.identifier.data = (void *)h_token->_strings[0];
		token.password.len = h_token->_lens[1];
		token.password.data = (void *)h_token->_strings[1];
		token.kind = h_token->_strings[2];
		token.service = h_token->_strings[3];

		bhdr.block = &ebp;
		bhdr.token = &token;

		hdr.baseheader = &bhdr;
		hdr.clientname = d->dn_client;

		opread.header = &hdr;
		opread.offset = offset;
		opread.len = len;

		/* Defaults to true ("send crcs") */
		if (!crcs) {
			opread.has_sendchecksums = true;
			opread.sendchecksums = false;
		}

		sz = op_read_block_proto__get_packed_size(&opread);
		_bappend_vlint(h, sz);
		_hbuf_reserve(h, sz);
		op_read_block_proto__pack(&opread, (void *)&h->buf[h->used]);
		h->used += sz;
	} else {
		_bappend_s64(h, d->dn_blkid);
		_bappend_s64(h, d->dn_gen);
		_bappend_s64(h, offset);
		_bappend_s64(h, len);
		_bappend_text(h, d->dn_client);
		hdfs_object_serialize(h, d->dn_token);
	}
}

static void
_compose_write_header(struct hdfs_heap_buf *h, struct hdfs_datanode *d, bool crcs)
{
	_bappend_s16(h, d->dn_proto);

	_bappend_s8(h, OP_WRITE);

	if (d->dn_proto >= HDFS_DATANODE_AP_2_0) {
		BlockTokenIdentifierProto token =
		    BLOCK_TOKEN_IDENTIFIER_PROTO__INIT;
		ExtendedBlockProto ebp = EXTENDED_BLOCK_PROTO__INIT;
		BaseHeaderProto bhdr = BASE_HEADER_PROTO__INIT;
		ClientOperationHeaderProto hdr =
		    CLIENT_OPERATION_HEADER_PROTO__INIT;
		ChecksumProto csum = CHECKSUM_PROTO__INIT;
		OpWriteBlockProto op = OP_WRITE_BLOCK_PROTO__INIT;

		struct hdfs_token *h_token;
		size_t sz;

		h_token = &d->dn_token->ob_val._token;

		ASSERT(d->dn_pool_id);
		ebp.poolid = d->dn_pool_id;
		ebp.blockid = d->dn_blkid;
		ebp.generationstamp = d->dn_gen;

		token.identifier.len = h_token->_lens[0];
		token.identifier.data = (void *)h_token->_strings[0];
		token.password.len = h_token->_lens[1];
		token.password.data = (void *)h_token->_strings[1];
		token.kind = h_token->_strings[2];
		token.service = h_token->_strings[3];

		bhdr.block = &ebp;
		bhdr.token = &token;

		hdr.baseheader = &bhdr;
		hdr.clientname = d->dn_client;

		csum.bytesperchecksum = 512;
		csum.type = CHECKSUM_TYPE_PROTO__NULL;
		if (crcs)
			csum.type = CHECKSUM_TYPE_PROTO__CRC32;

		op.header = &hdr;

		/* XXX maybe SETUP_APPEND iff located_block size > 0? */
		op.stage = OP_WRITE_BLOCK_PROTO__BLOCK_CONSTRUCTION_STAGE__PIPELINE_SETUP_CREATE;

		/* Not sure about any of this: */
		op.pipelinesize = 1;
		op.minbytesrcvd = d->dn_size;
		op.maxbytesrcvd = d->dn_size;
		op.latestgenerationstamp = d->dn_gen;
		op.requestedchecksum = &csum;

		sz = op_write_block_proto__get_packed_size(&op);
		_bappend_vlint(h, sz);
		_hbuf_reserve(h, sz);
		op_write_block_proto__pack(&op, (void *)&h->buf[h->used]);
		h->used += sz;
	} else {
		_bappend_s64(h, d->dn_blkid);
		_bappend_s64(h, d->dn_gen);
		_bappend_s32(h, 1);
		_bappend_s8(h, 0);
		_bappend_text(h, d->dn_client);
		_bappend_s8(h, 0);
		_bappend_s32(h, 0);
		hdfs_object_serialize(h, d->dn_token);
		_bappend_s8(h, !!crcs);
		_bappend_s32(h, CHUNK_SIZE/*checksum chunk size*/);
	}
}

static struct hdfs_error
_datanode_read(struct hdfs_datanode *d, off_t bloff, off_t len,
	int fd, off_t fdoff, void *buf, bool verify)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_heap_buf header = { 0 },
			     recvbuf = { 0 };
	struct _packet_state pstate = { 0 };
	struct _read_state rinfo = { 0 };

	ASSERT(d);
	ASSERT(len > 0);

	_lock(&d->dn_lock);

	ASSERT(!d->dn_used);
	d->dn_used = true;

	_compose_read_header(&header, d, bloff, len, verify);
	error = _write_all(d->dn_sock, header.buf, header.used);
	if (hdfs_is_error(error))
		goto out;

	if (d->dn_proto >= HDFS_DATANODE_AP_2_0)
		error = _read_read_status2(d, &recvbuf, &rinfo);
	else
		error = _read_read_status(d, &recvbuf, &rinfo);
	if (hdfs_is_error(error))
		goto out;

	if (!rinfo.has_crcs && verify) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_NO_CRCS);
		goto out;
	}

	// good to read. begin.
	rinfo.client_offset = bloff;

	pstate.sock = d->dn_sock;
	pstate.sendcrcs = verify;
	pstate.buf = buf;
	pstate.fd = fd;
	pstate.remains = len;
	pstate.fdoffset = fdoff;
	pstate.recvbuf = &recvbuf;
	pstate.proto = d->dn_proto;
	while (pstate.remains > 0) {
		error = _recv_packet(&pstate, &rinfo);
		if (hdfs_is_error(error))
			goto out;
	}

	// tell server the read was fine
	if (d->dn_proto >= HDFS_DATANODE_AP_2_0) {
		ClientReadStatusProto status = CLIENT_READ_STATUS_PROTO__INIT;
		size_t sz;

		if (rinfo.has_crcs)
			status.status = STATUS__CHECKSUM_OK;
		else
			status.status = STATUS__SUCCESS;

		sz = client_read_status_proto__get_packed_size(&status);

		/* Re-use headerbuf for DN ACK. */
		header.used = 0;
		_bappend_vlint(&header, sz);
		_hbuf_reserve(&header, sz);
		client_read_status_proto__pack(&status,
		    (void *)&header.buf[header.used]);
		header.used += sz;

		error = _write_all(d->dn_sock, header.buf, header.used);
	} else
		error = _write_all(d->dn_sock, DN_CHECKSUM_OK, 2);
	if (hdfs_is_error(error))
		goto out;

out:
	if (header.buf)
		free(header.buf);
	if (recvbuf.buf)
		free(recvbuf.buf);
	_unlock(&d->dn_lock);
	return error;
}

static struct hdfs_error
_datanode_write(struct hdfs_datanode *d, const void *buf, int fd, off_t len,
	off_t offset, bool sendcrcs)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_heap_buf header = { 0 },
			     recvbuf = { 0 };

	struct _packet_state pstate = { 0 };
	const int32_t zero = 0;

	ASSERT(d);
	ASSERT(len > 0);

	_lock(&d->dn_lock);

	ASSERT(!d->dn_used);
	d->dn_used = true;

	_compose_write_header(&header, d, sendcrcs);
	error = _write_all(d->dn_sock, header.buf, header.used);
	if (hdfs_is_error(error))
		goto out;

	if (d->dn_proto >= HDFS_DATANODE_AP_2_0)
		error = _read_write_status2(d, &recvbuf);
	else
		error = _read_write_status(d, &recvbuf);
	if (hdfs_is_error(error))
		goto out;

	// we're good to write. start sending packets.
	pstate.sock = d->dn_sock;
	pstate.sendcrcs = sendcrcs;
	pstate.buf = __DECONST(void*, buf);
	pstate.fd = fd;
	pstate.remains = len;
	pstate.fdoffset = offset;
	pstate.recvbuf = &recvbuf;
	pstate.proto = d->dn_proto;
	pstate.offset = d->dn_size;
	while (pstate.remains > 0) {
		error = _send_packet(&pstate);
		if (hdfs_is_error(error))
			goto out;
	}

	// Drain remaining acks to ensure write succeeded
	while (pstate.unacked_packets > 0) {
		if (d->dn_proto >= HDFS_DATANODE_AP_2_0)
			error = _wait_ack2(&pstate);
		else
			error = _wait_ack(&pstate);
		if (hdfs_is_error(error))
			goto out;
	}

	// Write final zero-len packet; error here doesn't always matter. I
	// think some HDFS versions drop the connection at this point, so we
	// want to be lenient.
	if (d->dn_proto < HDFS_DATANODE_AP_2_0)
		_write_all(d->dn_sock, __DECONST(void*, &zero), sizeof zero);

out:
	if (header.buf)
		free(header.buf);
	if (recvbuf.buf)
		free(recvbuf.buf);
	_unlock(&d->dn_lock);
	return error;
}

static struct hdfs_error
_read_read_status(struct hdfs_datanode *d, struct hdfs_heap_buf *h,
	struct _read_state *rs)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_heap_buf obuf = { 0 };
	int16_t status;
	int32_t chunk_size;
	int64_t server_offset;
	bool crcs;

	while (h->used < 2) {
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error))
			goto out;
	}

	obuf.buf = h->buf;
	obuf.size = h->used;

	status = _bslurp_s16(&obuf);
	ASSERT(obuf.used > 0);

	if (status != STATUS__SUCCESS) {
		error = error_from_datanode(status);
		goto out;
	}

	while (h->used < 15) {
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error))
			goto out;
	}

	obuf.size = h->used;

	crcs = _bslurp_s8(&obuf);
	ASSERT(obuf.used > 0);
	chunk_size = _bslurp_s32(&obuf);
	ASSERT(obuf.used > 0);
	server_offset = _bslurp_s64(&obuf);
	ASSERT(obuf.used > 0);

	rs->server_offset = server_offset;
	rs->chunk_size = chunk_size;
	rs->has_crcs = crcs;

	// Skip recvbuf past request status
	h->used -= obuf.used;
	memmove(h->buf, h->buf + obuf.used, h->used);

out:
	return error;
}

static struct hdfs_error
_read_blockop_resp_status(struct hdfs_datanode *d, struct hdfs_heap_buf *h,
	BlockOpResponseProto **opres_out)
{
	struct hdfs_heap_buf obuf = { 0 };
	BlockOpResponseProto *opres;
	struct hdfs_error error;
	int64_t sz;

	opres = NULL;
	do {
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error))
			goto out;

		obuf.buf = h->buf;
		obuf.used = 0;
		obuf.size = h->used;

		sz = _bslurp_vlint(&obuf);
		if (obuf.used == _H_PARSE_ERROR) {
			error = error_from_hdfs(HDFS_ERR_INVALID_VLINT);
			goto out;
		}
	} while (obuf.used < 0);

	ASSERT(sz < INT_MAX - obuf.used);
	while (h->used < obuf.used + (int)sz) {
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error))
			goto out;
	}
	obuf.buf = h->buf;
	obuf.size = h->used;

	opres = block_op_response_proto__unpack(NULL, sz,
	    (void *)&h->buf[obuf.used]);
	obuf.used += sz;
	if (opres == NULL) {
		error = error_from_hdfs(HDFS_ERR_INVALID_BLOCKOPRESPONSEPROTO);
		goto out;
	}

	_set_opres_msg(opres->message);
	if (opres->status != STATUS__SUCCESS)
		error = error_from_datanode(opres->status);

	// Shouldn't happen, I believe; call it a protocol error.
	if (opres->message != NULL)
		error = error_from_hdfs(HDFS_ERR_INVALID_DN_OPRESP_MSG);

	if (hdfs_is_error(error))
		block_op_response_proto__free_unpacked(opres, NULL);
	else
		*opres_out = opres;

	// Skip recvbuf past stuff we parsed here
	h->used -= obuf.used;
	memmove(h->buf, h->buf + obuf.used, h->used);

out:
	return error;
}

static struct hdfs_error
_read_read_status2(struct hdfs_datanode *d, struct hdfs_heap_buf *h,
	struct _read_state *rs)
{
	struct hdfs_error error;
	BlockOpResponseProto *opres;

	opres = NULL;

	error = _read_blockop_resp_status(d, h, &opres);
	if (hdfs_is_error(error))
		goto out;

	ASSERT(opres->readopchecksuminfo);
	ASSERT(opres->readopchecksuminfo->checksum);

	rs->server_offset = opres->readopchecksuminfo->chunkoffset;

	/* XXX check what kind of CRC? */
	rs->has_crcs = (opres->readopchecksuminfo->checksum->type !=
	    CHECKSUM_TYPE_PROTO__NULL);
	rs->chunk_size = opres->readopchecksuminfo->checksum->bytesperchecksum;

out:
	if (opres)
		block_op_response_proto__free_unpacked(opres, NULL);
	return error;
}

static struct hdfs_error
_read_write_status(struct hdfs_datanode *d, struct hdfs_heap_buf *h)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_heap_buf obuf = { 0 };
	int16_t status;
	char *statusmsg = NULL;
	size_t statussz;

	while (true) {
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error))
			goto out;

		obuf.buf = h->buf;
		obuf.used = 0;
		obuf.size = h->used;

		status = _bslurp_s16(&obuf);
		if (obuf.used >= 0)
			break;

		if (obuf.used == _H_PARSE_ERROR) {
			error = error_from_hdfs(HDFS_ERR_V1_DATANODE_PROTOCOL);
			goto out;
		}
	}

	statussz = obuf.used;

	while (true) {
		obuf.buf = h->buf + statussz;
		obuf.used = 0;
		obuf.size = h->used - statussz;

		statusmsg = _bslurp_text(&obuf);
		if (obuf.used >= 0)
			break;

		if (obuf.used == _H_PARSE_ERROR) {
			error = error_from_hdfs(HDFS_ERR_V1_DATANODE_PROTOCOL);
			goto out;
		}

		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error))
			goto out;
	}

	statussz += obuf.used;

	if (status == STATUS__SUCCESS && strlen(statusmsg) == 0)
		_set_opres_msg(NULL);
	else {
		// Shouldn't happen, I believe; call it a protocol error.
		if (status == STATUS__SUCCESS)
			error = error_from_hdfs(HDFS_ERR_INVALID_DN_OPRESP_MSG);
		else
			error = error_from_datanode(status);
		_set_opres_msg(statusmsg);
	}

	// Skip the recv buffer past the read objects
	h->used -= statussz;
	if (h->used)
		memmove(h->buf, h->buf + statussz, h->used);

out:
	if (statusmsg)
		free(statusmsg);
	return error;
}

static struct hdfs_error
_read_write_status2(struct hdfs_datanode *d, struct hdfs_heap_buf *h)
{
	struct hdfs_error error = HDFS_SUCCESS;
	BlockOpResponseProto *opres;

	opres = NULL;

	error = _read_blockop_resp_status(d, h, &opres);

	if (opres)
		block_op_response_proto__free_unpacked(opres, NULL);
	return error;
}

static struct hdfs_error
_recv_packet(struct _packet_state *ps, struct _read_state *rs)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_heap_buf *recvbuf = ps->recvbuf,
			     obuf = { 0 };
	int32_t plen, dlen;
	PacketHeaderProto *phdr;
	int64_t offset;
	bool lastpacket;
	uint16_t hlen;

	phdr = NULL;

	if (ps->proto < HDFS_DATANODE_AP_2_0) {
		// slurp packet header
		while (recvbuf->used < 25) {
			error = _read_to_hbuf(ps->sock, recvbuf);
			if (hdfs_is_error(error))
				goto out;
		}

		obuf.buf = recvbuf->buf;
		obuf.size = recvbuf->used;

		plen = _bslurp_s32(&obuf);
		ASSERT(obuf.used > 0);
		offset = _bslurp_s64(&obuf);
		ASSERT(obuf.used > 0);
		/*seqno = */_bslurp_s64(&obuf);
		ASSERT(obuf.used > 0);
		lastpacket = _bslurp_s8(&obuf);
		ASSERT(obuf.used > 0);
		dlen = _bslurp_s32(&obuf);
		ASSERT(obuf.used > 0);

		error = _process_recv_packet(ps, rs, 25, plen, dlen, offset,
		    lastpacket);
		goto out;
	}

	while (recvbuf->used < 6) {
		error = _read_to_hbuf(ps->sock, recvbuf);
		if (hdfs_is_error(error))
			goto out;
	}

	obuf.buf = recvbuf->buf;
	obuf.size = recvbuf->used;

	plen = _bslurp_s32(&obuf);
	ASSERT(obuf.used > 0);
	hlen = (uint16_t)_bslurp_s16(&obuf);
	ASSERT(obuf.used > 0);

	while (recvbuf->used < 6 + hlen) {
		error = _read_to_hbuf(ps->sock, recvbuf);
		if (hdfs_is_error(error))
			goto out;
	}

	phdr = packet_header_proto__unpack(NULL, hlen,
	    (void *)&recvbuf->buf[6]);
	if (phdr == NULL) {
		error = error_from_hdfs(HDFS_ERR_INVALID_PACKETHEADERPROTO);
		goto out;
	}

	offset = phdr->offsetinblock;
	lastpacket = phdr->lastpacketinblock;
	dlen = phdr->datalen;

	error = _process_recv_packet(ps, rs, 6 + hlen, plen, dlen, offset,
	    lastpacket);

out:
	if (phdr)
		packet_header_proto__free_unpacked(phdr, NULL);
	return error;
}

static struct hdfs_error
_process_recv_packet(struct _packet_state *ps, struct _read_state *rs,
	ssize_t hdr_len, ssize_t plen, ssize_t dlen, int64_t offset,
	bool lastpacket)
{
	struct hdfs_heap_buf *recvbuf = ps->recvbuf;
	const int ONEGB = 1024*1024*1024;
	struct hdfs_error error = HDFS_SUCCESS;
	int32_t c_begin, c_len;
	ssize_t crcdlen;

	crcdlen = plen - dlen - 4;
	if (plen < 0 || dlen < 0 || dlen > ONEGB || plen > ONEGB || crcdlen < 0)
		error = error_from_hdfs(HDFS_ERR_DATANODE_PACKET_SIZE);
	else if (rs->has_crcs && crcdlen != ((dlen + rs->chunk_size - 1) / rs->chunk_size) * 4)
		error = error_from_hdfs(HDFS_ERR_DATANODE_CRC_LEN);
	else if (!rs->has_crcs && crcdlen > 0)
		error = error_from_hdfs(HDFS_ERR_DATANODE_UNEXPECTED_CRC_LEN);
	if (hdfs_is_error(error))
		goto out;

	/*
	 * v2 appears to send an empty packet (just header) at the end,
	 * although we stop paying attention after we've gotten as many bytes
	 * as we expected (ps->remaining).
	 */
	if (dlen == 0) {
		ASSERT(lastpacket);
		ASSERT(crcdlen == 0);
		goto check_remainder;
	}

	while (recvbuf->used < hdr_len + crcdlen + dlen) {
		error = _read_to_hbuf(ps->sock, recvbuf);
		if (hdfs_is_error(error))
			goto out;
	}

	if (crcdlen > 0) {
		error = _verify_crcdata(recvbuf->buf + hdr_len, rs->chunk_size, crcdlen, dlen);
		if (hdfs_is_error(error)) {
			// On CRC errors, let the server know before aborting:
			_write_all(ps->sock, DN_ERROR_CHECKSUM, 2);
			goto out;
		}
	}

	// figure out where in the packet to start copying from, and how much to copy
	if (offset < rs->client_offset)
		c_begin = rs->client_offset - offset;
	else
		c_begin = 0;
	if (c_begin >= dlen) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_UNEXPECTED_READ_OFFSET);
		goto out;
	}
	c_len = _min(dlen - c_begin, ps->remains);

	// Copy the packet data out to the user's buf or to file:
	if (ps->buf) {
		memcpy(ps->buf, recvbuf->buf + hdr_len + crcdlen + c_begin, c_len);
	} else {
		int written = 0, rc;
		while (written < c_len) {
			rc = pwrite(ps->fd,
			    recvbuf->buf + hdr_len + crcdlen + c_begin + written,
			    c_len - written,
			    ps->fdoffset + written);
			if (rc == -1)
				error = error_from_errno(errno);
			else if (rc == 0)
				error = error_from_hdfs(HDFS_ERR_END_OF_FILE);

			if (hdfs_is_error(error))
				goto out;
			written += rc;
		}
	}

	ps->remains -= c_len;
	ps->fdoffset += c_len;
	if (ps->buf)
		ps->buf = (char*)ps->buf + c_len;

check_remainder:
	if (ps->remains > 0 && lastpacket)
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_LASTPACKET);

	// skip recvbuf over this packet. this is probably excessive memcopying
	// especially if/when multiple packets queue up. TODO: something
	// smarter.
	recvbuf->used -= (hdr_len + crcdlen + dlen);
	memmove(recvbuf->buf, recvbuf->buf + hdr_len + crcdlen + dlen, recvbuf->used);

out:
	return error;
}

static struct hdfs_error
_send_packet(struct _packet_state *ps)
{

	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_heap_buf phdr = { 0 };
	uint32_t *crcdata = NULL;
	size_t crclen = 0, tosend;
	unsigned char *data = NULL;
	bool last, datamalloced = false;
	struct iovec ios[3];

	tosend = _min(ps->remains, PACKET_SIZE);

	if (ps->offset % CHUNK_SIZE) {
		// N.B.: If you have a partial block, appending the unaligned
		// bits first makes the remaining writes aligned. We mostly do
		// this to match Apache HDFS behavior on append.
		int64_t remaining_in_chunk =
		    CHUNK_SIZE - (ps->offset % CHUNK_SIZE);

		tosend = _min(tosend, remaining_in_chunk);
	}

	last = (tosend == (size_t)ps->remains);

	// Delay sending data while N packets remain unacknowledged.
	// Apache Hadoop default is N=80, for a 5MB window.
	if (ps->unacked_packets >= MAX_UNACKED_PACKETS) {
		if (ps->proto >= HDFS_DATANODE_AP_2_0)
			error = _wait_ack2(ps);
		else
			error = _wait_ack(ps);
		if (hdfs_is_error(error))
			goto out;
	}

	if (!ps->buf) {
#if !defined(__linux__) && !defined(__FreeBSD__)
		datamalloced = true;
#else
		if (ps->sendcrcs) {
			datamalloced = true;
		} else {
			struct stat sb;
			int rc;
			rc = fstat(ps->fd, &sb);
			if (rc == -1) {
				error = error_from_errno(errno);
				goto out;
			}
			// Sendfile (on linux) doesn't work with device files
			if (!S_ISREG(sb.st_mode))
				datamalloced = true;
		}
#endif
	}

	if (datamalloced) {
		data = malloc(tosend);
		ASSERT(data);
		error = _pread_all(ps->fd, data, tosend, ps->fdoffset);
		if (hdfs_is_error(error))
			goto out;
	} else
		data = ps->buf;

	// calculate crcs, if requested
	if (ps->sendcrcs) {
		uint32_t crcinit;

		crclen = (tosend + CHUNK_SIZE - 1) / CHUNK_SIZE;
		crcdata = malloc(4*crclen);
		ASSERT(crcdata);

		crcinit = crc32(0L, Z_NULL, 0);
		for (unsigned i = 0; i < crclen; i++) {
			uint32_t chunklen = _min(CHUNK_SIZE, tosend - i * CHUNK_SIZE);
			uint32_t crc = crc32(crcinit, data + i * CHUNK_SIZE, chunklen);
			crcdata[i] = htonl(crc);
		}
	}

	// construct header:
	_bappend_s32(&phdr, tosend + 4*crclen + 4);
	if (ps->proto >= HDFS_DATANODE_AP_2_0) {
		PacketHeaderProto pkt = PACKET_HEADER_PROTO__INIT;
		size_t sz;

		pkt.offsetinblock = ps->offset;
		pkt.seqno = ps->seqno;
		pkt.lastpacketinblock = last;
		pkt.datalen = tosend;

		sz = packet_header_proto__get_packed_size(&pkt);
		_bappend_s16(&phdr, sz);
		_hbuf_reserve(&phdr, sz);
		packet_header_proto__pack(&pkt, (void *)&phdr.buf[phdr.used]);
		phdr.used += sz;
	} else {
		_bappend_s64(&phdr, ps->offset/*from beginning of block*/);
		_bappend_s64(&phdr, ps->seqno);
		_bappend_s8(&phdr, last);
		_bappend_s32(&phdr, tosend);
	}

	ios[0].iov_base = phdr.buf;
	ios[0].iov_len = phdr.used;

	if (data) {
		ios[1].iov_base = crcdata;
		ios[1].iov_len = 4*crclen;
		ios[2].iov_base = data;
		ios[2].iov_len = tosend;

		error = _writev_all(ps->sock, ios, 3);
		if (hdfs_is_error(error))
			goto out;
	} else {
#if defined(__linux__)
		_setsockopt(ps->sock, IPPROTO_TCP, TCP_CORK, 1);

		error = _writev_all(ps->sock, ios, 1);
		if (hdfs_is_error(error))
			goto out;
		error = _sendfile_all(ps->sock, ps->fd, ps->fdoffset, tosend);
		if (hdfs_is_error(error))
			goto out;

		_setsockopt(ps->sock, IPPROTO_TCP, TCP_CORK, 0);
#elif defined(__FreeBSD__)
		error = _sendfile_all_bsd(ps->sock, ps->fd, ps->fdoffset, tosend,
		    ios, 1);
		if (hdfs_is_error(error))
			goto out;
#else
		// !data => freebsd or linux. this branch should never be taken
		// on other platforms.
		ASSERT(false);
#endif
	}

	ps->unacked_packets++;
	ps->remains -= tosend;
	ps->fdoffset += tosend;
	ps->seqno++;
	ps->offset += tosend;
	if (ps->buf)
		ps->buf = (char*)ps->buf + tosend;

out:
	if (datamalloced)
		free(data);
	if (phdr.buf)
		free(phdr.buf);
	if (crcdata)
		free(crcdata);
	return error;
}

static void
_set_opres_msg(const char *newmsg)
{
	if (hdfs_datanode_opresult_message != NULL)
		free(__DECONST(char *, hdfs_datanode_opresult_message));
	if (newmsg == NULL)
		hdfs_datanode_opresult_message = NULL;
	else {
		hdfs_datanode_opresult_message = strdup(newmsg);
		ASSERT(hdfs_datanode_opresult_message != NULL);
	}
}

static struct hdfs_error
_verify_crcdata(void *crcdata, int32_t chunksize, int32_t crcdlen, int32_t dlen)
{
	uint32_t crcinit;
	void *data = (char*)crcdata + crcdlen;

	crcinit = crc32(0L, Z_NULL, 0);

	for (int i = 0; i < (dlen + chunksize - 1) / chunksize; i++) {
		int32_t chunklen = _min(chunksize, dlen - i*chunksize);
		uint32_t crc = crc32(crcinit,
		    (uint8_t*)data + i*chunksize, chunklen),
			 pcrc;

		pcrc = (((uint32_t) (*((uint8_t *)crcdata + i*4))) << 24) +
		    (((uint32_t) (*((uint8_t *)crcdata + i*4 + 1))) << 16) +
		    (((uint32_t) (*((uint8_t *)crcdata + i*4 + 2))) << 8) +
		    (uint32_t) (*((uint8_t *)crcdata + i*4 + 3));

		if (crc != pcrc)
			return error_from_hdfs(HDFS_ERR_DATANODE_BAD_CHECKSUM);
	}

	return HDFS_SUCCESS;
}

static struct hdfs_error
_wait_ack(struct _packet_state *ps)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_heap_buf obuf = { 0 };

	int64_t seqno;
	int16_t nacks, ack = STATUS__ERROR;

	int acksz = 0;

	ASSERT(ps->proto == HDFS_DATANODE_AP_1_0 ||
	    ps->proto == HDFS_DATANODE_CDH3);

	if (ps->proto == HDFS_DATANODE_AP_1_0)
		acksz = 8 + 2 + 2;
	else if (ps->proto == HDFS_DATANODE_CDH3)
		acksz = 8 + 2;

	while (ps->recvbuf->used < acksz) {
		error = _read_to_hbuf(ps->sock, ps->recvbuf);
		if (hdfs_is_error(error))
			goto out;
	}

	obuf.buf = ps->recvbuf->buf;
	obuf.used = 0;
	obuf.size = ps->recvbuf->used;

	seqno = _bslurp_s64(&obuf);
	ASSERT(obuf.used >= 0);

	if (seqno != ps->first_unacked) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_SEQNO);
		fprintf(stderr, "libhadoofus: Got unexpected ACK (%" PRIi64 ","
		    " expected %" PRIi64 "); aborting write.\n", seqno,
		    ps->first_unacked);
		goto out;
	}

	ps->first_unacked++;

	ASSERT(ps->proto == HDFS_DATANODE_AP_1_0 ||
	    ps->proto == HDFS_DATANODE_CDH3);

	if (ps->proto == HDFS_DATANODE_AP_1_0) {
		nacks = _bslurp_s16(&obuf);
		ASSERT(obuf.used >= 0);

		// We only connect to one datanode, we should only get one ack:
		if (nacks != 1) {
			error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_ACK_COUNT);
			goto out;
		}

		ack = _bslurp_s16(&obuf);
		ASSERT(obuf.used >= 0);
	} else if (ps->proto == HDFS_DATANODE_CDH3) {
		ack = _bslurp_s16(&obuf);
		ASSERT(obuf.used >= 0);
	}

	if (ack == STATUS__SUCCESS)
		ps->unacked_packets--;
	else
		error = error_from_datanode(ack);

	// Skip the recv buffer past the ack
	ps->recvbuf->used -= acksz;
	if (ps->recvbuf->used)
		memmove(ps->recvbuf->buf, ps->recvbuf->buf + acksz, ps->recvbuf->used);

out:
	return error;
}

static struct hdfs_error
_wait_ack2(struct _packet_state *ps)
{
	struct hdfs_heap_buf obuf = { 0 },
			     *h;
	PipelineAckProto *ack;
	struct hdfs_error error;
	int64_t sz;

	ASSERT(ps->proto >= HDFS_DATANODE_AP_2_0);

	h = ps->recvbuf;
	ack = NULL;
	do {
		error = _read_to_hbuf(ps->sock, h);
		if (hdfs_is_error(error))
			goto out;

		obuf.buf = h->buf;
		obuf.used = 0;
		obuf.size = h->used;

		sz = _bslurp_vlint(&obuf);
		if (obuf.used == _H_PARSE_ERROR) {
			error = error_from_hdfs(HDFS_ERR_INVALID_VLINT);
			goto out;
		}
	} while (obuf.used < 0);

	ASSERT(sz > 0 && sz < INT_MAX - obuf.used);
	while (h->used < obuf.used + (int)sz) {
		error = _read_to_hbuf(ps->sock, h);
		if (hdfs_is_error(error))
			goto out;
	}
	obuf.buf = h->buf;
	obuf.size = h->used;

	ack = pipeline_ack_proto__unpack(NULL, sz, (void *)&h->buf[obuf.used]);
	obuf.used += sz;
	if (ack == NULL) {
		error = error_from_hdfs(HDFS_ERR_INVALID_PIPELINEACKPROTO);
		goto out;
	}

	if (ack->seqno != ps->first_unacked) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_SEQNO);
		fprintf(stderr, "libhadoofus: Got unexpected ACK (%" PRIi64 ","
		    " expected %" PRIi64 "); aborting write.\n", ack->seqno,
		    ps->first_unacked);
		goto out;
	}
	ps->first_unacked++;

	// We only connect to one datanode, we should only get one ack:
	if (ack->n_status != 1) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_ACK_COUNT);
		goto out;
	}

	if (ack->status[0] == STATUS__SUCCESS)
		ps->unacked_packets--;
	else
		error = error_from_datanode(ack->status[0]);

	// Skip the recv buffer past the ack
	h->used -= obuf.used;
	if (h->used)
		memmove(h->buf, &h->buf[obuf.used], h->used);

out:
	if (ack)
		pipeline_ack_proto__free_unpacked(ack, NULL);
	return error;
}
