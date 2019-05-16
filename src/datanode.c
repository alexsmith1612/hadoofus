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

// TODO look into datatransfer encryption

_Thread_local int hdfs_datanode_unknown_status EXPORT_SYM;
_Thread_local const char *hdfs_datanode_opresult_message EXPORT_SYM;

static char DN_V1_CHECKSUM_OK[2] = {
	(char)(HADOOP__HDFS__STATUS__CHECKSUM_OK >> 8),
	(char)(HADOOP__HDFS__STATUS__CHECKSUM_OK & 0xff),
};
static char DN_V1_ERROR_CHECKSUM[2] = {
	(char)(HADOOP__HDFS__STATUS__ERROR_CHECKSUM >> 8),
	(char)(HADOOP__HDFS__STATUS__ERROR_CHECKSUM & 0xff),
};

// XXX make these configurable?
static const int MAX_UNACKED_PACKETS = 80 /*same as apache*/,
	     CHUNK_SIZE = 512,
	     PACKET_SIZE = 64 * 1024;
static const long HEART_BEAT_SEQNO = -1L;

static struct hdfs_error	error_from_datanode(int);

static struct hdfs_error	_datanode_read(struct hdfs_datanode *, off_t bloff, off_t len,
				int fd, off_t fdoff, void *buf, bool verify);
static struct hdfs_error	_datanode_write(struct hdfs_datanode *d, const void *buf, int fd, off_t len,
				off_t offset, bool sendcrcs, ssize_t *nwritten, ssize_t *nacked, int *err_idx);
static struct hdfs_error	_read_read_status(struct hdfs_datanode *, struct hdfs_heap_buf *,
				struct hdfs_read_info *);
static struct hdfs_error	_read_read_status2(struct hdfs_datanode *, struct hdfs_heap_buf *,
				struct hdfs_read_info *);
static struct hdfs_error	_read_write_status(struct hdfs_datanode *, struct hdfs_heap_buf *);
static struct hdfs_error	_read_write_status2(struct hdfs_datanode *, struct hdfs_heap_buf *);
static struct hdfs_error	_recv_packet(struct hdfs_packet_state *, struct hdfs_read_info *);
static struct hdfs_error	_process_recv_packet(struct hdfs_packet_state *ps, struct hdfs_read_info *ri,
				ssize_t hdr_len, ssize_t plen, ssize_t dlen, int64_t offset);
static struct hdfs_error	_recv_packet_copy_data(struct hdfs_packet_state *ps, struct hdfs_read_info *ri);
static struct hdfs_error	_send_packet(struct hdfs_packet_state *ps);
static void			_set_opres_msg(const char *);
static struct hdfs_error	_verify_crcdata(void *crcdata, int32_t chunksize,
				int32_t crcdlen, int32_t dlen);
static struct hdfs_error	_check_one_ack(struct hdfs_packet_state *ps, ssize_t *nacked, int *err_idx);
static struct hdfs_error	_check_one_ack2(struct hdfs_packet_state *ps, ssize_t *nacked, int *err_idx);
static struct hdfs_error	_check_acks(struct hdfs_packet_state *ps, ssize_t *nacked, int *err_idx);

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

	if (d->dn_sock != -1)
		close(d->dn_sock);
	hdfs_object_free(d->dn_token);
	free(d->dn_client);
	free(d->dn_pool_id);

	memset(d, 0, sizeof *d);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_connect_init(struct hdfs_datanode *d, const char *host, const char *port,
	bool numerichost)
{
	struct hdfs_error error;

	ASSERT(d);
	ASSERT(host);
	ASSERT(port);
	ASSERT(d->dn_sock == -1);
	ASSERT(d->dn_state == HDFS_DN_ST_INITED);

	error = _connect_init(&d->dn_sock, host, port, &d->dn_cctx, numerichost);
	if (!hdfs_is_error(error)) {
		d->dn_state = HDFS_DN_ST_CONNECTED;
	} else if (hdfs_is_again(error)) {
		d->dn_state = HDFS_DN_ST_CONNPENDING;
	} else {
		d->dn_state = HDFS_DN_ST_ERROR;
	}

	return error;
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_connect_finalize(struct hdfs_datanode *d)
{
	struct hdfs_error error = HDFS_SUCCESS;

	ASSERT(d);

	if (d->dn_state == HDFS_DN_ST_CONNECTED)
		goto out;
	ASSERT(d->dn_state == HDFS_DN_ST_CONNPENDING);
	error = _connect_finalize(&d->dn_sock, &d->dn_cctx);
	if (!hdfs_is_error(error)) {
		d->dn_state = HDFS_DN_ST_CONNECTED;
	} else if (!hdfs_is_again(error)) {
		d->dn_state = HDFS_DN_ST_ERROR;
	}

out:
	return error;
}

// TODO remove completely or write a blocking wrapper around the nonblocking
// datanode connect interface
EXPORT_SYM struct hdfs_error
hdfs_datanode_connect(struct hdfs_datanode *d, const char *host, const char *port)
{
	struct hdfs_error error;

	ASSERT(d);

	ASSERT(d->dn_sock == -1);
	error = _connect(&d->dn_sock, host, port);

	return error;
}

// Datanode write operations

EXPORT_SYM struct hdfs_error
hdfs_datanode_write_nb(struct hdfs_datanode *d, const void *buf, size_t len,
	ssize_t *nwritten, ssize_t *nacked, int *error_idx)
{
	ASSERT(buf);

	return _datanode_write(d, buf, -1/*fd*/, len, -1/*fdoff*/, nwritten, nacked, error_idx);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_write(struct hdfs_datanode *d, const void *buf, size_t len)
{
	ASSERT(buf);
	ASSERT(len > 0);

	// TODO write blocking wrapper around non-blocking interface
	return _datanode_write(d, buf, -1/*fd*/, len, -1/*fdoff*/);
}

// TODO hdfs_datanode_write_file_nb()

EXPORT_SYM struct hdfs_error
hdfs_datanode_write_file(struct hdfs_datanode *d, int fd, off_t len, off_t offset)
{
	ASSERT(offset >= 0);
	ASSERT(fd >= 0);
	ASSERT(len > 0);

	// TODO write blocking wrapper around non-blocking interface
	return _datanode_write(d, NULL, fd, len, offset);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_check_acks(struct hdfs_datanode *d, ssize_t *nacked, int *error_idx)
{
	struct hdfs_error error;

	ASSERT(d);
	ASSERT(nacked);
	ASSERT(error_idx);
	ASSERT(d->dn_op == HDFS_DN_OP_WRITE_BLOCK);
	ASSERT(d->dn_state >= HDFS_DN_ST_CONNECTED);

	error = _check_acks(&d->dn_pstate, nacked, error_idx);
	if (hdfs_is_error(error) && !hdfs_is_again(error)) {
		d->dn_state = HDFS_DN_ST_ERROR;
	}

	return error;
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_finish_block(struct hdfs_datanode *d, ssize_t *nacked, int *error_idx)
{
	struct hdfs_error error;
	ssize_t t_nwritten;

	ASSERT(d->dn_op == HDFS_DN_OP_WRITE_BLOCK);
	ASSERT(d->dn_state >= HDFS_DN_ST_CONNECTED);

	if (!d->dn_last) {
		// Must have already written all data passed by user. XXX consider?
		ASSERT(d->dn_pstate.remains_tot == 0);
		d->dn_last = true;
	}

	error = _datanode_write(d, NULL/*buf*/, -1/*fd*/, 0/*len*/, -1/*fdoff*/,
	    0/*sendcrcs*/, &t_nwritten, nacked, error_idx);

	// Only return HDFS_SUCCESS once all of the packets have been acknowledged
	if (!hdfs_is_error(error)) {
		error = d->dn_pstate.unacked.ua_num == 0 ? HDFS_SUCCESS : HDFS_AGAIN;
	}

	if (!hdfs_is_error(error) && d->dn_proto < HDFS_DATANODE_AP_2_0) {
		// Write final zero-len packet; error here doesn't always matter. I
		// think some HDFS versions drop the connection at this point, so we
		// want to be lenient. Since the tcp send buffer should be empty at
		// this point, it's unlikely for there to be a short write, so don't
		// worry about them.
		const int32_t zero = 0;
		_write(d->dn_sock, __DECONST(void *, &zero), sizeof(zero), &t_nwritten);
	}

	return error;
}

// Datanode read operations

EXPORT_SYM struct hdfs_error
hdfs_datanode_read(struct hdfs_datanode *d, size_t off, size_t len, void *buf)
{
	ASSERT(buf);

	// XXX TODO
	return _datanode_read(d, off, len, -1/*fd*/, -1/*fdoff*/, buf);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_read_file(struct hdfs_datanode *d, off_t bloff, off_t len,
	int fd, off_t fdoff)
{
	ASSERT(bloff >= 0);
	ASSERT(fdoff >= 0);
	ASSERT(fd >= 0);

	// XXX TODO
	return _datanode_read(d, bloff, len, fd, fdoff, NULL/*buf*/);
}

static struct hdfs_error
error_from_datanode(int dnstatus)
{
	enum hdfs_error_numeric ecode;

	ASSERT(dnstatus != HADOOP__HDFS__STATUS__SUCCESS);

	switch (dnstatus) {
	case HADOOP__HDFS__STATUS__ERROR:
		ecode = HDFS_ERR_DN_ERROR;
		break;
	case HADOOP__HDFS__STATUS__ERROR_CHECKSUM:
		ecode = HDFS_ERR_DN_ERROR_CHECKSUM;
		break;
	case HADOOP__HDFS__STATUS__ERROR_INVALID:
		ecode = HDFS_ERR_DN_ERROR_INVALID;
		break;
	case HADOOP__HDFS__STATUS__ERROR_EXISTS:
		ecode = HDFS_ERR_DN_ERROR_EXISTS;
		break;
	case HADOOP__HDFS__STATUS__ERROR_ACCESS_TOKEN:
		ecode = HDFS_ERR_DN_ERROR_ACCESS_TOKEN;
		break;
	case HADOOP__HDFS__STATUS__CHECKSUM_OK:
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
	off_t offset, off_t len)
{
	ASSERT(d->dn_op == HDFS_DN_OP_READ_BLOCK);

	_bappend_s16(h, d->dn_proto);
	_bappend_s8(h, HDFS_DN_OP_READ_BLOCK);

	if (d->dn_proto >= HDFS_DATANODE_AP_2_0) {
		Hadoop__Common__TokenProto token =
		    HADOOP__COMMON__TOKEN_PROTO__INIT;
		Hadoop__Hdfs__ExtendedBlockProto ebp = HADOOP__HDFS__EXTENDED_BLOCK_PROTO__INIT;
		Hadoop__Hdfs__BaseHeaderProto bhdr = HADOOP__HDFS__BASE_HEADER_PROTO__INIT;
		Hadoop__Hdfs__ClientOperationHeaderProto hdr =
		    HADOOP__HDFS__CLIENT_OPERATION_HEADER_PROTO__INIT;
		Hadoop__Hdfs__OpReadBlockProto opread = HADOOP__HDFS__OP_READ_BLOCK_PROTO__INIT;

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
		if (!d->dn_crcs) {
			opread.has_sendchecksums = true;
			opread.sendchecksums = false;
		}

		sz = hadoop__hdfs__op_read_block_proto__get_packed_size(&opread);
		_bappend_vlint(h, sz);
		_hbuf_reserve(h, sz);
		hadoop__hdfs__op_read_block_proto__pack(&opread, (void *)_hbuf_writeptr(h));
		_hbuf_append(h, sz);
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
_compose_client_read_status(struct hdfs_heap_buf *h, struct hdfs_datanode *d)
{
	if (d->dn_proto >= HDFS_DATANODE_AP_2_0) {
		Hadoop__Hdfs__ClientReadStatusProto status = HADOOP__HDFS__CLIENT_READ_STATUS_PROTO__INIT;
		size_t sz;

		if (d->dn_rinfo.bad_crcs)
			status.status = HADOOP__HDFS__STATUS__ERROR_CHECKSUM;
		else if (d->dn_rinfo.has_crcs)
			status.status = HADOOP__HDFS__STATUS__CHECKSUM_OK;
		else
			status.status = HADOOP__HDFS__STATUS__SUCCESS;

		sz = hadoop__hdfs__client_read_status_proto__get_packed_size(&status);

		_bappend_vlint(h, sz);
		_hbuf_reserve(h, sz);
		hadoop__hdfs__client_read_status_proto__pack(&status, (void *)_hbuf_writeptr(h));
		_hbuf_append(h, sz);
	} else {
		_hbuf_reserve(h, 2);
		if (d->dn_rinfo.bad_crcs)
			memcpy(_hbuf_writeptr(h), DN_V1_CHECKSUM_OK, 2);
		else
			memcpy(_hbuf_writeptr(h), DN_V1_ERROR_CHECKSUM, 2);
		_hbuf_append(h, 2);
	}
}

static void
_compose_write_header(struct hdfs_heap_buf *h, struct hdfs_datanode *d)
{
	ASSERT(d->dn_op == HDFS_DN_OP_WRITE_BLOCK);

	_bappend_s16(h, d->dn_proto);
	_bappend_s8(h, HDFS_DN_OP_WRITE_BLOCK);

	if (d->dn_proto >= HDFS_DATANODE_AP_2_0) {
		Hadoop__Common__TokenProto token =
		    HADOOP__COMMON__TOKEN_PROTO__INIT;
		Hadoop__Hdfs__ExtendedBlockProto ebp = HADOOP__HDFS__EXTENDED_BLOCK_PROTO__INIT;
		Hadoop__Hdfs__BaseHeaderProto bhdr = HADOOP__HDFS__BASE_HEADER_PROTO__INIT;
		Hadoop__Hdfs__ClientOperationHeaderProto hdr =
		    HADOOP__HDFS__CLIENT_OPERATION_HEADER_PROTO__INIT;
		Hadoop__Hdfs__ChecksumProto csum = HADOOP__HDFS__CHECKSUM_PROTO__INIT;
		Hadoop__Hdfs__OpWriteBlockProto op = HADOOP__HDFS__OP_WRITE_BLOCK_PROTO__INIT;

		// TODO add DatanodeInfoProto targets for proper pipeline creation

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

		// XXX TODO check how bytesperchecksum and type are supposed to
		// work for appends (i.e. consider the case where the file was
		// created with a different chunk size and/or type)
		csum.bytesperchecksum = CHUNK_SIZE;
		csum.type = HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_NULL;
		if (d->dn_crcs)
			csum.type = HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_CRC32;

		op.header = &hdr;

		// XXX TODO add support for recovery stages. Need to look into this more,
		// but there likely needs to be a way for the user to specify the offset
		// into the block at which they will begin writing (since they would start
		// by writing the first unacked bytes) in addition to simply specifying
		// that this will be a recovery pipeline. This could be done by having the
		// user directly mess with the located block object that gets passed to
		// _new()/_init(), but there should probably be a more clear way. Perhaps
		// hdfs_datanode_set_recovery(off_t off) to be called between _init() and
		// _connect() calls

		// XXX SETUP_APPEND iff located_block size > 0? Double check what the
		// correct behavior is with apache
		op.stage = HADOOP__HDFS__OP_WRITE_BLOCK_PROTO__BLOCK_CONSTRUCTION_STAGE__PIPELINE_SETUP_CREATE;

		/* Not sure about any of this: */
		op.pipelinesize = 1; // XXX TODO update when adding proper pipeline creation
		op.minbytesrcvd = d->dn_size;
		op.maxbytesrcvd = d->dn_size;
		op.latestgenerationstamp = d->dn_gen;
		op.requestedchecksum = &csum;

		sz = hadoop__hdfs__op_write_block_proto__get_packed_size(&op);
		_bappend_vlint(h, sz);
		_hbuf_reserve(h, sz);
		hadoop__hdfs__op_write_block_proto__pack(&op, (void *)_hbuf_writeptr(h));
		_hbuf_append(h, sz);
	} else {
		// TODO pipelining/datanode targets?
		_bappend_s64(h, d->dn_blkid);
		_bappend_s64(h, d->dn_gen);
		_bappend_s32(h, 1);
		_bappend_s8(h, 0);
		_bappend_text(h, d->dn_client);
		_bappend_s8(h, 0);
		_bappend_s32(h, 0);
		hdfs_object_serialize(h, d->dn_token);
		_bappend_s8(h, !!d->dn_crcs);
		_bappend_s32(h, CHUNK_SIZE/*checksum chunk size*/);
	}
}

static struct hdfs_error
_datanode_read_init(struct hdfs_datanode *d, off_t bloff, off_t len)
{
	struct hdfs_error error = HDFS_SUCCESS;

	ASSERT(d);
	ASSERT(len > 0);
	ASSERT(bloff >= 0);
	ASSERT(!d->dn_rinfo.inited);
	d->dn_rinfo.inited = true;

	if (d->dn_op == HDFS_DN_OP_NONE) {
		ASSERT(d->dn_state >= HDFS_DN_ST_INITED
		    && d->dn_state <= HDFS_DN_ST_CONNECTED);
		d->dn_op = HDFS_DN_OP_READ_BLOCK;
	}
	ASSERT(d->dn_op == HDFS_DN_OP_READ_BLOCK);

	d->dn_pstate.remains_tot = len;
	d->dn_rinfo.client_offset = bloff;

	return error;
}

// XXX may need to change return semantics here.
// Consider the case where the last packet has been read (and thus no more data is
// expected to become available for read on the socket), but the user gives a
// buffer that is shorter than the remaining data. We would return HDFS_AGAIN,
// indicating that there is more data to be read, but the user should not call
// _get_eventfd()/poll() until we both return HDFS_AGAIN and set *nread to a value
// strictly less than len.
static struct hdfs_error
_datanode_read(struct hdfs_datanode *d, off_t len, int fd, off_t fdoff,
	void *buf, ssize_t *nread)
{
	struct hdfs_error error = HDFS_SUCCESS;
	ssize_t wlen;

	ASSERT(d);
	ASSERT(len > 0 || (d->dn_pstate.remains_tot == 0 && len == 0));
	ASSERT(nread);
	ASSERT(d->dn_state >= HDFS_DN_ST_CONNECTED);
	ASSERT(d->dn_op == HDFS_DN_OP_READ_BLOCK);
	ASSERT(d->dn_rinfo.inited);

	*nread = 0;

	switch (d->dn_state) {
	case HDFS_DN_ST_CONNECTED:
		ASSERT(_hbuf_readlen(&d->dn_hdrbuf) == 0);
		_compose_read_header(&d->dn_hdrbuf, d, d->dn_rinfo.client_offset, d->dn_pstate.remains_tot);
		d->dn_state = HDFS_DN_ST_SENDOP;
		// fall through
	case HDFS_DN_ST_SENDOP:
		error = _write(d->dn_sock, _hbuf_readptr(&d->dn_hdrbuf), _hbuf_readlen(&d->dn_hdrbuf), &wlen);
		if (wlen < 0) {
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		_hbuf_consume(&d->dn_hdrbuf, wlen);
		if (hdfs_is_again(error))
			goto out;
		// complete write
		ASSERT(_hbuf_readlen(&d->dn_hdrbuf) == 0);
		ASSERT(_hbuf_readlen(&d->dn_recvbuf) == 0);
		d->dn_state = HDFS_DN_ST_RECVOP;
		// fall through
	case HDFS_DN_ST_RECVOP:
		if (d->dn_proto >= HDFS_DATANODE_AP_2_0)
			error = _read_read_status2(d, &d->dn_recvbuf, &d->dn_rinfo);
		else
			error = _read_read_status(d, &d->dn_recvbuf, &d->dn_rinfo);
		if (hdfs_is_again(error)) {
			goto out; // no state or buffer change
		} else if (hdfs_is_error(error)) {
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		// Success!

		// XXX perhaps do not set _ST_ERROR on checksum error, and instead move
		// this check to immediately before falling through to _ST_PKT. That was
		// we give users the option to continue with the read even without
		// checksums by just continuing to call _datanode_read()
		if (!d->dn_rinfo.has_crcs && d->dn_crcs) {
			d->dn_state = HDFS_DN_ST_ERROR;
			error = error_from_hdfs(HDFS_ERR_DATANODE_NO_CRCS);
			goto out;
		}
		d->dn_pstate.sock = d->dn_sock;
		d->dn_pstate.sendcrcs = d->dn_crcs; // XXX this is never used in _recv_packet() -- remove here?
		d->dn_pstate.recvbuf = &d->dn_recvbuf;
		d->dn_pstate.proto = d->dn_proto;
		d->dn_state = HDFS_DN_ST_PKT;
		// fall through
	case HDFS_DN_ST_PKT:
		d->dn_rinfo.rlen = len;
		d->dn_pstate.buf = buf;
		d->dn_pstate.fd = fd;
		d->dn_pstate.fdoffset = fdoff;
		while (d->dn_pstate.remains_tot > 0) {
			error = _recv_packet(&d->dn_pstate, &d->dn_rinfo);
			if (hdfs_is_again(error) || d->dn_rinfo.bad_crcs)
				break; // need to send status to server on checksum error
			else if (hdfs_is_error(error))
				goto out;
		}
		*nread = len - d->dn_rinfo.rlen; // XXX set *nread in all error cases?
		if (hdfs_is_again(error))
			goto out;
		_compose_client_read_status(&d->dn_hdrbuf, d);
		d->dn_state = HDFS_DN_ST_FINISHED;
		// fall through
	case HDFS_DN_ST_FINISHED:
		// Send the server our read status
		ASSERT(_hbuf_readlen(&d->dn_hdrbuf) > 0); // XXX consider
		error = _write(d->dn_sock, _hbuf_readptr(&d->dn_hdrbuf), _hbuf_readlen(&d->dn_hdrbuf), &wlen);
		if (wlen < 0) {
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		// complete or partial write (possible 0 bytes)
		_hbuf_consume(&d->dn_hdrbuf, wlen);
		// XXX should we call shutdown(2) here on HDFS_SUCCESS?
		if (d->dn_rinfo.bad_crcs && !hdfs_is_error(error)) {
			// Once the checksum error is reported to the server, tell the user
			error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_CHECKSUM);
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		break;

	case HDFS_DN_ST_ZERO:
	case HDFS_DN_ST_INITED:
	case HDFS_DN_ST_CONNPENDING:
		// XXX consider allowing connection to be handled here
	case HDFS_DN_ST_ERROR:
	default:
		ASSERT(false);
	}

out:
	return error;
}

// XXX TODO Add support for iovecs instead of just one contiguous buffer. Would
// need to have a dynamically allocated iovec array in struct hdfs_datanode or
// in struct hdfs_packet state (that only gets realloc'd up).  The iovec array
// passed in by the user would get shallow copied into our array, starting at
// index 1 to allow an iovec for the packet header/crcs buffer. There would be a
// pointer into our iovec array that could be modified by _send_packet() to
// advance through the array as data get sent. Our copy of the iovec array would
// also get modified by _send_packet() when a write (whether complete or short,
// but the complete writes are actually of interest here) finishes in the middle
// of an iovec. We may want to do some checks against IOV_MAX or
// sysconf(_SC_IOV_MAX), but these do not seem to be portably defined.
// send_packet() will have to loop through the iovecs to count how many to pass
// to writev() via iovcnt (although a temporary adjustment will have to be made
// to iov_len in the last iovec passed in order to pass exactly the correct
// number remains_pkt of bytes; this iov_len will then get restored after the
// call to writev() is made.
static struct hdfs_error
_datanode_write(struct hdfs_datanode *d, const void *buf, int fd, off_t len,
	off_t offset, ssize_t *nwritten, ssize_t *nacked, int *err_idx)
{
	ssize_t wlen, t_nacked = 0;
	struct hdfs_error error = HDFS_SUCCESS, ret;

	ASSERT(d);
	ASSERT(nwritten);
	ASSERT(nacked);
	ASSERT(err_idx);

	*nwritten = 0;
	*nacked = 0;
	*err_idx = -1;

	if (d->dn_op == HDFS_DN_OP_NONE) {
		ASSERT(d->dn_state == HDFS_DN_ST_CONNECTED);
		d->dn_op = HDFS_DN_OP_WRITE_BLOCK;
	}
	ASSERT(d->dn_op == HDFS_DN_OP_WRITE_BLOCK);
	ASSERT(d->dn_state >= HDFS_DN_ST_CONNECTED);
	ASSERT(len >= d->dn_pstate.remains_tot); // XXX consider returning error instead
	if (d->dn_last)
		ASSERT(len == 0);
	else
		ASSERT(len > 0);
	// XXX perhaps allow remains_tot == 0 in all cases (but only call _send_packet()
	// if (remains_tot > 0 || d->dn_last)) in order to simplify user logic for
	// checking for ACKs if they have no data to send
	d->dn_pstate.remains_tot = len;

	switch (d->dn_state) {
	case HDFS_DN_ST_CONNECTED:
		ASSERT(_hbuf_readlen(&d->dn_hdrbuf) == 0);
		_compose_write_header(&d->dn_hdrbuf, d);
		d->dn_state = HDFS_DN_ST_SENDOP;
		// fall through
	case HDFS_DN_ST_SENDOP:
		error = _write(d->dn_sock, _hbuf_readptr(&d->dn_hdrbuf), _hbuf_readlen(&d->dn_hdrbuf), &wlen);
		if (wlen < 0) {
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		_hbuf_consume(&d->dn_hdrbuf, wlen);
		if (hdfs_is_again(error))
			goto out;
		// complete write
		ASSERT(_hbuf_readlen(&d->dn_hdrbuf) == 0);
		ASSERT(_hbuf_readlen(&d->dn_recvbuf) == 0);
		d->dn_state = HDFS_DN_ST_RECVOP;
		// fall through
	case HDFS_DN_ST_RECVOP:
		if (d->dn_proto >= HDFS_DATANODE_AP_2_0)
			error = _read_write_status2(d, &d->dn_recvbuf);
		else
			error = _read_write_status(d, &d->dn_recvbuf);
		if (hdfs_is_again(error)) {
			goto out; // no state or buffer change.
		} else if (hdfs_is_error(error)) {
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		// Success!
		d->dn_pstate.sock = d->dn_sock;
		d->dn_pstate.sendcrcs = d->dn_crcs;
		d->dn_pstate.hdrbuf = &d->dn_hdrbuf;
		d->dn_pstate.recvbuf = &d->dn_recvbuf;
		d->dn_pstate.proto = d->dn_proto;
		d->dn_pstate.offset = d->dn_size;
		d->dn_state = HDFS_DN_ST_PKT;
		// fall through
	case HDFS_DN_ST_PKT:
		d->dn_pstate.buf = __DECONST(void *, buf);
		d->dn_pstate.fd = fd;
		d->dn_pstate.fdoffset = offset;
		do {
			// Try to drain any acks if we have many outstanding packets
			if (d->dn_pstate.unacked.ua_num >= MAX_UNACKED_PACKETS) {
				ret = _check_acks(&d->dn_pstate, &t_nacked, err_idx);
				if (hdfs_is_error(ret) && !hdfs_is_again(ret)) {
					error = ret;
					d->dn_state = HDFS_DN_ST_ERROR;
					goto out;
				}
				*nacked += t_nacked;
			}
			error = _send_packet(&d->dn_pstate);
			if (hdfs_is_again(error)) {
				break;
			} else if (hdfs_is_error(error)) {
				d->dn_state = HDFS_DN_ST_ERROR;
				goto out;
			}
			// Successfully wrote entire packet
			ASSERT(_hbuf_readlen(&d->dn_hdrbuf) == 0);
			ASSERT(d->dn_pstate.remains_pkt == 0);
			if (d->dn_last) {
				d->dn_state = HDFS_DN_ST_FINISHED;
				break;
			}
		} while (d->dn_pstate.remains_tot > 0);
		// XXX do we care about telling the user about the number of
		// written bytes if there's any error?
		*nwritten = len - d->dn_pstate.remains_tot;
		// fall through
	case HDFS_DN_ST_FINISHED:
		// Check if there are any acks to drain (without clobbering return value)
		ret = _check_acks(&d->dn_pstate, &t_nacked, err_idx);
		if (hdfs_is_error(ret) && !hdfs_is_again(ret)) {
			error = ret;
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		*nacked += t_nacked;
		break;

	case HDFS_DN_ST_ZERO:
	case HDFS_DN_ST_INITED:
	case HDFS_DN_ST_CONNPENDING:
		// XXX consider allowing connection to be handled here
	case HDFS_DN_ST_ERROR:
	default:
		ASSERT(false);
	}

out:
	return error;
}

static struct hdfs_error
_read_read_status(struct hdfs_datanode *d, struct hdfs_heap_buf *h,
	struct hdfs_read_info *ri)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_heap_buf obuf = { 0 };
	int16_t status;
	int32_t chunk_size;
	int64_t server_offset;
	bool crcs;

	while (_hbuf_readlen(h) < 2) {
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	obuf.buf = _hbuf_readptr(h);
	obuf.size = _hbuf_readlen(h);

	status = _bslurp_s16(&obuf);
	ASSERT(obuf.used > 0); // XXX reconsider

	if (status != HADOOP__HDFS__STATUS__SUCCESS) {
		error = error_from_datanode(status);
		goto out;
	}

	while (_hbuf_readlen(h) < 15) {
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	// must grab readptr() again due to possible realloc() in _read_to_hbuf()
	obuf.buf = _hbuf_readptr(h);
	obuf.size = _hbuf_readlen(h);

	crcs = _bslurp_s8(&obuf);
	ASSERT(obuf.used > 0); // XXX reconsider
	chunk_size = _bslurp_s32(&obuf);
	ASSERT(obuf.used > 0); // XXX reconsider
	server_offset = _bslurp_s64(&obuf);
	ASSERT(obuf.used > 0); // XXX reconsider

	ri->server_offset = server_offset;
	ri->chunk_size = chunk_size;
	ri->has_crcs = crcs;

	// Skip recvbuf past request status
	_hbuf_consume(h, obuf.used);

out:
	return error;
}

static struct hdfs_error
_read_blockop_resp_status(struct hdfs_datanode *d, struct hdfs_heap_buf *h,
	Hadoop__Hdfs__BlockOpResponseProto **opres_out)
{
	struct hdfs_heap_buf obuf = { 0 };
	Hadoop__Hdfs__BlockOpResponseProto *opres = NULL;
	struct hdfs_error error = HDFS_SUCCESS;
	int64_t sz;

	// If we don't have any data queued up, try to read first
	if (_hbuf_readlen(h) == 0) {
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	do {
		// try to parse what we already have
		obuf.buf = _hbuf_readptr(h);
		obuf.used = 0;
		obuf.size = _hbuf_readlen(h);

		sz = _bslurp_vlint(&obuf);
		if (obuf.used == _H_PARSE_ERROR) {
			error = error_from_hdfs(HDFS_ERR_INVALID_VLINT);
			goto out;
		}
		if (obuf.used == _H_PARSE_EOF) {
			// if we need more data, then try to read more and parse again
			error = _read_to_hbuf(d->dn_sock, h);
			if (hdfs_is_error(error)) // includes HDFS_AGAIN
				goto out;
		}
	} while (obuf.used < 0);


	// XXX TODO return error instead of assertion here, since this could
	// actually happen if we read malformed or maliciously generated data
	ASSERT(sz < INT_MAX - obuf.used);
	while (_hbuf_readlen(h) < obuf.used + (int)sz) {
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}
	obuf.buf = _hbuf_readptr(h);
	obuf.size = _hbuf_readlen(h);

	opres = hadoop__hdfs__block_op_response_proto__unpack(NULL, sz,
	    (void *)&obuf.buf[obuf.used]); // points past the already parsed vlint
	obuf.used += sz;
	if (opres == NULL) {
		error = error_from_hdfs(HDFS_ERR_INVALID_BLOCKOPRESPONSEPROTO);
		goto out;
	}

	_set_opres_msg(opres->message);
	if (opres->status != HADOOP__HDFS__STATUS__SUCCESS)
		error = error_from_datanode(opres->status);

	// Shouldn't happen, I believe; call it a protocol error.
	if (opres->message != NULL)
		error = error_from_hdfs(HDFS_ERR_INVALID_DN_OPRESP_MSG);

	if (hdfs_is_error(error))
		hadoop__hdfs__block_op_response_proto__free_unpacked(opres, NULL);
	else
		*opres_out = opres;

	// Skip recvbuf past stuff we parsed here
	_hbuf_consume(h, obuf.used);

out:
	return error;
}

static struct hdfs_error
_read_read_status2(struct hdfs_datanode *d, struct hdfs_heap_buf *h,
	struct hdfs_read_info *ri)
{
	struct hdfs_error error;
	Hadoop__Hdfs__BlockOpResponseProto *opres = NULL;

	error = _read_blockop_resp_status(d, h, &opres);
	if (hdfs_is_error(error)) // includes HDFS_AGAIN
		goto out;

	// XXX TODO change these assertions to return an error code.  We don't
	// want to terminate the process from the library based on potentially
	// malformed or maliciously generated data received over a socket
	ASSERT(opres->readopchecksuminfo);
	ASSERT(opres->readopchecksuminfo->checksum);

	ri->server_offset = opres->readopchecksuminfo->chunkoffset;

	// XXX TODO check what kind of CRC
	ri->has_crcs = (opres->readopchecksuminfo->checksum->type !=
	    HADOOP__HDFS__CHECKSUM_TYPE_PROTO__CHECKSUM_NULL);
	ri->chunk_size = opres->readopchecksuminfo->checksum->bytesperchecksum;

out:
	if (opres)
		hadoop__hdfs__block_op_response_proto__free_unpacked(opres, NULL);
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

	// Some data may be parsed multiple times with non-blocking
	// sockets, which is not ideal, but it greatly simplifies the
	// code and should not be a big performance hit

	// If we don't have any data queued up, try to read first
	if (_hbuf_readlen(h) == 0) {
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	// TODO change to while _hbuf_readlen(h) < 2?
	while (true) {
		// try to parse what we already have
		obuf.buf = _hbuf_readptr(h);
		obuf.used = 0;
		obuf.size = _hbuf_readlen(h);

		status = _bslurp_s16(&obuf);
		if (obuf.used >= 0)
			break;

		if (obuf.used == _H_PARSE_ERROR) {
			error = error_from_hdfs(HDFS_ERR_V1_DATANODE_PROTOCOL);
			goto out;
		}

		// if we need more data, then try to read more and parse again
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	statussz = obuf.used;

	// If we don't have any data queued up, try to read first
	if ((size_t)_hbuf_readlen(h) == statussz) {
		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	while (true) {
		obuf.buf = _hbuf_readptr(h) + statussz;
		obuf.used = 0;
		obuf.size = _hbuf_readlen(h) - statussz;

		statusmsg = _bslurp_text(&obuf);
		if (obuf.used >= 0)
			break;

		if (obuf.used == _H_PARSE_ERROR) {
			error = error_from_hdfs(HDFS_ERR_V1_DATANODE_PROTOCOL);
			goto out;
		}

		error = _read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	statussz += obuf.used;

	if (status == HADOOP__HDFS__STATUS__SUCCESS && strlen(statusmsg) == 0)
		_set_opres_msg(NULL);
	else {
		// Shouldn't happen, I believe; call it a protocol error.
		if (status == HADOOP__HDFS__STATUS__SUCCESS)
			error = error_from_hdfs(HDFS_ERR_INVALID_DN_OPRESP_MSG);
		else
			error = error_from_datanode(status);
		_set_opres_msg(statusmsg);
	}

	// Skip the recv buffer past the read objects
	_hbuf_consume(h, statussz);

out:
	if (statusmsg)
		free(statusmsg);
	return error;
}

static struct hdfs_error
_read_write_status2(struct hdfs_datanode *d, struct hdfs_heap_buf *h)
{
	struct hdfs_error error = HDFS_SUCCESS;
	Hadoop__Hdfs__BlockOpResponseProto *opres = NULL;

	error = _read_blockop_resp_status(d, h, &opres);

	if (opres)
		hadoop__hdfs__block_op_response_proto__free_unpacked(opres, NULL);
	return error;
}

static struct hdfs_error
_recv_packet(struct hdfs_packet_state *ps, struct hdfs_read_info *ri)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_heap_buf *recvbuf = ps->recvbuf,
			     obuf = { 0 };
	int32_t plen, dlen;
	Hadoop__Hdfs__PacketHeaderProto *phdr = NULL;
	int64_t offset;
	uint16_t hlen;

	// We already parsed the packet but still have data to give to the user
	if (ps->remains_pkt > 0) {
		ASSERT(_hbuf_readlen(recvbuf) >= ps->remains_pkt);
		error = _recv_packet_copy_data(ps, ri);
		goto out;
	}

	if (ps->proto < HDFS_DATANODE_AP_2_0) {
		// slurp packet header
		while (_hbuf_readlen(recvbuf) < 25) {
			error = _read_to_hbuf(ps->sock, recvbuf);
			if (hdfs_is_error(error)) // includes HDFS_AGAIN
				goto out;
		}

		obuf.buf = _hbuf_readptr(recvbuf);
		obuf.size = _hbuf_readlen(recvbuf);

		plen = _bslurp_s32(&obuf);
		ASSERT(obuf.used > 0); // XXX reconsider
		offset = _bslurp_s64(&obuf);
		ASSERT(obuf.used > 0); // XXX reconsider
		/*seqno = */_bslurp_s64(&obuf);
		ASSERT(obuf.used > 0); // XXX reconsider
		ri->lastpacket = _bslurp_s8(&obuf);
		ASSERT(obuf.used > 0); // XXX reconsider
		dlen = _bslurp_s32(&obuf);
		ASSERT(obuf.used > 0); // XXX reconsider

		error = _process_recv_packet(ps, ri, 25, plen, dlen, offset);
		goto out;
	}

	while (_hbuf_readlen(recvbuf) < 6) {
		error = _read_to_hbuf(ps->sock, recvbuf);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	obuf.buf = _hbuf_readptr(recvbuf);
	obuf.size = _hbuf_readlen(recvbuf);

	plen = _bslurp_s32(&obuf);
	ASSERT(obuf.used > 0); // XXX reconsider
	hlen = (uint16_t)_bslurp_s16(&obuf);
	ASSERT(obuf.used > 0); // XXX reconsider

	while (_hbuf_readlen(recvbuf) < 6 + hlen) {
		error = _read_to_hbuf(ps->sock, recvbuf);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	phdr = hadoop__hdfs__packet_header_proto__unpack(NULL, hlen,
	    (void *)&_hbuf_readptr(recvbuf)[6]);
	if (phdr == NULL) {
		error = error_from_hdfs(HDFS_ERR_INVALID_PACKETHEADERPROTO);
		goto out;
	}

	offset = phdr->offsetinblock;
	ri->lastpacket = phdr->lastpacketinblock;
	dlen = phdr->datalen;

	error = _process_recv_packet(ps, ri, 6 + hlen, plen, dlen, offset);

out:
	if (phdr)
		hadoop__hdfs__packet_header_proto__free_unpacked(phdr, NULL);
	return error;
}

static struct hdfs_error
_process_recv_packet(struct hdfs_packet_state *ps, struct hdfs_read_info *ri,
	ssize_t hdr_len, ssize_t plen, ssize_t dlen, int64_t offset)
{
	struct hdfs_heap_buf *recvbuf = ps->recvbuf;
	const int ONEGB = 1024*1024*1024;
	struct hdfs_error error = HDFS_SUCCESS;
	int32_t c_begin;
	ssize_t crcdlen;

	crcdlen = plen - dlen - 4;
	if (plen < 0 || dlen < 0 || dlen > ONEGB || plen > ONEGB || crcdlen < 0)
		error = error_from_hdfs(HDFS_ERR_DATANODE_PACKET_SIZE);
	else if (ri->has_crcs && crcdlen != ((dlen + ri->chunk_size - 1) / ri->chunk_size) * 4)
		error = error_from_hdfs(HDFS_ERR_DATANODE_CRC_LEN);
	else if (!ri->has_crcs && crcdlen > 0)
		error = error_from_hdfs(HDFS_ERR_DATANODE_UNEXPECTED_CRC_LEN);
	if (hdfs_is_error(error))
		goto out;

	// v2 sends an empty packet (just header) at the end, although we stop paying
	// attention after we've gotten as many bytes as we expected (ps->remaining_tot)
	if (dlen == 0) {
		if (!ri->lastpacket)
			error = error_from_hdfs(HDFS_ERR_DATANODE_PACKET_SIZE); // XXX consider different error
		else if (ps->remains_tot > 0) // server says last packet but we expect more data
			error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_LASTPACKET);
		_hbuf_consume(recvbuf, hdr_len);
		goto out;
	}

	// XXX do we want to allow some data to be returned to the user before the
	// entire packet is received? This would be tricky if we're verifying crcs.
	// Would need to review all packet processing/copying logic (especially the case
	// where we enter _recv_packet() when we're already in the middle of copying
	// the current packet's data to the user)
	while (_hbuf_readlen(recvbuf) < hdr_len + crcdlen + dlen) {
		error = _read_to_hbuf(ps->sock, recvbuf);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	if (crcdlen > 0) {
		error = _verify_crcdata(_hbuf_readptr(recvbuf) + hdr_len, ri->chunk_size, crcdlen, dlen);
		if (hdfs_is_error(error)) {
			ri->bad_crcs = true;
			goto out;
		}
	}

	// figure out where in the packet to start copying from, and how much to copy
	if (offset < ri->client_offset)
		c_begin = ri->client_offset - offset;
	else
		c_begin = 0;
	if (c_begin >= dlen) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_UNEXPECTED_READ_OFFSET);
		goto out;
	}
	_hbuf_consume(recvbuf, hdr_len + crcdlen + c_begin);
	ps->remains_pkt = _min(dlen - c_begin, ps->remains_tot);

	error = _recv_packet_copy_data(ps, ri);

out:
	return error;
}

static struct hdfs_error
_recv_packet_copy_data(struct hdfs_packet_state *ps, struct hdfs_read_info *ri)
{
	struct hdfs_error error = HDFS_SUCCESS;
	ssize_t c_len;

	ASSERT(ps);
	ASSERT(ri);
	ASSERT(ps->remains_pkt > 0);
	ASSERT(ri->rlen > 0);

	c_len = _min(ps->remains_pkt, ri->rlen);

	// Copy the packet data out to the user's buf or to file:
	if (ps->buf) {
		memcpy(ps->buf, _hbuf_readptr(ps->recvbuf), c_len);
	} else {
		// Note that this can block on the user's fd
		error = _pwrite_all(ps->fd, _hbuf_readptr(ps->recvbuf), c_len, ps->fdoffset);
		if (hdfs_is_error(error))
			goto out;
	}

	_hbuf_consume(ps->recvbuf, c_len);
	ps->remains_pkt -= c_len;
	ps->remains_tot -= c_len;
	ps->fdoffset += c_len;
	ri->rlen -= c_len;
	if (ps->buf)
		ps->buf = (char *)ps->buf + c_len;

	// Server indicated last packet, but user expects more data
	if (ps->remains_pkt == 0 && ri->lastpacket && ps->remains_tot > 0)
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_LASTPACKET);
	// There's more data to give the user, but no more space in the user's buffer
	else if (ri->rlen == 0 && ps->remains_tot > 0)
		error = HDFS_AGAIN;

out:
	return error;
}

static struct hdfs_error
_send_packet(struct hdfs_packet_state *ps)
{
	struct hdfs_error error = HDFS_SUCCESS;
	size_t crclen = 0;
	ssize_t wlen;
	unsigned char *data = NULL;
	bool datamalloced = false;
	struct iovec ios[2];
	int wlen_hdr = 0, wlen_data = 0;
	bool is_new;

	// if we have no data left to send from the last/current packet we're creating a new one
	is_new = (_hbuf_readlen(ps->hdrbuf) == 0 && ps->remains_pkt == 0);

	if (is_new) {
		// Delay sending data while N packets remain unacknowledged.
		// Apache Hadoop default is N=80, for a 5MB window.
		if (ps->unacked.ua_num >= MAX_UNACKED_PACKETS) {
			// let the caller handle draining acks
			error = HDFS_AGAIN;
			goto out;
		}

		ps->remains_pkt = _min(ps->remains_tot, PACKET_SIZE);

		if (ps->offset % CHUNK_SIZE) {
			// N.B.: If you have a partial block, appending the unaligned
			// bits first makes the remaining writes aligned.
			// Apache datanodes throw an error if an unaligned write has
			// data length greater than the chunk length.

			// XXX TODO make a comment in the public headers indicating
			// that users may want to refrain from performing many small
			// writes and instead buffer data until a larger write can be
			// performed in order to lower the number of header bytes per
			// data byte (and also help prevent unnecessarily bumping up
			// against the MAX_UNACKED_PACKETS limit). Perhaps we should
			// add an argument to _datanode_write() indicating whether or
			// not we should construct and send a packet if it will end
			// up being unaligned (i.e. if the last packet we would
			// construct in a given invocation would be of length
			// N*CHUNK_SIZE + remainder, make the packet with only
			// N*CHUNK_SIZE data bytes)
			int64_t remaining_in_chunk =
				CHUNK_SIZE - (ps->offset % CHUNK_SIZE);

			ps->remains_pkt = _min(ps->remains_pkt, remaining_in_chunk);
		}
	} // is_new

	if (!ps->buf && ps->remains_pkt != 0) {
#if !defined(__linux__) && !defined(__FreeBSD__)
		datamalloced = true;
#else
		if (ps->sendcrcs && is_new) {
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
		data = malloc(ps->remains_pkt);
		ASSERT(data);
		// Note that is can block on the user's fd
		error = _pread_all(ps->fd, data, ps->remains_pkt, ps->fdoffset);
		if (hdfs_is_error(error))
			goto out;
	} else
		data = ps->buf;

	if (is_new) {
		// calculate crc length, if requested
		crclen = (ps->sendcrcs) ? (ps->remains_pkt + CHUNK_SIZE - 1) / CHUNK_SIZE : 0;

		// construct header:
		_bappend_s32(ps->hdrbuf, ps->remains_pkt + 4*crclen + 4);
		if (ps->proto >= HDFS_DATANODE_AP_2_0) {
			Hadoop__Hdfs__PacketHeaderProto pkt = HADOOP__HDFS__PACKET_HEADER_PROTO__INIT;
			size_t sz;

			pkt.offsetinblock = ps->offset;
			pkt.seqno = ps->seqno;
			pkt.lastpacketinblock = (ps->remains_pkt == 0);
			pkt.datalen = ps->remains_pkt;

			sz = hadoop__hdfs__packet_header_proto__get_packed_size(&pkt);
			_bappend_s16(ps->hdrbuf, sz);
			_hbuf_reserve(ps->hdrbuf, sz);
			hadoop__hdfs__packet_header_proto__pack(&pkt, (void *)_hbuf_writeptr(ps->hdrbuf));
			_hbuf_append(ps->hdrbuf, sz);
		} else {
			_bappend_s64(ps->hdrbuf, ps->offset/*from beginning of block*/);
			_bappend_s64(ps->hdrbuf, ps->seqno);
			_bappend_s8(ps->hdrbuf, (ps->remains_pkt == 0));
			_bappend_s32(ps->hdrbuf, ps->remains_pkt);
		}

		// calculate the crcs, if requested
		if (crclen > 0) {
			uint32_t crcinit;

			_hbuf_reserve(ps->hdrbuf, 4 * crclen);

			crcinit = crc32(0L, Z_NULL, 0);
			for (unsigned i = 0; i < crclen; i++) {
				uint32_t chunklen = _min(CHUNK_SIZE, ps->remains_pkt - i * CHUNK_SIZE);
				uint32_t crc = crc32(crcinit, data + i * CHUNK_SIZE, chunklen);
				_be32enc(_hbuf_writeptr(ps->hdrbuf), crc);
				_hbuf_append(ps->hdrbuf, 4);
			}
		}

		// stash the size of this packet in order to give the user the number of bytes acked later
		if (ps->unacked.ua_list_pos + ps->unacked.ua_num >= ps->unacked.ua_list_size) {
			ps->unacked.ua_list_size += 128;
			ps->unacked.ua_list = realloc(ps->unacked.ua_list, ps->unacked.ua_list_size * sizeof(*ps->unacked.ua_list));
			ASSERT(ps->unacked.ua_list);
		}
		ps->unacked.ua_list[ps->unacked.ua_list_pos + ps->unacked.ua_num] = ps->remains_pkt;
		ps->unacked.ua_num++;
		ps->seqno++;
		ps->offset += ps->remains_pkt;
	} // is_new

	ios[0].iov_base = _hbuf_readptr(ps->hdrbuf);
	ios[0].iov_len = _hbuf_readlen(ps->hdrbuf);

	if (ps->remains_pkt == 0) { // remains_pkt is only 0 here if it's the last (empty) packet
		ASSERT(_hbuf_readlen(ps->hdrbuf) > 0);
		error = _writev(ps->sock, ios, 1, &wlen);
		if (wlen < 0)
			goto out;
		wlen_hdr = wlen;
	} else if (data) {
		ios[1].iov_base = data;
		ios[1].iov_len = ps->remains_pkt;

		// only pass the buffers with nonzero length to writev()
		if (_hbuf_readlen(ps->hdrbuf) > 0) {
			error = _writev(ps->sock, ios, 2, &wlen);
		} else {
			error = _writev(ps->sock, ios + 1, 1, &wlen);
		}
		if (wlen < 0)
			goto out;
		wlen_hdr = _min(wlen, _hbuf_readlen(ps->hdrbuf)); // written from the header/checksums
		wlen_data = wlen - wlen_hdr; // written from the user data
	} else {
#if defined(__linux__)
		_setsockopt(ps->sock, IPPROTO_TCP, TCP_CORK, 1);

		do {
			if (_hbuf_readlen(ps->hdrbuf) > 0) {
				error = _writev(ps->sock, ios, 1, &wlen);
				if (wlen < 0)
					goto out; // XXX clear TCP_CORK?
				wlen_hdr = wlen;
				if (hdfs_is_again(error))
					break;
			}
			error = _sendfile(ps->sock, ps->fd, ps->fdoffset, ps->remains_pkt, &wlen);
			if (wlen < 0)
				goto out; // XXX clear TCP_CORK?
			wlen_data = wlen;
		} while (0); // not a loop

		_setsockopt(ps->sock, IPPROTO_TCP, TCP_CORK, 0);
#elif defined(__FreeBSD__)
		if (_hbuf_readlen(ps->hdrbuf) > 0) {
			error = _sendfile_bsd(ps->sock, ps->fd, ps->fdoffset, ps->remains_pkt,
			    ios, 1, &wlen);
		} else {
			error = _sendfile_bsd(ps->sock, ps->fd, ps->fdoffset, ps->remains_pkt,
			    NULL, 0, &wlen);
		}
		if (wlen < 0)
			goto out;
		wlen_hdr = _min(wlen, _hbuf_readlen(ps->hdrbuf)); // written from the header/checksums
		wlen_data = wlen - wlen_hdr; // written from the user data
#else
		// !data => freebsd or linux. this branch should never be taken
		// on other platforms.
		ASSERT(false);
#endif
	}

	_hbuf_consume(ps->hdrbuf, wlen_hdr);
	ps->remains_pkt -= wlen_data;
	ps->remains_tot -= wlen_data;
	ps->fdoffset += wlen_data;
	if (ps->buf)
		ps->buf = (char *)ps->buf + wlen_data;

out:
	if (datamalloced)
		free(data);
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
	void *data = (char *)crcdata + crcdlen;

	crcinit = crc32(0L, Z_NULL, 0);

	for (int i = 0; i < (dlen + chunksize - 1) / chunksize; i++) {
		int32_t chunklen = _min(chunksize, dlen - i*chunksize);
		uint32_t crc = crc32(crcinit,
		    (uint8_t *)data + i*chunksize, chunklen),
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
_check_one_ack(struct hdfs_packet_state *ps, ssize_t *nacked, int *err_idx)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_heap_buf obuf = { 0 };

	int64_t seqno;
	int16_t nacks, ack = HADOOP__HDFS__STATUS__ERROR;

	int acksz = 0;

	ASSERT(ps->proto == HDFS_DATANODE_AP_1_0 ||
	    ps->proto == HDFS_DATANODE_CDH3);

	*nacked = 0;
	*err_idx = -1;

	if (ps->proto == HDFS_DATANODE_AP_1_0)
		acksz = 8 + 2 + 2;
	else if (ps->proto == HDFS_DATANODE_CDH3)
		acksz = 8 + 2;

	while (_hbuf_readlen(ps->recvbuf) < acksz) {
		error = _read_to_hbuf(ps->sock, ps->recvbuf);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	obuf.buf = _hbuf_readptr(ps->recvbuf);
	obuf.used = 0;
	obuf.size = _hbuf_readlen(ps->recvbuf);

	seqno = _bslurp_s64(&obuf);
	ASSERT(obuf.used >= 0); // XXX reconsider

	// Unsure if these are ever actually sent, or existed in v1, but
	// Apache and libhdfs3 check for/skip heartbeat acks in v2.2+
	if (seqno == HEART_BEAT_SEQNO) {
		_hbuf_consume(ps->recvbuf, acksz);
		goto out;
	}

	if (seqno != ps->first_unacked) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_SEQNO);
#if 0 // TODO stash the bad seqno in struct hdfs_datanode for users to access if they desire
		fprintf(stderr, "libhadoofus: Got unexpected ACK (%" PRIi64 ","
		    " expected %" PRIi64 "); aborting write.\n", seqno,
		    ps->first_unacked);
#endif
		goto out;
	}

	ps->first_unacked++;

	if (ps->proto == HDFS_DATANODE_AP_1_0) {
		nacks = _bslurp_s16(&obuf);
		ASSERT(obuf.used >= 0); // XXX reconsider

		// We only connect to one datanode, we should only get one ack:
		if (nacks != 1) { // XXX TODO update when proper pipelining implemented
			error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_ACK_COUNT);
			goto out;
		}

		ack = _bslurp_s16(&obuf);
		ASSERT(obuf.used >= 0); // XXX reconsider
	} else if (ps->proto == HDFS_DATANODE_CDH3) {
		// XXX TODO update when proper pipelining implemented (unsure how
		// that works for CDH3 --- can it even give multiple acks?)
		ack = _bslurp_s16(&obuf);
		ASSERT(obuf.used >= 0); // XXX reconsider
	}

	if (ack == HADOOP__HDFS__STATUS__SUCCESS) {
		*nacked = ps->unacked.ua_list[ps->unacked.ua_list_pos];
		ps->unacked.ua_list_pos++;
		ps->unacked.ua_num--;
	} else {
		error = error_from_datanode(ack);
		*err_idx = 0; // XXX TODO change this when implementing full pipelining
	}

	// Skip the recv buffer past the ack
	_hbuf_consume(ps->recvbuf, acksz);

out:
	return error;
}

static struct hdfs_error
_check_one_ack2(struct hdfs_packet_state *ps, ssize_t *nacked, int *err_idx)
{
	struct hdfs_heap_buf obuf = { 0 },
			     *h = ps->recvbuf;
	Hadoop__Hdfs__PipelineAckProto *ack = NULL;
	struct hdfs_error error = HDFS_SUCCESS;
	int64_t sz;

	ASSERT(ps->proto >= HDFS_DATANODE_AP_2_0);

	*nacked = 0;
	*err_idx = -1;

	// If we don't have any data queued up, try to read first
	if (_hbuf_readlen(h) == 0) {
		error = _read_to_hbuf(ps->sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	do {
		obuf.buf = _hbuf_readptr(h);
		obuf.used = 0;
		obuf.size = _hbuf_readlen(h);

		sz = _bslurp_vlint(&obuf);
		if (obuf.used == _H_PARSE_ERROR) {
			error = error_from_hdfs(HDFS_ERR_INVALID_VLINT);
			goto out;
		}
		if (obuf.used == _H_PARSE_EOF) {
			error = _read_to_hbuf(ps->sock, h);
			if (hdfs_is_error(error)) // includes HDFS_AGAIN
				goto out;
		}
	} while (obuf.used < 0);

	// XXX TODO return error instead of assertion here, since this could
	// actually happen if we read malformed or maliciously generated data
	ASSERT(sz > 0 && sz < INT_MAX - obuf.used);
	while (_hbuf_readlen(h) < obuf.used + (int)sz) {
		error = _read_to_hbuf(ps->sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	obuf.buf = _hbuf_readptr(h);
	obuf.size = _hbuf_readlen(h);

	ack = hadoop__hdfs__pipeline_ack_proto__unpack(NULL, sz, (void *)&obuf.buf[obuf.used]);
	obuf.used += sz;
	if (ack == NULL) {
		error = error_from_hdfs(HDFS_ERR_INVALID_PIPELINEACKPROTO);
		goto out;
	}

	// Unsure if these are ever actually sent, but Apache and libhdfs3
	// check for/skip heartbeat acks
	if (ack->seqno == HEART_BEAT_SEQNO) {
		_hbuf_consume(ps->recvbuf, obuf.used);
		goto out;
	}

	if (ack->seqno != ps->first_unacked) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_SEQNO);
#if 0 // TODO stash the bad seqno in struct hdfs_datanode for users to access if they desire
		fprintf(stderr, "libhadoofus: Got unexpected ACK (%" PRIi64 ","
		    " expected %" PRIi64 "); aborting write.\n", ack->seqno,
		    ps->first_unacked);
#endif
		goto out;
	}

	// We only connect to one datanode, we should only get one ack:
	if (ack->n_reply != 1) { // XXX TODO update when proper pipelining implemented
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_ACK_COUNT);
		goto out;
	}

	for (unsigned i = 0; i < ack->n_reply; i++) {
		if (ack->reply[i] != HADOOP__HDFS__STATUS__SUCCESS) {
			error = error_from_datanode(ack->reply[i]);
			*err_idx = i;
			goto out;
		}
	}

	// Pop the length of this acked packet off of the list
	ps->first_unacked++;
	*nacked = ps->unacked.ua_list[ps->unacked.ua_list_pos];
	ps->unacked.ua_list_pos++;
	ps->unacked.ua_num--;

	// Skip the recv buffer past the ack
	_hbuf_consume(h, obuf.used);

out:
	if (ack)
		hadoop__hdfs__pipeline_ack_proto__free_unpacked(ack, NULL);
	return error;
}

struct hdfs_error
_check_acks(struct hdfs_packet_state *ps, ssize_t *nacked, int *error_idx)
{
	struct hdfs_error error = HDFS_SUCCESS;
	ssize_t t_nacked;
	struct hdfs_unacked_packets *ua = &ps->unacked;

	*nacked = 0;
	*error_idx = -1;

	while (ua->ua_num > 0) {
		if (ps->proto >= HDFS_DATANODE_AP_2_0)
			error = _check_one_ack2(ps, &t_nacked, error_idx);
		else
			error = _check_one_ack(ps, &t_nacked, error_idx);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			break;
		*nacked += t_nacked;
	}

	// Reset the unacked list if empty or skip processed acks if past a threshold
	if (ua->ua_num == 0)
		ua->ua_list_pos = 0;
	else if (ua->ua_list_pos > 32) { // XXX reconsider this threshold
		memmove(ua->ua_list, ua->ua_list + ua->ua_list_pos, ua->ua_num * sizeof(*ua->ua_list));
		ua->ua_list_pos = 0;
	}

	return error;
}
