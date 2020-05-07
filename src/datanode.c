#include <netinet/in.h>
#include <netinet/tcp.h>

#include <errno.h>
#include <inttypes.h>
#include <poll.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <zlib.h>

#include <hadoofus/highlevel.h>

#include "crc32c.h"
#include "heapbuf.h"
#include "net.h"
#include "objects-internal.h"
#include "util.h"

#include "datatransfer.pb-c.h"

// TODO look into datatransfer encryption

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

static struct hdfs_error	error_from_datanode(int err, int *unknown_status);

// TODO Consider: define structs for passing arguments to these functions, as
// several have an unwieldy number now. XXX also consider changing the public
// API in a similar way. This may be particularly helpful for functions with
// consecutive arguments of the same type.
static struct hdfs_error	_datanode_read_init(struct hdfs_datanode *d, bool verifycsum, off_t bloff,
				off_t len);
static struct hdfs_error	_datanode_read(struct hdfs_datanode *d, off_t len, int fd, off_t fdoff,
				const struct iovec *iov, int iovcnt, size_t iov_offt, ssize_t *nread);
static struct hdfs_error	_datanode_read_blocking(struct hdfs_datanode *d, bool verifycsum, off_t bloff,
				off_t len, int fd, off_t fdoff, const struct iovec *iov, int iovcnt);
static struct hdfs_error	_datanode_write_init(struct hdfs_datanode *d, enum hdfs_checksum_type csum);
static struct hdfs_error	_setup_write_pipeline(struct hdfs_datanode *d, int *err_idx);
static struct hdfs_error	_setup_write_pipeline_blocking(struct hdfs_datanode *d, enum hdfs_checksum_type csum,
				int *err_idx);
static struct hdfs_error	_datanode_write(struct hdfs_datanode *d, const struct iovec *iov, int iovcnt, size_t iov_offt,
				int fd, off_t len, off_t fdoff, ssize_t *nwritten, ssize_t *nacked, int *err_idx, bool heartbeat);
static struct hdfs_error	_datanode_write_blocking(struct hdfs_datanode *d, enum hdfs_checksum_type csum,
				const struct iovec *iov, int iovcnt, int fd, off_t len, off_t fdoff, ssize_t *nwritten,
				ssize_t *nacked, int *err_idx);
static struct hdfs_error	_datanode_transfer_init(struct hdfs_datanode *d, struct hdfs_transfer_targets *targets);
static struct hdfs_error	_datanode_transfer(struct hdfs_datanode *d);
static struct hdfs_error	_datanode_transfer_blocking(struct hdfs_datanode *d, struct hdfs_transfer_targets *targets);
static struct hdfs_error	_read_read_status(struct hdfs_datanode *, struct hdfs_heap_buf *,
				struct hdfs_read_info *);
static struct hdfs_error	_read_read_status2(struct hdfs_datanode *, struct hdfs_heap_buf *,
				struct hdfs_read_info *);
static struct hdfs_error	_read_write_status(struct hdfs_datanode *, struct hdfs_heap_buf *);
static struct hdfs_error	_read_write_status2(struct hdfs_datanode *, struct hdfs_heap_buf *, int *err_idx);
static struct hdfs_error	_read_transfer_status(struct hdfs_datanode *, struct hdfs_heap_buf *);
static struct hdfs_error	_recv_packet(struct hdfs_packet_state *, struct hdfs_read_info *);
static struct hdfs_error	_process_recv_packet(struct hdfs_packet_state *ps, struct hdfs_read_info *ri,
				ssize_t hdr_len, ssize_t plen, ssize_t dlen, int64_t offset);
static struct hdfs_error	_recv_packet_copy_data(struct hdfs_packet_state *ps, struct hdfs_read_info *ri);
static struct hdfs_error	_send_packet(struct hdfs_packet_state *ps, int *err_idx, bool heartbeat);
static void			_compose_data_packet_header(struct hdfs_packet_state *ps);
static void			_compose_heartbeat_packet_header(struct hdfs_packet_state *ps);
static void			_set_opres_msg(struct hdfs_datanode *d, const char *);
static struct hdfs_error	_verify_crcdata(void *crcdata, int32_t chunksize,
				int32_t crcdlen, int32_t dlen, enum hdfs_checksum_type ctype);
static struct hdfs_error	_check_one_ack(struct hdfs_packet_state *ps, ssize_t *nacked, int *err_idx);
static struct hdfs_error	_check_one_ack2(struct hdfs_packet_state *ps, ssize_t *nacked, int *err_idx);
static struct hdfs_error	_check_acks(struct hdfs_packet_state *ps, ssize_t *nacked, int *err_idx);

//
// high-level api
//

EXPORT_SYM struct hdfs_datanode *
hdfs_datanode_new(struct hdfs_object *located_block, const char *client,
	int proto, enum hdfs_datanode_op op, struct hdfs_error *error_out)
{
	struct hdfs_datanode *d = NULL;
	struct hdfs_error error = HDFS_SUCCESS;

	ASSERT(located_block);
	ASSERT(located_block->ob_type == H_LOCATED_BLOCK);

	/* Bail early if the LB is non-actionable */
	if (__predict_false(located_block->ob_val._located_block._num_locs == 0)) {
		error = error_from_hdfs(HDFS_ERR_ZERO_DATANODES);
		goto out;
	}

	d = hdfs_datanode_alloc();
	error = hdfs_datanode_init(d, located_block, client, proto, op);
	if (hdfs_is_error(error))
		goto out;
	error = hdfs_datanode_connect(d);
	if (hdfs_is_error(error))
		goto out;

out:
	if (hdfs_is_error(error)) {
		if (d) {
			hdfs_datanode_delete(d);
			d = NULL;
		}
		*error_out = error;
	}
	return d;
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

EXPORT_SYM struct hdfs_datanode *
hdfs_datanode_alloc(void)
{
	struct hdfs_datanode *d;

	d = malloc(sizeof(*d));
	ASSERT(d);
	memset(d, 0, sizeof(*d));
	return d;
}

// It is assumed that the struct hdfs_datanode is initialized to all 0s prior to
// the first invocation of hdfs_datanode_init() (which is performed by
// hdfs_datanode_alloc()), and passed to hdfs_datanode_clean() or
// hdfs_datanode_destroy() immediately prior to any subsequent calls to
// hdfs_datanode_init()
//
// XXX Do we want a seprate init function that takes separate arguments instead
// of a located block object? We'd have to enforce that they would then use
// hdfs_datanode_connect_init() and hdfs_datanode_connect_finalize directly
// instead of using hdfs_datanode_connect()/hdfs_datanode_connect_nb(). If
// not, then we should probably make hdfs_datanode_set_pool_id() static (or
// just remove it entirely and move its functionality to hdfs_datanode_init())
EXPORT_SYM struct hdfs_error
hdfs_datanode_init(struct hdfs_datanode *d, struct hdfs_object *located_block,
	const char *client, int proto, enum hdfs_datanode_op op)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_located_block *lb;

	ASSERT(d);
	ASSERT(located_block);
	ASSERT(located_block->ob_type == H_LOCATED_BLOCK);
	ASSERT(client);
	ASSERT(proto == HDFS_DATANODE_AP_1_0 || proto == HDFS_DATANODE_CDH3 ||
	    proto == HDFS_DATANODE_AP_2_0);
	ASSERT(d->dn_state == HDFS_DN_ST_ZERO);

	lb = &located_block->ob_val._located_block;

	/* Bail early if the LB is non-actionable */
	if (__predict_false(lb->_num_locs == 0)) {
		error = error_from_hdfs(HDFS_ERR_ZERO_DATANODES);
		goto out;
	}

	// ensure that the located block arrays are consistent
	if (__predict_false(lb->_num_storage_ids != lb->_num_storage_ids)) {
		error = error_from_hdfs(HDFS_ERR_LOCATED_BLOCK_BAD_STORAGE_IDS);
		goto out;
	}
	if (__predict_false(lb->_num_storage_ids > 0 && lb->_num_storage_ids != lb->_num_locs)) {
		error = error_from_hdfs(HDFS_ERR_LOCATED_BLOCK_BAD_STORAGE_IDS);
		goto out;
	}
	if (__predict_false(lb->_num_storage_types > 0 && lb->_num_storage_types != lb->_num_locs)) {
		error = error_from_hdfs(HDFS_ERR_LOCATED_BLOCK_BAD_STORAGE_TYPES);
		goto out;
	}

	d->dn_sock = -1;
	d->dn_proto = proto;
	d->dn_op = op;
	d->dn_client = strdup(client);
	ASSERT(d->dn_client);

	d->dn_blkid = lb->_blockid;
	d->dn_size = lb->_len;
	d->dn_maxbytesrcvd = d->dn_size; // gets adjusted in set_recovery() if need be
	d->dn_gen = lb->_generation;
	d->dn_newgen = lb->_generation;
	d->dn_offset = lb->_offset;

	d->dn_nlocs = lb->_num_locs;
	ASSERT(d->dn_nlocs > 0);
	d->dn_locs = malloc(d->dn_nlocs * sizeof(*d->dn_locs));
	ASSERT(d->dn_locs);
	for (int i = 0; i < d->dn_nlocs; i++) {
		d->dn_locs[i] = hdfs_datanode_info_copy(lb->_locs[i]);
	}
	ASSERT(lb->_num_storage_ids == lb->_num_storage_types);
	if (lb->_num_storage_ids > 0) {
		d->dn_storage_ids = malloc(d->dn_nlocs * sizeof(*d->dn_storage_ids));
		ASSERT(d->dn_storage_ids);
		d->dn_storage_types = malloc(d->dn_nlocs * sizeof(*d->dn_storage_types));
		ASSERT(d->dn_storage_types);

		for (int i = 0; i < d->dn_nlocs; i++) {
			char *sid_copy = strdup(lb->_storage_ids[i]);
			ASSERT(sid_copy);
			d->dn_storage_ids[i] = sid_copy;
			d->dn_storage_types[i] = lb->_storage_types[i];
		}
	}

	if (lb->_token)
		d->dn_token = hdfs_token_copy(lb->_token);
	else
		d->dn_token = hdfs_token_new_empty();

	if (proto >= HDFS_DATANODE_AP_2_0)
		hdfs_datanode_set_pool_id(d, lb->_pool_id);

	d->dn_state = HDFS_DN_ST_INITED;

out:
	return error;
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

// XXX Might want to just unconditionally do the 0 setting instead of
// only in the reuse case for all of the _*_clean() functions
static void
_unacked_packets_clean(struct hdfs_unacked_packets *ua, bool reuse)
{
	ASSERT(ua);

	if (reuse) {
		ua->ua_num = 0;
		ua->ua_list_pos = 0;
	} else {
		PTR_FREE(ua->ua_list);
		ua->ua_list_size = 0;
	}
}

static void
_packet_state_clean(struct hdfs_packet_state *ps, bool reuse)
{
	ASSERT(ps);

	if (reuse) {
		ps->seqno = 0;
		ps->first_unacked = 0;
		ps->remains_tot = 0;
		ps->remains_pkt = 0;
	} else {
		PTR_FREE(ps->databuf.buf);
		PTR_FREE(ps->iovbuf);
	}
	_unacked_packets_clean(&ps->unacked, reuse);
}

static void
_read_info_clean(struct hdfs_read_info *ri, bool reuse)
{
	ASSERT(ri);

	if (reuse) {
		ri->bad_crcs = false;
		ri->lastpacket = false;
	}
}

static void
_datanode_clean(struct hdfs_datanode *d, bool reuse)
{
	ASSERT(d);

	if (d->dn_sock != -1) {
		// XXX if we get a bad checksum during a datanode read, then we
		// stop reading from the socket and send an ERROR_CHECKSUM status
		// to the server. Closing the socket with unread data in the recv
		// buffer can cause the OS (at least on linux) to send a TCP RST,
		// which could possibly happen before the ERROR_CHECKSUM status is
		// sent. This should be a rare corner case (and I'm not sure how
		// the datanodes even react to an ERROR_CHECKSUM status), but we
		// may want to consider trying to handle it
		close(d->dn_sock);
		d->dn_sock = -1;
	}

	// XXX could potentially try to reuse much of this memory
	if (d->dn_token) {
		hdfs_object_free(d->dn_token);
		d->dn_token = NULL;
	}
	PTR_FREE(d->dn_client);
	PTR_FREE(d->dn_pool_id);
	for (int i = 0; i <  d->dn_nlocs; i++) {
		hdfs_object_free(d->dn_locs[i]);
	}
	PTR_FREE(d->dn_locs);
	if (d->dn_storage_ids) {
		for (int i = 0; i < d->dn_nlocs; i++) {
			free(d->dn_storage_ids[i]);
		}
	}
	PTR_FREE(d->dn_storage_ids);
	PTR_FREE(d->dn_storage_types);
	d->dn_nlocs = 0;
	_hdfs_conn_ctx_free(&d->dn_cctx);
	if (d->dn_opresult_message)
		free(__DECONST(char *, d->dn_opresult_message));
	d->dn_opresult_message = NULL;

	if (d->dn_ttrgs) {
		hdfs_transfer_targets_free(d->dn_ttrgs);
		d->dn_ttrgs = NULL;
	}

	_packet_state_clean(&d->dn_pstate, reuse);
	_read_info_clean(&d->dn_rinfo, reuse);

	if (reuse) {
		_hdfs_hbuf_reset(&d->dn_hdrbuf);
		_hdfs_hbuf_reset(&d->dn_recvbuf);
		d->dn_state = HDFS_DN_ST_ZERO;
		d->dn_op_inited = false;
		d->dn_append_or_recovery = false;
		d->dn_conn_idx = 0;
		d->dn_last = false;
		d->dn_blocking_pipeline_setup = false;
		d->dn_recovery = HDFS_DN_RECOVERY_NONE;
	} else {
		PTR_FREE(d->dn_hdrbuf.buf);
		PTR_FREE(d->dn_recvbuf.buf);
		memset(d, 0, sizeof(*d));
	}
}

EXPORT_SYM void
hdfs_datanode_clean(struct hdfs_datanode *d)
{
	_datanode_clean(d, true);
}

EXPORT_SYM void
hdfs_datanode_destroy(struct hdfs_datanode *d)
{
	_datanode_clean(d, false);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_connect(struct hdfs_datanode *d)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct pollfd pfd;

	do {
		error = hdfs_datanode_connect_nb(d);
		if (!hdfs_is_again(error))
			break;
		error = hdfs_datanode_get_eventfd(d, &pfd.fd, &pfd.events);
		if (hdfs_is_error(error))
			break;
		poll(&pfd, 1, -1);
		// XXX check that poll returns 1 (EINTR?) and/or check revents?
	} while (true);

	return error;
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_connect_nb(struct hdfs_datanode *d)
{
	struct hdfs_error error = HDFS_SUCCESS;

	ASSERT(d);
	ASSERT(d->dn_state == HDFS_DN_ST_INITED || d->dn_state == HDFS_DN_ST_CONNPENDING);

	// XXX consider moving this to hdfs_datanode_init()
	// XXX consider checking this against the replication factor?
	if (d->dn_nlocs <= 0) {
		error = error_from_hdfs(HDFS_ERR_ZERO_DATANODES);
		d->dn_state = HDFS_DN_ST_ERROR;
		goto out;
	}

	do {
		if (d->dn_state == HDFS_DN_ST_INITED) {
			struct hdfs_object *di = d->dn_locs[d->dn_conn_idx];
			const char *host, *port;
			ASSERT(d->dn_sock == -1);
			host = di->ob_val._datanode_info._ipaddr;
			port = di->ob_val._datanode_info._port;
			error = hdfs_datanode_connect_init(d, host, port, true);
		} else {
			error = hdfs_datanode_connect_finalize(d);
		}
		// Only try to connect to the first datanode listed for block writes for pipeline setup
		// XXX should we report to the user which datanodes we failed to connect to?
		if (d->dn_state == HDFS_DN_ST_ERROR && d->dn_op != HDFS_DN_OP_WRITE_BLOCK
		    && d->dn_conn_idx + 1 < d->dn_nlocs)
		{
			d->dn_state = HDFS_DN_ST_INITED;
			d->dn_conn_idx++;
		}
	} while (d->dn_state == HDFS_DN_ST_INITED);

out:
	return error;
}

// XXX Consider making datanode_connect_init/finalize() static and force users
// to use either hdfs_datanode_connect() or hdfs_datanode_connect_nb()
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

	error = _hdfs_connect_init(&d->dn_sock, host, port, &d->dn_cctx, numerichost);
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
	error = _hdfs_connect_finalize(&d->dn_sock, &d->dn_cctx);
	if (!hdfs_is_error(error)) {
		d->dn_state = HDFS_DN_ST_CONNECTED;
	} else if (!hdfs_is_again(error)) {
		d->dn_state = HDFS_DN_ST_ERROR;
	}

out:
	return error;
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_get_eventfd(struct hdfs_datanode *d, int *fd, short *events)
{
	ASSERT(d);
	ASSERT(fd);
	ASSERT(events);
	ASSERT(d->dn_state >= HDFS_DN_ST_CONNPENDING); // XXX consider returning an error instead?

	*fd = d->dn_sock;
	*events = 0;

	switch (d->dn_state) {
	case HDFS_DN_ST_CONNPENDING:
	case HDFS_DN_ST_SENDOP:
		*events |= POLLOUT;
		break;
	case HDFS_DN_ST_RECVOP:
		*events |= POLLIN;
		break;
	case HDFS_DN_ST_PKT:
		if (d->dn_op == HDFS_DN_OP_READ_BLOCK)
			*events |= POLLIN;
		else if (d->dn_op == HDFS_DN_OP_WRITE_BLOCK)
			// Set POLLOUT if we are in the middle of sending a packet or if we have data
			// to send in a new packet and we are not blocked waiting for pending ACKs
			if (_hdfs_hbuf_readlen(&d->dn_hdrbuf) > 0 ||
			    d->dn_pstate.remains_pkt > 0 ||
			    (d->dn_pstate.remains_tot > 0 && d->dn_pstate.unacked.ua_num < MAX_UNACKED_PACKETS))
				*events |= POLLOUT;
		break;
	case HDFS_DN_ST_FINISHED:
		// May be waiting to send the client read status
		if (d->dn_op == HDFS_DN_OP_READ_BLOCK && _hdfs_hbuf_readlen(&d->dn_hdrbuf) > 0)
			*events |= POLLOUT;
		// Only potentially waiting on ACKs in for writes
		break;
	case HDFS_DN_ST_ZERO:
	case HDFS_DN_ST_INITED:
	case HDFS_DN_ST_CONNECTED: // we should never have to wait in HDFS_DN_ST_CONNECTED
	case HDFS_DN_ST_ERROR:
	default:
		ASSERT(false);
	}

	// Set POLLIN if there are any outstanding ACKs regardless of state
	// (should only happen for writes in ST_PKT or ST_FINISHED)
	if (d->dn_pstate.unacked.ua_num > 0)
		*events |= POLLIN;

	return HDFS_SUCCESS;
}

// Datanode write operations

EXPORT_SYM struct hdfs_error
hdfs_datanode_write_set_append_or_recovery(struct hdfs_datanode *d, struct hdfs_object *ubfp_lb,
	enum hdfs_datanode_write_recovery_type type, off_t maxbytesrcvd)
{
	struct hdfs_error error = HDFS_SUCCESS;

	ASSERT(d);
	ASSERT(d->dn_state >= HDFS_DN_ST_INITED && d->dn_state <= HDFS_DN_ST_CONNECTED);
	ASSERT(d->dn_op == HDFS_DN_OP_WRITE_BLOCK);
	// Cannot be called after passing data to _datanode_write()
	ASSERT(d->dn_pstate.remains_tot == 0);
	ASSERT(d->dn_recovery == HDFS_DN_RECOVERY_NONE);
	ASSERT(ubfp_lb);
	ASSERT(ubfp_lb->ob_type == H_LOCATED_BLOCK);

	// Generation stamp must monotonically increase
	if (ubfp_lb->ob_val._located_block._generation <= d->dn_gen) {
		error = error_from_hdfs(HDFS_ERR_APPEND_OR_RECOVERY_BAD_GENERATION);
		goto out;
	}

	// we use the token immediately
	hdfs_object_free(d->dn_token);
	d->dn_token = hdfs_token_copy(ubfp_lb->ob_val._located_block._token);

	// the generation stamp gets sent as lastgenerationstamp in the blockop
	d->dn_newgen = ubfp_lb->ob_val._located_block._generation;

	d->dn_recovery = type;

	switch (d->dn_recovery) {
	case HDFS_DN_RECOVERY_NONE: // Regular append
	case HDFS_DN_RECOVERY_APPEND_SETUP:
	case HDFS_DN_RECOVERY_CLOSE:
		ASSERT(d->dn_size > 0);
		break;

	case HDFS_DN_RECOVERY_STREAMING:
		// if maxbytes received is negative just leave it as d->dn_size
		if (maxbytesrcvd >= 0) {
			ASSERT(maxbytesrcvd >= d->dn_size);
			d->dn_maxbytesrcvd = maxbytesrcvd;
		}
		break;

	default:
		ASSERT(false);
	}

	// XXX consider a better API for setting dn_size (i.e. pstate.offset and
	// minbytesrcvd) than having the user manually adjust the located block
	// length

	d->dn_append_or_recovery = true;

out:
	return error;
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_write_nb_init(struct hdfs_datanode *d, enum hdfs_checksum_type csum)
{
	return _datanode_write_init(d, csum);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_write_setup_pipeline_nb(struct hdfs_datanode *d, int *error_idx)
{
	return _setup_write_pipeline(d, error_idx);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_write_nb(struct hdfs_datanode *d, const void *buf, size_t len,
	ssize_t *nwritten, ssize_t *nacked, int *error_idx)
{
	struct iovec iov;

	ASSERT(buf || len == 0);

	iov.iov_base = __DECONST(void *, buf);
	iov.iov_len = len;

	return _datanode_write(d, &iov, 1/*iovcnt*/, 0/*iov_offt*/, -1/*fd*/, len,
	    -1/*fdoff*/, nwritten, nacked, error_idx, false/*heartbeat*/);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_write_setup_pipeline(struct hdfs_datanode *d, enum hdfs_checksum_type csum,
	int *error_idx)
{
	return _setup_write_pipeline_blocking(d, csum, error_idx);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_write(struct hdfs_datanode *d, const void *buf, size_t len,
	enum hdfs_checksum_type csum, ssize_t *nwritten, ssize_t *nacked, int *error_idx)
{
	struct iovec iov;

	ASSERT(buf);
	ASSERT(len > 0);

	iov.iov_base = __DECONST(void *, buf);
	iov.iov_len = len;

	return _datanode_write_blocking(d, csum, &iov, 1/*iovcnt*/, -1/*fd*/,
	    len, -1/*fdoff*/, nwritten, nacked, error_idx);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_writev_nb(struct hdfs_datanode *d, const struct iovec *iov, int iovcnt,
	ssize_t *nwritten, ssize_t *nacked, int *error_idx)
{
	size_t len = 0;

	ASSERT(iovcnt >= 0);
	ASSERT(iov || iovcnt == 0);

	// XXX consider making the user pass in a len instead
	for (int i = 0; i < iovcnt; i++) {
		len += iov[i].iov_len; // XXX check for overflow?
	}

	return _datanode_write(d, iov, iovcnt, 0/*iov_offt*/, -1/*fd*/, len,
	    -1/*fdoff*/, nwritten, nacked, error_idx, false/*heartbeat*/);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_writev(struct hdfs_datanode *d, const struct iovec *iov, int iovcnt,
	enum hdfs_checksum_type csum, ssize_t *nwritten, ssize_t *nacked, int *error_idx)
{
	size_t len = 0;

	ASSERT(iov);
	ASSERT(iovcnt > 0);

	// XXX consider making the user pass in a len instead
	for (int i = 0; i < iovcnt; i++) {
		len += iov[i].iov_len; // XXX check for overflow?
	}

	return _datanode_write_blocking(d, csum, iov, iovcnt, -1/*fd*/,
	    len, -1/*fdoff*/, nwritten, nacked, error_idx);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_write_file_nb(struct hdfs_datanode *d, int fd, off_t len, off_t offset,
	ssize_t *nwritten, ssize_t *nacked, int *error_idx)
{
	ASSERT(offset >= 0);
	ASSERT(fd >= 0);
	ASSERT(len > 0);

	return _datanode_write(d, NULL/*iov*/, 0/*iovcnt*/, 0/*iov_offt*/, fd, len,
	    offset, nwritten, nacked, error_idx, false/*heartbeat*/);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_write_file(struct hdfs_datanode *d, int fd, off_t len, off_t offset,
	enum hdfs_checksum_type csum, ssize_t *nwritten, ssize_t *nacked, int *error_idx)
{
	ASSERT(offset >= 0);
	ASSERT(fd >= 0);
	ASSERT(len > 0);

	return _datanode_write_blocking(d, csum, NULL/*iov*/, 0/*iovcnt*/, fd,
	    len, offset, nwritten, nacked, error_idx);
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
hdfs_datanode_send_heartbeat(struct hdfs_datanode *d, ssize_t *nacked, int *error_idx)
{
	ssize_t t_nwritten;

	return _datanode_write(d, NULL/*iov*/, 0/*iovcnt*/, 0/*iov_offt*/, -1/*fd*/,
	    0/*len*/, -1/*fdoff*/, &t_nwritten, nacked, error_idx, true/*heartbeat*/);
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
		// If there's still header data to be sent (i.e. a heartbeat packet), then
		// we need to get HDFS_SUCCESS from _send_packet() twice
		d->dn_repeat_last = (_hdfs_hbuf_readlen(&d->dn_hdrbuf) > 0);
	}

	// will write the last packet if necessary and check for any outstanding ACKs
	error = _datanode_write(d, NULL/*iov*/, 0/*iovcnt*/, 0/*iov_offt*/, -1/*fd*/,
	    0/*len*/, -1/*fdoff*/, &t_nwritten, nacked, error_idx, false/*heartbeat*/);

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
		_hdfs_write(d->dn_sock, __DECONST(void *, &zero), sizeof(zero), &t_nwritten);
	}

	return error;
}

// Datanode read operations

// XXX this indirection really isn't necessary except for the function name
EXPORT_SYM struct hdfs_error
hdfs_datanode_read_nb_init(struct hdfs_datanode *d, off_t bloff,
	off_t len, bool verifycsum)
{
	return _datanode_read_init(d, verifycsum, bloff, len);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_read_nb(struct hdfs_datanode *d, size_t len, void *buf, ssize_t *nread)
{
	struct iovec iov;

	ASSERT(buf);

	iov.iov_base = buf;
	iov.iov_len = len;

	return _datanode_read(d, len, -1/*fd*/, -1/*fdoff*/, &iov, 1/*iovcnt*/,
	    0/*iov_offt*/, nread);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_read(struct hdfs_datanode *d, size_t off, size_t len, void *buf,
	bool verifycsum)
{
	struct iovec iov;

	ASSERT(buf);

	iov.iov_base = buf;
	iov.iov_len = len;

	return _datanode_read_blocking(d, verifycsum, off, len, -1/*fd*/,
	    -1/*fdoff*/, &iov, 1/*iovcnt*/);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_readv_nb(struct hdfs_datanode *d, const struct iovec *iov, int iovcnt,
	ssize_t *nread)
{
	size_t len = 0;

	ASSERT(iov);
	ASSERT(iovcnt > 0);

	for (int i = 0; i < iovcnt; i++) {
		len += iov[i].iov_len; // XXX check for overflow?
	}

	return _datanode_read(d, len, -1/*fd*/, -1/*fdoff*/, iov, iovcnt,
	    0/*iov_offt*/, nread);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_readv(struct hdfs_datanode *d, size_t off, const struct iovec *iov,
	int iovcnt, bool verifycsum)
{
	size_t len = 0;

	ASSERT(iov);
	ASSERT(iovcnt > 0);

	for (int i = 0; i < iovcnt; i++) {
		len += iov[i].iov_len; // XXX check for overflow?
	}

	return _datanode_read_blocking(d, verifycsum, off, len, -1/*fd*/,
	    -1/*fdoff*/, iov, iovcnt);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_read_file_nb(struct hdfs_datanode *d, off_t len, int fd,
	off_t fdoff, ssize_t *nread)
{
	ASSERT(fd >= 0);
	ASSERT(fdoff >= 0);

	return _datanode_read(d, len, fd, fdoff, NULL/*iov*/, 0/*iovcnt*/,
	    0/*iov_offt*/, nread);
}

EXPORT_SYM struct hdfs_error
hdfs_datanode_read_file(struct hdfs_datanode *d, off_t bloff, off_t len, int fd,
	off_t fdoff, bool verifycsum)
{
	ASSERT(fdoff >= 0);
	ASSERT(fd >= 0);

	return _datanode_read_blocking(d, verifycsum, bloff, len, fd, fdoff,
	    NULL/*iov*/, 0/*iovcnt*/);
}

// Datanode transfer operations

// XXX this indirection really isn't necessary except for the function name
EXPORT_SYM struct hdfs_error
hdfs_datanode_transfer_nb_init(struct hdfs_datanode *d, struct hdfs_transfer_targets *targets)
{
	return _datanode_transfer_init(d, targets);
}

// XXX this indirection really isn't necessary except for the function name
EXPORT_SYM struct hdfs_error
hdfs_datanode_transfer_nb(struct hdfs_datanode *d)
{
	return _datanode_transfer(d);
}

// XXX this indirection really isn't necessary except for the function name
EXPORT_SYM struct hdfs_error
hdfs_datanode_transfer(struct hdfs_datanode *d, struct hdfs_transfer_targets *targets)
{
	return _datanode_transfer_blocking(d, targets);
}

static struct hdfs_error
error_from_datanode(int dnstatus, int *unknown_status)
{
	enum hdfs_error_numeric ecode;

	ASSERT(unknown_status);
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
		*unknown_status = dnstatus;
		break;
	default:
		ecode = HDFS_ERR_UNRECOGNIZED_DN_ERROR;
		*unknown_status = dnstatus;
		break;
	}
	return error_from_hdfs(ecode);
}

static void
_compose_read_header(struct hdfs_heap_buf *h, struct hdfs_datanode *d,
	off_t offset, off_t len)
{
	ASSERT(d->dn_op == HDFS_DN_OP_READ_BLOCK);

	_hdfs_bappend_s16(h, d->dn_proto);
	_hdfs_bappend_s8(h, HDFS_DN_OP_READ_BLOCK);

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
		if (d->dn_csum == HDFS_CSUM_NULL) {
			opread.has_sendchecksums = true;
			opread.sendchecksums = false;
		}

		sz = hadoop__hdfs__op_read_block_proto__get_packed_size(&opread);
		_hdfs_bappend_vlint(h, sz);
		_hdfs_hbuf_reserve(h, sz);
		hadoop__hdfs__op_read_block_proto__pack(&opread, (void *)_hdfs_hbuf_writeptr(h));
		_hdfs_hbuf_append(h, sz);
	} else {
		_hdfs_bappend_s64(h, d->dn_blkid);
		_hdfs_bappend_s64(h, d->dn_gen);
		_hdfs_bappend_s64(h, offset);
		_hdfs_bappend_s64(h, len);
		_hdfs_bappend_text(h, d->dn_client);
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
		else if (d->dn_rinfo.csum_type != HDFS_CSUM_NULL)
			status.status = HADOOP__HDFS__STATUS__CHECKSUM_OK;
		else
			status.status = HADOOP__HDFS__STATUS__SUCCESS;

		sz = hadoop__hdfs__client_read_status_proto__get_packed_size(&status);

		_hdfs_bappend_vlint(h, sz);
		_hdfs_hbuf_reserve(h, sz);
		hadoop__hdfs__client_read_status_proto__pack(&status, (void *)_hdfs_hbuf_writeptr(h));
		_hdfs_hbuf_append(h, sz);
	} else {
		_hdfs_hbuf_reserve(h, 2);
		if (d->dn_rinfo.bad_crcs)
			memcpy(_hdfs_hbuf_writeptr(h), DN_V1_CHECKSUM_OK, 2);
		else
			memcpy(_hdfs_hbuf_writeptr(h), DN_V1_ERROR_CHECKSUM, 2);
		_hdfs_hbuf_append(h, 2);
	}
}

static void
_compose_write_header(struct hdfs_heap_buf *h, struct hdfs_datanode *d)
{
	ASSERT(d->dn_op == HDFS_DN_OP_WRITE_BLOCK);

	_hdfs_bappend_s16(h, d->dn_proto);
	_hdfs_bappend_s8(h, HDFS_DN_OP_WRITE_BLOCK);

	if (d->dn_proto >= HDFS_DATANODE_AP_2_0) {
		Hadoop__Common__TokenProto token =
		    HADOOP__COMMON__TOKEN_PROTO__INIT;
		Hadoop__Hdfs__ExtendedBlockProto ebp = HADOOP__HDFS__EXTENDED_BLOCK_PROTO__INIT;
		Hadoop__Hdfs__BaseHeaderProto bhdr = HADOOP__HDFS__BASE_HEADER_PROTO__INIT;
		Hadoop__Hdfs__ClientOperationHeaderProto hdr =
		    HADOOP__HDFS__CLIENT_OPERATION_HEADER_PROTO__INIT;
		Hadoop__Hdfs__ChecksumProto csum = HADOOP__HDFS__CHECKSUM_PROTO__INIT;
		Hadoop__Hdfs__OpWriteBlockProto op = HADOOP__HDFS__OP_WRITE_BLOCK_PROTO__INIT;

		Hadoop__Hdfs__DatanodeInfoProto *dinfo_arr = NULL;
		Hadoop__Hdfs__DatanodeIDProto *did_arr = NULL;

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
		csum.type = _hdfs_csum_to_proto(d->dn_csum);

		op.header = &hdr;

		ASSERT(!d->dn_storage_ids == !d->dn_storage_types); // either both or neither are defined
		if (d->dn_storage_ids) {
			ASSERT(d->dn_storage_types);
			op.storageid = d->dn_storage_ids[0];
			op.storagetype = _hdfs_storage_type_to_proto(d->dn_storage_types[0]);
		}

		// Tell this datanode about the others in the pipeline
		if (d->dn_nlocs > 1) {
			// TODO try to avoid these local malloc()s
			op.n_targets = d->dn_nlocs - 1;
			op.targets = malloc(op.n_targets * sizeof(*op.targets));
			ASSERT(op.targets);
			dinfo_arr = malloc(op.n_targets * sizeof(*dinfo_arr));
			ASSERT(dinfo_arr);
			did_arr = malloc(op.n_targets * sizeof(*did_arr));
			ASSERT(did_arr);

			for (unsigned i = 0; i < op.n_targets; i++) {
				struct hdfs_datanode_info *h_dinfo = &d->dn_locs[i + 1]->ob_val._datanode_info;
				Hadoop__Hdfs__DatanodeInfoProto *dinfo = &dinfo_arr[i];
				Hadoop__Hdfs__DatanodeIDProto *did = &did_arr[i];

				hadoop__hdfs__datanode_info_proto__init(dinfo);
				hadoop__hdfs__datanode_idproto__init(did);

				did->ipaddr = h_dinfo->_ipaddr;
				did->hostname = h_dinfo->_hostname;
				did->datanodeuuid = h_dinfo->_uuid;
				did->xferport = strtol(h_dinfo->_port, NULL, 10); // XXX ep/error checking?
				did->infoport = h_dinfo->_infoport;
				did->ipcport = h_dinfo->_namenodeport;

				dinfo->id = did;
				if (h_dinfo->_location[0] != '\0')
					dinfo->location = h_dinfo->_location;
				// All of the other fields are listed as optional. It's unclear
				// what's actually necessary to include here

				op.targets[i] = dinfo;
			}

			if (d->dn_storage_ids) {
				// no allocation necessary
				op.n_targetstorageids = d->dn_nlocs - 1;
				op.targetstorageids = d->dn_storage_ids + 1;
				op.n_targetstoragetypes = d->dn_nlocs - 1;
				op.targetstoragetypes = _hdfs_storage_type_ptr_to_proto(d->dn_storage_types + 1);
			}
		}

		// When to send each block construction stage:
		//
		// PIPELINE_SETUP_CREATE:
		//     Writing to a new block that has never had any data sent to
		//     it. There is no corresponding recovery stage, as errors
		//     that occur prior to sending any data/pipeline creation are
		//     handled by simply abandoning the block and adding a new
		//     block with any problematic datanodes excluded. minbytesrcvd
		//     and maxbytesrcvd do not really matter in this case, but in
		//     principle they should both be 0.
		//
		// PIPELINE_SETUP_APPEND:
		//     Writing to a block that already exists and was finalized
		//     (and has more space remaining). The key point being that
		//     the block/replica to be written to MUST be finalized prior
		//     to this. minbytesrcvd must be the number of bytes already
		//     in the finalized replica to be appended. maxbytesrcvd does
		//     not really matter in this case, but in principle should be
		//     equal to minbytesrcvd.
		//
		// PIPELINE_SETUP_APPEND_RECOVERY:
		//     Recovering a failed attempt at setting up a
		//     PIPELINE_SETUP_APPEND pipeline. If an error occurred after
		//     an append pipeline was successfully created/data was
		//     written to the pipeline, then this should not be sent;
		//     PIPELINE_SETUP_STREAMING_RECOVERY should be sent
		//     instead. minbytesrcvd must be the number of bytes already
		//     in the finalized replica to be appended. maxbytesrcvd does
		//     not really matter in this case, but in principle should be
		//     equal to minbytesrcvd.
		//
		// PIPELINE_SETUP_STREAMING_RECOVERY:
		//     Recovering a pipeline that was successfully created and for
		//     which there is still more data to be sent and/or
		//     acknowledged. minbytesrcvd should be the number of bytes
		//     that are known to be in all of the replicas in the
		//     pipeline, that is, the number of bytes acknowledged in the
		//     pipeline that is being recovered plus the number of bytes
		//     that were already in the replica prior to the failed
		//     pipeline (i.e. the minbytes received sent to that
		//     pipeline). maxbytesrcvd should be the total number of bytes
		//     sent to this replica in the previous pipeline, including
		//     any that already existed in the replica.
		//
		// PIPELINE_CLOSE_RECOVERY:
		//     Recovering a pipeline was being closed. That is, a pipeline
		//     for which all data has already been acknowledged but the
		//     lastpacketinblock packet was either not sent or not
		//     acknowledged. minbytesrcvd MUST be the number of bytes in
		//     the to-be-finalized replica. maxbytesrcvd does not really
		//     matter, but in principle should be equal to minbytesrcvd.

		switch (d->dn_recovery) {
		case HDFS_DN_RECOVERY_NONE:
			ASSERT(d->dn_maxbytesrcvd == d->dn_size);
			if (d->dn_size > 0) {
				ASSERT(d->dn_append_or_recovery);
				op.stage = HADOOP__HDFS__OP_WRITE_BLOCK_PROTO__BLOCK_CONSTRUCTION_STAGE__PIPELINE_SETUP_APPEND;
			} else {
				ASSERT(!d->dn_append_or_recovery);
				op.stage = HADOOP__HDFS__OP_WRITE_BLOCK_PROTO__BLOCK_CONSTRUCTION_STAGE__PIPELINE_SETUP_CREATE;
			}
			break;

		case HDFS_DN_RECOVERY_APPEND_SETUP:
			ASSERT(d->dn_size > 0);
			ASSERT(d->dn_append_or_recovery);
			ASSERT(d->dn_maxbytesrcvd == d->dn_size);
			op.stage = HADOOP__HDFS__OP_WRITE_BLOCK_PROTO__BLOCK_CONSTRUCTION_STAGE__PIPELINE_SETUP_APPEND_RECOVERY;
			break;

		case HDFS_DN_RECOVERY_STREAMING:
			ASSERT(d->dn_append_or_recovery);
			ASSERT(d->dn_maxbytesrcvd >= d->dn_size);
			op.stage = HADOOP__HDFS__OP_WRITE_BLOCK_PROTO__BLOCK_CONSTRUCTION_STAGE__PIPELINE_SETUP_STREAMING_RECOVERY;
			break;

		case HDFS_DN_RECOVERY_CLOSE:
			ASSERT(d->dn_size > 0);
			ASSERT(d->dn_append_or_recovery);
			ASSERT(d->dn_maxbytesrcvd == d->dn_size);
			op.stage = HADOOP__HDFS__OP_WRITE_BLOCK_PROTO__BLOCK_CONSTRUCTION_STAGE__PIPELINE_CLOSE_RECOVERY;
			break;

		default:
			ASSERT(false);
		}

		op.pipelinesize = d->dn_nlocs;
		op.minbytesrcvd = d->dn_size;
		op.maxbytesrcvd = d->dn_maxbytesrcvd;
		op.latestgenerationstamp = d->dn_newgen;
		op.requestedchecksum = &csum;

		sz = hadoop__hdfs__op_write_block_proto__get_packed_size(&op);
		_hdfs_bappend_vlint(h, sz);
		_hdfs_hbuf_reserve(h, sz);
		hadoop__hdfs__op_write_block_proto__pack(&op, (void *)_hdfs_hbuf_writeptr(h));
		_hdfs_hbuf_append(h, sz);

		if (d->dn_nlocs > 1) {
			free(dinfo_arr);
			free(did_arr);
			free(op.targets);
		}
	} else {
		// TODO pipelining/datanode targets?
		_hdfs_bappend_s64(h, d->dn_blkid);
		_hdfs_bappend_s64(h, d->dn_gen);
		_hdfs_bappend_s32(h, 1);
		_hdfs_bappend_s8(h, 0);
		_hdfs_bappend_text(h, d->dn_client);
		_hdfs_bappend_s8(h, 0);
		_hdfs_bappend_s32(h, 0);
		hdfs_object_serialize(h, d->dn_token);
		_hdfs_bappend_s8(h, d->dn_csum != HDFS_CSUM_NULL);
		_hdfs_bappend_s32(h, CHUNK_SIZE/*checksum chunk size*/);
	}
}

static void
_compose_transfer_header(struct hdfs_heap_buf *h, struct hdfs_datanode *d)
{
	ASSERT(d->dn_op == HDFS_DN_OP_TRANSFER_BLOCK);

	_hdfs_bappend_s16(h, d->dn_proto);
	_hdfs_bappend_s8(h, HDFS_DN_OP_TRANSFER_BLOCK);

	if (d->dn_proto >= HDFS_DATANODE_AP_2_0) {
		Hadoop__Common__TokenProto token =
		    HADOOP__COMMON__TOKEN_PROTO__INIT;
		Hadoop__Hdfs__ExtendedBlockProto ebp = HADOOP__HDFS__EXTENDED_BLOCK_PROTO__INIT;
		Hadoop__Hdfs__BaseHeaderProto bhdr = HADOOP__HDFS__BASE_HEADER_PROTO__INIT;
		Hadoop__Hdfs__ClientOperationHeaderProto hdr =
		    HADOOP__HDFS__CLIENT_OPERATION_HEADER_PROTO__INIT;
		Hadoop__Hdfs__OpTransferBlockProto op = HADOOP__HDFS__OP_TRANSFER_BLOCK_PROTO__INIT;

		Hadoop__Hdfs__DatanodeInfoProto *dinfo_arr = NULL;
		Hadoop__Hdfs__DatanodeIDProto *did_arr = NULL;

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

		op.header = &hdr;

		// TODO try to avoid these local mallocs
		op.n_targets = d->dn_ttrgs->_num_targets;
		op.targets = malloc(op.n_targets * sizeof(*op.targets));
		ASSERT(op.targets);
		dinfo_arr = malloc(op.n_targets * sizeof(*dinfo_arr));
		ASSERT(dinfo_arr);
		did_arr = malloc(op.n_targets * sizeof(*did_arr));
		ASSERT(did_arr);

		for (unsigned i = 0; i < op.n_targets; i++) {
			struct hdfs_datanode_info *h_dinfo = &d->dn_ttrgs->_locs[i]->ob_val._datanode_info;
			Hadoop__Hdfs__DatanodeInfoProto *dinfo = &dinfo_arr[i];
			Hadoop__Hdfs__DatanodeIDProto *did = &did_arr[i];

			hadoop__hdfs__datanode_info_proto__init(dinfo);
			hadoop__hdfs__datanode_idproto__init(did);

			did->ipaddr = h_dinfo->_ipaddr;
			did->hostname = h_dinfo->_hostname;
			did->datanodeuuid = h_dinfo->_uuid;
			did->xferport = strtol(h_dinfo->_port, NULL, 10); // XXX ep/error checking?
			did->infoport = h_dinfo->_infoport;
			did->ipcport = h_dinfo->_namenodeport;

			dinfo->id = did;
			if (h_dinfo->_location[0] != '\0')
				dinfo->location = h_dinfo->_location;
			// All of the other fields are listed as optional. It's unclear
			// what's actually necessary to include here

			op.targets[i] = dinfo;
		}

		// either both or neither of th target storage ids and storage types must be given
		ASSERT(!d->dn_ttrgs->_storage_ids == !d->dn_ttrgs->_storage_types);
		if (d->dn_ttrgs->_storage_ids) {
			// no allocation necessary
			op.n_targetstorageids = d->dn_ttrgs->_num_targets;
			op.targetstorageids = d->dn_ttrgs->_storage_ids;
			op.n_targetstoragetypes = d->dn_ttrgs->_num_targets;
			op.targetstoragetypes = _hdfs_storage_type_ptr_to_proto(d->dn_ttrgs->_storage_types);
		}

		sz = hadoop__hdfs__op_transfer_block_proto__get_packed_size(&op);
		_hdfs_bappend_vlint(h, sz);
		_hdfs_hbuf_reserve(h, sz);
		hadoop__hdfs__op_transfer_block_proto__pack(&op, (void *)_hdfs_hbuf_writeptr(h));
		_hdfs_hbuf_append(h, sz);

		free(dinfo_arr);
		free(did_arr);
		free(op.targets);
	} else {
		// It's unclear if v1 had a transfer block operation nor what it's format
		// looked like if it existed
		ASSERT(false);
	}
}

static struct hdfs_error
_datanode_read_init(struct hdfs_datanode *d, bool verifycsum, off_t bloff, off_t len)
{
	struct hdfs_error error = HDFS_SUCCESS;

	ASSERT(d);
	ASSERT(len > 0);
	ASSERT(bloff >= 0);
	ASSERT(d->dn_state >= HDFS_DN_ST_INITED && d->dn_state <= HDFS_DN_ST_CONNECTED);
	ASSERT(d->dn_op == HDFS_DN_OP_READ_BLOCK);
	ASSERT(!d->dn_op_inited);

	d->dn_csum = verifycsum ? HDFS_CSUM_CRC32 : HDFS_CSUM_NULL; // XXX just directly assign?
	d->dn_pstate.remains_tot = len;
	d->dn_rinfo.client_offset = bloff;
	d->dn_op_inited = true;

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
	const struct iovec *iov, int iovcnt, size_t iov_offt, ssize_t *nread)
{
	struct hdfs_error error = HDFS_SUCCESS;
	ssize_t wlen;

	ASSERT(d);
	ASSERT(len > 0 || (d->dn_pstate.remains_tot == 0 && len == 0));
	ASSERT(nread);
	ASSERT(d->dn_state >= HDFS_DN_ST_INITED);
	ASSERT(d->dn_op == HDFS_DN_OP_READ_BLOCK);
	ASSERT(d->dn_op_inited);
	ASSERT(iovcnt >= 0);
	if (iovcnt > 0) {
		ASSERT(iov);
		ASSERT(iov_offt <= iov[0].iov_len);
	}

	*nread = 0;

	switch (d->dn_state) {
	case HDFS_DN_ST_INITED:
	case HDFS_DN_ST_CONNPENDING:
		error = hdfs_datanode_connect_nb(d);
		// state transitions handled by hdfs_datanode_connect_nb()
		if (hdfs_is_error(error)) { // includes HDFS_AGAIN
			ASSERT(d->dn_state < HDFS_DN_ST_CONNECTED);
			goto out;
		}
		ASSERT(d->dn_state == HDFS_DN_ST_CONNECTED);
		// fall through
	case HDFS_DN_ST_CONNECTED:
		ASSERT(_hdfs_hbuf_readlen(&d->dn_hdrbuf) == 0);
		_compose_read_header(&d->dn_hdrbuf, d, d->dn_rinfo.client_offset, d->dn_pstate.remains_tot);
		d->dn_state = HDFS_DN_ST_SENDOP;
		// fall through
	case HDFS_DN_ST_SENDOP:
		error = _hdfs_write(d->dn_sock, _hdfs_hbuf_readptr(&d->dn_hdrbuf), _hdfs_hbuf_readlen(&d->dn_hdrbuf), &wlen);
		if (wlen < 0) {
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		_hdfs_hbuf_consume(&d->dn_hdrbuf, wlen);
		if (hdfs_is_again(error))
			goto out;
		// complete write
		ASSERT(_hdfs_hbuf_readlen(&d->dn_hdrbuf) == 0);
		ASSERT(_hdfs_hbuf_readlen(&d->dn_recvbuf) == 0);
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
		if (d->dn_rinfo.csum_type == HDFS_CSUM_NULL && d->dn_csum != HDFS_CSUM_NULL) {
			d->dn_state = HDFS_DN_ST_ERROR;
			error = error_from_hdfs(HDFS_ERR_DATANODE_NO_CRCS);
			goto out;
		}
		d->dn_pstate.sock = d->dn_sock;
		d->dn_pstate.sendcsum_type = d->dn_csum; // XXX this is never used in _recv_packet() -- remove here?
		d->dn_pstate.recvbuf = &d->dn_recvbuf;
		d->dn_pstate.proto = d->dn_proto;
		d->dn_state = HDFS_DN_ST_PKT;
		// fall through
	case HDFS_DN_ST_PKT:
		d->dn_rinfo.rlen = len;
		d->dn_rinfo.iov_offt = iov_offt;
		// Directly use a reference to the caller's iov, unlike _datanode_write()
		d->dn_pstate.iovp = __DECONST(struct iovec *, iov);
		d->dn_pstate.data_iovcnt = iovcnt;
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
		ASSERT(_hdfs_hbuf_readlen(&d->dn_hdrbuf) > 0); // XXX consider
		error = _hdfs_write(d->dn_sock, _hdfs_hbuf_readptr(&d->dn_hdrbuf), _hdfs_hbuf_readlen(&d->dn_hdrbuf), &wlen);
		if (wlen < 0) {
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		// complete or partial write (possibly 0 bytes)
		_hdfs_hbuf_consume(&d->dn_hdrbuf, wlen);
		// XXX should we call shutdown(2) here on HDFS_SUCCESS?
		if (d->dn_rinfo.bad_crcs && !hdfs_is_error(error)) {
			// Once the checksum error is reported to the server, tell the user
			error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_CHECKSUM);
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		break;

	case HDFS_DN_ST_ZERO:
	case HDFS_DN_ST_ERROR:
	default:
		ASSERT(false);
	}

out:
	return error;
}

static struct hdfs_error
_datanode_read_blocking(struct hdfs_datanode *d, bool verifycsum, off_t bloff, off_t len,
	int fd, off_t fdoff, const struct iovec *iov, int iovcnt)
{
	struct hdfs_error error;
	ssize_t nr = 0;
	size_t iov_offt = 0;
	struct pollfd pfd = { 0 };

	ASSERT(d->dn_state >= HDFS_DN_ST_INITED && d->dn_state <= HDFS_DN_ST_CONNECTED);

	error = _datanode_read_init(d, verifycsum, bloff, len);
	if (hdfs_is_error(error))
		goto out;

	while (true) {
		error = _datanode_read(d, len, fd, fdoff, iov, iovcnt, iov_offt, &nr);
		if (!hdfs_is_error(error)) // success
			break;
		if (!hdfs_is_again(error)) // error
			goto out;
		// again
		if (iovcnt > 0) {
			size_t tnr = nr;

			while (tnr >= iov->iov_len - iov_offt) {
				tnr -= iov->iov_len - iov_offt;
				iov_offt = 0;
				iov++;
				iovcnt--;
				ASSERT(iovcnt > 0);
			}
			// We use iov_offt because we cannot change
			// iov_base/iov_len due to iov being a const pointer
			iov_offt += tnr;
		}
		fdoff += nr;
		len -= nr;
		error = hdfs_datanode_get_eventfd(d, &pfd.fd, &pfd.events);
		if (hdfs_is_error(error))
			goto out;
		poll(&pfd, 1, -1);
		// XXX check that poll returns 1 (EINTR?) and/or check revents?
	}

out:
	return error;
}

static struct hdfs_error
_datanode_write_init(struct hdfs_datanode *d, enum hdfs_checksum_type csum)
{
	struct hdfs_error error = HDFS_SUCCESS;

	ASSERT(d);
	ASSERT(d->dn_state >= HDFS_DN_ST_INITED && d->dn_state <= HDFS_DN_ST_CONNECTED);
	ASSERT(d->dn_op == HDFS_DN_OP_WRITE_BLOCK);
	ASSERT(csum == HDFS_CSUM_NULL || csum == HDFS_CSUM_CRC32 || csum == HDFS_CSUM_CRC32C);
	// crc32c only supported in v2.0+
	ASSERT(csum != HDFS_CSUM_CRC32C || d->dn_proto >= HDFS_DATANODE_AP_2_0);
	ASSERT(!d->dn_op_inited);

	d->dn_csum = csum;
	d->dn_op_inited = true;

	return error;
}

static struct hdfs_error
_setup_write_pipeline(struct hdfs_datanode *d, int *err_idx)
{
	struct hdfs_error error = HDFS_SUCCESS;
	ssize_t wlen;

	ASSERT(d);
	ASSERT(err_idx);
	ASSERT(d->dn_state >= HDFS_DN_ST_INITED && d->dn_state <= HDFS_DN_ST_RECVOP);
	ASSERT(d->dn_op == HDFS_DN_OP_WRITE_BLOCK);
	ASSERT(d->dn_op_inited);

	*err_idx = -1;

	switch (d->dn_state) {
	case HDFS_DN_ST_INITED:
	case HDFS_DN_ST_CONNPENDING:
		error = hdfs_datanode_connect_nb(d);
		// state transitions handled by hdfs_datanode_connect_nb()
		if (hdfs_is_again(error)) {
			ASSERT(d->dn_state < HDFS_DN_ST_CONNECTED);
			goto out;
		} else if (hdfs_is_error(error)) {
			ASSERT(d->dn_state == HDFS_DN_ST_ERROR);
			*err_idx = 0;
			goto out;
		}
		ASSERT(d->dn_state == HDFS_DN_ST_CONNECTED);
		// fall through
	case HDFS_DN_ST_CONNECTED:
		ASSERT(_hdfs_hbuf_readlen(&d->dn_hdrbuf) == 0);
		_compose_write_header(&d->dn_hdrbuf, d);
		d->dn_state = HDFS_DN_ST_SENDOP;
		// fall through
	case HDFS_DN_ST_SENDOP:
		error = _hdfs_write(d->dn_sock, _hdfs_hbuf_readptr(&d->dn_hdrbuf), _hdfs_hbuf_readlen(&d->dn_hdrbuf), &wlen);
		if (wlen < 0) {
			*err_idx = 0;
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		_hdfs_hbuf_consume(&d->dn_hdrbuf, wlen);
		if (hdfs_is_again(error))
			goto out;
		// complete write
		ASSERT(_hdfs_hbuf_readlen(&d->dn_hdrbuf) == 0);
		ASSERT(_hdfs_hbuf_readlen(&d->dn_recvbuf) == 0);
		d->dn_state = HDFS_DN_ST_RECVOP;
		// fall through
	case HDFS_DN_ST_RECVOP:
		if (d->dn_proto >= HDFS_DATANODE_AP_2_0)
			error = _read_write_status2(d, &d->dn_recvbuf, err_idx);
		else
			error = _read_write_status(d, &d->dn_recvbuf);
		if (hdfs_is_again(error)) {
			goto out; // no state or buffer change.
		} else if (hdfs_is_error(error)) {
			// Say the primary datanode failed if err_idx not already
			// set (e.g. a read(2) error)
			// XXX Consider: Only set err_idx to 0 is error.her_kind == he_errno?
			// If it's an hdfs error, then setting err_idx is disingenuous
			if (*err_idx < 0)
				*err_idx = 0;
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		// Success!
		if (d->dn_recovery == HDFS_DN_RECOVERY_CLOSE) {
			// If we sent PIPELINE_CLOSE_RECOVERY we are done
			d->dn_state = HDFS_DN_ST_FINISHED;
			break;
		}
		d->dn_pstate.sock = d->dn_sock;
		d->dn_pstate.sendcsum_type = d->dn_csum;
		d->dn_pstate.hdrbuf = &d->dn_hdrbuf;
		d->dn_pstate.recvbuf = &d->dn_recvbuf;
		d->dn_pstate.proto = d->dn_proto;
		d->dn_pstate.unknown_status = &d->dn_unknown_status;
		d->dn_pstate.offset = d->dn_size;
		d->dn_pstate.pipelinesize = d->dn_nlocs;
		d->dn_state = HDFS_DN_ST_PKT;
		break;

	case HDFS_DN_ST_ZERO:
	case HDFS_DN_ST_PKT:
	case HDFS_DN_ST_FINISHED:
	case HDFS_DN_ST_ERROR:
		ASSERT(false);
	}

out:
	return error;
}

#define ROUNDUP2(x, y) (((x) + (y) - 1) & ~((y) - 1)) // only works for powers of 2

static struct hdfs_error
_datanode_write(struct hdfs_datanode *d, const struct iovec *iov, int iovcnt, size_t iov_offt,
	int fd, off_t len, off_t fdoff, ssize_t *nwritten, ssize_t *nacked, int *err_idx, bool heartbeat)
{
	ssize_t t_nacked = 0;
	struct hdfs_error error = HDFS_SUCCESS, ret;

	ASSERT(d);
	ASSERT(nwritten);
	ASSERT(nacked);
	ASSERT(err_idx);
	ASSERT(d->dn_state >= HDFS_DN_ST_INITED);
	ASSERT(d->dn_op == HDFS_DN_OP_WRITE_BLOCK);
	ASSERT(d->dn_op_inited);
	ASSERT(len >= d->dn_pstate.remains_tot);
	ASSERT(!d->dn_last || len == 0); // Cannot try to write more data after calling finish_block
	ASSERT(!heartbeat || len == 0); // No data may be passed in when sending a heartbeat
	ASSERT(!d->dn_last || !heartbeat); // Cannot send a heartbeat after calling finish_block
	// PIPELINE_CLOSE_RECOVERY must be handled by the separate setup_write_pipeline API
	ASSERT(d->dn_recovery != HDFS_DN_RECOVERY_CLOSE);
	ASSERT(d->dn_size == 0 || d->dn_append_or_recovery);
	ASSERT(iovcnt >= 0);
	if (iovcnt > 0) {
		ASSERT(iov);
		ASSERT(iov_offt <= iov[0].iov_len);
	}

	*nacked = 0;
	*err_idx = -1;
	// *nwritten is set in the out label, so it will always be initialized before return

	d->dn_pstate.remains_tot = len;

	switch (d->dn_state) {
	case HDFS_DN_ST_INITED:
	case HDFS_DN_ST_CONNPENDING:
	case HDFS_DN_ST_CONNECTED:
	case HDFS_DN_ST_SENDOP:
	case HDFS_DN_ST_RECVOP:
		ASSERT(!d->dn_append_or_recovery); // appends/recovery must call setup pipeline separately
		error = _setup_write_pipeline(d, err_idx);
		// state transitions handled by _setup_write_pipeline()
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
		ASSERT(d->dn_state == HDFS_DN_ST_PKT);
		// fall through
	case HDFS_DN_ST_PKT:
		// Shallow copy the iovec array to be manipulated by _send_packet(),
		// leaving the 0th iovec free for headers and checksums
		if (iovcnt + 1 > d->dn_pstate.iovbuf_size) {
			d->dn_pstate.iovbuf_size = ROUNDUP2(iovcnt + 1, 16); // _send_packet() requires at least 2
			d->dn_pstate.iovbuf = realloc(d->dn_pstate.iovbuf,
			    d->dn_pstate.iovbuf_size * sizeof(*d->dn_pstate.iovbuf));
		}
		for (int i = 0; i < iovcnt; i++) { // XXX consider memcpy instead
			d->dn_pstate.iovbuf[i + 1] = iov[i];
		}
		if (iovcnt > 0) { // Apply the shift to the first iovec
			d->dn_pstate.iovbuf[1].iov_base = (char *)d->dn_pstate.iovbuf[1].iov_base + iov_offt;
			d->dn_pstate.iovbuf[1].iov_len -= iov_offt;
		}
		d->dn_pstate.iovp = d->dn_pstate.iovbuf;
		d->dn_pstate.data_iovcnt = iovcnt;
		d->dn_pstate.fd = fd;
		d->dn_pstate.fdoffset = fdoff;
		while (d->dn_pstate.remains_tot > 0 || d->dn_last || heartbeat) {
			// Try to drain any acks if we have many outstanding packets
			if (d->dn_pstate.unacked.ua_num >= MAX_UNACKED_PACKETS) {
				ret = _check_acks(&d->dn_pstate, &t_nacked, err_idx);
				*nacked += t_nacked; // update nacked even if there's an ACK error
				if (hdfs_is_error(ret) && !hdfs_is_again(ret)) {
					error = ret;
					d->dn_state = HDFS_DN_ST_ERROR;
					goto out;
				}
			}
			error = _send_packet(&d->dn_pstate, err_idx, heartbeat);
			if (hdfs_is_again(error)) {
				break; // proceed to check acks below
			} else if (hdfs_is_error(error)) {
				d->dn_state = HDFS_DN_ST_ERROR;
				goto out;
			}
			// Successfully wrote entire packet
			ASSERT(_hdfs_hbuf_readlen(&d->dn_hdrbuf) == 0);
			ASSERT(d->dn_pstate.remains_pkt == 0);
			if (d->dn_last && !d->dn_repeat_last) {
				d->dn_state = HDFS_DN_ST_FINISHED;
				break;
			}
			d->dn_repeat_last = false;
			if (heartbeat) {
				break;
			}
		}
		// fall through
	case HDFS_DN_ST_FINISHED:
		// Check if there are any acks to drain (without clobbering return value)
		ret = _check_acks(&d->dn_pstate, &t_nacked, err_idx);
		*nacked += t_nacked; // update nacked even if there's an ACK error
		if (hdfs_is_error(ret) && !hdfs_is_again(ret)) {
			error = ret;
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		break;

	case HDFS_DN_ST_ZERO:
	case HDFS_DN_ST_ERROR:
	default:
		ASSERT(false);
	}

out:
	// ensure *nwritten is set even if there is an error
	*nwritten = len - d->dn_pstate.remains_tot;
	return error;
}

static struct hdfs_error
_setup_write_pipeline_blocking(struct hdfs_datanode *d, enum hdfs_checksum_type csum, int *err_idx)
{
	struct hdfs_error error;
	struct pollfd pfd = { 0 };

	ASSERT(d);
	ASSERT(err_idx);
	ASSERT(d->dn_state >= HDFS_DN_ST_INITED && d->dn_state <= HDFS_DN_ST_CONNECTED);

	d->dn_blocking_pipeline_setup = true;
	*err_idx = -1;
	error = _datanode_write_init(d, csum);
	if (hdfs_is_error(error))
		goto out;

	while (true) {
		error = _setup_write_pipeline(d, err_idx);
		if (!hdfs_is_error(error)) // success
			break;
		if (!hdfs_is_again(error)) // error
			goto out;
		// again
		error = hdfs_datanode_get_eventfd(d, &pfd.fd, &pfd.events);
		if (hdfs_is_error(error))
			goto out;
		poll(&pfd, 1, -1);
		// XXX check that poll returns 1 (EINTR?) and/or check revents?
	}

out:
	return error;
}

static struct hdfs_error
_datanode_write_blocking(struct hdfs_datanode *d, enum hdfs_checksum_type csum,
	const struct iovec *iov, int iovcnt, int fd, off_t len, off_t fdoff,
	ssize_t *nwritten, ssize_t *nacked, int *err_idx)
{
	struct hdfs_error error;
	ssize_t nw = 0, na = 0;
	size_t iov_offt = 0;
	struct pollfd pfd = { 0 };

	ASSERT(d);
	ASSERT(nwritten);
	ASSERT(nacked);
	ASSERT(err_idx);

	*nwritten = 0;
	*nacked = 0;
	*err_idx = -1;

	// Only call _datanode_write_init() if it hasn't already been called by
	// hdfs_datanode_write_setup_pipeline()
	if (d->dn_blocking_pipeline_setup) {
		ASSERT(d->dn_state == HDFS_DN_ST_PKT);
		ASSERT(d->dn_pstate.remains_tot == 0);
		ASSERT(d->dn_pstate.unacked.ua_num == 0);
	} else {
		ASSERT(d->dn_state >= HDFS_DN_ST_INITED && d->dn_state <= HDFS_DN_ST_CONNECTED);
		error = _datanode_write_init(d, csum);
		if (hdfs_is_error(error))
			goto out;
	}

	while (true) {
		error = _datanode_write(d, iov, iovcnt, iov_offt, fd, len, fdoff,
		    &nw, &na, err_idx, false/*heartbeat*/);
		*nwritten += nw; // update even in error case
		*nacked += na;
		if (!hdfs_is_error(error)) // success
			break;
		if (!hdfs_is_again(error)) // error
			goto out;
		// again
		if (iovcnt > 0) { // advance through the iovec array
			size_t tnw = nw;

			while (tnw >= iov->iov_len - iov_offt) {
				tnw -= iov->iov_len - iov_offt;
				iov_offt = 0;
				iov++;
				iovcnt--;
				ASSERT(iovcnt > 0);
			}
			// We use iov_offt because we cannot change
			// iov_base/iov_len due to iov being a const pointer
			iov_offt += tnw;
		}
		fdoff += nw;
		len -= nw;
		error = hdfs_datanode_get_eventfd(d, &pfd.fd, &pfd.events);
		if (hdfs_is_error(error))
			goto out;
		poll(&pfd, 1, -1);
		// XXX check that poll returns 1 (EINTR?) and/or check revents?
	}

	while (true) {
		error = hdfs_datanode_finish_block(d, &na, err_idx);
		*nacked += na; // update even in error case
		if (!hdfs_is_error(error)) // success
			break;
		if (!hdfs_is_again(error)) // error
			goto out;
		// again
		error = hdfs_datanode_get_eventfd(d, &pfd.fd, &pfd.events);
		if (hdfs_is_error(error))
			goto out;
		poll(&pfd, 1, -1);
		// XXX check that poll returns 1 (EINTR?) and/or check revents?
	}

out:
	return error;
}

static struct hdfs_error
_datanode_transfer_init(struct hdfs_datanode *d, struct hdfs_transfer_targets *targets)
{
	struct hdfs_error error = HDFS_SUCCESS;

	ASSERT(d);
	ASSERT(d->dn_state >= HDFS_DN_ST_INITED && d->dn_state <= HDFS_DN_ST_CONNECTED);
	ASSERT(d->dn_op == HDFS_DN_OP_TRANSFER_BLOCK);
	ASSERT(!d->dn_op_inited);
	ASSERT(!d->dn_ttrgs);
	ASSERT(targets);

	if (targets->_num_targets <= 0) {
		error = error_from_hdfs(HDFS_ERR_ZERO_DATANODES);
		goto out;
	}

	d->dn_ttrgs = _hdfs_transfer_targets_copy(targets);
	d->dn_op_inited = true;

out:
	return error;
}

static struct hdfs_error
_datanode_transfer(struct hdfs_datanode *d)
{
	struct hdfs_error error = HDFS_SUCCESS;
	ssize_t wlen;

	ASSERT(d);
	ASSERT(d->dn_state >= HDFS_DN_ST_INITED);
	ASSERT(d->dn_op == HDFS_DN_OP_TRANSFER_BLOCK);
	ASSERT(d->dn_op_inited);
	ASSERT(d->dn_ttrgs);

	switch (d->dn_state) {
	case HDFS_DN_ST_INITED:
	case HDFS_DN_ST_CONNPENDING:
		error = hdfs_datanode_connect_nb(d);
		// state transitions handled by hdfs_datanode_connect_nb()
		if (hdfs_is_error(error)) { // includes HDFS_AGAIN
			ASSERT(d->dn_state < HDFS_DN_ST_CONNECTED);
			goto out;
		}
		ASSERT(d->dn_state == HDFS_DN_ST_CONNECTED);
		// fall through
	case HDFS_DN_ST_CONNECTED:
		ASSERT(_hdfs_hbuf_readlen(&d->dn_hdrbuf) == 0);
		_compose_transfer_header(&d->dn_hdrbuf, d);
		d->dn_state = HDFS_DN_ST_SENDOP;
		// fall through
	case HDFS_DN_ST_SENDOP:
		error = _hdfs_write(d->dn_sock, _hdfs_hbuf_readptr(&d->dn_hdrbuf), _hdfs_hbuf_readlen(&d->dn_hdrbuf), &wlen);
		if (wlen < 0) {
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		_hdfs_hbuf_consume(&d->dn_hdrbuf, wlen);
		if (hdfs_is_again(error))
			goto out;
		// complete write
		ASSERT(_hdfs_hbuf_readlen(&d->dn_hdrbuf) == 0);
		ASSERT(_hdfs_hbuf_readlen(&d->dn_recvbuf) == 0);
		d->dn_state = HDFS_DN_ST_RECVOP;
		// fall through
	case HDFS_DN_ST_RECVOP:
		error = _read_transfer_status(d, &d->dn_recvbuf);
		if (hdfs_is_again(error)) {
			goto out; // no state or buffer change.
		} else if (hdfs_is_error(error)) {
			d->dn_state = HDFS_DN_ST_ERROR;
			goto out;
		}
		// Success!
		d->dn_state = HDFS_DN_ST_FINISHED;
		break;

	case HDFS_DN_ST_ZERO:
	case HDFS_DN_ST_PKT:
	case HDFS_DN_ST_FINISHED:
	case HDFS_DN_ST_ERROR:
	default:
		ASSERT(false);
	}

out:
	return error;
}

static struct hdfs_error
_datanode_transfer_blocking(struct hdfs_datanode *d, struct hdfs_transfer_targets *targets)
{
	struct hdfs_error error;
	struct pollfd pfd = { 0 };

	ASSERT(d->dn_state >= HDFS_DN_ST_INITED && d->dn_state <= HDFS_DN_ST_CONNECTED);

	error = _datanode_transfer_init(d, targets);
	if (hdfs_is_error(error))
		goto out;

	while (true) {
		error = _datanode_transfer(d);
		if (!hdfs_is_error(error)) // success
			break;
		if (!hdfs_is_again(error)) // error
			goto out;
		// again
		error = hdfs_datanode_get_eventfd(d, &pfd.fd, &pfd.events);
		if (hdfs_is_error(error))
			goto out;
		poll(&pfd, 1, -1);
		// XXX check that poll returns 1 (EINTR?) and/or check revents?
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

	while (_hdfs_hbuf_readlen(h) < 2) {
		error = _hdfs_read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	obuf.buf = _hdfs_hbuf_readptr(h);
	obuf.size = _hdfs_hbuf_readlen(h);

	status = _hdfs_bslurp_s16(&obuf);
	ASSERT(obuf.used > 0); // should not be able to fail

	if (status != HADOOP__HDFS__STATUS__SUCCESS) {
		error = error_from_datanode(status, &d->dn_unknown_status);
		goto out;
	}

	while (_hdfs_hbuf_readlen(h) < 15) {
		error = _hdfs_read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	// must grab readptr() again due to possible realloc() in _hdfs_read_to_hbuf()
	obuf.buf = _hdfs_hbuf_readptr(h);
	obuf.size = _hdfs_hbuf_readlen(h);

	crcs = _hdfs_bslurp_s8(&obuf);
	ASSERT(obuf.used > 0); // should not be able to fail
	chunk_size = _hdfs_bslurp_s32(&obuf);
	ASSERT(obuf.used > 0); // should not be able to fail
	server_offset = _hdfs_bslurp_s64(&obuf);
	ASSERT(obuf.used > 0); // should not be able to fail

	ri->server_offset = server_offset;
	ri->chunk_size = chunk_size;
	ri->csum_type = crcs ? HDFS_CSUM_CRC32 : HDFS_CSUM_NULL;

	// Skip recvbuf past request status
	_hdfs_hbuf_consume(h, obuf.used);

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
	if (_hdfs_hbuf_readlen(h) == 0) {
		error = _hdfs_read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	do {
		// try to parse what we already have
		obuf.buf = _hdfs_hbuf_readptr(h);
		obuf.used = 0;
		obuf.size = _hdfs_hbuf_readlen(h);

		sz = _hdfs_bslurp_vlint(&obuf);
		if (obuf.used == _H_PARSE_ERROR) {
			error = error_from_hdfs(HDFS_ERR_INVALID_VLINT);
			goto out;
		}
		if (obuf.used == _H_PARSE_EOF) {
			// if we need more data, then try to read more and parse again
			error = _hdfs_read_to_hbuf(d->dn_sock, h);
			if (hdfs_is_error(error)) // includes HDFS_AGAIN
				goto out;
		}
	} while (obuf.used < 0);

	if (sz >= INT_MAX - obuf.used) {
		error = error_from_hdfs(HDFS_ERR_INVALID_BLOCKOPRESPONSEPROTO); // XXX consider different error
		goto out;
	}

	while (_hdfs_hbuf_readlen(h) < obuf.used + (int)sz) {
		error = _hdfs_read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}
	obuf.buf = _hdfs_hbuf_readptr(h);
	obuf.size = _hdfs_hbuf_readlen(h);

	opres = hadoop__hdfs__block_op_response_proto__unpack(NULL, sz,
	    (void *)&obuf.buf[obuf.used]); // points past the already parsed vlint
	obuf.used += sz;
	if (opres == NULL) {
		error = error_from_hdfs(HDFS_ERR_INVALID_BLOCKOPRESPONSEPROTO);
		goto out;
	}

	_set_opres_msg(d, opres->message);

	if (opres->status != HADOOP__HDFS__STATUS__SUCCESS)
		error = error_from_datanode(opres->status, &d->dn_unknown_status);
	// Shouldn't happen, I believe; call it a protocol error.
	else if (opres->message != NULL)
		error = error_from_hdfs(HDFS_ERR_INVALID_DN_OPRESP_MSG);

	*opres_out = opres;

	// Skip recvbuf past stuff we parsed here
	_hdfs_hbuf_consume(h, obuf.used);

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

	if (!opres->readopchecksuminfo || !opres->readopchecksuminfo->checksum) {
		error = error_from_hdfs(HDFS_ERR_INVALID_BLOCKOPRESPONSEPROTO); // XXX consider different error code
		goto out;
	}

	ri->server_offset = opres->readopchecksuminfo->chunkoffset;
	ri->csum_type = _hdfs_csum_from_proto(opres->readopchecksuminfo->checksum->type);
	ri->chunk_size = opres->readopchecksuminfo->checksum->bytesperchecksum;

	switch (ri->csum_type) {
	case HDFS_CSUM_NULL:
	case HDFS_CSUM_CRC32:
	case HDFS_CSUM_CRC32C:
		break;
	default:
		error = error_from_hdfs(HDFS_ERR_DATANODE_UNSUPPORTED_CHECKSUM);
		goto out;
	}

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

	while (_hdfs_hbuf_readlen(h) < 2) {
		error = _hdfs_read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	obuf.buf = _hdfs_hbuf_readptr(h);
	obuf.size = _hdfs_hbuf_readlen(h);

	status = _hdfs_bslurp_s16(&obuf);
	if (obuf.used == _H_PARSE_ERROR) {
		error = error_from_hdfs(HDFS_ERR_V1_DATANODE_PROTOCOL);
		goto out;
	}
	ASSERT(obuf.used > 0); // should not be able to fail

	statussz = obuf.used;

	// If we don't have any data queued up, try to read first
	if ((size_t)_hdfs_hbuf_readlen(h) == statussz) {
		error = _hdfs_read_to_hbuf(d->dn_sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	do {
		// try to parse what we already have
		obuf.buf = _hdfs_hbuf_readptr(h) + statussz;
		obuf.used = 0;
		obuf.size = _hdfs_hbuf_readlen(h) - statussz;

		statusmsg = _hdfs_bslurp_text(&obuf);
		if (obuf.used == _H_PARSE_ERROR) {
			error = error_from_hdfs(HDFS_ERR_V1_DATANODE_PROTOCOL);
			goto out;
		}
		if (obuf.used == _H_PARSE_EOF) {
			// if we need more data, then try to read more and parse again
			error = _hdfs_read_to_hbuf(d->dn_sock, h);
			if (hdfs_is_error(error)) // includes HDFS_AGAIN
				goto out;
		}
	} while (obuf.used < 0);

	statussz += obuf.used;

	if (status == HADOOP__HDFS__STATUS__SUCCESS && strlen(statusmsg) == 0)
		_set_opres_msg(d, NULL);
	else {
		// Shouldn't happen, I believe; call it a protocol error.
		if (status == HADOOP__HDFS__STATUS__SUCCESS)
			error = error_from_hdfs(HDFS_ERR_INVALID_DN_OPRESP_MSG);
		else
			error = error_from_datanode(status, &d->dn_unknown_status);
		_set_opres_msg(d, statusmsg);
	}

	// Skip the recv buffer past the read objects
	_hdfs_hbuf_consume(h, statussz);

out:
	if (statusmsg)
		free(statusmsg);
	return error;
}

static struct hdfs_error
_read_write_status2(struct hdfs_datanode *d, struct hdfs_heap_buf *h, int *err_idx)
{
	struct hdfs_error error = HDFS_SUCCESS;
	Hadoop__Hdfs__BlockOpResponseProto *opres = NULL;

	error = _read_blockop_resp_status(d, h, &opres);

	if (opres && opres->firstbadlink && opres->firstbadlink[0] != '\0') {
		int i;
		for (i = 0; i < d->dn_nlocs; i++) {
			char buf[32]; // big enough for 111.222.333.444:65535
			struct hdfs_datanode_info *di = &d->dn_locs[i]->ob_val._datanode_info;
			snprintf(buf, sizeof(buf), "%s:%s", di->_ipaddr, di->_port);
			if (!strcmp(opres->firstbadlink, buf))
				*err_idx = i;
		}
		// This shouldn't happend, but if the status was SUCCESS or the
		// badfirstlink doesn't match any of the targets, set an error
		if (!hdfs_is_error(error) || i == d->dn_nlocs) {
			error = error_from_hdfs(HDFS_ERR_INVALID_DN_OPRESP_MSG);
		}
	}

	if (opres)
		hadoop__hdfs__block_op_response_proto__free_unpacked(opres, NULL);
	return error;
}

static struct hdfs_error
_read_transfer_status(struct hdfs_datanode *d, struct hdfs_heap_buf *h)
{
	struct hdfs_error error = HDFS_SUCCESS;
	Hadoop__Hdfs__BlockOpResponseProto *opres = NULL;

	// TODO change this if v1 support is added
	ASSERT(d->dn_proto == HDFS_DATANODE_AP_2_0);

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
		ASSERT(_hdfs_hbuf_readlen(recvbuf) >= ps->remains_pkt);
		error = _recv_packet_copy_data(ps, ri);
		goto out;
	}

	if (ps->proto < HDFS_DATANODE_AP_2_0) {
		// slurp packet header
		while (_hdfs_hbuf_readlen(recvbuf) < 25) {
			error = _hdfs_read_to_hbuf(ps->sock, recvbuf);
			if (hdfs_is_error(error)) // includes HDFS_AGAIN
				goto out;
		}

		obuf.buf = _hdfs_hbuf_readptr(recvbuf);
		obuf.size = _hdfs_hbuf_readlen(recvbuf);

		plen = _hdfs_bslurp_s32(&obuf);
		ASSERT(obuf.used > 0); // should not be able to fail
		offset = _hdfs_bslurp_s64(&obuf);
		ASSERT(obuf.used > 0); // should not be able to fail
		/*seqno = */_hdfs_bslurp_s64(&obuf);
		ASSERT(obuf.used > 0); // should not be able to fail
		ri->lastpacket = _hdfs_bslurp_s8(&obuf);
		ASSERT(obuf.used > 0); // should not be able to fail
		dlen = _hdfs_bslurp_s32(&obuf);
		ASSERT(obuf.used > 0); // should not be able to fail

		error = _process_recv_packet(ps, ri, 25, plen, dlen, offset);
		goto out;
	}

	while (_hdfs_hbuf_readlen(recvbuf) < 6) {
		error = _hdfs_read_to_hbuf(ps->sock, recvbuf);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	obuf.buf = _hdfs_hbuf_readptr(recvbuf);
	obuf.size = _hdfs_hbuf_readlen(recvbuf);

	plen = _hdfs_bslurp_s32(&obuf);
	ASSERT(obuf.used > 0); // should not be able to fail
	hlen = (uint16_t)_hdfs_bslurp_s16(&obuf);
	ASSERT(obuf.used > 0); // should not be able to fail

	while (_hdfs_hbuf_readlen(recvbuf) < 6 + hlen) {
		error = _hdfs_read_to_hbuf(ps->sock, recvbuf);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	phdr = hadoop__hdfs__packet_header_proto__unpack(NULL, hlen,
	    (void *)&_hdfs_hbuf_readptr(recvbuf)[6]);
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
	else if (ri->csum_type != HDFS_CSUM_NULL && crcdlen != ((dlen + ri->chunk_size - 1) / ri->chunk_size) * 4)
		error = error_from_hdfs(HDFS_ERR_DATANODE_CRC_LEN);
	else if (ri->csum_type == HDFS_CSUM_NULL && crcdlen > 0)
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
		_hdfs_hbuf_consume(recvbuf, hdr_len);
		goto out;
	}

	// XXX do we want to allow some data to be returned to the user before the
	// entire packet is received? This would be tricky if we're verifying crcs.
	// Would need to review all packet processing/copying logic (especially the case
	// where we enter _recv_packet() when we're already in the middle of copying
	// the current packet's data to the user)
	while (_hdfs_hbuf_readlen(recvbuf) < hdr_len + crcdlen + dlen) {
		error = _hdfs_read_to_hbuf(ps->sock, recvbuf);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	if (crcdlen > 0) {
		error = _verify_crcdata(_hdfs_hbuf_readptr(recvbuf) + hdr_len, ri->chunk_size, crcdlen, dlen, ri->csum_type);
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
	_hdfs_hbuf_consume(recvbuf, hdr_len + crcdlen + c_begin);
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

	// Copy the packet data out to the user's iovec array or to file:
	if (ps->data_iovcnt > 0) {
		size_t remc_len = c_len;
		while (remc_len > 0) {
			void *dst = (char *)ps->iovp->iov_base + ri->iov_offt;
			size_t dstlen = _min(remc_len, ps->iovp->iov_len - ri->iov_offt);

			memcpy(dst, _hdfs_hbuf_readptr(ps->recvbuf), dstlen);
			_hdfs_hbuf_consume(ps->recvbuf, dstlen);
			remc_len -= dstlen;

			// We cannot update the struct iovec members themselves (as they
			// belong to the caller), so we maintain an offset into the next
			// iovec
			ri->iov_offt += dstlen;
			if (ri->iov_offt == ps->iovp->iov_len) {
				ps->iovp++;
				ps->data_iovcnt--;
				ri->iov_offt = 0;
				ASSERT(ps->data_iovcnt > 0 || (size_t)c_len == ri->rlen);
			}
		}
	} else {
		// Note that this can block on the user's fd
		error = _hdfs_pwrite_all(ps->fd, _hdfs_hbuf_readptr(ps->recvbuf), c_len, ps->fdoffset);
		if (hdfs_is_error(error))
			goto out;
		_hdfs_hbuf_consume(ps->recvbuf, c_len);
	}

	ps->remains_pkt -= c_len;
	ps->remains_tot -= c_len;
	ps->fdoffset += c_len;
	ri->rlen -= c_len;

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
_send_packet(struct hdfs_packet_state *ps, int *err_idx, bool heartbeat)
{
	struct hdfs_error error = HDFS_SUCCESS;
	ssize_t wlen;
	bool copydata = false;
	int wlen_hdr = 0, wlen_data = 0;
	bool is_new;

	ASSERT(ps->remains_pkt >= 0);
	ASSERT(ps->remains_tot >= ps->remains_pkt);
	ASSERT(ps->data_iovcnt >= 0);
	ASSERT(ps->iovbuf_size >= ps->data_iovcnt + 1);
	ASSERT(ps->iovbuf_size >= 2); // ensured by the ROUNDUP2() at realloc()
	ASSERT(!heartbeat || ps->remains_tot == 0);

	// If there is data to send, we must either have a valid fd or data
	// buffers, but not both. If no data remains we should have neither.
	if (ps->remains_tot > 0)
		ASSERT((ps->fd >= 0) != (ps->data_iovcnt > 0));
	else
		ASSERT(ps->fd < 0 && ps->data_iovcnt == 0);

	*err_idx = -1;

	// if we have no data left to send from the last/current packet we're creating a new one
	is_new = (_hdfs_hbuf_readlen(ps->hdrbuf) == 0 && ps->remains_pkt == 0);

	// No need to enforce MAX_UNACKED_PACKETS or calculate remains_pkt for heartbeats
	if (is_new && !heartbeat) {
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
	} // is_new && !heartbeat

	if (ps->data_iovcnt == 0 && ps->remains_pkt != 0) {
#if !defined(__linux__) && !defined(__FreeBSD__)
		copydata = true;
#else
		if (ps->sendcsum_type != HDFS_CSUM_NULL && is_new) {
			copydata = true;
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
				copydata = true;
		}
#endif
	}

	// this copied data is at most used for this function invocation, so reset every time
	_hdfs_hbuf_reset(&ps->databuf);
	if (copydata) {
		_hdfs_hbuf_reserve(&ps->databuf, ps->remains_pkt);
		// Note that is can block on the user's fd
		error = _hdfs_pread_all(ps->fd, _hdfs_hbuf_writeptr(&ps->databuf), ps->remains_pkt, ps->fdoffset);
		if (hdfs_is_error(error))
			goto out;
		_hdfs_hbuf_append(&ps->databuf, ps->remains_pkt);
		ASSERT(ps->data_iovcnt == 0);
		ASSERT(ps->iovp == ps->iovbuf);
		ps->iovp[1].iov_base = _hdfs_hbuf_readptr(&ps->databuf);
		ps->iovp[1].iov_len = ps->remains_pkt;
	}

	if (is_new) {
		if (heartbeat) {
			_compose_heartbeat_packet_header(ps);
		} else {
			_compose_data_packet_header(ps);
		}
	}

	ps->iovp[0].iov_base = _hdfs_hbuf_readptr(ps->hdrbuf);
	ps->iovp[0].iov_len = _hdfs_hbuf_readlen(ps->hdrbuf);

	if (ps->remains_pkt == 0) { // remains_pkt is only 0 here if it's the last (empty) packet or a heartbeat packet
		ASSERT(_hdfs_hbuf_readlen(ps->hdrbuf) > 0);
		ASSERT(ps->remains_tot == 0);
		ASSERT(ps->data_iovcnt == 0);
		ASSERT(!copydata);
		error = _hdfs_writev(ps->sock, ps->iovp, 1, &wlen);
		if (wlen < 0) {
			*err_idx = 0;
			goto out;
		}
		wlen_hdr = wlen;
	} else if (ps->data_iovcnt > 0 || copydata) {
		size_t lastiov_len_orig, tpktlen = 0;
		struct iovec *tiovp = ps->iovp; // points to header iovec
		int tiovcnt = 1; // start at the first data iovec

		// Count the number of data iovecs required for this pkt write
		while (tiovp[tiovcnt].iov_len < (size_t)ps->remains_pkt - tpktlen) {
			tpktlen += tiovp[tiovcnt].iov_len;
			tiovcnt++;
		}

		// Don't pass the header iovec if it's empty
		if (_hdfs_hbuf_readlen(ps->hdrbuf) > 0) {
			tiovcnt++;
		} else {
			tiovp++;
		}

		// Temporarily adjust the last iovec to finish at the appropriate byte for this pkt
		lastiov_len_orig = tiovp[tiovcnt - 1].iov_len;
		tiovp[tiovcnt - 1].iov_len = ps->remains_pkt - tpktlen;

		error = _hdfs_writev(ps->sock, tiovp, tiovcnt, &wlen);
		if (wlen < 0) {
			*err_idx = 0;
			goto out;
		}
		wlen_hdr = _min(wlen, _hdfs_hbuf_readlen(ps->hdrbuf)); // written from the header/checksums
		wlen_data = wlen - wlen_hdr; // written from the user data

		// Restore the original iov_len of the last iovec used
		tiovp[tiovcnt - 1].iov_len = lastiov_len_orig;
	} else {
#if defined(__linux__)
		_hdfs_setsockopt(ps->sock, IPPROTO_TCP, TCP_CORK, 1);

		do {
			if (_hdfs_hbuf_readlen(ps->hdrbuf) > 0) {
				error = _hdfs_writev(ps->sock, ps->iovp, 1, &wlen);
				if (wlen < 0) {
					*err_idx = 0;
					goto out; // XXX clear TCP_CORK?
				}
				wlen_hdr = wlen;
				if (hdfs_is_again(error))
					break;
			}
			error = _hdfs_sendfile(ps->sock, ps->fd, ps->fdoffset, ps->remains_pkt, &wlen);
			if (wlen < 0) {
				*err_idx = 0; // XXX could technically be an error reading the local file
				goto out; // XXX clear TCP_CORK?
			}
			wlen_data = wlen;
		} while (0); // not a loop

		_hdfs_setsockopt(ps->sock, IPPROTO_TCP, TCP_CORK, 0);
#elif defined(__FreeBSD__)
		struct iovec *tiovp = NULL;
		int tiovcnt = 0;

		// Only send the header if it's non-empty
		if (_hdfs_hbuf_readlen(ps->hdrbuf) > 0) {
			tiovp = ps->iovp;
			tiovcnt = 1;
		}

		error = _hdfs_sendfile_bsd(ps->sock, ps->fd, ps->fdoffset, ps->remains_pkt,
		    tiovp, tiovcnt, &wlen);
		if (wlen < 0) {
			*err_idx = 0; // XXX could technically be an error reading the local file
			goto out;
		}
		wlen_hdr = _min(wlen, _hdfs_hbuf_readlen(ps->hdrbuf)); // written from the header/checksums
		wlen_data = wlen - wlen_hdr; // written from the user data
#else
		// !data => freebsd or linux. this branch should never be taken
		// on other platforms.
		ASSERT(false);
#endif
	}

	_hdfs_hbuf_consume(ps->hdrbuf, wlen_hdr);
	ps->remains_pkt -= wlen_data;
	ps->remains_tot -= wlen_data;
	ps->fdoffset += wlen_data;

	while (ps->data_iovcnt > 0 && (size_t)wlen_data >= ps->iovp[1].iov_len) {
		// Note that ps->iovp always points one iovec behind the next user
		// data to be sent (and thus always to a valid member of ps->iovbuf)
		wlen_data -= ps->iovp[1].iov_len;
		ps->iovp++;
		ps->data_iovcnt--;
	}
	if (ps->data_iovcnt > 0) {
		ASSERT((size_t)wlen_data < ps->iovp[1].iov_len);
		ps->iovp[1].iov_base = (char *)ps->iovp[1].iov_base + wlen_data;
		ps->iovp[1].iov_len -= wlen_data;
	}

out:
	return error;
}

static void
_compose_data_packet_header(struct hdfs_packet_state *ps)
{
	size_t crclen = 0;

	ASSERT(_hdfs_hbuf_readlen(ps->hdrbuf) == 0);

	// calculate crc length, if requested
	crclen = (ps->sendcsum_type != HDFS_CSUM_NULL) ? (ps->remains_pkt + CHUNK_SIZE - 1) / CHUNK_SIZE : 0;

	// construct header:
	_hdfs_bappend_s32(ps->hdrbuf, ps->remains_pkt + 4*crclen + 4); // pktlen = sizeof(pktlen) + #crcbytes + #databytes
	if (ps->proto >= HDFS_DATANODE_AP_2_0) {
		Hadoop__Hdfs__PacketHeaderProto pkt = HADOOP__HDFS__PACKET_HEADER_PROTO__INIT;
		size_t sz;

		pkt.offsetinblock = ps->offset;
		pkt.seqno = ps->seqno;
		pkt.lastpacketinblock = (ps->remains_pkt == 0);
		pkt.datalen = ps->remains_pkt;

		sz = hadoop__hdfs__packet_header_proto__get_packed_size(&pkt);
		_hdfs_bappend_s16(ps->hdrbuf, sz);
		_hdfs_hbuf_reserve(ps->hdrbuf, sz);
		hadoop__hdfs__packet_header_proto__pack(&pkt, (void *)_hdfs_hbuf_writeptr(ps->hdrbuf));
		_hdfs_hbuf_append(ps->hdrbuf, sz);
	} else {
		_hdfs_bappend_s64(ps->hdrbuf, ps->offset); // from beginning of block
		_hdfs_bappend_s64(ps->hdrbuf, ps->seqno);
		_hdfs_bappend_s8(ps->hdrbuf, (ps->remains_pkt == 0)); // lastpacketinblock
		_hdfs_bappend_s32(ps->hdrbuf, ps->remains_pkt);
	}

	// calculate the crcs, if requested
	if (crclen > 0) {
		uint32_t crcinit;
		struct iovec *tiovp;
		size_t tiov_offt = 0;

		// if we have crcs to calculate, there must be data iovecs, or we must have copied
		// this packet's worth of data from a file into databuf and pointed to it with our
		// first iovec
		ASSERT(ps->data_iovcnt > 0 || (ps->iovp[1].iov_base == _hdfs_hbuf_readptr(&ps->databuf)
		    && ps->iovp[1].iov_len == (size_t)_hdfs_hbuf_readlen(&ps->databuf)
		    && ps->iovp[1].iov_len == (size_t)ps->remains_pkt));
		ASSERT(ps->sendcsum_type == HDFS_CSUM_CRC32 || ps->sendcsum_type == HDFS_CSUM_CRC32C);

		_hdfs_hbuf_reserve(ps->hdrbuf, 4 * crclen);
		tiovp = &ps->iovp[1]; // 0th iovec is used for hdrbuf

		if (ps->sendcsum_type == HDFS_CSUM_CRC32)
			crcinit = crc32(0L, Z_NULL, 0);
		else
			crcinit = 0;

		for (unsigned i = 0; i < crclen; i++) {
			uint32_t crc = crcinit;
			uint32_t chunklen = _min(CHUNK_SIZE, ps->remains_pkt - i * CHUNK_SIZE);

			while (chunklen > 0) {
				uint32_t tlen = _min(chunklen, tiovp->iov_len - tiov_offt);
				void *tptr = (char *)tiovp->iov_base + tiov_offt;

				if (ps->sendcsum_type == HDFS_CSUM_CRC32)
					crc = crc32(crc, tptr, tlen);
				else
					crc = _hdfs_crc32c(crc, tptr, tlen);

				chunklen -= tlen;
				tiov_offt += tlen;
				if (tiov_offt == tiovp->iov_len) {
					tiovp++;
					tiov_offt = 0;
				}
			}

			_be32enc(_hdfs_hbuf_writeptr(ps->hdrbuf), crc);
			_hdfs_hbuf_append(ps->hdrbuf, 4);
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
}

static void
_compose_heartbeat_packet_header(struct hdfs_packet_state *ps)
{
	ASSERT(_hdfs_hbuf_readlen(ps->hdrbuf) == 0);
	ASSERT(ps->remains_tot == 0 && ps->remains_pkt == 0);

	// construct header:
	_hdfs_bappend_s32(ps->hdrbuf, 4); // pktlen = sizeof(len) + #crcbytes + #databytes
	if (ps->proto >= HDFS_DATANODE_AP_2_0) {
		Hadoop__Hdfs__PacketHeaderProto pkt = HADOOP__HDFS__PACKET_HEADER_PROTO__INIT;
		size_t sz;

		pkt.offsetinblock = 0;
		pkt.seqno = HEART_BEAT_SEQNO;
		pkt.lastpacketinblock = 0;
		pkt.datalen = 0;

		sz = hadoop__hdfs__packet_header_proto__get_packed_size(&pkt);
		_hdfs_bappend_s16(ps->hdrbuf, sz);
		_hdfs_hbuf_reserve(ps->hdrbuf, sz);
		hadoop__hdfs__packet_header_proto__pack(&pkt, (void *)_hdfs_hbuf_writeptr(ps->hdrbuf));
		_hdfs_hbuf_append(ps->hdrbuf, sz);
	} else {
		_hdfs_bappend_s64(ps->hdrbuf, 0); // offset
		_hdfs_bappend_s64(ps->hdrbuf, HEART_BEAT_SEQNO);
		_hdfs_bappend_s8(ps->hdrbuf, 0);  // lastpacketinblock
		_hdfs_bappend_s32(ps->hdrbuf, 0); // datalen
	}
}

static void
_set_opres_msg(struct hdfs_datanode *d, const char *newmsg)
{
	if (d->dn_opresult_message != NULL)
		free(__DECONST(char *, d->dn_opresult_message));
	if (newmsg == NULL)
		d->dn_opresult_message = NULL;
	else {
		d->dn_opresult_message = strdup(newmsg);
		ASSERT(d->dn_opresult_message != NULL);
	}
}

static struct hdfs_error
_verify_crcdata(void *crcdata, int32_t chunksize, int32_t crcdlen, int32_t dlen,
	enum hdfs_checksum_type ctype)
{
	uint32_t crcinit;
	void *data = (char *)crcdata + crcdlen;

	ASSERT(ctype == HDFS_CSUM_CRC32 || ctype == HDFS_CSUM_CRC32C);

	if (ctype == HDFS_CSUM_CRC32)
		crcinit = crc32(0L, Z_NULL, 0);
	else
		crcinit = 0;

	for (int i = 0; i < (dlen + chunksize - 1) / chunksize; i++) {
		int32_t chunklen = _min(chunksize, dlen - i*chunksize);
		uint8_t *chunk = (uint8_t *)data + i*chunksize;
		uint8_t *crcptr = (uint8_t *)crcdata + i*4;
		uint32_t crc, pcrc;

		if (ctype == HDFS_CSUM_CRC32)
			crc = crc32(crcinit, chunk, chunklen);
		else
			crc = _hdfs_crc32c(crcinit, chunk, chunklen);

		pcrc = _be32dec(crcptr);

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

	while (_hdfs_hbuf_readlen(ps->recvbuf) < acksz) {
		error = _hdfs_read_to_hbuf(ps->sock, ps->recvbuf);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	obuf.buf = _hdfs_hbuf_readptr(ps->recvbuf);
	obuf.used = 0;
	obuf.size = _hdfs_hbuf_readlen(ps->recvbuf);

	seqno = _hdfs_bslurp_s64(&obuf);
	ASSERT(obuf.used >= 0); // should not be able to fail

	if (seqno != ps->first_unacked && seqno != HEART_BEAT_SEQNO) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_SEQNO);
#if 0 // TODO stash the bad seqno in struct hdfs_datanode for users to access if they desire
		fprintf(stderr, "libhadoofus: Got unexpected ACK (%" PRIi64 ","
		    " expected %" PRIi64 "); aborting write.\n", seqno,
		    ps->first_unacked);
#endif
		goto out;
	}

	if (ps->proto == HDFS_DATANODE_AP_1_0) {
		nacks = _hdfs_bslurp_s16(&obuf);
		ASSERT(obuf.used >= 0); // should not be able to fail

		// We only connect to one datanode, we should only get one ack:
		if (nacks != 1) { // XXX TODO update when proper pipelining implemented for v1
			error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_ACK_COUNT);
			goto out;
		}

		ack = _hdfs_bslurp_s16(&obuf);
		ASSERT(obuf.used >= 0); // should not be able to fail
	} else if (ps->proto == HDFS_DATANODE_CDH3) {
		// XXX TODO update when proper pipelining implemented (unsure how
		// that works for CDH3 --- can it even give multiple acks?)
		ack = _hdfs_bslurp_s16(&obuf);
		ASSERT(obuf.used >= 0); // should not be able to fail
	}

	if (ack != HADOOP__HDFS__STATUS__SUCCESS) {
		error = error_from_datanode(ack, ps->unknown_status);
		*err_idx = 0; // XXX TODO change this when implementing full pipelining for v1
		goto out;
	}

	// Pop the length of this acked packet off of the list
	if (seqno != HEART_BEAT_SEQNO) {
		ps->first_unacked++;
		*nacked = ps->unacked.ua_list[ps->unacked.ua_list_pos];
		ps->unacked.ua_list_pos++;
		ps->unacked.ua_num--;
	}

	// Skip the recv buffer past the ack
	_hdfs_hbuf_consume(ps->recvbuf, acksz);

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
	if (_hdfs_hbuf_readlen(h) == 0) {
		error = _hdfs_read_to_hbuf(ps->sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	do {
		obuf.buf = _hdfs_hbuf_readptr(h);
		obuf.used = 0;
		obuf.size = _hdfs_hbuf_readlen(h);

		sz = _hdfs_bslurp_vlint(&obuf);
		if (obuf.used == _H_PARSE_ERROR) {
			error = error_from_hdfs(HDFS_ERR_INVALID_VLINT);
			goto out;
		}
		if (obuf.used == _H_PARSE_EOF) {
			error = _hdfs_read_to_hbuf(ps->sock, h);
			if (hdfs_is_error(error)) // includes HDFS_AGAIN
				goto out;
		}
	} while (obuf.used < 0);

	if (sz <= 0 || sz >= INT_MAX - obuf.used) {
		error = error_from_hdfs(HDFS_ERR_INVALID_PIPELINEACKPROTO); // XXX consider different error code
		goto out;
	}

	while (_hdfs_hbuf_readlen(h) < obuf.used + (int)sz) {
		error = _hdfs_read_to_hbuf(ps->sock, h);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
	}

	obuf.buf = _hdfs_hbuf_readptr(h);
	obuf.size = _hdfs_hbuf_readlen(h);

	ack = hadoop__hdfs__pipeline_ack_proto__unpack(NULL, sz, (void *)&obuf.buf[obuf.used]);
	obuf.used += sz;
	if (ack == NULL) {
		error = error_from_hdfs(HDFS_ERR_INVALID_PIPELINEACKPROTO);
		goto out;
	}

	if (ack->seqno != ps->first_unacked && ack->seqno != HEART_BEAT_SEQNO) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_SEQNO);
#if 0 // TODO stash the bad seqno in struct hdfs_datanode for users to access if they desire
		fprintf(stderr, "libhadoofus: Got unexpected ACK (%" PRIi64 ","
		    " expected %" PRIi64 "); aborting write.\n", ack->seqno,
		    ps->first_unacked);
#endif
		goto out;
	}

	// First check that we don't have too many replies, but still check the statuses
	// if there are too few in order to report err_idx
	if (ack->n_reply > ps->pipelinesize) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_ACK_COUNT);
		goto out;
	}

	for (unsigned i = 0; i < ack->n_reply; i++) {
		if (ack->reply[i] != HADOOP__HDFS__STATUS__SUCCESS) {
			error = error_from_datanode(ack->reply[i], ps->unknown_status);
			*err_idx = i;
			goto out;
		}
	}

	// Now that we've confirmed all of the replies are success, ensure that we got the
	// correct number of replies
	if (ack->n_reply != ps->pipelinesize) {
		error = error_from_hdfs(HDFS_ERR_DATANODE_BAD_ACK_COUNT);
		goto out;
	}

	// Pop the length of this acked packet off of the list
	if (ack->seqno != HEART_BEAT_SEQNO) {
		ps->first_unacked++;
		*nacked = ps->unacked.ua_list[ps->unacked.ua_list_pos];
		ps->unacked.ua_list_pos++;
		ps->unacked.ua_num--;
	}

	// Skip the recv buffer past the ack
	_hdfs_hbuf_consume(h, obuf.used);

out:
	if (ack)
		hadoop__hdfs__pipeline_ack_proto__free_unpacked(ack, NULL);
	return error;
}

static struct hdfs_error
_check_acks(struct hdfs_packet_state *ps, ssize_t *nacked, int *err_idx)
{
	struct hdfs_error error = HDFS_SUCCESS;
	ssize_t t_nacked;
	struct hdfs_unacked_packets *ua = &ps->unacked;

	*nacked = 0;
	*err_idx = -1;

	while (ua->ua_num > 0) {
		if (ps->proto >= HDFS_DATANODE_AP_2_0)
			error = _check_one_ack2(ps, &t_nacked, err_idx);
		else
			error = _check_one_ack(ps, &t_nacked, err_idx);
		if (hdfs_is_again(error)) {
			break;
		} else if (hdfs_is_error(error)) {
			// XXX Consider: Only set err_idx to 0 is error.her_kind == he_errno?
			// If it's an hdfs error, then setting err_idx is disingenuous
			if (*err_idx < 0)
				*err_idx = 0;
			goto out;
		}
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

out:
	return error;
}
