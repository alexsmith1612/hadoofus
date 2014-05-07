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

EXPORT_SYM const char *HDFS_DATANODE_ERR_NO_CRCS =
    "Server doesn't send CRCs, can't verify. Aborting read";

#define OP_WRITE 0x50
#define OP_READ 0x51

enum {
	STATUS_SUCCESS = 0,
	STATUS_ERROR,
	STATUS_ERROR_CHECKSUM,
	STATUS_ERROR_INVALID,
	STATUS_ERROR_EXISTS,
	STATUS_ERROR_ACCESS_TOKEN,
	STATUS_CHECKSUM_OK,
};

static char DN_CHECKSUM_OK[2] = {
	(char)(STATUS_CHECKSUM_OK >> 8),
	(char)(STATUS_CHECKSUM_OK & 0xff),
};
static char DN_ERROR_CHECKSUM[2] = {
	(char)(STATUS_ERROR_CHECKSUM >> 8),
	(char)(STATUS_ERROR_CHECKSUM & 0xff),
};

static const char *dn_error_msgs[] = {
	"Datanode write success, ???",
	"Datanode error, aborting write",
	"Datanode checksum error, aborting write",
	"Datanode error 'invalid', aborting write",
	"Datanode error 'exists', aborting write",
	"Datanode access token error, aborting write",
};

static const int MAX_UNACKED_PACKETS = 80/*same as apache*/,
	     CHUNK_SIZE = 512,
	     PACKET_SIZE = 64*1024;

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


static void		_compose_read_header(struct hdfs_heap_buf *, struct hdfs_datanode *,
			off_t offset, off_t len);
static void		_compose_write_header(struct hdfs_heap_buf *, struct hdfs_datanode *,
			bool crcs);
static const char *	_datanode_read(struct hdfs_datanode *, off_t bloff, off_t len,
			int fd, off_t fdoff, void *buf, bool verify);
static const char *	_datanode_write(struct hdfs_datanode *, const void *buf, int fd, off_t len,
			off_t offset, bool sendcrcs);
static const char *	_read_read_status(struct hdfs_datanode *, struct hdfs_heap_buf *,
			struct _read_state *);
static const char *	_read_write_status(struct hdfs_datanode *, struct hdfs_heap_buf *);
static const char *	_recv_packet(struct _packet_state *, struct _read_state *);
static const char *	_send_packet(struct _packet_state *);
static const char *	_verify_crcdata(void *crcdata, int32_t chunksize,
			int32_t crcdlen, int32_t dlen);
static const char *	_wait_ack(struct _packet_state *ps);

//
// high-level api
//

EXPORT_SYM struct hdfs_datanode *
hdfs_datanode_new(struct hdfs_object *located_block, const char *client,
	int proto, const char **error_out)
{
	const char *err = "LocatedBlock has zero datanodes";
	struct hdfs_datanode *d = malloc(sizeof *d);
	int32_t n;

	ASSERT(d);
	ASSERT(located_block);
	ASSERT(located_block->ob_type == H_LOCATED_BLOCK);

	hdfs_datanode_init(d,
	    located_block->ob_val._located_block._blockid,
	    located_block->ob_val._located_block._len,
	    located_block->ob_val._located_block._generation,
	    located_block->ob_val._located_block._offset,
	    client,
	    NULL/*token*/,
	    proto);

	// Try each datanode in the LocatedBlock until one successfully
	// connects
	n = located_block->ob_val._located_block._num_locs;
	for (int32_t i = 0; i < n; i++) {
		struct hdfs_object *di =
		    located_block->ob_val._located_block._locs[i];
		err = hdfs_datanode_connect(d,
		    di->ob_val._datanode_info._hostname,
		    di->ob_val._datanode_info._port);
		if (!err)
			return d;
	}

	hdfs_datanode_destroy(d);
	free(d);
	*error_out = err;
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
	ASSERT(proto == HDFS_DATANODE_AP_1_0 || proto == HDFS_DATANODE_CDH3);

	d->dn_lock = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
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
	_unlock(&d->dn_lock);

	memset(d, 0, sizeof *d);
}

EXPORT_SYM const char *
hdfs_datanode_connect(struct hdfs_datanode *d, const char *host, const char *port)
{
	const char *err;

	ASSERT(d);

	_lock(&d->dn_lock);

	ASSERT(d->dn_sock == -1);
	err = _connect(&d->dn_sock, host, port);

	_unlock(&d->dn_lock);

	return err;
}

// Datanode write operations

EXPORT_SYM const char *
hdfs_datanode_write(struct hdfs_datanode *d, const void *buf, size_t len, bool sendcrcs)
{
	ASSERT(buf);

	return _datanode_write(d, buf, -1, len, -1, sendcrcs);
}

EXPORT_SYM const char *
hdfs_datanode_write_file(struct hdfs_datanode *d, int fd, off_t len, off_t offset,
	bool sendcrcs)
{
	ASSERT(offset >= 0);
	ASSERT(fd >= 0);

	return _datanode_write(d, NULL, fd, len, offset, sendcrcs);
}

// Datanode read operations

EXPORT_SYM const char *
hdfs_datanode_read(struct hdfs_datanode *d, size_t off, size_t len, void *buf,
	bool verifycrc)
{
	ASSERT(buf);

	return _datanode_read(d, off, len, -1/*fd*/, -1/*fdoff*/, buf,
	    verifycrc);
}

EXPORT_SYM const char *
hdfs_datanode_read_file(struct hdfs_datanode *d, off_t bloff, off_t len,
	int fd, off_t fdoff, bool verifycrc)
{
	ASSERT(bloff >= 0);
	ASSERT(fdoff >= 0);
	ASSERT(fd >= 0);

	return _datanode_read(d, bloff, len, fd, fdoff, NULL/*buf*/, verifycrc);
}

static void
_compose_read_header(struct hdfs_heap_buf *h, struct hdfs_datanode *d,
	off_t offset, off_t len)
{
	_bappend_s16(h, d->dn_proto);

	_bappend_s8(h, OP_READ);

	_bappend_s64(h, d->dn_blkid);
	_bappend_s64(h, d->dn_gen);
	_bappend_s64(h, offset);
	_bappend_s64(h, len);
	_bappend_text(h, d->dn_client);
	hdfs_object_serialize(h, d->dn_token);
}

static void
_compose_write_header(struct hdfs_heap_buf *h, struct hdfs_datanode *d, bool crcs)
{
	_bappend_s16(h, d->dn_proto);

	_bappend_s8(h, OP_WRITE);

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

const char *
_datanode_read(struct hdfs_datanode *d, off_t bloff, off_t len,
	int fd, off_t fdoff, void *buf, bool verify)
{
	const char *err = NULL;
	struct hdfs_heap_buf header = { 0 },
			     recvbuf = { 0 };
	struct _packet_state pstate = { 0 };
	struct _read_state rinfo = { 0 };

	ASSERT(d);
	ASSERT(len > 0);

	_lock(&d->dn_lock);

	ASSERT(!d->dn_used);
	d->dn_used = true;

	_compose_read_header(&header, d, bloff, len);
	err = _write_all(d->dn_sock, header.buf, header.used);
	if (err)
		goto out;

	err = _read_read_status(d, &recvbuf, &rinfo);
	if (err)
		goto out;

	if (!rinfo.has_crcs && verify) {
		err = HDFS_DATANODE_ERR_NO_CRCS;
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
		err = _recv_packet(&pstate, &rinfo);
		if (err)
			goto out;
	}

	// tell server the read was fine
	err = _write_all(d->dn_sock, DN_CHECKSUM_OK, 2);
	if (err)
		goto out;

out:
	if (header.buf)
		free(header.buf);
	if (recvbuf.buf)
		free(recvbuf.buf);
	_unlock(&d->dn_lock);
	return err;
}

const char *
_datanode_write(struct hdfs_datanode *d, const void *buf, int fd, off_t len,
	off_t offset, bool sendcrcs)
{
	const char *err = NULL;
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
	err = _write_all(d->dn_sock, header.buf, header.used);
	if (err)
		goto out;

	err = _read_write_status(d, &recvbuf);
	if (err)
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
		err = _send_packet(&pstate);
		if (err)
			goto out;
	}

	// Drain remaining acks to ensure write succeeded
	while (pstate.unacked_packets > 0) {
		err = _wait_ack(&pstate);
		if (err)
			goto out;
	}

	// Write final zero-len packet; error here doesn't always matter. I
	// think some HDFS versions drop the connection at this point, so we
	// want to be lenient.
	_write_all(d->dn_sock, __DECONST(void*, &zero), sizeof zero);

out:
	if (header.buf)
		free(header.buf);
	if (recvbuf.buf)
		free(recvbuf.buf);
	_unlock(&d->dn_lock);
	return err;
}

static const char *
_read_read_status(struct hdfs_datanode *d, struct hdfs_heap_buf *h,
	struct _read_state *rs)
{
	const char *err = NULL;
	struct hdfs_heap_buf obuf = { 0 };
	int16_t status;
	int32_t chunk_size;
	int64_t server_offset;
	bool crcs;

	while (h->used < 2) {
		err = _read_to_hbuf(d->dn_sock, h);
		if (err)
			goto out;
	}

	obuf.buf = h->buf;
	obuf.size = h->used;

	status = _bslurp_s16(&obuf);
	ASSERT(obuf.used > 0);

	if (status != STATUS_SUCCESS) {
		err = "Server reported error with read request; aborting read";
		goto out;
	}

	while (h->used < 15) {
		err = _read_to_hbuf(d->dn_sock, h);
		if (err)
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
	return err;
}

static const char *
_read_write_status(struct hdfs_datanode *d, struct hdfs_heap_buf *h)
{
	const char *err = NULL;
	struct hdfs_heap_buf obuf = { 0 };
	int16_t status;
	char *statusmsg = NULL;
	size_t statussz;

	while (true) {
		err = _read_to_hbuf(d->dn_sock, h);
		if (err)
			goto out;

		obuf.buf = h->buf;
		obuf.used = 0;
		obuf.size = h->used;

		status = _bslurp_s16(&obuf);
		if (obuf.used >= 0)
			break;

		if (obuf.used == -2) {
			err = "Invalid protocol data";
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

		if (obuf.used == -2) {
			err = "Invalid protocol data";
			goto out;
		}

		err = _read_to_hbuf(d->dn_sock, h);
		if (err)
			goto out;
	}

	statussz += obuf.used;

	if (status != 0) {
		err = "Datanode responded with error; aborting write";
		fprintf(stderr, "libhadoofus: datanode error message: %s\n",
		    statusmsg);
		goto out;
	}

	ASSERT(strlen(statusmsg) == 0); /* no message on success */

	// Skip the recv buffer past the read objects
	h->used -= statussz;
	if (h->used)
		memmove(h->buf, h->buf + statussz, h->used);

out:
	if (statusmsg)
		free(statusmsg);
	return err;
}

static const char *
_recv_packet(struct _packet_state *ps, struct _read_state *rs)
{
	const char *err = NULL;
	const int ONEGB = 1024*1024*1024;
	struct hdfs_heap_buf *recvbuf = ps->recvbuf,
			     obuf = { 0 };
	int32_t plen, dlen, crcdlen;
	int64_t offset;
	bool lastpacket;

	int32_t c_begin, c_len;

	// slurp packet header
	while (recvbuf->used < 25) {
		err = _read_to_hbuf(ps->sock, recvbuf);
		if (err)
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

	crcdlen = plen - dlen - 4;
	if (plen < 0 || dlen < 0 || dlen > ONEGB || plen > ONEGB)
		err = "got bogus packet; aborting read";
	else if (crcdlen < 0)
		err = "got bogus packet size; aborting read";
	else if (rs->has_crcs && crcdlen != ((dlen + rs->chunk_size - 1) / rs->chunk_size) * 4)
		err = "got bogus packet crc data; aborting read";
	else if (!rs->has_crcs && crcdlen > 0)
		err = "didn't expect crc data but got some anyway; aborting read";

	if (err)
		goto out;

	while (recvbuf->used < 25 + crcdlen + dlen) {
		err = _read_to_hbuf(ps->sock, recvbuf);
		if (err)
			goto out;
	}

	if (crcdlen > 0) {
		err = _verify_crcdata(recvbuf->buf + 25, rs->chunk_size, crcdlen, dlen);
		if (err) {
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
		err = "Server started read before requested offset; aborting read";
		goto out;
	}
	c_len = _min(dlen - c_begin, ps->remains);

	// Copy the packet data out to the user's buf or to file:
	if (ps->buf) {
		memcpy(ps->buf, recvbuf->buf + 25 + crcdlen + c_begin, c_len);
	} else {
		int written = 0, rc;
		while (written < c_len) {
			rc = pwrite(ps->fd,
			    recvbuf->buf + 25 + crcdlen + c_begin + written,
			    c_len - written,
			    ps->fdoffset + written);
			if (rc == -1)
				err = strerror(errno);
			else if (rc == 0)
				err = "EOF writing out to file; aborting read";

			if (err)
				goto out;
			written += rc;
		}
	}

	ps->remains -= c_len;
	ps->fdoffset += c_len;
	if (ps->buf)
		ps->buf = (char*)ps->buf + c_len;

	if (ps->remains > 0 && lastpacket) {
		err = "Got last packet before read completed; aborting read";
		goto out;
	}

	// skip recvbuf over this packet. this is probably excessive memcopying
	// especially if/when multiple packets queue up. TODO: something
	// smarter.
	recvbuf->used -= (25 + crcdlen + dlen);
	memmove(recvbuf->buf, recvbuf->buf + 25 + crcdlen + dlen, recvbuf->used);

out:
	return err;
}

static const char *
_send_packet(struct _packet_state *ps)
{

	const char *err = NULL;
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
		err = _wait_ack(ps);
		if (err)
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
				err = strerror(errno);
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
		err = _pread_all(ps->fd, data, tosend, ps->fdoffset);
		if (err)
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
	_bappend_s64(&phdr, ps->offset/*from beginning of block*/);
	_bappend_s64(&phdr, ps->seqno);
	_bappend_s8(&phdr, last);
	_bappend_s32(&phdr, tosend);

	ios[0].iov_base = phdr.buf;
	ios[0].iov_len = phdr.used;

	if (data) {
		ios[1].iov_base = crcdata;
		ios[1].iov_len = 4*crclen;
		ios[2].iov_base = data;
		ios[2].iov_len = tosend;

		err = _writev_all(ps->sock, ios, 3);
		if (err)
			goto out;
	} else {
#if defined(__linux__)
		_setsockopt(ps->sock, IPPROTO_TCP, TCP_CORK, 1);

		err = _writev_all(ps->sock, ios, 1);
		if (err)
			goto out;
		err = _sendfile_all(ps->sock, ps->fd, ps->fdoffset, tosend);
		if (err)
			goto out;

		_setsockopt(ps->sock, IPPROTO_TCP, TCP_CORK, 0);
#elif defined(__FreeBSD__)
		err = _sendfile_all_bsd(ps->sock, ps->fd, ps->fdoffset, tosend,
		    ios, 1);
		if (err)
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
	return err;
}

static const char *
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
			return "Got bad CRC during read; aborting";
	}

	return NULL;
}

static const char *
_wait_ack(struct _packet_state *ps)
{
	const char *err = NULL;
	struct hdfs_heap_buf obuf = { 0 };

	int64_t seqno;
	int16_t nacks, ack = STATUS_ERROR;

	int acksz = 0;

	ASSERT(ps->proto == HDFS_DATANODE_AP_1_0 ||
	    ps->proto == HDFS_DATANODE_CDH3);

	if (ps->proto == HDFS_DATANODE_AP_1_0)
		acksz = 8 + 2 + 2;
	else if (ps->proto == HDFS_DATANODE_CDH3)
		acksz = 8 + 2;

	while (ps->recvbuf->used < acksz) {
		err = _read_to_hbuf(ps->sock, ps->recvbuf);
		if (err)
			goto out;
	}

	obuf.buf = ps->recvbuf->buf;
	obuf.used = 0;
	obuf.size = ps->recvbuf->used;

	seqno = _bslurp_s64(&obuf);
	ASSERT(obuf.used >= 0);

	if (seqno != ps->first_unacked) {
		err = "Got unexpected ACK";
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
			err = "Got bogus number of ACKs; expected 1";
			goto out;
		}

		ack = _bslurp_s16(&obuf);
		ASSERT(obuf.used >= 0);
	} else if (ps->proto == HDFS_DATANODE_CDH3) {
		ack = _bslurp_s16(&obuf);
		ASSERT(obuf.used >= 0);
	}

	if (ack != STATUS_SUCCESS) {
		if (ack < 0 || ack > (int)nelem(dn_error_msgs)) {
			err = "Bogus ack number, aborting write";
			fprintf(stderr, "libhadoofus: Got bogus ack status %"
			    PRIi16 ", aborting write", ack);
			goto out;
		}

		err = dn_error_msgs[ack];
		goto out;
	}

	ps->unacked_packets--;

	// Skip the recv buffer past the ack
	ps->recvbuf->used -= acksz;
	if (ps->recvbuf->used)
		memmove(ps->recvbuf->buf, ps->recvbuf->buf + acksz, ps->recvbuf->used);

out:
	return err;
}
