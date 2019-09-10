#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <hadoofus/lowlevel.h>

#include "net.h"
#include "objects-internal.h"
#include "pthread_wrappers.h"
#include "rpc2-internal.h"
#include "util.h"

static void *			_namenode_recv_worker(void *);

static short			_namenode_auth_sasl_get_events(struct hdfs_namenode *n);
static struct hdfs_error	_namenode_auth_simple(struct hdfs_namenode *n);
static struct hdfs_error	_namenode_auth_kerb(struct hdfs_namenode *n);
static struct hdfs_error	_namenode_auth_sasl_loop(struct hdfs_namenode *n);
static struct hdfs_error	_namenode_auth_sasl_send_resp(struct hdfs_namenode *n);

static void	_namenode_pending_insert_unlocked(struct hdfs_namenode *n,
		int64_t msgno, struct hdfs_rpc_response_future *future,
		hdfs_object_slurper slurper);

static struct hdfs_rpc_response_future *
		_namenode_pending_remove(struct hdfs_namenode *n, int64_t msgno);

static void	_future_complete(struct hdfs_rpc_response_future *future,
		struct hdfs_object *obj);

// SASL helpers
static struct hdfs_error	_conn_try_desasl(struct hdfs_namenode *n);
static int			_getssf(sasl_conn_t *);
static void			_sasl_interacts(sasl_interact_t *);

EXPORT_SYM struct hdfs_namenode *
hdfs_namenode_allocate(void)
{
	struct hdfs_namenode *res;

	res = malloc(sizeof(*res));
	ASSERT(res);
	memset(res, 0, sizeof(*res));
	return res;
}

// XXX consider adding host and port as arguments and strdup() into the nn struct in order
// to simplify non-blocking namenode connects into a single function call (instead
// of _init() and _finalize()) and handle full connection in _connauth_nb().
//
// XXX consider adding username and real_user arguments and combining
// the auth_nb_init() functions into the namenode_init() function
EXPORT_SYM void
hdfs_namenode_init(struct hdfs_namenode *n, enum hdfs_kerb kerb_prefs)
{
	return hdfs_namenode_init_ver(n, kerb_prefs, _HDFS_NN_vLATEST); // XXX consider version
}

EXPORT_SYM void
hdfs_namenode_init_ver(struct hdfs_namenode *n, enum hdfs_kerb kerb_prefs,
	enum hdfs_namenode_proto ver)
{
	ASSERT(n);
	// TODO argument assertions?
	ASSERT(n->nn_state == HDFS_NN_ST_ZERO); // XXX reconsider --- this would require users to initialize nn to all zeros prior to this

	n->nn_state = HDFS_NN_ST_INITED;
	// Note: nn_sasl_state gets initialized in _namenode_auth_kerb() prior to being used

	_mtx_init(&n->nn_lock);
	_mtx_init(&n->nn_sendlock);
	n->nn_sock = -1;
	n->nn_dead = false;
	n->nn_msgno = 0;
	n->nn_pending = NULL;
	n->nn_pending_len = 0;
	n->nn_recver_started = false;

	n->nn_kerb = kerb_prefs;
	n->nn_sasl_ctx = NULL;
	n->nn_sasl_ssf = 0;
	n->nn_sasl_interactions = NULL;
	n->nn_sasl_out = NULL;
	n->nn_sasl_outlen = 0;

	memset(&n->nn_recvbuf, 0, sizeof(n->nn_recvbuf));
	memset(&n->nn_objbuf, 0, sizeof(n->nn_objbuf));
	memset(&n->nn_sendbuf, 0, sizeof(n->nn_sendbuf));

	n->nn_proto = ver;
	if (n->nn_proto >= HDFS_NN_v2_2) {
		ssize_t rd;
		int fd;

		fd = open("/dev/urandom", O_RDONLY);
		ASSERT(fd >= 0);

		rd = read(fd, n->nn_client_id, sizeof(n->nn_client_id));
		ASSERT(rd == sizeof(n->nn_client_id));

		close(fd);
	} else {
		memset(n->nn_client_id, 0, sizeof(n->nn_client_id));
	}

	n->nn_error = 0;

	memset(&n->nn_cctx, 0, sizeof(n->nn_cctx));
	n->nn_authhdr = NULL;
}

// XXX reconsider name
EXPORT_SYM struct hdfs_error
hdfs_namenode_connauth_nb(struct hdfs_namenode *n)
{
	struct hdfs_error error = HDFS_SUCCESS;

	// XXX locking for state checks? Consider interaction with called functions

	ASSERT(n);
	ASSERT(n->nn_state >= HDFS_NN_ST_CONNPENDING && n->nn_state < HDFS_NN_ST_RPC);
	ASSERT(n->nn_authhdr);

	// all state transitions handled in called functions
	switch (n->nn_state) {
	case HDFS_NN_ST_CONNPENDING:
		error = hdfs_namenode_connect_finalize(n);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
		// fall through
	case HDFS_NN_ST_CONNECTED:
	case HDFS_NN_ST_AUTHPENDING:
		error = hdfs_namenode_authenticate_nb(n);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
		break;

	case HDFS_NN_ST_ZERO:
	case HDFS_NN_ST_INITED:
		// XXX handle connect here if we move host and port to hdfs_namenode_init()
	case HDFS_NN_ST_RPC:
		// XXX consider returning HDFS_SUCCESS for ST_RPC, or call
		// hdfs_namenode_continue() if _hbuf_readlen(&n->nn_sendbuf) > 0
		// (or unconditionally?)
	case HDFS_NN_ST_ERROR:
	default:
		ASSERT(false);
	}

out:
	return error;
}

EXPORT_SYM struct hdfs_error
hdfs_namenode_connect(struct hdfs_namenode *n, const char *host, const char *port)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct pollfd pfd;

	// XXX consider locking scheme

	error = hdfs_namenode_connect_init(n, host, port, false);
	while (hdfs_is_again(error)) {
		error = hdfs_namenode_get_eventfd(n, &pfd.fd, &pfd.events);
		if (hdfs_is_error(error))
			goto out;
		poll(&pfd, 1, -1);
		// XXX check that poll returns 1 (EINTR?) and/or check revents?
		error = hdfs_namenode_connect_finalize(n);
	}

out:
	return error;
}

EXPORT_SYM struct hdfs_error
hdfs_namenode_connect_init(struct hdfs_namenode *n, const char *host, const char *port,
	bool numerichost)
{
	struct hdfs_error error = HDFS_SUCCESS;

	ASSERT(n);
	ASSERT(host);
	ASSERT(port);

	_lock(&n->nn_lock);
	ASSERT(n->nn_state == HDFS_NN_ST_INITED);
	ASSERT(n->nn_sock == -1);

	// XXX the second argument of sasl_client_new() is described as
	// serverFQDN, which sounds like it may not like an IP address. look into
	// this and see if we need an optional serverFQDN argument that can be
	// used for sasl while keeping the host an IP to avoid dns lookups by
	// getaddrinfo()
	if (n->nn_kerb == HDFS_TRY_KERB || n->nn_kerb == HDFS_REQUIRE_KERB) {
		int r = sasl_client_new("hdfs", host, NULL/*localip*/,
		    NULL/*remoteip*/, NULL/*CBs*/, 0/*sec flags*/, &n->nn_sasl_ctx);
		if (r != SASL_OK) {
			error = error_from_sasl(r);
			n->nn_state = HDFS_NN_ST_ERROR;
			goto out;
		}
	}

	error = _connect_init(&n->nn_sock, host, port, &n->nn_cctx, numerichost);
	if (!hdfs_is_error(error))
		n->nn_state = HDFS_NN_ST_CONNECTED;
	else if (hdfs_is_again(error))
		n->nn_state = HDFS_NN_ST_CONNPENDING;
	else
		n->nn_state = HDFS_NN_ST_ERROR;

out:
	_unlock(&n->nn_lock);
	return error;
}

EXPORT_SYM struct hdfs_error
hdfs_namenode_connect_finalize(struct hdfs_namenode *n)
{
	struct hdfs_error error = HDFS_SUCCESS;

	ASSERT(n);

	_lock(&n->nn_lock);
	ASSERT(n->nn_sock >= 0);
	if (n->nn_state == HDFS_NN_ST_CONNECTED)
		goto out;
	ASSERT(n->nn_state == HDFS_NN_ST_CONNPENDING);

	error = _connect_finalize(&n->nn_sock, &n->nn_cctx);
	if (!hdfs_is_error(error))
		n->nn_state = HDFS_NN_ST_CONNECTED;
	else if (!hdfs_is_again(error))
		n->nn_state = HDFS_NN_ST_ERROR;

out:
	_unlock(&n->nn_lock);
	return error;
}

EXPORT_SYM struct hdfs_error
hdfs_namenode_get_eventfd(struct hdfs_namenode *n, int *fd, short *events)
{
	ASSERT(n);
	ASSERT(fd);
	ASSERT(events);

	_lock(&n->nn_lock);
	ASSERT(n->nn_sock >= 0);
	ASSERT(n->nn_state >= HDFS_NN_ST_CONNPENDING);

	*fd = n->nn_sock;
	*events = 0;

	switch (n->nn_state) {
	case HDFS_NN_ST_CONNPENDING:
		*events |= POLLOUT;
		break;

	case HDFS_NN_ST_AUTHPENDING:
		switch (n->nn_kerb) {
		case HDFS_NO_KERB:
			*events |= POLLOUT;
			break;

		case HDFS_TRY_KERB:
		case HDFS_REQUIRE_KERB:
			*events |= _namenode_auth_sasl_get_events(n);
			break;

		default:
			ASSERT(false);
		}
		break;

	case HDFS_NN_ST_RPC:
		// XXX revisit this locking pattern
		_lock(&n->nn_sendlock);
		if (_hbuf_readlen(&n->nn_sendbuf) > 0)
			*events |= POLLOUT;
		_unlock(&n->nn_sendlock);
		// XXX if we remove worker thread set POLLIN if there are pending RPCs
		break;

	case HDFS_NN_ST_ZERO:
	case HDFS_NN_ST_INITED:
	case HDFS_NN_ST_CONNECTED:
	case HDFS_NN_ST_ERROR:
	default:
		ASSERT(false);
	}

	_unlock(&n->nn_lock);
	return HDFS_SUCCESS;
}

// XXX Assumes that nn_lock is held
static short
_namenode_auth_sasl_get_events(struct hdfs_namenode *n)
{
	switch (n->nn_sasl_state) {
	case HDFS_NN_SASL_ST_SEND:
	case HDFS_NN_SASL_ST_FINISHED:
		return POLLOUT;

	case HDFS_NN_SASL_ST_RECV:
		return POLLIN;

	case HDFS_NN_SASL_ST_ERROR:
	default:
		ASSERT(false);
	}
}

EXPORT_SYM void
hdfs_namenode_auth_nb_init(struct hdfs_namenode *n, const char *username)
{
	return hdfs_namenode_auth_nb_init_full(n, username, NULL);
}

EXPORT_SYM void
hdfs_namenode_auth_nb_init_full(struct hdfs_namenode *n, const char *username,
	const char *real_user)
{
	_lock(&n->nn_lock);
	ASSERT(n->nn_proto == HDFS_NN_v1 || n->nn_proto == HDFS_NN_v2 || n->nn_proto == HDFS_NN_v2_2);
	ASSERT(n->nn_state >= HDFS_NN_ST_INITED && n->nn_state < HDFS_NN_ST_AUTHPENDING);
	ASSERT(!n->nn_authhdr);

	// XXX why is this forced to HDFS_NO_KERB for v1?
	n->nn_authhdr = hdfs_authheader_new_ext(n->nn_proto, username, real_user,
	    n->nn_proto == HDFS_NN_v1 ? HDFS_NO_KERB : n->nn_kerb);

	if (n->nn_proto == HDFS_NN_v2_2)
		_authheader_set_clientid(n->nn_authhdr, n->nn_client_id);

	_unlock(&n->nn_lock);
}

EXPORT_SYM struct hdfs_error
hdfs_namenode_authenticate(struct hdfs_namenode *n, const char *username)
{

	return hdfs_namenode_authenticate_full(n, username, NULL);
}

EXPORT_SYM struct hdfs_error
hdfs_namenode_authenticate_full(struct hdfs_namenode *n, const char *username,
	const char *real_user)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct pollfd pfd;

	// XXX consider locking scheme

	hdfs_namenode_auth_nb_init_full(n, username, real_user);

	while (true) {
		error = hdfs_namenode_authenticate_nb(n);
		if (!hdfs_is_again(error))
			break;
		error = hdfs_namenode_get_eventfd(n, &pfd.fd, &pfd.events);
		if (hdfs_is_error(error))
			break;
		poll(&pfd, 1, -1);
		// XXX check that poll returns 1 (EINTR?) and/or check revents?
	}

	return error;
}

// TODO move this below hdfs_namenode_authenticate_nb()
// TODO change this to take a heapbuf and return void
// Returns the number of bytes used in the buffer
static size_t
_namenode_compose_auth_preamble(char *buf, size_t buflen,
	enum hdfs_namenode_proto proto, enum hdfs_kerb kerb)
{
	size_t preamble_len = 0;

	ASSERT(buf);
	ASSERT(buflen >= 7);
	ASSERT(kerb == HDFS_NO_KERB || kerb == HDFS_TRY_KERB || kerb == HDFS_REQUIRE_KERB);

	switch (proto) {
	case HDFS_NN_v1:
		sprintf(buf, "hrpc\x04%c",
		    (kerb == HDFS_NO_KERB)? 0x50 : 0x51);
		preamble_len = 6;
		break;

	case HDFS_NN_v2:
		/* HDFSv2 has used both version 7 (2.0.0-2.0.2) and 8 (2.0.3+). */
		sprintf(buf, "hrpc%c%c", 8 /* XXX Configurable? */,
		    (kerb == HDFS_NO_KERB)? 0x50 : 0x51);
		/* There is a zero at the end: */
		preamble_len = 7;
		break;

	case HDFS_NN_v2_2:
		memcpy(buf, "hrpc\x09", 5);
		buf[5] = 0;
		buf[6] = (kerb == HDFS_NO_KERB)? 0 : -33;
		preamble_len = 7;
		break;

	default:
		ASSERT(false);
	}

	return preamble_len;
}

// XXX consider name
EXPORT_SYM struct hdfs_error
hdfs_namenode_authenticate_nb(struct hdfs_namenode *n)
{
	struct hdfs_error error = HDFS_SUCCESS;

	ASSERT(n);

	_lock(&n->nn_lock);
	ASSERT(n->nn_state >= HDFS_NN_ST_CONNECTED && n->nn_state < HDFS_NN_ST_RPC);
	ASSERT(n->nn_sock != -1);
	ASSERT(n->nn_authhdr);

	switch (n->nn_kerb) {
	case HDFS_REQUIRE_KERB:
	case HDFS_TRY_KERB:
		error = _namenode_auth_kerb(n);
		if (!hdfs_is_error(error)) // success
			break;
		else if (n->nn_kerb != HDFS_TRY_KERB || error.her_kind != he_hdfserr
		    || error.her_num != HDFS_ERR_KERBEROS_DOWNGRADE)
			goto out;
		// HDFS_TRY_KERB and we got downgraded
		// XXX TODO reset send and recv bufs?
		n->nn_kerb = HDFS_NO_KERB;
		n->nn_state = HDFS_NN_ST_CONNECTED;
		// fall through
	case HDFS_NO_KERB:
		error = _namenode_auth_simple(n);
		if (hdfs_is_error(error)) // includes HDFS_AGAIN
			goto out;
		break;

	default:
		ASSERT(false);
	}

out:
	_unlock(&n->nn_lock);
	return error;
}

// XXX Assumes that nn_lock is held
static struct hdfs_error
_namenode_auth_simple(struct hdfs_namenode *n)
{
	struct hdfs_error error = HDFS_SUCCESS;
	size_t preamble_len;
	ssize_t wlen;

	ASSERT(n);
	ASSERT(n->nn_state >= HDFS_NN_ST_CONNECTED && n->nn_state < HDFS_NN_ST_RPC);
	ASSERT(n->nn_authhdr);

	switch (n->nn_state) {
	case HDFS_NN_ST_CONNECTED:
		ASSERT(_hbuf_readlen(&n->nn_recvbuf) == 0);
		ASSERT(_hbuf_readlen(&n->nn_sendbuf) == 0);
		// reserve enough space for the preamble
		_hbuf_reserve(&n->nn_sendbuf, 12);
		// add preamble to buf
		preamble_len = _namenode_compose_auth_preamble(
		    _hbuf_writeptr(&n->nn_sendbuf), 12, n->nn_proto, n->nn_kerb);
		_hbuf_append(&n->nn_sendbuf, preamble_len);
		// Serialize the connection header object (I am speaking ClientProtocol
		// and this is my username)
		hdfs_object_serialize(&n->nn_sendbuf, n->nn_authhdr);
		n->nn_state = HDFS_NN_ST_AUTHPENDING;
		// fall through
	case HDFS_NN_ST_AUTHPENDING:
		error = _write(n->nn_sock, _hbuf_readptr(&n->nn_sendbuf), _hbuf_readlen(&n->nn_sendbuf), &wlen);
		if (wlen < 0) {
			n->nn_state = HDFS_NN_ST_ERROR;
			goto out;
		}
		_hbuf_consume(&n->nn_sendbuf, wlen);
		if (hdfs_is_again(error))
			goto out;
		// Fully sent auth header
		ASSERT(_hbuf_readlen(&n->nn_sendbuf) == 0);
		hdfs_object_free(n->nn_authhdr);
		n->nn_authhdr = NULL;
		n->nn_state = HDFS_NN_ST_RPC;
		break;

	case HDFS_NN_ST_ZERO:
	case HDFS_NN_ST_INITED:
	case HDFS_NN_ST_CONNPENDING:
	case HDFS_NN_ST_RPC:
	case HDFS_NN_ST_ERROR:
	default:
		ASSERT(false);
	}

out:
	return error;
}

// XXX Assumes that nn_lock is held
static struct hdfs_error
_namenode_auth_kerb(struct hdfs_namenode *n)
{
	/*
	 * XXX This is probably totally wrong for HDFSv2+. They start
	 * using protobufs at this point to wrap the SASL packets.
	 *
	 * To be fair, it's probably broken for HDFSv1 too :). I need
	 * to find a kerberized HDFS to test against.
	 */
	struct hdfs_error error = HDFS_SUCCESS;
	int r;
	const char *mechusing;
	size_t preamble_len;

	// XXX TODO review this entire function

	ASSERT(n);
	ASSERT(n->nn_state >= HDFS_NN_ST_CONNECTED && n->nn_state < HDFS_NN_ST_RPC);
	ASSERT(n->nn_sasl_ctx);
	ASSERT(n->nn_authhdr);

	switch (n->nn_state) {
	case HDFS_NN_ST_CONNECTED:
		ASSERT(_hbuf_readlen(&n->nn_recvbuf) == 0);
		ASSERT(_hbuf_readlen(&n->nn_sendbuf) == 0);

		do {
			r = sasl_client_start(n->nn_sasl_ctx, "GSSAPI",
			    &n->nn_sasl_interactions, &n->nn_sasl_out,
			    &n->nn_sasl_outlen, &mechusing);

			if (r == SASL_INTERACT)
				_sasl_interacts(n->nn_sasl_interactions);
		} while (r == SASL_INTERACT);

		if (r != SASL_CONTINUE) {
			n->nn_state = HDFS_NN_ST_ERROR;
			// XXX consider also setting HDFS_NN_SASL_ST_ERROR
			error = error_from_sasl(r);
			goto out;
		}

		// reserve enough space for the preamble and the encoded length
		_hbuf_reserve(&n->nn_sendbuf, 12/*preamble*/ + 4/*sasl_outlen*/);
		// add preamble to buf
		preamble_len = _namenode_compose_auth_preamble(
		    _hbuf_writeptr(&n->nn_sendbuf), 12, n->nn_proto, n->nn_kerb);
		_hbuf_append(&n->nn_sendbuf, preamble_len);
		// add sasl_outlen to buf
		_be32enc(_hbuf_writeptr(&n->nn_sendbuf), n->nn_sasl_outlen);
		_hbuf_append(&n->nn_sendbuf, 4);
		n->nn_sasl_state = HDFS_NN_SASL_ST_SEND;
		n->nn_state = HDFS_NN_ST_AUTHPENDING;
		// fall through
	case HDFS_NN_ST_AUTHPENDING:
		error = _namenode_auth_sasl_loop(n);
		if (hdfs_is_again(error)) {
			goto out;
		} else if (hdfs_is_error(error)) {
			n->nn_state = HDFS_NN_ST_ERROR;
			goto out;
		}
		ASSERT(_hbuf_readlen(&n->nn_sendbuf) == 0);
		hdfs_object_free(n->nn_authhdr);
		n->nn_authhdr = NULL;
		n->nn_state = HDFS_NN_ST_RPC;
		break;

	case HDFS_NN_ST_ZERO:
	case HDFS_NN_ST_INITED:
	case HDFS_NN_ST_CONNPENDING:
	case HDFS_NN_ST_RPC:
	case HDFS_NN_ST_ERROR:
	default:
		ASSERT(false);
	}

out:
	return error;
}

// XXX Assumes that nn_lock is held
static struct hdfs_error
_namenode_auth_sasl_loop(struct hdfs_namenode *n)
{
	struct hdfs_error error = HDFS_SUCCESS;
	int r, pre_rlen, post_rlen, authhdr_len, authhdr_offset;
	uint32_t token_len;
	const uint32_t SWITCH_TO_SIMPLE_AUTH = (uint32_t)-1;
	uint8_t zero[4] = { 0 };
	ssize_t wlen;

	// XXX TODO review this entire function

	ASSERT(n);
	ASSERT(n->nn_state == HDFS_NN_ST_AUTHPENDING);

	do {
		switch (n->nn_sasl_state) {
		case HDFS_NN_SASL_ST_SEND:
			error = _namenode_auth_sasl_send_resp(n);
			if (hdfs_is_again(error)) {
				goto out;
			} else if (hdfs_is_error(error)) {
				n->nn_sasl_state = HDFS_NN_SASL_ST_ERROR;
				goto out;
			}
			ASSERT(_hbuf_readlen(&n->nn_sendbuf) == 0 && n->nn_sasl_outlen == 0);
			n->nn_sasl_state = HDFS_NN_SASL_ST_RECV;
			// fall through
		case HDFS_NN_SASL_ST_RECV:
			// We don't skip past any input data until we have successfully
			// received the entire message in order to simplify stateful
			// parsing

			// 1. read success / error status
			while (_hbuf_readlen(&n->nn_recvbuf) < 4) {
				error =_read_to_hbuf(n->nn_sock, &n->nn_recvbuf);
				if (hdfs_is_again(error)) {
					goto out;
				} else if (hdfs_is_error(error)) {
					n->nn_sasl_state = HDFS_NN_SASL_ST_ERROR;
					goto out;
				}
			}
			if (memcmp(_hbuf_readptr(&n->nn_recvbuf), zero, 4)) {
				// error. exception will be next on the wire, but let's skip it.
				n->nn_sasl_state = HDFS_NN_SASL_ST_ERROR;
				error = error_from_hdfs(HDFS_ERR_KERBEROS_NEGOTIATION);
				goto out;
			}

			// 2. read token len
			while (_hbuf_readlen(&n->nn_recvbuf) < 4/*status*/ + 4/*token len*/) {
				error =_read_to_hbuf(n->nn_sock, &n->nn_recvbuf);
				if (hdfs_is_again(error)) {
					goto out;
				} else if (hdfs_is_error(error)) {
					n->nn_sasl_state = HDFS_NN_SASL_ST_ERROR;
					goto out;
				}
			}
			token_len = _be32dec(_hbuf_readptr(&n->nn_recvbuf) + 4/*skip status*/);
			if (token_len == SWITCH_TO_SIMPLE_AUTH) {
				error = error_from_hdfs(HDFS_ERR_KERBEROS_DOWNGRADE);
				goto out;
			}

			// 3. read token
			while ((uint32_t)_hbuf_readlen(&n->nn_recvbuf) < 4/*status*/ + 4/*token len*/ + token_len) {
				error =_read_to_hbuf(n->nn_sock, &n->nn_recvbuf);
				if (hdfs_is_again(error)) {
					goto out;
				} else if (hdfs_is_error(error)) {
					n->nn_sasl_state = HDFS_NN_SASL_ST_ERROR;
					goto out;
				}
			}
			// At this point we cannot get HDFS_AGAIN for this token,
			// so skip past the status and token len (the token itself
			// still has to be passed to sasl)
			_hbuf_consume(&n->nn_recvbuf, 4/*status*/ + 4/*token len*/);

			// 4. Proceed through SASL
			n->nn_sasl_out = NULL;
			n->nn_sasl_outlen = 0;
			r = sasl_client_step(n->nn_sasl_ctx, _hbuf_readptr(&n->nn_recvbuf),
			    token_len, &n->nn_sasl_interactions, &n->nn_sasl_out, &n->nn_sasl_outlen);

			// XXX TODO review the interactions logic here, it seems like we may
			// need to have a tight do {} while (r == SASL_INTERACT) around
			// sasl_client_step() and this r == SASL_INTERACT block. Currently we
			// would start looking for another status/token from the server before
			// calling sasl_client_step() again after receiving SASL_INTERACT,
			// which seems incorrect. If we do add a tight loop here, we may be
			// able to remove the sasl_interact_t pointer from struct hdfs_namenode
			// and have it be local to this function
			if (r == SASL_INTERACT)
				_sasl_interacts(n->nn_sasl_interactions);

			// XXX TODO remove SASL_INTERACT condition here if tight do-while() added
			// around sasl_client_step() above
			if (r != SASL_INTERACT && r != SASL_CONTINUE && r != SASL_OK) {
				n->nn_sasl_state = HDFS_NN_SASL_ST_ERROR;
				error = error_from_sasl(r);
				goto out;
			}

			// Skip past the token
			_hbuf_consume(&n->nn_recvbuf, token_len);

			if (r == SASL_CONTINUE || (r == SASL_OK && n->nn_sasl_out != NULL)) {
				// encode sasl_outlen into sendbuf
				_hbuf_reserve(&n->nn_sendbuf, 4);
				_be32enc(_hbuf_writeptr(&n->nn_sendbuf), n->nn_sasl_outlen);
				_hbuf_append(&n->nn_sendbuf, 4);
			}

			// XXX TODO remove SASL_INTERACT condition here if tight do-while() added
			// around sasl_client_step() above
			if (r == SASL_INTERACT || r == SASL_CONTINUE) {
				n->nn_sasl_state = HDFS_NN_SASL_ST_SEND;
				break; // continue with the loop
			}

			ASSERT(r == SASL_OK);
			// sasl connection established
			n->nn_sasl_ssf = _getssf(n->nn_sasl_ctx);

			// for state simplicity, copy the final nn_sasl_out data into nn_sendbuf
			if (n->nn_sasl_out && n->nn_sasl_outlen > 0) {
				_hbuf_reserve(&n->nn_sendbuf, n->nn_sasl_outlen);
				memcpy(_hbuf_writeptr(&n->nn_sendbuf), n->nn_sasl_out, n->nn_sasl_outlen);
				_hbuf_append(&n->nn_sendbuf, n->nn_sasl_outlen);
			}

			// Serialize the connection header object (I am speaking ClientProtocol
			// and this is my username)

			// XXX HACK determine the offset of the beginning of this serialized authhdr
			// for sasl encoding. Since hdfs_object_serialize() can lead to
			// _hbuf_reserve() calls which may lead to memmove() calls, we cannot simply
			// stash nn_sendbuf.used prior to serializing the authhdr, as that offset
			// may no longer be valid after serialization. As a workaround, compare the
			// _hbuf_readlen() values before and after serialization, and subtract the
			// difference from nn_sendbuf.used.
			pre_rlen = _hbuf_readlen(&n->nn_sendbuf);
			hdfs_object_serialize(&n->nn_sendbuf, n->nn_authhdr);
			post_rlen = _hbuf_readlen(&n->nn_sendbuf);
			authhdr_len = post_rlen - pre_rlen;
			authhdr_offset = n->nn_sendbuf.used - authhdr_len;
			if (n->nn_sasl_ssf > 0) {
				error = _sasl_encode_at_offset(n->nn_sasl_ctx, &n->nn_sendbuf, authhdr_offset);
				if (hdfs_is_error(error)) {
					n->nn_sasl_state = HDFS_NN_SASL_ST_ERROR;
					goto out;
				}
			}

			n->nn_sasl_state = HDFS_NN_SASL_ST_FINISHED;
			// fall through
		case HDFS_NN_SASL_ST_FINISHED:
			error = _write(n->nn_sock, _hbuf_readptr(&n->nn_sendbuf), _hbuf_readlen(&n->nn_sendbuf), &wlen);
			if (wlen < 0) {
				n->nn_sasl_state = HDFS_NN_SASL_ST_ERROR;
				goto out;
			}
			_hbuf_consume(&n->nn_sendbuf, wlen);
			if (hdfs_is_again(error))
				goto out;
			// complete write;
			break;

		case HDFS_NN_SASL_ST_ERROR:
		default:
			ASSERT(false);
		}
	} while (n->nn_sasl_state != HDFS_NN_SASL_ST_FINISHED);

out:
	return error;
}

// XXX Assumes nn_lock is held
static struct hdfs_error
_namenode_auth_sasl_send_resp(struct hdfs_namenode *n)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct iovec iov[2], *iovp = iov;
	int iovcnt = 1;
	ssize_t wlen, wlen_buf, wlen_sasl;

	ASSERT(_hbuf_readlen(&n->nn_sendbuf) > 0 || n->nn_sasl_outlen > 0);

	iov[0].iov_base = _hbuf_readptr(&n->nn_sendbuf);
	iov[0].iov_len = _hbuf_readlen(&n->nn_sendbuf);
	iov[1].iov_base = __DECONST(void *, n->nn_sasl_out);
	iov[1].iov_len = n->nn_sasl_outlen;

	if (_hbuf_readlen(&n->nn_sendbuf) > 0 && n->nn_sasl_outlen > 0)
		iovcnt = 2;
	else if (_hbuf_readlen(&n->nn_sendbuf) <= 0)
		iovp++;
	// the only other possible case is sasl_outlen <= 0 and
	// nn_sendbuf len > 0, with iovp = iov and iovcnt = 1 as is default

	error = _writev(n->nn_sock, iovp, iovcnt, &wlen);
	if (wlen < 0)
		goto out;

	wlen_buf = _min(wlen, _hbuf_readlen(&n->nn_sendbuf));
	wlen_sasl = wlen - wlen_buf;

	_hbuf_consume(&n->nn_sendbuf, wlen_buf);
	n->nn_sasl_out += wlen_sasl;
	n->nn_sasl_outlen -= wlen_sasl;

out:
	return error;
}

EXPORT_SYM int64_t
hdfs_namenode_get_msgno(struct hdfs_namenode *n)
{
	int64_t res;

	_lock(&n->nn_lock);
	res = n->nn_msgno;
	_unlock(&n->nn_lock);

	return res;
}

EXPORT_SYM struct hdfs_error
hdfs_namenode_invoke(struct hdfs_namenode *n, struct hdfs_object *rpc,
	struct hdfs_rpc_response_future *future)
{
	struct hdfs_error error = HDFS_SUCCESS;
	bool nnlocked = false, needs_kick = false, sllocked = false;
	int64_t msgno;
	int offset, pre_rlen, post_rlen, rpc_len;
	ssize_t wlen;

	ASSERT(future);
	ASSERT(future->fu_inited && !future->fu_invoked);

	ASSERT(rpc);
	ASSERT(rpc->ob_type == H_RPC_INVOCATION);

	_lock(&n->nn_lock);
	nnlocked = true;

	ASSERT(!n->nn_dead);

	if (n->nn_sock == -1 || n->nn_state < HDFS_NN_ST_CONNECTED) {
		error = error_from_hdfs(HDFS_ERR_NAMENODE_UNCONNECTED);
		goto out;
	}
	if (n->nn_state != HDFS_NN_ST_RPC) {
		error = error_from_hdfs(HDFS_ERR_NAMENODE_UNAUTHENTICATED);
		goto out;
	}

	// Take a number
	msgno = n->nn_msgno;
	n->nn_msgno++;

	future->fu_invoked = true;
	_namenode_pending_insert_unlocked(n, msgno, future,
	    _rpc2_slurper_for_rpc(rpc));

	if (!n->nn_recver_started)
		n->nn_recver_started = needs_kick = true;

	_unlock(&n->nn_lock);
	nnlocked = false;

	if (needs_kick) {
		int rc;

		rc = pipe(n->nn_recv_sigpipe);
		ASSERT(rc == 0);
		rc = pthread_create(&n->nn_recv_thr, NULL,
		    _namenode_recv_worker, n);
		ASSERT(rc == 0);
	}

	// Serialize rpc and transmit
	_rpc_invocation_set_msgno(rpc, msgno);
	_rpc_invocation_set_proto(rpc, n->nn_proto);
	_rpc_invocation_set_clientid(rpc, n->nn_client_id);

	_lock(&n->nn_sendlock);
	sllocked = true;

	// XXX HACK determine the offset of the beginning of this serialized rpc for sasl
	// encoding. Since hdfs_object_serialize() can lead to _hbuf_reserve() calls which
	// may lead to memmove() calls, we cannot simply stash nn_sendbuf.used prior to
	// serializeing the RPC, as that offset may no longer be valid after
	// serialization. As a workaround, compare the _hbuf_readlen() values before and
	// after serialization, and subtract the difference from nn_sendbuf.used.
	pre_rlen = _hbuf_readlen(&n->nn_sendbuf);
	hdfs_object_serialize(&n->nn_sendbuf, rpc);
	post_rlen = _hbuf_readlen(&n->nn_sendbuf);
	rpc_len = post_rlen - pre_rlen;
	offset = n->nn_sendbuf.used - rpc_len;

	if (n->nn_sasl_ssf > 0) {
		error = _sasl_encode_at_offset(n->nn_sasl_ctx, &n->nn_sendbuf, offset);
		if (hdfs_is_error(error))
			goto out; // XXX TODO set nn_state
	}

	error = _write(n->nn_sock, _hbuf_readptr(&n->nn_sendbuf), _hbuf_readlen(&n->nn_sendbuf), &wlen);
	if (wlen < 0)
		goto out;
	_hbuf_consume(&n->nn_sendbuf, wlen);
	_unlock(&n->nn_sendlock);
	sllocked = false;

out:
	if (nnlocked)
		_unlock(&n->nn_lock);
	if (sllocked)
		_unlock(&n->nn_sendlock);
	return error;
}

// XXX reconsider name
EXPORT_SYM struct hdfs_error
hdfs_namenode_continue(struct hdfs_namenode *n)
{
	struct hdfs_error error = HDFS_SUCCESS;
	bool nnlocked = false, sllocked = false;
	ssize_t wlen;


	_lock(&n->nn_lock);
	nnlocked = true;

	ASSERT(!n->nn_dead); // XXX return error instead?

	if (n->nn_sock == -1 || n->nn_state < HDFS_NN_ST_CONNECTED) {
		error = error_from_hdfs(HDFS_ERR_NAMENODE_UNCONNECTED);
		goto out;
	}
	if (n->nn_state != HDFS_NN_ST_RPC) {
		error = error_from_hdfs(HDFS_ERR_NAMENODE_UNAUTHENTICATED);
		goto out;
	}

	_unlock(&n->nn_lock);
	nnlocked = false;

	_lock(&n->nn_sendlock);
	sllocked = true;

	if (_hbuf_readlen(&n->nn_sendbuf) > 0) {
		error = _write(n->nn_sock, _hbuf_readptr(&n->nn_sendbuf), _hbuf_readlen(&n->nn_sendbuf), &wlen);
		if (wlen < 0)
			goto out;
		_hbuf_consume(&n->nn_sendbuf, wlen);
	}
	_unlock(&n->nn_sendlock);
	sllocked = false;

out:
	if (nnlocked)
		_unlock(&n->nn_lock);
	if (sllocked)
		_unlock(&n->nn_sendlock);
	return error;
}

EXPORT_SYM struct hdfs_rpc_response_future *
hdfs_rpc_response_future_alloc(void)
{
	struct hdfs_rpc_response_future *result;

	result = malloc(sizeof(*result));
	ASSERT(result != NULL);
	result->fu_inited = false;
	return result;
}

EXPORT_SYM void
hdfs_rpc_response_future_free(struct hdfs_rpc_response_future **f)
{

	ASSERT(f != NULL && !(*f)->fu_inited);
	free(*f);
	*f = NULL;
}

// XXX: Some optimization could be done for repeated users of the same
// heap-allocated futures.  There is no need to destroy and init the pthread
// objects every reuse.  Be careful not to break the use in src/highlevel.c,
// though, where pthread object destruction is necessary.
EXPORT_SYM void
hdfs_rpc_response_future_init(struct hdfs_rpc_response_future *future)
{

	ASSERT(!future->fu_inited);

	_mtx_init(&future->fu_lock);
	_cond_init(&future->fu_cond);
	future->fu_res = NULL;
	future->fu_invoked = false;
	future->fu_inited = true;
}

EXPORT_SYM void
hdfs_rpc_response_future_clean(struct hdfs_rpc_response_future *future)
{

	if (future->fu_inited) {
		_mtx_destroy(&future->fu_lock);
		_cond_destroy(&future->fu_cond);
		memset(future, 0, sizeof *future);
	}
}

EXPORT_SYM void
hdfs_future_get(struct hdfs_rpc_response_future *future, struct hdfs_object **object)
{

	ASSERT(future->fu_inited && future->fu_invoked);

	_lock(&future->fu_lock);
	while (!future->fu_res)
		_wait(&future->fu_lock, &future->fu_cond);
	_unlock(&future->fu_lock);
	*object = future->fu_res;

	hdfs_rpc_response_future_clean(future);
}

EXPORT_SYM bool
hdfs_future_get_timeout(struct hdfs_rpc_response_future *future, struct hdfs_object **object, uint64_t ms)
{
	uint64_t absms;

	ASSERT(future->fu_inited && future->fu_invoked);

	absms = _now_ms() + ms;
	_lock(&future->fu_lock);
	while (!future->fu_res && _now_ms() < absms)
		_waitlimit(&future->fu_lock, &future->fu_cond, absms);
	if (!future->fu_res) {
		_unlock(&future->fu_lock);
		return false;
	}
	_unlock(&future->fu_lock);
	*object = future->fu_res;

	hdfs_rpc_response_future_clean(future);
	return true;
}

// Releases and cleans resources associated with the namenode 'n'. Callers
// *must not* use the namenode object after this, aside from free() or
// hdfs_namenode_init().
EXPORT_SYM void
hdfs_namenode_destroy(struct hdfs_namenode *n)
{
	int rc;
	bool thread_started;

	_lock(&n->nn_lock);
	ASSERT(!n->nn_dead);
	n->nn_dead = true;

	thread_started = n->nn_recver_started;
	if (n->nn_recver_started) {
		rc = write(n->nn_recv_sigpipe[1], "a", 1);
		ASSERT(rc == 1);
	}
	_unlock(&n->nn_lock);

	if (thread_started) {
		rc = pthread_join(n->nn_recv_thr, NULL);
		ASSERT(rc == 0);
	}

	ASSERT(!n->nn_recver_started);
	if (n->nn_sock != -1)
		close(n->nn_sock);
	if (n->nn_pending)
		free(n->nn_pending);
	if (n->nn_recvbuf.buf)
		free(n->nn_recvbuf.buf);
	if (n->nn_objbuf.buf)
		free(n->nn_objbuf.buf);
	if (n->nn_sendbuf.buf)
		free(n->nn_sendbuf.buf);
	if (n->nn_sasl_ctx)
		sasl_dispose(&n->nn_sasl_ctx);
	if (n->nn_authhdr)
		hdfs_object_free(n->nn_authhdr);

	hdfs_conn_ctx_free(&n->nn_cctx);

	_mtx_destroy(&n->nn_lock);
	_mtx_destroy(&n->nn_sendlock);
	memset(n, 0, sizeof *n);
	// XXX Consider: set nn_sock = -1?
}

static void *
_namenode_recv_worker(void *v_nn)
{
	const size_t RESIZE_AT = 4*1024, RESIZE_BY = 16*1024;

	bool nnlocked = false;
	int sock, obj_size, error, i;

	struct _hdfs_result *result;
	struct hdfs_rpc_response_future *future;
	struct hdfs_namenode *n = v_nn;
	struct hdfs_error herror;

	error = 0;

	while (true) {
		struct hdfs_heap_buf *objbuf;

		_lock(&n->nn_lock);
		nnlocked = true;
		sock = n->nn_sock;

		// If hdfs_namenode_destroy() happened, die:
		if (n->nn_dead) {
			herror = error_from_hdfs(HDFS_ERR_NAMENODE_UNCONNECTED);
			break;
		}

		_unlock(&n->nn_lock);
		nnlocked = false;

		ASSERT(sock != -1);

		if (n->nn_sasl_ssf > 0) {
			objbuf = &n->nn_objbuf;
		} else {
			objbuf = &n->nn_recvbuf;
		}

		if (n->nn_proto == HDFS_NN_v1)
			result = _hdfs_result_deserialize(_hbuf_readptr(objbuf),
			    _hbuf_readlen(objbuf), &obj_size);
		else if (n->nn_proto == HDFS_NN_v2) {
			_lock(&n->nn_lock);
			result = _hdfs_result_deserialize_v2(_hbuf_readptr(objbuf),
			    _hbuf_readlen(objbuf), &obj_size, n->nn_pending,
			    n->nn_pending_len);
			_unlock(&n->nn_lock);
		} else if (n->nn_proto == HDFS_NN_v2_2) {
			_lock(&n->nn_lock);
			result = _hdfs_result_deserialize_v2_2(_hbuf_readptr(objbuf),
			    _hbuf_readlen(objbuf), &obj_size, n->nn_pending,
			    n->nn_pending_len);
			_unlock(&n->nn_lock);
		} else
			ASSERT(false);

		if (!result) {
			ssize_t r;

			_hbuf_resize(&n->nn_recvbuf, RESIZE_AT, RESIZE_BY);

			r = recv(sock, _hbuf_writeptr(&n->nn_recvbuf),
			    _hbuf_remsize(&n->nn_recvbuf), MSG_DONTWAIT);
			if (r == 0) {
				herror = error_from_hdfs(HDFS_ERR_END_OF_STREAM);
				goto out;
			}
			if (r > 0) {
				_hbuf_append(&n->nn_recvbuf, r);
				if (n->nn_sasl_ssf > 0) {
					herror = _conn_try_desasl(n);
					// bail on sasl decode errors
					if (hdfs_is_error(herror))
						goto out;
				}
			} else {
				if (errno != EAGAIN && errno != EWOULDBLOCK) {
					// bail on socket errors
					error = errno;
					herror = error_from_errno(error);
					goto out;
				}

				struct pollfd pfds[] = { {
					.fd = sock,
					.events = POLLIN,
				}, {
					.fd = n->nn_recv_sigpipe[0],
					.events = POLLIN,
				} };
				int rc;

				do {
					rc = poll(pfds, nelem(pfds),
					    -1 /* infinite */);
				} while (rc < 0 && errno == EINTR);
				ASSERT(rc > 0);
			}
			continue;
		}

		if (result == _HDFS_INVALID_PROTO) {
			// bail on protocol errors
			error = EBADMSG;
			herror = error_from_hdfs(HDFS_ERR_NAMENODE_PROTOCOL);
			goto out;
		}

		// if we got here, we have read a valid / complete hdfs result
		// off the wire; skip the buffer forward:
		_hbuf_consume(objbuf, obj_size);

		future = _namenode_pending_remove(n, result->rs_msgno);
		ASSERT(future); // got a response to a msgno we didn't request

		_future_complete(future, result->rs_obj);

		// don't free the object we just handed the user:
		result->rs_obj = NULL;
		_hdfs_result_free(result);
	}

out:
	if (!nnlocked)
		_lock(&n->nn_lock);
	if (error != 0)
		n->nn_error = error;
	n->nn_dead = true;
	n->nn_recver_started = false;

	close(n->nn_recv_sigpipe[1]);
	close(n->nn_recv_sigpipe[0]);
	n->nn_recv_sigpipe[1] = -1;
	n->nn_recv_sigpipe[0] = -1;

	/* Clear pending RPCs with an error */
	for (i = 0; i < n->nn_pending_len; i++) {
		struct hdfs_object *obj;

		future = n->nn_pending[i].pd_future;
		obj = hdfs_pseudo_exception_new(herror);
		_future_complete(future, obj);
	}
	n->nn_pending_len = 0;
	_unlock(&n->nn_lock);

	return NULL;
}

static void
_namenode_pending_insert_unlocked(struct hdfs_namenode *n, int64_t msgno,
	struct hdfs_rpc_response_future *future, hdfs_object_slurper slurper)
{
	const int RESIZE_FACTOR = 16;

	if (n->nn_pending_len % RESIZE_FACTOR == 0) {
		n->nn_pending = realloc(n->nn_pending,
		    (n->nn_pending_len + RESIZE_FACTOR) * sizeof *n->nn_pending);
		ASSERT(n->nn_pending);
	}

	n->nn_pending[n->nn_pending_len].pd_msgno = msgno;
	n->nn_pending[n->nn_pending_len].pd_future = future;
	n->nn_pending[n->nn_pending_len].pd_slurper = slurper;
	n->nn_pending_len++;
}

static struct hdfs_rpc_response_future *
_namenode_pending_remove(struct hdfs_namenode *n, int64_t msgno)
{
	struct hdfs_rpc_response_future *res = NULL;

	_lock(&n->nn_lock);

	for (int i = 0; i < n->nn_pending_len; i++) {
		if (n->nn_pending[i].pd_msgno == msgno) {
			res = n->nn_pending[i].pd_future;
			if (i != n->nn_pending_len - 1)
				n->nn_pending[i] = n->nn_pending[n->nn_pending_len - 1];
			n->nn_pending_len--;
			break;
		}
	}

	_unlock(&n->nn_lock);

	return res;
}

static void
_future_complete(struct hdfs_rpc_response_future *f, struct hdfs_object *o)
{
	ASSERT(o);

	_lock(&f->fu_lock);

	ASSERT(!f->fu_res);
	f->fu_res = o;
	_notifyall(&f->fu_cond);

	_unlock(&f->fu_lock);
}

static int
_getssf(sasl_conn_t *ctx)
{
	const void *pval;
	int r;

	r = sasl_getprop(ctx, SASL_SSF, &pval);
	ASSERT(r == SASL_OK);

	return *(const int *)pval;
}

static void
_sasl_interacts(sasl_interact_t *in)
{
	// Fill in default values...
	while (in->id != SASL_CB_LIST_END) {
		in->result = (in->defresult && *in->defresult) ?
		    in->defresult : __DECONST(char *, "");
		in->len = strlen(in->result);
		in++;
	}
}

// Attempt to de-sasl data from recvbuf to objbuf. Assume ssf > 0.
static struct hdfs_error
_conn_try_desasl(struct hdfs_namenode *n)
{
	int o = 0;
	// XXX TODO review this function. It seems like o is always 0
	// and recvbuf never skips past the read data
	while (o + 4 <= _hbuf_readlen(&n->nn_recvbuf)) {
		uint32_t clen;
		int r;
		const char *out;
		unsigned outlen;

		clen = _be32dec(_hbuf_readptr(&n->nn_recvbuf));
		if (clen > INT32_MAX) // XXX consider different error code
			return error_from_hdfs(HDFS_ERR_NAMENODE_PROTOCOL);

		// did we get an incomplete sasl chunk?
		if ((int32_t)clen > _hbuf_readlen(&n->nn_recvbuf) - o - 4)
			break;

		r = sasl_decode(n->nn_sasl_ctx, _hbuf_readptr(&n->nn_recvbuf) + o + 4,
		    clen, &out, &outlen);
		if (r != SASL_OK)
			return error_from_sasl(r);

		if (outlen > INT32_MAX) // XXX consider different error code
			return error_from_hdfs(HDFS_ERR_NAMENODE_PROTOCOL);

		_hbuf_resize(&n->nn_objbuf, outlen, outlen + 4*1024);

		memcpy(_hbuf_writeptr(&n->nn_objbuf), out, outlen);
		_hbuf_append(&n->nn_objbuf, outlen);
	}

	if (o > 0) {
		_hbuf_consume(&n->nn_recvbuf, o);
	}

	return HDFS_SUCCESS;
}
