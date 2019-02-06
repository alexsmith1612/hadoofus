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

static struct hdfs_namenode *	_namenode_copyref_unlocked(struct hdfs_namenode *);
static void			_namenode_decref(struct hdfs_namenode *);
static void *			_namenode_recv_worker(void *);

static void	_namenode_pending_insert_unlocked(struct hdfs_namenode *n,
		int64_t msgno, struct hdfs_rpc_response_future *future,
		hdfs_object_slurper slurper);

static struct hdfs_rpc_response_future *
		_namenode_pending_remove(struct hdfs_namenode *n, int64_t msgno);

static void	_future_complete(struct hdfs_rpc_response_future *future,
		struct hdfs_object *obj);

// SASL helpers
static void	_conn_try_desasl(struct hdfs_namenode *n);
static int	_getssf(sasl_conn_t *);
static void	_sasl_interacts(sasl_interact_t *);

EXPORT_SYM struct hdfs_namenode *
hdfs_namenode_allocate(void)
{
	struct hdfs_namenode *res;

	res = malloc(sizeof(*res));
	ASSERT(res);
	memset(res, 0, sizeof(*res));
	return res;
}

EXPORT_SYM void
hdfs_namenode_init(struct hdfs_namenode *n, enum hdfs_kerb kerb_prefs)
{
	_mtx_init(&n->nn_lock);
	_mtx_init(&n->nn_sendlock);
	n->nn_refs = 1;
	n->nn_sock = -1;
	n->nn_dead = false;
	n->nn_authed = false;
	n->nn_msgno = 0;
	n->nn_destroy_cb = NULL;
	n->nn_pending = NULL;
	n->nn_pending_len = 0;
	n->nn_recver_started = false;

	n->nn_kerb = kerb_prefs;
	n->nn_sasl_ctx = NULL;
	n->nn_sasl_ssf = 0;

	n->nn_recvbuf = NULL;
	n->nn_recvbuf_used = 0;
	n->nn_recvbuf_size = 0;

	n->nn_objbuf = NULL;
	n->nn_objbuf_used = 0;
	n->nn_objbuf_size = 0;

	n->nn_proto = HDFS_NN_v1;
	memset(n->nn_client_id, 0, sizeof(n->nn_client_id));
	n->nn_error = 0;
}

EXPORT_SYM void
hdfs_namenode_set_version(struct hdfs_namenode *n, enum hdfs_namenode_proto vers)
{

	/* Only allowed before we are connected. */
	ASSERT(n->nn_sock == -1);

	ASSERT(vers >= HDFS_NN_v1 && vers <= _HDFS_NN_vLATEST);
	n->nn_proto = vers;

	if (vers >= HDFS_NN_v2_2) {
		ssize_t rd;
		int fd;

		fd = open("/dev/urandom", O_RDONLY);
		ASSERT(fd >= 0);

		rd = read(fd, n->nn_client_id, sizeof(n->nn_client_id));
		ASSERT(rd == sizeof(n->nn_client_id));

		close(fd);
	}
}

EXPORT_SYM struct hdfs_error
hdfs_namenode_connect(struct hdfs_namenode *n, const char *host, const char *port)
{
	struct hdfs_error error = HDFS_SUCCESS;

	_lock(&n->nn_lock);
	ASSERT(n->nn_sock == -1);

	if (n->nn_kerb == HDFS_TRY_KERB || n->nn_kerb == HDFS_REQUIRE_KERB) {
		int r = sasl_client_new("hdfs", host, NULL/*localip*/,
		    NULL/*remoteip*/, NULL/*CBs*/, 0/*sec flags*/, &n->nn_sasl_ctx);
		if (r != SASL_OK) {
			error = error_from_sasl(r);
			goto out;
		}
	}

	error = _connect(&n->nn_sock, host, port);
	if (hdfs_is_error(error))
		goto out;

out:
	_unlock(&n->nn_lock);
	return error;
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
	struct hdfs_object *header;
	struct hdfs_heap_buf hbuf = { 0 };
	char *inh = NULL, preamble[12];
	size_t preamble_len;

	_lock(&n->nn_lock);
	ASSERT(!n->nn_authed);
	ASSERT(n->nn_sock != -1);

	memset(preamble, 0, sizeof(preamble));
	if (n->nn_proto == HDFS_NN_v1) {
		sprintf(preamble, "hrpc\x04%c",
		    (n->nn_kerb == HDFS_NO_KERB)? 0x50 : 0x51);
		preamble_len = 6;

		header = hdfs_authheader_new_ext(n->nn_proto, username,
		    real_user, HDFS_NO_KERB);
	} else if (n->nn_proto == HDFS_NN_v2) {
		/* HDFSv2 has used both version 7 (2.0.0-2.0.2) and 8 (2.0.3+). */
		sprintf(preamble, "hrpc%c%c", 8 /* XXX Configurable? */,
		    (n->nn_kerb == HDFS_NO_KERB)? 0x50 : 0x51);
		/* There is a zero at the end: */
		preamble_len = 7;

		header = hdfs_authheader_new_ext(n->nn_proto, username,
		    real_user, n->nn_kerb);
	} else if (n->nn_proto == HDFS_NN_v2_2) {
		memcpy(preamble, "hrpc\x09", 5);
		preamble[5] = 0;
		preamble[6] = (n->nn_kerb == HDFS_NO_KERB)? 0 : -33;
		preamble_len = 7;

		header = hdfs_authheader_new_ext(n->nn_proto, username,
		    real_user, n->nn_kerb);
		_authheader_set_clientid(header, n->nn_client_id);
	} else {
		ASSERT(false);
	}

	// Serialize the connection header object (I am speaking ClientProtocol
	// and this is my username)
	hdfs_object_serialize(&hbuf, header);
	hdfs_object_free(header);

	if (n->nn_kerb == HDFS_NO_KERB) {
		// Prefix the header object with the protocol preamble
		hbuf.buf = realloc(hbuf.buf, hbuf.size + preamble_len);
		ASSERT(hbuf.buf);
		memmove(hbuf.buf + preamble_len, hbuf.buf, hbuf.used);
		memcpy(hbuf.buf, preamble, preamble_len);
		hbuf.used += preamble_len;
		hbuf.size += preamble_len;
	} else {
		/*
		 * XXX This is probably totally wrong for HDFSv2+. They start
		 * using protobufs at this point to wrap the SASL packets.
		 *
		 * To be fair, it's probably broken for HDFSv1 too :). I need
		 * to find a kerberized HDFS to test against.
		 */
		int r;
		sasl_interact_t *interactions = NULL;
		const char *out, *mechusing;
		unsigned outlen;
		struct iovec iov[3];
		uint32_t inlen;
		const uint32_t SWITCH_TO_SIMPLE_AUTH = (uint32_t)-1;

		uint8_t in[4], zero[4] = { 0 };

		do {
			r = sasl_client_start(n->nn_sasl_ctx, "GSSAPI",
			    &interactions, &out, &outlen, &mechusing);

			if (r == SASL_INTERACT)
				_sasl_interacts(interactions);
		} while (r == SASL_INTERACT);

		if (r != SASL_CONTINUE) {
			error = error_from_sasl(r);
			goto out;
		}

		// Send prefix, first auth token
		_be32enc(in, outlen);
		iov[0].iov_base = preamble;
		iov[0].iov_len = preamble_len;
		iov[1].iov_base = in;
		iov[1].iov_len = 4;
		iov[2].iov_base = __DECONST(void *, out);
		iov[2].iov_len = outlen;

		error = _writev_all(n->nn_sock, iov, 3);
		if (hdfs_is_error(error))
			goto out;

		do {
			// read success / error status
			error = _read_all(n->nn_sock, in, 4);
			if (hdfs_is_error(error))
				goto out;

			if (memcmp(in, zero, 4)) {
				// error. exception will be next on the wire,
				// but let's skip it.
				error = error_from_hdfs(HDFS_ERR_KERBEROS_NEGOTIATION);
				goto out;
			}

			// read token len
			error = _read_all(n->nn_sock, in, 4);
			if (hdfs_is_error(error))
				goto out;
			inlen = _be32dec(in);

			if (inlen == SWITCH_TO_SIMPLE_AUTH) {
				if (n->nn_kerb == HDFS_REQUIRE_KERB) {
					error = error_from_hdfs(HDFS_ERR_KERBEROS_DOWNGRADE);
					goto out;
				}
				goto send_header;
			}

			// read token
			if (inh)
				free(inh);
			inh = NULL;
			if (inlen > 0) {
				inh = malloc(inlen);
				ASSERT(inh);
				error = _read_all(n->nn_sock, inh, inlen);
				if (hdfs_is_error(error))
					goto out;
			}

			out = NULL;
			outlen = 0;
			r = sasl_client_step(n->nn_sasl_ctx, inh,
			    inlen, &interactions, &out, &outlen);

			if (r == SASL_INTERACT)
				_sasl_interacts(interactions);

			if (r == SASL_CONTINUE ||
			    (r == SASL_OK && out != NULL)) {
				_be32enc(in, outlen);
				iov[0].iov_base = in;
				iov[0].iov_len = 4;
				iov[1].iov_base = __DECONST(void *, out);
				iov[1].iov_len = outlen;

				error = _writev_all(n->nn_sock, iov, 2);
				if (hdfs_is_error(error))
					goto out;
			}
		} while (r == SASL_INTERACT || r == SASL_CONTINUE);

		if (r != SASL_OK) {
			error = error_from_sasl(r);
			goto out;
		}

		// sasl connection established
		n->nn_sasl_ssf = _getssf(n->nn_sasl_ctx);
send_header:
		if (n->nn_sasl_ssf > 0)
			_sasl_encode_inplace(n->nn_sasl_ctx, &hbuf);
	}

	// send auth header
	error = _write_all(n->nn_sock, hbuf.buf, hbuf.used);
	n->nn_authed = true;

out:
	if (inh)
		free(inh);
	_unlock(&n->nn_lock);
	free(hbuf.buf);

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
	bool nnlocked, needs_kick;
	int64_t msgno;
	struct hdfs_heap_buf hbuf = { 0 };

	needs_kick = false;

	ASSERT(future);
	ASSERT(!future->fu_namenode);

	ASSERT(rpc);
	ASSERT(rpc->ob_type == H_RPC_INVOCATION);

	_lock(&n->nn_lock);
	nnlocked = true;

	ASSERT(n->nn_refs >= 1);
	ASSERT(!n->nn_dead);

	if (n->nn_sock == -1) {
		error = error_from_hdfs(HDFS_ERR_NAMENODE_UNCONNECTED);
		goto out;
	}
	if (!n->nn_authed) {
		error = error_from_hdfs(HDFS_ERR_NAMENODE_UNAUTHENTICATED);
		goto out;
	}

	// Take a number
	msgno = n->nn_msgno;
	n->nn_msgno++;

	future->fu_namenode = _namenode_copyref_unlocked(n);
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

	hdfs_object_serialize(&hbuf, rpc);

	if (n->nn_sasl_ssf > 0)
		_sasl_encode_inplace(n->nn_sasl_ctx, &hbuf);

	_lock(&n->nn_sendlock);
	error = _write_all(n->nn_sock, hbuf.buf, hbuf.used);
	_unlock(&n->nn_sendlock);

	free(hbuf.buf);

out:
	if (nnlocked)
		_unlock(&n->nn_lock);
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
	future->fu_namenode = NULL;
	future->fu_inited = true;
}

EXPORT_SYM void
hdfs_rpc_response_future_clean(struct hdfs_rpc_response_future *future)
{

	if (future->fu_inited) {
		_mtx_destroy(&future->fu_lock);
		_cond_destroy(&future->fu_cond);
		if (future->fu_namenode != NULL)
			_namenode_decref(future->fu_namenode);
		memset(future, 0, sizeof *future);
	}
}

EXPORT_SYM void
hdfs_future_get(struct hdfs_rpc_response_future *future, struct hdfs_object **object)
{

	ASSERT(future->fu_inited);

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

	ASSERT(future->fu_inited);

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

// Caller *must not* use the namenode object after this. If 'cb' is non-null,
// they will receive a callback when the memory is no longer used, which they
// can use to free memory or whatever.
EXPORT_SYM void
hdfs_namenode_destroy(struct hdfs_namenode *n, hdfs_namenode_destroy_cb cb)
{
	_lock(&n->nn_lock);
	ASSERT(n->nn_refs >= 1);
	ASSERT(!n->nn_dead);
	n->nn_dead = true;
	n->nn_destroy_cb = cb;
	_unlock(&n->nn_lock);

	_namenode_decref(n);
}

// Caller must have n->nn_lock.
static struct hdfs_namenode *
_namenode_copyref_unlocked(struct hdfs_namenode *n)
{
	ASSERT(n->nn_refs >= 1);
	n->nn_refs++;
	return n;
}

static void
_namenode_decref(struct hdfs_namenode *n)
{
	bool lastref = false;
	int rc;

	_lock(&n->nn_lock);
	ASSERT(n->nn_refs >= 1);
	n->nn_refs--;
	if (n->nn_refs == 0)
		lastref = true;
	_unlock(&n->nn_lock);

	if (lastref) {
		hdfs_namenode_destroy_cb dcb = NULL;

		ASSERT(n->nn_dead);
		if (n->nn_recver_started) {
			rc = write(n->nn_recv_sigpipe[1], "a", 1);
			ASSERT(rc == 1);

			rc = pthread_join(n->nn_recv_thr, NULL);
			ASSERT(rc == 0);

			close(n->nn_recv_sigpipe[1]);
			close(n->nn_recv_sigpipe[0]);
		}
		if (n->nn_sock != -1)
			close(n->nn_sock);
		if (n->nn_pending)
			free(n->nn_pending);
		if (n->nn_recvbuf)
			free(n->nn_recvbuf);
		if (n->nn_objbuf)
			free(n->nn_objbuf);
		if (n->nn_destroy_cb)
			dcb = n->nn_destroy_cb;
		if (n->nn_sasl_ctx)
			sasl_dispose(&n->nn_sasl_ctx);
		_mtx_destroy(&n->nn_lock);
		_mtx_destroy(&n->nn_sendlock);
		memset(n, 0, sizeof *n);
		if (dcb)
			dcb(n);
	}
}

static void *
_namenode_recv_worker(void *v_nn)
{
	const size_t RESIZE = 16*1024;

	bool nnlocked = false;
	int sock, obj_size, error, i;

	struct _hdfs_result *result;
	struct hdfs_rpc_response_future *future;
	struct hdfs_namenode *n = v_nn;
	struct hdfs_error herror;

	error = 0;

	while (true) {
		char *objbuffer;
		size_t *objused;

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
			objbuffer = n->nn_objbuf;
			objused = &n->nn_objbuf_used;
		} else {
			objbuffer = n->nn_recvbuf;
			objused = &n->nn_recvbuf_used;
		}

		if (n->nn_proto == HDFS_NN_v1)
			result = _hdfs_result_deserialize(objbuffer, *objused,
			    &obj_size);
		else if (n->nn_proto == HDFS_NN_v2) {
			_lock(&n->nn_lock);
			result = _hdfs_result_deserialize_v2(objbuffer,
			    *objused, &obj_size, n->nn_pending,
			    n->nn_pending_len);
			_unlock(&n->nn_lock);
		} else if (n->nn_proto == HDFS_NN_v2_2) {
			_lock(&n->nn_lock);
			result = _hdfs_result_deserialize_v2_2(objbuffer,
			    *objused, &obj_size, n->nn_pending,
			    n->nn_pending_len);
			_unlock(&n->nn_lock);
		} else
			ASSERT(false);

		if (!result) {
			ssize_t r, remain = n->nn_recvbuf_size - n->nn_recvbuf_used;

			if (remain < 4*1024) {
				n->nn_recvbuf_size += RESIZE;
				n->nn_recvbuf = realloc(n->nn_recvbuf, n->nn_recvbuf_size);
				ASSERT(n->nn_recvbuf);
				remain = n->nn_recvbuf_size - n->nn_recvbuf_used;
			}

			r = recv(sock, n->nn_recvbuf + n->nn_recvbuf_used,
			    remain, MSG_DONTWAIT);
			if (r == 0) {
				herror = error_from_hdfs(HDFS_ERR_END_OF_STREAM);
				goto out;
			}
			if (r > 0) {
				n->nn_recvbuf_used += r;
				if (n->nn_sasl_ssf > 0)
					_conn_try_desasl(n);
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

		*objused -= obj_size;
		if (*objused)
			memmove(objbuffer, objbuffer + obj_size, *objused);

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
	if (n->nn_sock >= 0) {
		close(n->nn_sock);
		n->nn_sock = -1;
	}
	n->nn_dead = true;
	n->nn_recver_started = false;

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

// Attempt to de-sasl data from recvbuf to objbuf. Assume ssf > 0. On bad data,
// aborts.
static void
_conn_try_desasl(struct hdfs_namenode *n)
{
	size_t o = 0;
	while (o + 4 <= n->nn_recvbuf_used) {
		uint32_t clen;
		int r;
		const char *out;
		unsigned outlen;

		clen = _be32dec(n->nn_recvbuf);
		ASSERT(clen <= INT32_MAX);

		// did we get an incomplete sasl chunk?
		if (clen > n->nn_recvbuf_used - o - 4)
			break;

		r = sasl_decode(n->nn_sasl_ctx, n->nn_recvbuf + o + 4, clen,
		    &out, &outlen);
		if (r != SASL_OK)
			abort();

		if (outlen > n->nn_objbuf_size - n->nn_objbuf_used) {
			n->nn_objbuf_size = n->nn_objbuf_used + outlen + 4*1024;
			n->nn_objbuf = realloc(n->nn_objbuf, n->nn_objbuf_size);
			ASSERT(n->nn_objbuf);
		}

		memcpy(n->nn_objbuf + n->nn_objbuf_used, out, outlen);
		n->nn_objbuf_used += outlen;
	}

	if (o > 0) {
		n->nn_recvbuf_used -= o;
		if (n->nn_recvbuf_used > 0)
			memmove(n->nn_recvbuf, n->nn_recvbuf + o, n->nn_recvbuf_used);
	}
}
