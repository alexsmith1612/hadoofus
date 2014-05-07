#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <hadoofus/lowlevel.h>

#include "net.h"
#include "objects-internal.h"
#include "pthread_wrappers.h"
#include "util.h"

static struct hdfs_namenode *	_namenode_copyref_unlocked(struct hdfs_namenode *);
static void			_namenode_decref(struct hdfs_namenode *);
static bool			_namenode_recv(struct hdfs_rpc_response_future *);

static void	_namenode_pending_insert_unlocked(struct hdfs_namenode *n,
		int64_t msgno, struct hdfs_rpc_response_future *future);

static struct hdfs_rpc_response_future *
		_namenode_pending_remove(struct hdfs_namenode *n, int64_t msgno);

static void	_future_complete(struct hdfs_rpc_response_future *future,
		struct hdfs_object *obj);
static void	_future_complete_unlocked(struct hdfs_rpc_response_future *f,
		struct hdfs_object *o);

// SASL helpers
static void	_conn_try_desasl(struct hdfs_namenode *n);
static int	_getssf(sasl_conn_t *);
static void	_sasl_interacts(sasl_interact_t *);

EXPORT_SYM void
hdfs_namenode_init(struct hdfs_namenode *n, enum hdfs_kerb kerb_prefs)
{
	n->nn_lock = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
	n->nn_sendlock = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
	n->nn_refs = 1;
	n->nn_sock = -1;
	n->nn_dead = false;
	n->nn_authed = false;
	n->nn_msgno = 0;
	n->nn_destroy_cb = NULL;
	n->nn_pending = NULL;
	n->nn_pending_len = 0;
	n->nn_worked = false;

	n->nn_kerb = kerb_prefs;
	n->nn_sasl_ctx = NULL;
	n->nn_sasl_ssf = 0;

	n->nn_recvbuf = NULL;
	n->nn_recvbuf_used = 0;
	n->nn_recvbuf_size = 0;

	n->nn_objbuf = NULL;
	n->nn_objbuf_used = 0;
	n->nn_objbuf_size = 0;
}

EXPORT_SYM const char *
hdfs_namenode_connect(struct hdfs_namenode *n, const char *host, const char *port)
{
	const char *err = NULL;

	_lock(&n->nn_lock);
	ASSERT(n->nn_sock == -1);

	if (n->nn_kerb == HDFS_TRY_KERB || n->nn_kerb == HDFS_REQUIRE_KERB) {
		int r = sasl_client_new("hdfs", host, NULL/*localip*/,
		    NULL/*remoteip*/, NULL/*CBs*/, 0/*sec flags*/, &n->nn_sasl_ctx);
		if (r != SASL_OK) {
			err = sasl_errstring(r, NULL, NULL);
			goto out;
		}
	}

	err = _connect(&n->nn_sock, host, port);
	if (err)
		goto out;

out:
	_unlock(&n->nn_lock);
	return err;
}

EXPORT_SYM const char *
hdfs_namenode_authenticate(struct hdfs_namenode *n, const char *username)
{
	const char *err = NULL, *preamble = "hrpc\x04\x50";
	struct hdfs_object *header;
	struct hdfs_heap_buf hbuf = { 0 };
	char *inh = NULL;

	_lock(&n->nn_lock);
	ASSERT(!n->nn_authed);
	ASSERT(n->nn_sock != -1);

	// Create / serialize the connection header object
	header = hdfs_authheader_new(username);
	hdfs_object_serialize(&hbuf, header);
	hdfs_object_free(header);

	if (n->nn_kerb == HDFS_NO_KERB) {
		// Prefix the header object with the protocol preamble
		hbuf.buf = realloc(hbuf.buf, hbuf.size + strlen(preamble));
		ASSERT(hbuf.buf);
		memmove(hbuf.buf + strlen(preamble), hbuf.buf, hbuf.used);
		memcpy(hbuf.buf, preamble, strlen(preamble));
		hbuf.used += strlen(preamble);
		hbuf.size += strlen(preamble);

		// Write the entire thing to the socket. Honestly, the first write
		// should succeed (we have the entire outbound sockbuf) but we call it
		// in a loop for correctness.
		err = _write_all(n->nn_sock, hbuf.buf, hbuf.used);
		n->nn_authed = true;
	} else {
		int r;
		sasl_interact_t *interactions = NULL;
		const char *out, *mechusing, *prefix = "hrpc\x04\x51";
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
			err = sasl_errstring(r, NULL, NULL);
			goto out;
		}

		// Send prefix, first auth token
		_be32enc(in, outlen);
		iov[0].iov_base = __DECONST(void *, prefix);
		iov[0].iov_len = strlen(prefix);
		iov[1].iov_base = in;
		iov[1].iov_len = 4;
		iov[2].iov_base = __DECONST(void *, out);
		iov[2].iov_len = outlen;

		err = _writev_all(n->nn_sock, iov, 3);
		if (err)
			goto out;

		do {
			// read success / error status
			err = _read_all(n->nn_sock, in, 4);
			if (err)
				goto out;

			if (memcmp(in, zero, 4)) {
				// error. exception will be next on the wire,
				// but let's skip it.
				err = "Got error from server, bailing";
				goto out;
			}

			// read token len
			err = _read_all(n->nn_sock, in, 4);
			if (err)
				goto out;
			inlen = _be32dec(in);

			if (inlen == SWITCH_TO_SIMPLE_AUTH) {
				if (n->nn_kerb == HDFS_REQUIRE_KERB) {
					err = "Server tried to drop kerberos but "
					    "client requires it";
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
				err = _read_all(n->nn_sock, inh, inlen);
				if (err)
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

				err = _writev_all(n->nn_sock, iov, 2);
				if (err)
					goto out;
			}
		} while (r == SASL_INTERACT || r == SASL_CONTINUE);

		if (r != SASL_OK) {
			err = sasl_errstring(r, NULL, NULL);
			goto out;
		}

		// sasl connection established
		n->nn_sasl_ssf = _getssf(n->nn_sasl_ctx);
send_header:
		if (n->nn_sasl_ssf > 0)
			_sasl_encode_inplace(n->nn_sasl_ctx, &hbuf);

		// send auth header
		err = _write_all(n->nn_sock, hbuf.buf, hbuf.used);
		n->nn_authed = true;
	}

out:
	if (inh)
		free(inh);
	_unlock(&n->nn_lock);
	free(hbuf.buf);

	return err;
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

EXPORT_SYM const char *
hdfs_namenode_invoke(struct hdfs_namenode *n, struct hdfs_object *rpc,
	struct hdfs_rpc_response_future *future)
{
	const char *err = NULL;
	bool nnlocked;
	int64_t msgno;
	struct hdfs_heap_buf hbuf = { 0 };

	ASSERT(future);
	ASSERT(!future->fu_namenode);

	ASSERT(rpc);
	ASSERT(rpc->ob_type == H_RPC_INVOCATION);

	_lock(&n->nn_lock);
	nnlocked = true;

	ASSERT(n->nn_refs >= 1);
	ASSERT(!n->nn_dead);

	if (n->nn_sock == -1) {
		err = "Not connected";
		goto out;
	}
	if (!n->nn_authed) {
		err = "Not authenticated";
		goto out;
	}

	// Take a number
	msgno = n->nn_msgno;
	n->nn_msgno++;

	future->fu_namenode = _namenode_copyref_unlocked(n);
	_namenode_pending_insert_unlocked(n, msgno, future);

	_unlock(&n->nn_lock);
	nnlocked = false;

	// Serialize rpc and transmit
	_rpc_invocation_set_msgno(rpc, msgno);
	hdfs_object_serialize(&hbuf, rpc);

	if (n->nn_sasl_ssf > 0)
		_sasl_encode_inplace(n->nn_sasl_ctx, &hbuf);

	_lock(&n->nn_sendlock);
	err = _write_all(n->nn_sock, hbuf.buf, hbuf.used);
	_unlock(&n->nn_sendlock);

	free(hbuf.buf);

out:
	if (nnlocked)
		_unlock(&n->nn_lock);
	return err;
}

EXPORT_SYM void
hdfs_future_get(struct hdfs_rpc_response_future *future, struct hdfs_object **object)
{
	bool found;

	_lock(&future->fu_lock);
	while (!future->fu_res) {
		found = _namenode_recv(future);
		if (found) {
			ASSERT(future->fu_res);
			break;
		}

		/* we can't miss the wake-up here because we're holding fu_lock */

		_wait(&future->fu_lock, &future->fu_cond);
	}
	_unlock(&future->fu_lock);
	*object = future->fu_res;

	_namenode_decref(future->fu_namenode);
	memset(future, 0, sizeof *future);
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

	_lock(&n->nn_lock);
	ASSERT(n->nn_refs >= 1);
	n->nn_refs--;
	if (n->nn_refs == 0)
		lastref = true;
	_unlock(&n->nn_lock);

	if (lastref) {
		hdfs_namenode_destroy_cb dcb = NULL;
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
		memset(n, 0, sizeof *n);
		if (dcb)
			dcb(n);
	}
}

struct _hdfs_pending {
	int64_t pd_msgno;
	struct hdfs_rpc_response_future *pd_future;
};

static bool
_namenode_recv(struct hdfs_rpc_response_future *goal_future)
{
	const size_t RESIZE = 16*1024;

	bool nnlocked = false, res = false, nnworked;
	int sock, obj_size;

	struct _hdfs_result *result;
	struct hdfs_rpc_response_future *future;
	struct hdfs_namenode *n = goal_future->fu_namenode;

	_lock(&n->nn_lock);
	nnworked = n->nn_worked;
	if (!nnworked)
		n->nn_worked = true;
	_unlock(&n->nn_lock);

	// someone else is working this connection for now
	if (nnworked)
		goto out;

	// only one thread should be able to enter this section at a time:
	while (true) {
		char *objbuffer;
		size_t *objused;

		_lock(&n->nn_lock);
		nnlocked = true;
		sock = n->nn_sock;

		// If hdfs_namenode_destroy() happened, die:
		if (n->nn_dead || n->nn_refs == 1)
			break;

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

		result = _hdfs_result_deserialize(objbuffer, *objused, &obj_size);
		if (!result) {
			int r, remain = n->nn_recvbuf_size - n->nn_recvbuf_used;

			if (remain < 4*1024) {
				n->nn_recvbuf_size += RESIZE;
				n->nn_recvbuf = realloc(n->nn_recvbuf, n->nn_recvbuf_size);
				ASSERT(n->nn_recvbuf);
				remain = n->nn_recvbuf_size - n->nn_recvbuf_used;
			}

			r = read(sock, n->nn_recvbuf + n->nn_recvbuf_used, remain);
			if (r == 0)
				goto out;
			if (r > 0) {
				n->nn_recvbuf_used += r;
				if (n->nn_sasl_ssf > 0)
					_conn_try_desasl(n);
			} else {

				// bail on socket errors
				_lock(&n->nn_lock);
				close(n->nn_sock);
				n->nn_sock = -1;
				n->nn_dead = true;
				_unlock(&n->nn_lock);

				// We need to do something more intelligent if
				// we want to handle socket errors gracefully.
				abort();
			}

			continue;
		}

		if (result == _HDFS_INVALID_PROTO) {
			// bail on protocol errors
			_lock(&n->nn_lock);
			close(n->nn_sock);
			n->nn_sock = -1;
			n->nn_dead = true;
			_unlock(&n->nn_lock);

			// We should do something more intelligent if we want
			// to handle bad protocol data gracefully.
			abort();
			break;
		}

		// if we got here, we have read a valid / complete hdfs result
		// off the wire; skip the buffer forward:

		*objused -= obj_size;
		if (*objused)
			memmove(objbuffer, objbuffer + obj_size, *objused);

		future = _namenode_pending_remove(n, result->rs_msgno);
		ASSERT(future); // got a response to a msgno we didn't request

		if (future == goal_future)
			_future_complete_unlocked(future, result->rs_obj);
		else
			_future_complete(future, result->rs_obj);

		// don't free the object we just handed the user:
		result->rs_obj = NULL;
		_hdfs_result_free(result);

		// all done here, we found our response
		if (future == goal_future) {
			res = true;
			break;
		}
	}

	// End the critical section:
	if (!nnlocked)
		_lock(&n->nn_lock);
	ASSERT(n->nn_worked);
	n->nn_worked = false;

	// If any thread is pending, wake it
	if (n->nn_pending_len > 0) {
		for (int i = 0; i < n->nn_pending_len; i++) {
			struct hdfs_rpc_response_future *pf = n->nn_pending[0].pd_future;
			if (pf == goal_future)
				continue;
			_lock(&pf->fu_lock);
			_notifyall(&pf->fu_cond);
			_unlock(&pf->fu_lock);
			break;
		}
	}
	_unlock(&n->nn_lock);
	nnlocked = false;

out:
	if (nnlocked)
		_unlock(&n->nn_lock);

	return res;
}

static void
_namenode_pending_insert_unlocked(struct hdfs_namenode *n, int64_t msgno,
	struct hdfs_rpc_response_future *future)
{
	const int RESIZE_FACTOR = 16;

	if (n->nn_pending_len % RESIZE_FACTOR == 0) {
		n->nn_pending = realloc(n->nn_pending,
		    (n->nn_pending_len + RESIZE_FACTOR) * sizeof *n->nn_pending);
		ASSERT(n->nn_pending);
	}

	n->nn_pending[n->nn_pending_len].pd_msgno = msgno;
	n->nn_pending[n->nn_pending_len].pd_future = future;
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

static void
_future_complete_unlocked(struct hdfs_rpc_response_future *f, struct hdfs_object *o)
{
	ASSERT(o);

	ASSERT(!f->fu_res);
	f->fu_res = o;
}

static int
_getssf(sasl_conn_t *ctx)
{
	int *ssfp, r;

	r = sasl_getprop(ctx, SASL_SSF, (const void **)&ssfp);
	ASSERT(r == SASL_OK);

	return *ssfp;
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
