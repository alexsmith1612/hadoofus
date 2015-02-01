#ifndef HADOOFUS_LOWLEVEL_H
#define HADOOFUS_LOWLEVEL_H

//
// This is the low-level HDFS API. It can be used to send arbitrary RPCs and
// exploit pipelining / out-of-order execution from a single thread.
//

#include <err.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sysexits.h>

#include <sasl/sasl.h>

// EINTR is, explicitly, handled poorly.

#include <hadoofus/objects.h>

struct hdfs_namenode;
struct _hdfs_pending;

typedef void (*hdfs_namenode_destroy_cb)(struct hdfs_namenode *);

struct hdfs_namenode {
	pthread_mutex_t nn_lock;
	int64_t nn_msgno;
	char *nn_recvbuf,
	     *nn_objbuf;
	hdfs_namenode_destroy_cb nn_destroy_cb;
	struct _hdfs_pending *nn_pending;
	sasl_conn_t *nn_sasl_ctx;
	int nn_refs,
	    nn_sock,
	    nn_pending_len,
	    nn_sasl_ssf;
	enum hdfs_kerb nn_kerb;
	bool nn_dead/*user-killed*/,
	     nn_authed,
	     nn_recver_started;
	pthread_mutex_t nn_sendlock;
	size_t nn_recvbuf_used,
	       nn_recvbuf_size,
	       nn_objbuf_used,
	       nn_objbuf_size;

	pthread_t nn_recv_thr;
	int nn_recv_sigpipe[2];

	enum hdfs_namenode_proto nn_proto;
	uint8_t nn_client_id[_HDFS_CLIENT_ID_LEN];
	int nn_error;
};

struct hdfs_datanode {
	pthread_mutex_t dn_lock;
	int64_t dn_blkid,
		dn_gen,
		dn_offset,
		dn_size;
	struct hdfs_object *dn_token;
	char *dn_client;
	int dn_sock,
	    dn_proto;
	bool dn_used;

	/* v2+ */
	char *dn_pool_id;
};

struct hdfs_rpc_response_future {
	pthread_mutex_t fu_lock;
	pthread_cond_t fu_cond;
	struct hdfs_object *fu_res;
	struct hdfs_namenode *fu_namenode;
};

//
// Namenode operations
//

#define HDFS_RPC_RESPONSE_FUTURE_INITIALIZER		\
	(struct hdfs_rpc_response_future) {		\
		.fu_lock = PTHREAD_MUTEX_INITIALIZER,	\
		.fu_cond = PTHREAD_COND_INITIALIZER,	\
		.fu_res = NULL,				\
		.fu_namenode = NULL,			\
	}

static inline void
hdfs_rpc_response_future_init(struct hdfs_rpc_response_future *future)
{
	int rc;

	memset(future, 0, sizeof(*future));

	/* This really shouldn't fail. Abort hard if it does. */
	rc = pthread_mutex_init(&future->fu_lock, NULL);
	if (rc) {
		errno = rc;
		err(EX_SOFTWARE, "pthread_mutex_init");
	}
	rc = pthread_cond_init(&future->fu_cond, NULL);
	if (rc) {
		errno = rc;
		err(EX_SOFTWARE, "pthread_cond_init");
	}
	future->fu_res = NULL;
	future->fu_namenode = NULL;
}

// Allocate a namenode object. (This allows us to add fields to hdfs_namenode
// without breaking ABI in the future.)
//
// Free with free(3).
struct hdfs_namenode *	hdfs_namenode_allocate(void);

// Initialize the connection object. Doesn't actually connect or authenticate
// with the namenode.
//
// Kerb setting one of:
//   HDFS_NO_KERB      -- "Authenticate" with plaintext username (hadoop default)
//   HDFS_TRY_KERB     -- attempt kerb, but allow fallback to plaintext
//   HDFS_REQUIRE_KERB -- fail if server attempts to fallback to plaintext
void		hdfs_namenode_init(struct hdfs_namenode *, enum hdfs_kerb);

// Set the protocol version used to communicate with the namenode. It is only
// valid to do this BEFORE connecting to any namenode.
//
// Versions are one of:
//   HDFS_NN_v1        -- v1.x
//   HDFS_NN_v2        -- v2.0
//   HDFS_NN_v2_2      -- v2.2
void		hdfs_namenode_set_version(struct hdfs_namenode *, enum hdfs_namenode_proto);

// Connect to the given host/port. You should only use this on a freshly
// initialized namenode object (don't re-use the same object until it's been
// destroyed / re-initialized).
const char *	hdfs_namenode_connect(struct hdfs_namenode *, const char *host, const char *port);

// Sends the authentication header. You must do this before issuing any RPCs.
const char *	hdfs_namenode_authenticate(struct hdfs_namenode *, const char *username);
const char *	hdfs_namenode_authenticate_full(struct hdfs_namenode *,
		const char *username, const char *real_user);

int64_t		hdfs_namenode_get_msgno(struct hdfs_namenode *);

// The caller must initialize the future object before invoking the rpc. Once
// this routine is called, the future belongs to this library until one of two
// things happens:
//   1) hdfs_future_get() on that future returns, or:
//   2) hdfs_namenode_destroy()'s user callback is invoked.
const char *	hdfs_namenode_invoke(struct hdfs_namenode *, struct hdfs_object *,
		struct hdfs_rpc_response_future *);

// After this returns, caller can do whatever they like with the "future"
// object.
void		hdfs_future_get(struct hdfs_rpc_response_future *, struct hdfs_object **);

// Returns 'false' if the RPC received no response in the time limit. The
// Namenode object still references the future.
// If 'true' is returned, same result as hdfs_future_get.
bool		hdfs_future_get_timeout(struct hdfs_rpc_response_future *, struct hdfs_object **, uint64_t limitms);

// Destroys the connection. Note that the memory may still be in use by a child
// thread when this function returns. However, the memory can be freed or
// re-used when the user's callback is invoked.
void		hdfs_namenode_destroy(struct hdfs_namenode *, hdfs_namenode_destroy_cb cb);

//
// Datanode operations
//

// "DATA_TRANSFER_VERSION"
// The datanode protocol used by Apache Hadoop 1.0.x (also 0.20.20x):
#define HDFS_DATANODE_AP_1_0 0x11
// The datanode protocol used by Cloudera CDH3 (derived from apache 0.20.1):
#define HDFS_DATANODE_CDH3 0x10
// The datanode protocol used by Apache Hadoop v2.0+ (at least v2.0.0-2.6.0):
#define HDFS_DATANODE_AP_2_0 0x1C

// Initializes a datanode connection object. Doesn't connect to the datanode.
void		hdfs_datanode_init(struct hdfs_datanode *,
		int64_t blkid, int64_t size, int64_t gen, /* block */
		int64_t offset, const char *client, struct hdfs_object *token,
		int proto);

// Sets the pool_id (required in HDFSv2+)
void		hdfs_datanode_set_pool_id(struct hdfs_datanode *, const char *);

// Attempt to connect to a host and port. Should only be called on a freshly-
// initialized datanode struct.
const char *	hdfs_datanode_connect(struct hdfs_datanode *, const char *host,
		const char *port);

// Attempt to write a buffer to the block associated with this connection.
// Returns NULL on success or an error message on failure.
const char *	hdfs_datanode_write(struct hdfs_datanode *, const void *buf,
		size_t len, bool sendcrcs);

// Attempt to write from an fd to the block associated with this connection.
// Returns NULL on success or an error message on failure.
const char *	hdfs_datanode_write_file(struct hdfs_datanode *, int fd,
		off_t len, off_t offset, bool sendcrcs);

// Attempt to read the block associated with this connection. Returns NULL on
// success. The passed buf should be large enough for the entire block. The
// caller knows the block size ahead of time.
const char *	hdfs_datanode_read(struct hdfs_datanode *d, size_t off, size_t len,
		void *buf, bool verifycrc);

// Attempt to read the block associated with this connection. The block is
// written to the passed fd at the given offset. If the block is larger than
// len, returns an error (and the state of the file in the region [off,
// off+len) is undefined).
const char *	hdfs_datanode_read_file(struct hdfs_datanode *, off_t bloff,
		off_t len, int fd, off_t fdoff, bool verifycrc);

// Destroys a datanode object (caller should free).
void		hdfs_datanode_destroy(struct hdfs_datanode *);

// Error returned on reads if the user requested CRC validation but the server
// did not transmit CRCs.
extern const char *HDFS_DATANODE_ERR_NO_CRCS;

#endif
