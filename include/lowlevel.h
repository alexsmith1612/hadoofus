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

// EINTR is, explicitly, handled poorly.  I encourage application developers to
// mask signals in threads that interact with libhadoofus.

#include <hadoofus/objects.h>

struct hdfs_namenode;
struct _hdfs_pending;
struct hdfs_rpc_response_future;

struct hdfs_namenode {
	pthread_mutex_t nn_lock;
	int64_t nn_msgno;
	char *nn_recvbuf,
	     *nn_objbuf;
	struct _hdfs_pending *nn_pending;
	sasl_conn_t *nn_sasl_ctx;
	int nn_sock,
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

// TODO Consider changing this to a circular buffer of size MAX_UNACKED_PACKETS
struct hdfs_unacked_packets {
	int64_t ua_num,
		ua_list_pos,
		ua_list_size;
	int64_t *ua_list; // array of lengths of unacked packets
};

struct hdfs_packet_state {
	int64_t seqno,
		first_unacked,
		offset;
	off_t remains_tot,
	      remains_pkt,
	      fdoffset;
	void *buf;
	struct hdfs_heap_buf *hdrbuf;
	struct hdfs_heap_buf *recvbuf;
	int sock,
	    proto,
	    fd;
	bool sendcrcs;
	struct hdfs_unacked_packets unacked;
};

struct hdfs_read_info {
	int64_t client_offset,
		server_offset; // XXX this is unused, remove?
	size_t rlen;
	int32_t chunk_size;
	bool inited,
	     has_crcs,
	     bad_crcs,
	     lastpacket;
};

// XXX perhaps move to net.c and only use opaque pointers here?
struct hdfs_conn_ctx {
	struct addrinfo  *ai;
	struct addrinfo  *rp;
	int serrno;
};

enum hdfs_datanode_state {
	HDFS_DN_ST_ERROR = -1,
	HDFS_DN_ST_ZERO = 0,
	HDFS_DN_ST_INITED,
	HDFS_DN_ST_CONNPENDING,
	HDFS_DN_ST_CONNECTED,
	HDFS_DN_ST_SENDOP,
	HDFS_DN_ST_RECVOP,
	HDFS_DN_ST_PKT,
	HDFS_DN_ST_FINISHED
};

// These are HDFS wire values (except NONE)
enum hdfs_datanode_op {
	HDFS_DN_OP_NONE = 0,
	HDFS_DN_OP_WRITE_BLOCK = 0x50,
	HDFS_DN_OP_READ_BLOCK = 0x51
	// TODO HDFS_DN_OP_TRANSFER_BLOCK = 0x56
};

// Access to struct hdfs_datanode must be serialized by the user
struct hdfs_datanode {
	enum hdfs_datanode_state dn_state;
	enum hdfs_datanode_op dn_op;
	int64_t dn_blkid,
		dn_gen,
		dn_offset,
		dn_size;
	struct hdfs_object *dn_token;
	char *dn_client;
	int dn_sock,
	    dn_proto;
	bool dn_last,
	     dn_crcs;

	/* v2+ */
	char *dn_pool_id;

	struct hdfs_conn_ctx dn_cctx;
	struct hdfs_heap_buf dn_hdrbuf;
	struct hdfs_heap_buf dn_recvbuf;
	struct hdfs_packet_state dn_pstate;
	struct hdfs_read_info dn_rinfo;
};

extern _Thread_local int hdfs_datanode_unknown_status;
extern _Thread_local const char *hdfs_datanode_opresult_message;

//
// Namenode operations
//

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
struct hdfs_error	hdfs_namenode_connect(struct hdfs_namenode *, const char *host, const char *port);

// Sends the authentication header. You must do this before issuing any RPCs.
struct hdfs_error	hdfs_namenode_authenticate(struct hdfs_namenode *, const char *username);
struct hdfs_error	hdfs_namenode_authenticate_full(struct hdfs_namenode *,
		const char *username, const char *real_user);

int64_t		hdfs_namenode_get_msgno(struct hdfs_namenode *);

// The caller must initialize the input future object before invoking the rpc.
// Once this routine is called, the future belongs to the library until one of
// two things happens:
//   1) hdfs_future_get() on that future returns, or:
//   2) the caller cancels the future by invoking hdfs_namenode_destroy().
struct hdfs_error	hdfs_namenode_invoke(struct hdfs_namenode *,
			struct hdfs_object *, struct hdfs_rpc_response_future *);

// Allocate, initialize, clean, and release resources associated with RPC future objects.
struct hdfs_rpc_response_future *hdfs_rpc_response_future_alloc(void);
// Allocated futures are uninitialized.  Use init() before invoking an RPC with one.
void		hdfs_rpc_response_future_init(struct hdfs_rpc_response_future *);
// Futures may be reused multiple times by clean()ing and re-init()ing.
// We lack a cancellation system at this time, so it is invalid to clean or free
// a future that has been passed to hdfs_namenode_invoke().
void		hdfs_rpc_response_future_clean(struct hdfs_rpc_response_future *);
// Either way, clean() and free() to release allocated resources.
void		hdfs_rpc_response_future_free(struct hdfs_rpc_response_future **);

// hdfs_future_get waits until the promised object is available and emits it in
// the outparameter.  It will either be the appropriate response type, NULL (of
// the appropriate type), or an H_PROTOCOL_EXCEPTION.
//
// H_PROTOCOL_EXCEPTION objects have an _etype member that can be used to
// determine the kind of exception raised.
//
// If the future was aborted due to a socket error, protocol parse error, or
// user induced shutdown of the namenode connection, a specific exception of
// _etype H_HADOOFUS_RPC_ABORTED is raised.  If that is the etype, the
// hdfs_error value can be retrieved with hdfs_pseudo_exception_get_error().
//
// Possible errors include:
//
//   * Caller told us to shutdown (destroy):
//     he_hdfserr: HDFS_ERR_NAMENODE_UNCONNECTED
//
//   * Server or middlebox closed the socket (TCP RST):
//     he_hdfserr: HDFS_ERR_END_OF_STREAM
//
//   * Other socket errors:
//     he_errno: Anything recv(2) may return, aside from EAGAIN/EWOULDBLOCK
//
//   * We were unable to parse the response from the Namenode:
//     he_hdfserr: HDFS_ERR_NAMENODE_PROTOCOL
//
// (hdfs_future_get invokes hdfs_rpc_response_future_clean, and the caller does
// not need to in order to reuse the future.)
void		hdfs_future_get(struct hdfs_rpc_response_future *, struct hdfs_object **);

// Like regular hdfs_future_get, but with bounded wait.
//
// Returns 'false' if the RPC received no response in the time limit. The
// Namenode object still owns the future.
bool		hdfs_future_get_timeout(struct hdfs_rpc_response_future *, struct hdfs_object **, uint64_t limitms);

// Aborts any pending completions, stops the worker thread, if any, and releases
// resources associated with a namenode object.
//
// Like any other RPC cancellation, pending operations that have not been
// acknowledged by the server are indeterminate.
void		hdfs_namenode_destroy(struct hdfs_namenode *);

//
// Datanode operations
//

// "DATA_TRANSFER_VERSION" XXX consider moving these values to an enum?
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
struct hdfs_error	hdfs_datanode_connect(struct hdfs_datanode *, const char *host,
			const char *port);

// Attempt to write a buffer to the block associated with this connection.
// Returns HDFS_SUCCESS, or an error message on failure.
struct hdfs_error	hdfs_datanode_write(struct hdfs_datanode *, const void *buf,
			size_t len, bool sendcrcs);

// Attempt to write from an fd to the block associated with this connection.
// Returns HDFS_SUCCESS, or an error message on failure.
struct hdfs_error	hdfs_datanode_write_file(struct hdfs_datanode *, int fd,
			off_t len, off_t offset, bool sendcrcs);

// Attempt to read the block associated with this connection. Returns
// HDFS_SUCCESS on success.  The passed buf should be large enough for the
// entire block.  The caller knows the block size ahead of time.
struct hdfs_error	hdfs_datanode_read(struct hdfs_datanode *d, size_t off, size_t len,
			void *buf, bool verifycrc);

// Attempt to read the block associated with this connection. The block is
// written to the passed fd at the given offset. If the block is larger than
// len, returns an error (and the state of the file in the region [off,
// off+len) is undefined).
struct hdfs_error	hdfs_datanode_read_file(struct hdfs_datanode *, off_t bloff,
			off_t len, int fd, off_t fdoff, bool verifycrc);

// Destroys a datanode object (caller should free).
void		hdfs_datanode_destroy(struct hdfs_datanode *);

#endif
