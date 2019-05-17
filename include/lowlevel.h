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
	bool has_crcs,
	     bad_crcs,
	     lastpacket;
};

// Do not directly access this struct
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

// hdfs_datanode structs must either be created with hdfs_datanode_alloc() or
// be initialized to all 0's before calling hdfs_datanode_init()
// Access to struct hdfs_datanode must be serialized by the user
struct hdfs_datanode {
	enum hdfs_datanode_state dn_state;
	enum hdfs_datanode_op dn_op;
	int64_t dn_blkid,
		dn_gen,
		dn_offset,
		dn_size;
	struct hdfs_object *dn_token;
	struct hdfs_object **dn_locs;
	int dn_nlocs;
	char *dn_client;
	int dn_sock,
	    dn_proto,
	    dn_conn_idx;
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

//
// Common Datanode API (between blocking and non-blocking)
//

// "DATA_TRANSFER_VERSION" XXX consider moving these values to an enum?
// The datanode protocol used by Apache Hadoop 1.0.x (also 0.20.20x):
#define HDFS_DATANODE_AP_1_0 0x11
// The datanode protocol used by Cloudera CDH3 (derived from apache 0.20.1):
#define HDFS_DATANODE_CDH3 0x10
// The datanode protocol used by Apache Hadoop v2.0+ (at least v2.0.0-2.6.0):
#define HDFS_DATANODE_AP_2_0 0x1C

// Allocate a datanode object and set it to all 0s.
//
// Free with free(3) after calling hdfs_datanode_destroy()
struct hdfs_datanode *	hdfs_datanode_alloc(void);

// Initialize a datanode connection object. Doesn't connect to the datanode.
//
// Prior to calling hdfs_datanode_init() on a datanode object for the first time
// the caller must set the datanode object to all 0s (this is already performed by
// hdfs_datanode_alloc()).
//
// A datanode object may be reused by calling hdfs_datanode_clean() prior to
// calling hdfs_datanode_init() again.
void			hdfs_datanode_init(struct hdfs_datanode *d,
			struct hdfs_object *located_block, const char *client,
			int proto);

// Sets the pool_id (required in HDFSv2+). This should not be called by users who
// pass a located_block to hdfs_datanode_init().
//
// XXX This should either be removed, or a lower level init function that does not
// require a located block object (or both, with pool_id being an optional
// argument to the lower level init function)
void			hdfs_datanode_set_pool_id(struct hdfs_datanode *, const char *);

// Clean a datanode struct prior to re-init()ing and reuse.
//
// Note that only one complete operation may be performed with a datanode
// connection object per init()/connect()/clean() cycle
void			hdfs_datanode_clean(struct hdfs_datanode *d);

// Destroy a datanode object (caller should free).
void			hdfs_datanode_destroy(struct hdfs_datanode *);

//
// Blocking Datanode API
//

// Attempt to connect (blocking) to the datanode associated with this struct.
//
// Returns HDFS_SUCCESS or an error code on failure.
//
// Should only be called on a freshly-initialized datanode struct.
struct hdfs_error	hdfs_datanode_connect(struct hdfs_datanode *d);

// Attempt to write (blocking) a buffer to the block associated with this
// connection.
//
// Returns HDFS_SUCCESS or an error code on failure.
struct hdfs_error	hdfs_datanode_write(struct hdfs_datanode *d, const void *buf,
			size_t len, bool sendcrcs);

// Attempt to write (blocking) from an fd at the given offset to the block
// associated with this connection.
//
// Returns HDFS_SUCCESS or an error code on failure.
struct hdfs_error	hdfs_datanode_write_file(struct hdfs_datanode *d, int fd,
			off_t len, off_t offset, bool sendcrcs);

// Attempt to read (blocking) from the block associated with this connection into
// the given buffer.
//
// len bytes are read, starting at the given byte offset into the block.
//
// Returns HDFS_SUCCESS or an error code on failure.
struct hdfs_error	hdfs_datanode_read(struct hdfs_datanode *d, size_t off,
			size_t len, void *buf, bool verifycrcs);

// Attempt to read (blocking) from the block associated with this connection.
//
// len bytes are read, starting at the given byte offset bloff into the block. The
// read bytes are written to the given fd starting at offset fdoff.
//
// Returns HDFS_SUCCESS or an error code on failure.
struct hdfs_error	hdfs_datanode_read_file(struct hdfs_datanode *d, off_t bloff, off_t len,
			int fd, off_t fdoff, bool verifycrcs);

//
// Non-blocking Datanode API
//

// Get the fd and event types to be waited on while using the non-blocking API and
// HDFS_AGAIN is returned.
//
// *fd is set to the fd to be waited on, and *events is set to a bit mask of
// events as defined with poll(2) (such as POLLOUT|POLLIN) that the user should
// wait on.
//
// The returned fd and events are not invariant across datanode API calls, so
// hdfs_datanode_get_eventfd() should be called before every time the user waits
// on events.
//
// This function should only be called after the first time a connect API has been
// invoked.
//
// Returns HDFS_SUCCESS or an error code on failure.
struct hdfs_error	hdfs_datanode_get_eventfd(struct hdfs_datanode *d, int *fd, short *events);

// Attempt to connect (non-blocking) to the datanode associated with this struct.
//
// Returns HDFS_SUCCESS when the connection is completed. Returns HDFS_AGAIN while
// the connection is in progress. Returns another error code on failure.
//
// hdfs_datanode_connect_nb() should be called repeatedly until a value other than
// HDFS_AGAIN is returned.
struct hdfs_error	hdfs_datanode_connect_nb(struct hdfs_datanode *d);

// Begin a connection attempt (non-blocking) to the given host and port.
//
// numerichost indicates whether or not AI_NUMERICHOST should be set when calling
// getaddrinfo(2). That is, set numerichost to true if host is a numeric IPv4
// address.
//
// Returns HDFS_SUCCESS if the connection is completed. Returns HDFS_AGAIN if the
// connection is in progress but has not yet been completed. Returns another error
// code on failure.
//
// If HDFS_AGAIN is returned, hdfs_datanode_connect_finalize() should be called
// next.
//
// This function (and hdfs_datanode_connect_finalize()) will typically not need to
// be called by users; use hdfs_datanode_connect_nb() instead.
struct hdfs_error	hdfs_datanode_connect_init(struct hdfs_datanode *d, const char *host,
			const char *port, bool numerichost);

// Attempt to finalize a currently-in-progress connection attempt begun by
// hdfs_datanode_connect_init().
//
// Returns HDFS_SUCCESS if the connection in completed. Returns HDFS_AGAIN if the
// connection is still in progress but has not yet been completed. Returns another
// error code on failure.
//
// hdfs_datanode_connect_finalize() should be called repeatedly until a value
// other than HDFS_AGAIN is returned.
//
// This function (and hdfs_datanode_connect_init()) will typically not need to be
// called by users; use hdfs_datanode_connect_nb() instead.
struct hdfs_error	hdfs_datanode_connect_finalize(struct hdfs_datanode *d);

// Initialize the datanode struct for non-blocking writes, and specifiy whether or
// not crcs should be calculated and sent during the write.
//
// This function must be called prior to the first invocation of
// hdfs_datanode_write_nb() or hdfs_datanode_write_file_nb().
//
// Returns HDFS_SUCCESS or another error code on failure.
struct hdfs_error	hdfs_datanode_write_nb_init(struct hdfs_datanode *d, bool sendcrcs);

// Attempt to write (non-blocking) a buffer to the block associated with this
// connection.
//
// *nwritten is set to the number of bytes that have been written to the data node
// during this invocation (which may be 0). *nacked is set to the number of bytes
// that were acknowledged by the datanode during this inovcation (which may be 0).
//
// Returns HDFS_SUCCESS if the entire buffer was written, HDFS_AGAIN if there are
// more bytes to be written, or another error code on failure.
//
// This function should be repeatedly called until all bytes are sent
// (i.e. *nwritten is set to len and HDFS_SUCCESS is returned instead of
// HDFS_AGAIN) or another error code is returned.
//
// If an error is received from a datanode along the pipeline, *error_idx is set
// to the index of the datanode in the pipeline that reported the error; in all
// other cases *error_idx is set to -1.
//
// Once data has been passed to this function, it must continue to be passed at
// every invocation until *nwritten indicates that it has been sent to the
// datanode.
//
// buf must point to the first unsent byte, and len must be set to the number of
// to-be-sent bytes passed to the function for each invocation; that is, the user
// must update buf and len between invocations
//
// Additional data may be passed to this function between invocations; that is,
// the user may add more data to the end of buf (and appropriately increase len)
// between invocations, regardless of whether HDFS_SUCCESS or HDFS_AGAIN were
// returned in the previous invocation.
//
// The user is responsible for keeping track of how many total bytes have been
// written to the block, and how much space is left in the block.
//
// Once all desired bytes have been written to the datanode (but not necessarily
// acknowledged), the user should call hdfs_datanode_finish_block().
//
// For convenience purposes this function will initialize and/or finalize the
// connection to the datanode if not already done so. This allows users to not
// have to maintain state information regarding the connection setup.
struct hdfs_error	hdfs_datanode_write_nb(struct hdfs_datanode *d, const void *buf, size_t len,
			ssize_t *nwritten, ssize_t *nacked, int *error_idx);

// Attempt to write (non-blocking) from an fd at the given offset to the block
// associated with this connection.
//
// Everything is as described for hdfs_datanode_write_nb(), except instead of buf
// pointing to the first byte to be sent for every invocation, the byte at the
// given offset into the fd must be the first byte to be sent for every
// invocation.
//
// Note that this can potentially block during I/O on the user-provided fd, even
// if the user has set it to be non-blocking
struct hdfs_error	hdfs_datanode_write_file_nb(struct hdfs_datanode *d, int fd, off_t len,
			off_t offset, ssize_t *nwritten, ssize_t *nacked, int *error_idx);

// Check (non-blocking) if any acknowledgement packets are available during a
// datanode write.
//
// *nacked is set to the number of bytes that have been acknowledged during this
// invocation (which may be 0).
//
// Returns HDFS_SUCCESS if all written bytes have been acknowledged, HDFS_AGAIN if
// there are more bytes to be acknowledged, or another error code on failure.
//
// If an error is received by a datanode along the pipeline, *error_idx is set to
// the index of the datanode in the pipeline that reported the error; in all other
// cases *error_idx is set to -1.
struct hdfs_error	hdfs_datanode_check_acks(struct hdfs_datanode *d, ssize_t *nacked,
			int *error_idx);

// Attempt to finish (non-blocking) the block associated with this connection.
//
// This function should only be called after hdfs_datanode_write_nb() or
// hdfs_datanode_write_file_nb() have indicated that all data passed to them have
// been sent to the datanode (but not necessarily acknowledged). *nacked is set to
// the number of bytes that have been acknowledged during this invocation (which
// may be 0).
//
// Returns HDFS_SUCCESS if all written bytes have been acknowledged and the
// pipeline has acknowledged that we have finished writing to the block. Returns
// HDFS_AGAIN if there are more bytes to be acknowledged or the pipeline still
// needs to acknowleged that we have finished writing to the block. Returns
// another error on failure.
//
// If an error is received by a datanode along the pipeline, *error_idx is set to
// the index of the datanode in the pipeline that reported the error; in all other
// cases *error_idx is set to -1.
//
// This function should be called repeatedly until it returns HDFS_SUCCESS (or
// another error code).
struct hdfs_error	hdfs_datanode_finish_block(struct hdfs_datanode *d, ssize_t *nacked,
			int *error_idx);

// Initialize the datanode struct with the block offset at which to begin a read
// operation and the total number of bytes to be read, and specify whether or not
// crcs are to be requested and validated.
//
// This function must be called prior to the first invocation of
// hdfs_datanode_read_nb() or hdfs_datanode_read_file_nb().
//
// Returns HDFS_SUCCESS or another error code on failure.
struct hdfs_error	hdfs_datanode_read_nb_init(struct hdfs_datanode *d, off_t bloff,
			off_t len, bool verifycrcs);

// Attempt to read (non-blocking) up to len bytes from the block associated with
// this connection into the given buffer.
//
// *nread is set to the number of bytes copied into buf (which may be 0). len may
// be different than the total number of bytes left to be read from the block (as
// given by the user in hdfs_datanode_read_nb_init() and internally updated as
// data is passed to the user).
//
// Returns HDFS_SUCCESS once all of the data (as given to
// hdfs_datanode_read_nb_init()) has been given to the user and the read status
// has been returned to the server. Returns HDFS_AGAIN if there is more data to be
// given to the user from the block or there is a status message still to be sent
// to the the server. Returns another error code on failure.
//
// This function should be called repeatedly until all requested data from the
// block has been passed to the user and HDFS_SUCCESS is returned or another error
// code is returned. Note, however, that the user should not call
// hdfs_datanode_get_eventfd() and poll() (or other event wait system) until this
// function both returns HDFS_AGAIN and sets *nread to a value strictly less than
// len (since there may be some data buffered by the library available to be read
// that would not indicate a read event to the eventfd).
//
// For convenience purposes this function will initialize and/or finalize the
// connection to the datanode if not already done so. This allows users to not
// have to maintain state information regarding the connection setup.
struct hdfs_error	hdfs_datanode_read_nb(struct hdfs_datanode *d, size_t len, void *buf,
			ssize_t *nread);

// Attempt to read (non-blocking) up to len bytes from the block associated with
// this connection and written to the given fd starting at offset fdoff.
//
// Everything else is as described for hdfs_datanode_read_nb().
//
// Note that this can potentially block during I/O on the user-provided fd, even
// if the user has set it to be non-blocking
struct hdfs_error	hdfs_datanode_read_file_nb(struct hdfs_datanode *d, off_t len, int fd,
			off_t fdoff, ssize_t *nread);

#endif // HADOOFUS_LOWLEVEL_H
