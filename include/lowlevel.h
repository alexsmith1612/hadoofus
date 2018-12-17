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

typedef void (*hdfs_namenode_destroy_cb)(struct hdfs_namenode *);

enum hdfs_error_kind {
	he_errno,
	he_gaierr,
	he_saslerr,
	he_hdfserr,
};
#define _he_num_kinds (he_hdfserr + 1)

#define _HDFS_ERR_MINIMUM HDFS_ERR_END_OF_STREAM
enum hdfs_error_numeric {
	// I.e., remote end closed the TCP connection or middleman injected
	// RST.
	HDFS_ERR_END_OF_STREAM = 1,
	// Input file shorter than expected, or unexpected zero write(2)ing
	// output.
	HDFS_ERR_END_OF_FILE,

	// Programmer misuse of API
	HDFS_ERR_NAMENODE_UNCONNECTED,
	HDFS_ERR_NAMENODE_UNAUTHENTICATED,

	// Error returned on reads if the user requested CRC validation but the
	// server did not transmit CRCs.
	HDFS_ERR_DATANODE_NO_CRCS,
	// Need at least one datanode to connect, obviously.
	HDFS_ERR_ZERO_DATANODES,

// Direct translations of Datanode STATUS codes.  More information may be
// available in the hdfs_datanode_opresult_message (optionally provided by HDFS
// datanode).  If no message was provided, the pointer will be NULL.
	HDFS_ERR_DN_ERROR,
	HDFS_ERR_DN_ERROR_CHECKSUM,
	HDFS_ERR_DN_ERROR_INVALID,
	HDFS_ERR_DN_ERROR_EXISTS,
	HDFS_ERR_DN_ERROR_ACCESS_TOKEN,
	// Or STATUS code not recognized by libhadoofus; check
	// hdfs_datanode_unknown_status to determine value.
	HDFS_ERR_UNRECOGNIZED_DN_ERROR,
	// STATUS code was recognized, but not valid in the scenario.  The
	// value can be checked with hdfs_datanode_unknown_status.
	HDFS_ERR_INVALID_DN_ERROR,

// Protocol encoding errors
	// HDFS wire messages are prefixed with a length.  That length is
	// itself variable in length.  This error means the encoding of that
	// length prefix was erroneous.
	HDFS_ERR_INVALID_VLINT,
	// Protobuf decode failures:
	HDFS_ERR_INVALID_BLOCKOPRESPONSEPROTO,
	HDFS_ERR_INVALID_PACKETHEADERPROTO,
	HDFS_ERR_INVALID_PIPELINEACKPROTO,
	// v1 DN wire protocol errors
	HDFS_ERR_V1_DATANODE_PROTOCOL,

// Other protocol errors
	// A SUCCESS OpRes had a message attached; as usual, the message can be
	// found in hdfs_datanode_opresult_message.
	HDFS_ERR_INVALID_DN_OPRESP_MSG,
	HDFS_ERR_DATANODE_PACKET_SIZE,
	// The packet's expressed CRCs length doesn't match the data length
	HDFS_ERR_DATANODE_CRC_LEN,
	// We got a non-zero CRCs length when we didn't expect CRCs
	HDFS_ERR_DATANODE_UNEXPECTED_CRC_LEN,
	HDFS_ERR_DATANODE_UNEXPECTED_READ_OFFSET,
	HDFS_ERR_DATANODE_BAD_CHECKSUM,
	HDFS_ERR_DATANODE_BAD_SEQNO,
	// Packet ACK count did not match the number of datanodes in the
	// pipeline
	HDFS_ERR_DATANODE_BAD_ACK_COUNT,
	// Last packet flag set when we expected more packets
	HDFS_ERR_DATANODE_BAD_LASTPACKET,
	// Kerberos downgrade attempt when client requested enforcing mode;
	// possible MITM attack.
	HDFS_ERR_KERBEROS_DOWNGRADE,
	// Unexpected and unhandled error negotiating kerberos
	HDFS_ERR_KERBEROS_NEGOTIATION,
	_HDFS_ERR_END
};
#define _HDFS_ERR_MAXIMUM (_HDFS_ERR_END - 1)

#define _HE_KIND_BITS 2
#define _HE_NUM_BITS (32 - _HE_KIND_BITS)
struct hdfs_error {
	enum hdfs_error_kind her_kind : _HE_KIND_BITS;
	int her_num : _HE_NUM_BITS;
};
_Static_assert((1 << _HE_KIND_BITS) >= _he_num_kinds,
    "if we grow more kinds of error, we need to expand the width of her_kind");
_Static_assert(sizeof(struct hdfs_error) == sizeof(uint32_t),
    "for now, attempt to return a 32-bit value for 32-bit platforms where a "
    "64-bit return is slightly more expenisve");

#define HDFS_SUCCESS	(struct hdfs_error) {}

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

extern _Thread_local int hdfs_datanode_unknown_status;
extern _Thread_local const char *hdfs_datanode_opresult_message;

// Return true if the struct represents an unsucessful result.
static inline bool
hdfs_is_error(struct hdfs_error herr)
{
	return (herr.her_kind != 0 || herr.her_num != 0);
}

// Return a string representation of hdfs_error::her_kind.
const char *hdfs_error_str_kind(struct hdfs_error);

// Return a string representation of hdfs_error::her_num.
const char *hdfs_error_str(struct hdfs_error);

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

// The caller must initialize the future object before invoking the rpc. Once
// this routine is called, the future belongs to this library until one of two
// things happens:
//   1) hdfs_future_get() on that future returns, or:
//   2) hdfs_namenode_destroy()'s user callback is invoked.
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

// hdfs_future_get invokes hdfs_rpc_response_future_clean, and the caller does
// not need to.
void		hdfs_future_get(struct hdfs_rpc_response_future *, struct hdfs_object **);

// Returns 'false' if the RPC received no response in the time limit. The
// Namenode object still references the future.
// If 'true' is returned, same result as hdfs_future_get (including
// hdfs_rpc_response_future_clean).
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
