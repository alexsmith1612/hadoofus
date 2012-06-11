#ifndef HADOOFUS_LOWLEVEL_H
#define HADOOFUS_LOWLEVEL_H

//
// This is the low-level HDFS API. It can be used to send arbitrary RPCs and
// exploit pipelining / out-of-order execution from a single thread.
//

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

// EINTR is, explicitly, handled poorly.

struct hdfs_namenode;
struct _hdfs_pending;

typedef void (*hdfs_namenode_destroy_cb)(struct hdfs_namenode *);

struct hdfs_namenode {
	pthread_mutex_t nn_lock;
	int64_t nn_msgno;
	pthread_t nn_recvthr;
	hdfs_namenode_destroy_cb nn_destroy_cb;
	struct _hdfs_pending *nn_pending;
	int nn_refs,
	    nn_sock,
	    nn_pending_len;
	bool nn_dead/*user-killed*/,
	     nn_authed,
	     nn_recvthr_alive;
	pthread_mutex_t nn_sendlock;
};

struct hdfs_rpc_response_future {
	pthread_mutex_t fu_lock;
	pthread_cond_t fu_cond;
	struct hdfs_object *fu_res;
};

#define HDFS_RPC_RESPONSE_FUTURE_INITIALIZER \
    (struct hdfs_rpc_response_future) { \
	    .fu_lock = PTHREAD_MUTEX_INITIALIZER, \
	    .fu_cond = PTHREAD_COND_INITIALIZER, \
	    .fu_res = NULL \
    }
static inline void
hdfs_rpc_response_future_init(struct hdfs_rpc_response_future *future)
{
	*future = HDFS_RPC_RESPONSE_FUTURE_INITIALIZER;
}

// Initialize the connection object. Doesn't actually connect or authenticate
// with the namenode.
void		hdfs_namenode_init(struct hdfs_namenode *);

// Connect to the given host/port. You should only use this on a freshly
// initialized namenode object (don't re-use the same object until it's been
// destroyed / re-initialized).
const char *	hdfs_namenode_connect(struct hdfs_namenode *, const char *host, const char *port);

// Sends the authentication header. You must do this before issuing any RPCs.
const char *	hdfs_namenode_authenticate(struct hdfs_namenode *, const char *username);

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

// Destroys the connection. Note that the memory may still be in use by a child
// thread when this function returns. However, the memory can be freed or
// re-used when the user's callback is invoked.
void		hdfs_namenode_destroy(struct hdfs_namenode *, hdfs_namenode_destroy_cb cb);

#endif
