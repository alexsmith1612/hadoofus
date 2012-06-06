#ifndef HADOOFUS_H
#define HADOOFUS_H

#include <pthread.h>

struct hdfs_namenode {
	pthread_mutex_t nn_lock;
	int nn_refs;
	XXX;
};

struct hdfs_rpc_invocation {
	YYY;
};

struct hdfs_object {
	AAA;
};

struct hdfs_rpc_response_future {
	pthread_mutex_t fu_lock;
	pthread_cond_t fu_cond;
	struct hdfs_object *fu_res;
};

typedef void (*hdfs_namenode_destroy_cb)(struct hdfs_namenode *);

void		hdfs_namenode_init(struct hdfs_namenode *);

const char *	hdfs_namenode_connect(struct hdfs_namenode *, const char *host, int port);
const char *	hdfs_namenode_authenticate(struct hdfs_namenode *, const char *username);
const char *	hdfs_namenode_invoke(struct hdfs_namenode *, struct hdfs_rpc_invocation *, struct hdfs_rpc_response_future *);

const char *	hdfs_future_get(struct hdfs_namenode *, struct hdfs_rpc_response_future *, struct hdfs_object **);

void		hdfs_namenode_destroy(struct hdfs_namenode *, hdfs_namenode_destroy_cb cb);

#endif
