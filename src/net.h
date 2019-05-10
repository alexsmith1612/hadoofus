#ifndef _HADOOFUS_NET_H
#define _HADOOFUS_NET_H

#include <sys/types.h>
#include <sys/socket.h>

#include <netdb.h>

#include "heapbuf.h"

struct hdfs_conn_ctx {
	struct addrinfo  *ai;
	struct addrinfo  *rp;
	int serrno;
};

struct hdfs_error	_connect(int *s, const char *host, const char *port);
struct hdfs_error	_connect_init(int *s, const char *host, const char *port,
			struct hdfs_conn_ctx *cctx, bool numerichost);
struct hdfs_error	_connect_finalize(int *s, struct hdfs_conn_ctx *cctx);
struct hdfs_error	_write_all(int s, void *buf, size_t buflen);
struct hdfs_error	_read_to_hbuf(int s, struct hdfs_heap_buf *);
struct hdfs_error	_pread_all(int fd, void *buf, size_t len, off_t offset);
struct hdfs_error	_read_all(int fd, void *buf, size_t len);
struct hdfs_error	_writev_all(int s, struct iovec *iov, int iovcnt);
#if defined(__linux__)
struct hdfs_error	_sendfile_all(int s, int fd, off_t offset, size_t tosend);
#elif defined(__FreeBSD__)
struct hdfs_error	_sendfile_all_bsd(int s, int fd, off_t offset, size_t tosend,
			struct iovec *hdrs, int hdrcnt);
#endif
void		_setsockopt(int s, int level, int optname, int optval);
void		_getsockopt(int s, int level, int optname, int *optval);
void		_set_nbio(int fd);
void		hdfs_conn_ctx_free(struct hdfs_conn_ctx *cctx);

#endif // _HADOOFUS_NET_H
