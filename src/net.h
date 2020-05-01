#ifndef _HADOOFUS_NET_H
#define _HADOOFUS_NET_H

#include <sys/types.h>
#include <sys/socket.h>

#include <netdb.h>

#include "heapbuf.h"

struct hdfs_conn_ctx;

struct hdfs_error	_hdfs_connect_init(int *s, const char *host, const char *port,
			struct hdfs_conn_ctx *cctx, bool numerichost);
struct hdfs_error	_hdfs_connect_finalize(int *s, struct hdfs_conn_ctx *cctx);
struct hdfs_error	_hdfs_write(int s, const void *vbuf, size_t buflen, ssize_t *wlen);
struct hdfs_error	_hdfs_writev(int s, const struct iovec *iov, int iovcnt, ssize_t *wlen);
struct hdfs_error	_hdfs_read_to_hbuf(int s, struct hdfs_heap_buf *);
struct hdfs_error	_hdfs_pread_all(int fd, void *buf, size_t len, off_t offset);
struct hdfs_error	_hdfs_pwrite_all(int fd, const void *vbuf, size_t len, off_t offset);
#if defined(__linux__)
struct hdfs_error	_hdfs_sendfile(int s, int fd, off_t offset, size_t tosend, ssize_t *wlen);
#elif defined(__FreeBSD__)
struct hdfs_error	_hdfs_sendfile_bsd(int s, int fd, off_t offset, size_t tosend,
			struct iovec *hdrs, int hdrcnt, ssize_t *wlen);
#endif
void		_hdfs_setsockopt(int s, int level, int optname, int optval);
void		_hdfs_getsockopt(int s, int level, int optname, int *optval);
void		_hdfs_set_nbio(int fd);
void		_hdfs_conn_ctx_free(struct hdfs_conn_ctx *cctx);

#endif // _HADOOFUS_NET_H
