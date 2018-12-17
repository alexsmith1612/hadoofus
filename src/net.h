#ifndef _HADOOFUS_NET_H
#define _HADOOFUS_NET_H

#include <sys/types.h>
#include <sys/socket.h>

#include <netdb.h>

#include "heapbuf.h"

struct hdfs_error	_connect(int *s, const char *host, const char *port);
struct hdfs_error	_write_all(int s, void *buf, int buflen);
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

#endif
