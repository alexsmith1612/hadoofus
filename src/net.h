#ifndef _HADOOFUS_NET_H
#define _HADOOFUS_NET_H

#include <sys/types.h>
#include <sys/socket.h>

#include <netdb.h>

#include "heapbuf.h"

const char *	_connect(int *s, const char *host, const char *port);
const char *	_write_all(int s, void *buf, int buflen);
const char *	_read_to_hbuf(int s, struct hdfs_heap_buf *);
const char *	_pread_all(int fd, void *buf, size_t len, off_t offset);
const char *	_read_all(int fd, void *buf, size_t len);
const char *	_writev_all(int s, struct iovec *iov, int iovcnt);
#if defined(__linux__)
const char *	_sendfile_all(int s, int fd, off_t offset, size_t tosend);
#elif defined(__FreeBSD__)
const char *	_sendfile_all_bsd(int s, int fd, off_t offset size_t tosend,
		struct iovec *hdrs, int hdrcnt);
#endif
void		_setsockopt(int s, int level, int optname, int optval);

#endif
