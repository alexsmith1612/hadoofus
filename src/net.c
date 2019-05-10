#include <netinet/in.h>
#include <netinet/tcp.h>
#ifdef __linux__
# include <sys/sendfile.h>
#endif
#include <sys/uio.h>

#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <hadoofus/lowlevel.h>

#include "net.h"
#include "util.h"

struct hdfs_error
_connect(int *s, const char *host, const char *port)
{
	struct addrinfo *ai, *rp,
			hints = { 0 };
	int rc, sfd = -1, serrno;
	struct hdfs_error error = HDFS_SUCCESS;

	hints.ai_family = AF_INET/* hadoop is ipv4-only for now */;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICSERV | AI_ADDRCONFIG;

	rc = getaddrinfo(host, port, &hints, &ai);
	if (rc) {
		if (rc == EAI_SYSTEM)
			return error_from_errno(errno);
		return error_from_gai(rc);
	}

	for (rp = ai; rp; rp = rp->ai_next) {
		sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sfd == -1)
			continue;
		rc = connect(sfd, rp->ai_addr, rp->ai_addrlen);
		if (rc != -1)
			break;

		serrno = errno;
		close(sfd);
		sfd = -1;
		errno = serrno;
	}

	if (rp == NULL || sfd == -1)
		error = error_from_errno(errno);

	freeaddrinfo(ai);

	if (!hdfs_is_error(error)) {
		ASSERT(sfd != -1);
		_setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, 1);
		_setsockopt(sfd, SOL_SOCKET, SO_RCVBUF, 1024*1024);
		_setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, 1024*1024);
		*s = sfd;
	}

	return error;
}

struct hdfs_error
_write_all(int s, void *vbuf, size_t buflen)
{
	char *buf = vbuf;
	struct hdfs_error error = HDFS_SUCCESS;

	while (buflen > 0) {
		ssize_t w;
		w = write(s, buf, buflen);
		if (w == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				struct pollfd pfd = { .fd = s, .events = POLLOUT };
				poll(&pfd, 1, -1); // XXX check return (EINTR?) and/or revents?
				continue;
			}
			error = error_from_errno(errno);
			goto out;
		}
		buf += w;
		buflen -= w;
	}
out:
	return error;
}

struct hdfs_error
_read_to_hbuf(int s, struct hdfs_heap_buf *h)
{
	const int RESIZE_BY = 8*1024,
	      RESIZE_AT = 2*1024;

	int remain = h->size - h->used,
	    rc;

	if (remain < RESIZE_AT) {
		h->size += RESIZE_BY;
		h->buf = realloc(h->buf, h->size);
		ASSERT(h->buf);
		remain = h->size - h->used;
	}

	rc = read(s, h->buf + h->used, remain);
	if (rc == 0)
		return error_from_hdfs(HDFS_ERR_END_OF_STREAM);
	if (rc < 0)
		return error_from_errno(errno);

	h->used += rc;
	return HDFS_SUCCESS;
}

struct hdfs_error
_pread_all(int fd, void *vbuf, size_t len, off_t offset)
{
	char *buf = vbuf;
	int rc;
	while (len > 0) {
		rc = pread(fd, buf, len, offset);
		if (rc == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				struct pollfd pfd = { .fd = fd, .events = POLLIN };
				poll(&pfd, 1, -1); // XXX check return (EINTR?) and/or revents?
				continue;
			}
			return error_from_errno(errno);
		}
		if (rc == 0)
			return error_from_hdfs(HDFS_ERR_END_OF_FILE);
		len -= rc;
		buf += rc;
		offset += rc;
	}
	return HDFS_SUCCESS;
}

struct hdfs_error
_read_all(int fd, void *vbuf, size_t len)
{
	char *buf = vbuf;
	int rc;
	while (len > 0) {
		rc = read(fd, buf, len);
		if (rc == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				struct pollfd pfd = { .fd = fd, .events = POLLIN };
				poll(&pfd, 1, -1); // XXX check return (EINTR?) and/or revents?
				continue;
			}
			return error_from_errno(errno);
		}
		if (rc == 0)
			return error_from_hdfs(HDFS_ERR_END_OF_STREAM);
		len -= rc;
		buf += rc;
	}
	return HDFS_SUCCESS;
}

struct hdfs_error
_writev_all(int s, struct iovec *iov, int iovcnt)
{
	int rc = 0;
	while (iovcnt > 0) {
		if (rc >= (int)iov->iov_len) {
			rc -= iov->iov_len;
			iov++;
			iovcnt--;
			continue;
		} else if (rc > 0) {
			iov->iov_base = (char*)iov->iov_base + rc;
			iov->iov_len -= rc;
		}

		rc = writev(s, iov, iovcnt);
		if (rc == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				struct pollfd pfd = { .fd = s, .events = POLLOUT };
				poll(&pfd, 1, -1); // XXX check return (EINTR?) and/or revents?
				continue;
			}
			return error_from_errno(errno);
		}
		if (rc == 0)
			return error_from_hdfs(HDFS_ERR_END_OF_STREAM);
	}
	return HDFS_SUCCESS;
}

#if defined(__linux__)

struct hdfs_error
_sendfile_all(int s, int fd, off_t offset, size_t tosend)
{
	int rc;

	while (tosend > 0) {
		rc = sendfile(s, fd, &offset, tosend);
		if (rc == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				struct pollfd pfd = { .fd = s, .events = POLLOUT };
				poll(&pfd, 1, -1); // XXX check return (EINTR?) and/or revents?
				continue;
			}
			return error_from_errno(errno);
		}
		if (rc == 0)
			return error_from_hdfs(HDFS_ERR_END_OF_STREAM);

		tosend -= rc;
	}

	return HDFS_SUCCESS;
}

#elif defined(__FreeBSD__)

struct hdfs_error
_sendfile_all_bsd(int s, int fd, off_t offset, size_t tosend,
	struct iovec *hdrs, int hdrcnt)
{
	int rc;
	off_t sent;

	struct sf_hdtr hdtr = {
		.headers = hdrs,
		.hdr_cnt = hdrcnt,
	};

	while (hdtr.hdr_cnt > 0 || tosend > 0) {
		rc = sendfile(fd, s, offset, tosend, &hdtr, &sent, 0);
		if (rc == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				struct pollfd pfd = { .fd = s, .events = POLLOUT };
				poll(&pfd, 1, -1); // XXX check return (EINTR?) and/or revents?
				continue;
			}
			return error_from_errno(errno);
		}
		if (sent == 0)
			return error_from_hdfs(HDFS_ERR_END_OF_STREAM);

		while (hdtr.hdr_cnt > 0 && sent > 0) {
			if ((size_t)sent >= hdtr.headers->iov_len) {
				sent -= hdtr.headers->iov_len;
				hdtr.headers++;
				hdtr.hdr_cnt--;
				continue;
			} else {
				hdtr.headers->iov_base =
					(char *)hdtr.headers->iov_base + sent;
				hdtr.headers->iov_len -= sent;
				sent = 0;
			}
		}

		if (sent > 0) {
			offset += sent;
			tosend -= sent;
		}
	}

	return HDFS_SUCCESS;
}

#endif

void
_setsockopt(int s, int level, int optname, int optval)
{
	int rc;
	rc = setsockopt(s, level, optname, &optval, sizeof(optval));
	ASSERT(rc == 0);
}
