#include <netinet/in.h>
#include <netinet/tcp.h>
#ifdef __linux__
# include <sys/sendfile.h>
#endif
#include <sys/uio.h>

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <hadoofus/lowlevel.h>

#include "net.h"
#include "util.h"


void
hdfs_conn_ctx_free(struct hdfs_conn_ctx *cctx)
{
	if (cctx->ai)
		freeaddrinfo(cctx->ai);
	memset(cctx, 0, sizeof(*cctx));
}

static void
_connect_success(int *s, struct hdfs_conn_ctx *cctx)
{
	_setsockopt(*s, IPPROTO_TCP, TCP_NODELAY, 1);
	_setsockopt(*s, SOL_SOCKET, SO_RCVBUF, 1024*1024);
	_setsockopt(*s, SOL_SOCKET, SO_SNDBUF, 1024*1024);
	hdfs_conn_ctx_free(cctx);
}

static struct hdfs_error
_connect_attempt(int *s, struct hdfs_conn_ctx *cctx)
{
	struct hdfs_error error = HDFS_SUCCESS;
	int rc, sfd = -1;

	for (/* already initialized */; cctx->rp; cctx->rp = cctx->rp->ai_next) {
		sfd = socket(cctx->rp->ai_family, cctx->rp->ai_socktype, cctx->rp->ai_protocol);
		if (sfd == -1)
			continue;
		_set_nbio(sfd);
		rc = connect(sfd, cctx->rp->ai_addr, cctx->rp->ai_addrlen);
		if (rc != -1) // unusual to get immediate connect on non-blocking TCP socket
			break;

		if (errno == EINPROGRESS) {
			*s = sfd;
			error = HDFS_AGAIN;
			goto out;
		}

		cctx->serrno = errno;
		close(sfd);
		sfd = -1;
	}

	if (cctx->rp == NULL || sfd == -1) {
		error = error_from_errno(cctx->serrno);
		hdfs_conn_ctx_free(cctx);
		goto out;
	}

	ASSERT(sfd != -1);
	*s = sfd;
	_connect_success(s, cctx);

out:
	return error;
}

// Note that this function may still block on getaddrinfo() if host is
// a hostname and not a numeric IP address.
struct hdfs_error
_connect_init(int *s, const char *host, const char *port,
	struct hdfs_conn_ctx *cctx, bool numerichost)
{
	int rc;
	struct addrinfo hints = { 0 };
	struct hdfs_error error;

	hints.ai_family = AF_INET/* hadoop is ipv4-only for now */;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICSERV | AI_ADDRCONFIG;
	if (numerichost)
		hints.ai_flags |= AI_NUMERICHOST;

	rc = getaddrinfo(host, port, &hints, &cctx->ai);
	if (rc) {
		if (rc == EAI_SYSTEM)
			return error_from_errno(errno);
		return error_from_gai(rc);
	}

	cctx->rp = cctx->ai;
	cctx->serrno = 0;
	error = _connect_attempt(s, cctx);

	return error;
}

struct hdfs_error
_connect_finalize(int *s, struct hdfs_conn_ctx *cctx)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct pollfd pfd = { 0 };
	int rc;

	ASSERT(s);
	ASSERT(*s != -1);

	pfd.fd = *s;
	pfd.events = POLLOUT;

	rc = poll(&pfd, 1, 0);
	if (rc == 0 || (rc == -1 && errno == EINTR)) {
		error = HDFS_AGAIN;
		goto out;
	}
	ASSERT(rc != -1);

	_getsockopt(*s, SOL_SOCKET, SO_ERROR, &cctx->serrno);
	if (cctx->serrno != 0) { // connection failed, close and try a new addrinfo
		close(*s);
		*s = -1;
		cctx->rp = cctx->rp->ai_next;
		error = _connect_attempt(s, cctx);
		goto out;
	}

	// connection succeeded
	_connect_success(s, cctx);

out:
	return error;
}

// XXX blocking. This should be removed once non-blocking namenodes have been implemented
struct hdfs_error
_connect(int *s, const char *host, const char *port)
{
	struct hdfs_error error = HDFS_SUCCESS;
	struct hdfs_conn_ctx cctx = { 0 };

	error = _connect_init(s, host, port, &cctx, false);

	while (hdfs_is_again(error)) {
		struct pollfd pfd = { .fd = *s, .events = POLLOUT };
		ASSERT(*s != -1);
		poll(&pfd, 1, -1); // XXX check return (EINTR?) and/or revents?
		error = _connect_finalize(s, &cctx);
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

// If complete write, returns HDFS_SUCCESS and sets *wlen to buflen.
// If short write (including EAGAIN/EWOULDBLOCK), returns HDFS_AGAIN and
// sets *wlen to number of bytes written (which is 0 for EAGAIN/EWOULDBLOCK).
// If error, returns error_from_errno() and sets *wlen to -1.
struct hdfs_error
_write(int s, void *vbuf, size_t buflen, ssize_t *wlen)
{
	struct hdfs_error error = HDFS_SUCCESS;
	char *buf = vbuf;

	*wlen = write(s, buf, buflen);
	if (*wlen == -1) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			*wlen = 0; // hide the -1
			error = HDFS_AGAIN;
		} else {
			error = error_from_errno(errno);
		}
	} else if ((size_t)*wlen < buflen) {
		error = HDFS_AGAIN;
	}

	return error;
}

struct hdfs_error
_writev(int s, struct iovec *iov, int iovcnt, ssize_t *wlen)
{
	struct hdfs_error error = HDFS_SUCCESS;
	ssize_t totlen = 0;

	*wlen = writev(s, iov, iovcnt);
	if (*wlen == -1) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			*wlen = 0; // hide the -1
			error = HDFS_AGAIN;
		} else {
			error = error_from_errno(errno);
		}
	} else {
		for (int i = 0; i < iovcnt; i++) {
			totlen += iov[i].iov_len;
			if (*wlen < totlen) {
				error = HDFS_AGAIN;
				break;
			}
		}
	}

	return error;
}

struct hdfs_error
_read_to_hbuf(int s, struct hdfs_heap_buf *h)
{
	const int RESIZE_BY = 8*1024,
	      RESIZE_AT = 2*1024;

	int rc;

	_hbuf_resize(h, RESIZE_AT, RESIZE_BY);

	rc = read(s, _hbuf_writeptr(h), _hbuf_remsize(h));
	if (rc == 0)
		return error_from_hdfs(HDFS_ERR_END_OF_STREAM);
	if (rc < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return HDFS_AGAIN;
		}
		return error_from_errno(errno);
	}

	_hbuf_append(h, rc);
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
_pwrite_all(int fd, const void *vbuf, size_t len, off_t offset)
{
	const char *buf = vbuf;
	ssize_t wlen = 0;

	while (len > 0) {
		wlen = pwrite(fd, buf, len, offset);
		if (wlen == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				struct pollfd pfd = { .fd = fd, .events = POLLOUT };
				poll(&pfd, 1, -1); // XXX check return (EINTR?) and/or revents?
				continue;
			}
			return error_from_errno(errno);
		}
		if (wlen == 0)
			return error_from_hdfs(HDFS_ERR_END_OF_FILE);
		len -= wlen;
		buf += wlen;
		offset += wlen;
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

// XXX note that this can still block on reading from the fd
struct hdfs_error
_sendfile(int s, int fd, off_t offset, size_t tosend, ssize_t *wlen)
{
	struct hdfs_error error = HDFS_SUCCESS;

	*wlen = sendfile(s, fd, &offset, tosend);
	if (*wlen == -1) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			*wlen = 0; // hide the -1
			error = HDFS_AGAIN;
		} else {
			error = error_from_errno(errno);
		}
	} else if ((size_t)*wlen < tosend) {
		error = HDFS_AGAIN;
	}

	return error;
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

// XXX note that this can still block on reading from the fd
struct hdfs_error
_sendfile_bsd(int s, int fd, off_t offset, size_t tosend,
	struct iovec *hdrs, int hdrcnt, ssize_t *wlen)
{
	struct hdfs_error error = HDFS_SUCCESS;
	int rc;
	off_t sent = 0;
	ssize_t totlen = tosend;

	struct sf_hdtr hdtr = {
		.headers = hdrs,
		.hdr_cnt = hdrcnt,
	};

	rc = sendfile(fd, s, offset, tosend, &hdtr, &sent, 0); // not worrying about SF_NODISKIO/EBUSY
	*wlen = sent;
	if (rc == -1) {
		// it shouldn't return EWOULDBLOCK instead of EAGAIN, but check both anyways
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			error = HDFS_AGAIN;
		} else {
			// XXX there are some cases where sent may be significant here, e.g. EINTR/EBUSY
			*wlen = -1;
			error = error_from_errno(errno);
		}
	} else {
		for (int i = 0; i < hdrcnt; i++) {
			totlen += hdrs[i].iov_len;
			if (sent < totlen) {
				error = HDFS_AGAIN;
				break;
			}
		}
	}

	return error;
}

#endif

void
_setsockopt(int s, int level, int optname, int optval)
{
	int rc;
	rc = setsockopt(s, level, optname, &optval, sizeof(optval));
	ASSERT(rc == 0);
}

void
_getsockopt(int s, int level, int optname, int *optval)
{
	int rc;
	socklen_t optlen = sizeof(*optval);
	rc = getsockopt(s, level, optname, optval, &optlen);
	ASSERT(rc == 0);
	ASSERT(optlen == sizeof(*optval));
}

void
_set_nbio(int fd)
{
	int rc, flags;
	flags = fcntl(fd, F_GETFL);
	ASSERT(flags != -1);

	flags |= O_NONBLOCK;
	rc = fcntl(fd, F_SETFL, flags);
	ASSERT(rc == 0);
}
