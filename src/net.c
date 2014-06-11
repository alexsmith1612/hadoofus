#include <netinet/tcp.h>
#ifdef __linux__
# include <sys/sendfile.h>
#endif
#include <sys/uio.h>

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "net.h"
#include "util.h"

const char *
_connect(int *s, const char *host, const char *port)
{
	struct addrinfo *ai, *rp,
			hints = { 0 };
	int rc, sfd = -1, serrno;
	const char *error = NULL;

	hints.ai_family = AF_INET/* hadoop is ipv4-only for now */;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICSERV | AI_ADDRCONFIG;

	rc = getaddrinfo(host, port, &hints, &ai);
	if (rc) {
		if (rc == EAI_SYSTEM)
			return strerror(errno);
		return gai_strerror(rc);
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
		error = strerror(errno);

	freeaddrinfo(ai);

	if (!error) {
		ASSERT(sfd != -1);
		_setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, 1);
		_setsockopt(sfd, SOL_SOCKET, SO_RCVBUF, 1024*1024);
		_setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, 1024*1024);
		*s = sfd;
	}

	return error;
}

const char *
_write_all(int s, void *vbuf, int buflen)
{
	char *buf = vbuf;
	const char *error = NULL;
	while (buflen > 0) {
		ssize_t w;
		w = write(s, buf, buflen);
		if (w == -1) {
			error = strerror(errno);
			goto out;
		}
		buf += w;
		buflen -= w;
	}
out:
	return error;
}

const char *
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
		return "EOS";
	if (rc < 0)
		return strerror(errno);

	h->used += rc;
	return NULL;
}

const char *
_pread_all(int fd, void *vbuf, size_t len, off_t offset)
{
	char *buf = vbuf;
	int rc;
	while (len > 0) {
		rc = pread(fd, buf, len, offset);
		if (rc == -1)
			return strerror(errno);
		if (rc == 0)
			return "EOF reading from fd; bailing";
		len -= rc;
		buf += rc;
		offset += rc;
	}
	return NULL;
}

const char *
_read_all(int fd, void *vbuf, size_t len)
{
	char *buf = vbuf;
	int rc;
	while (len > 0) {
		rc = read(fd, buf, len);
		if (rc == -1)
			return strerror(errno);
		if (rc == 0)
			return "EOF reading from fd; bailing";
		len -= rc;
		buf += rc;
	}
	return NULL;
}

const char *
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
		if (rc == -1)
			return strerror(errno);
		if (rc == 0)
			return "EOS writing packet; aborting write";
	}
	return NULL;
}

#if defined(__linux__)

const char *
_sendfile_all(int s, int fd, off_t offset, size_t tosend)
{
	int rc;

	while (tosend > 0) {
		rc = sendfile(s, fd, &offset, tosend);
		if (rc == -1)
			return strerror(errno);
		if (rc == 0)
			return "EOS writing packet data; aborting write";

		tosend -= rc;
	}

	return NULL;
}

#elif defined(__FreeBSD__)

const char *
_sendfile_all_bsd(int s, int fd, off_t offset size_t tosend,
	struct iovec *hdrs, int hdrcnt)
{
	int rc;
	off_t sent;

	struct sf_hdtr hdtr = {
		.headers = hdrs;
		.hdr_cnt = hdrcnt;
	};

	while (hdtr.hdr_cnt > 0 || tosend > 0) {
		rc = sendfile(fd, s, offset, tosend, &hdtr, &sent, 0);
		if (rc == -1)
			return strerror(errno);
		if (sent == 0)
			return "EOS writing packet data; aborting write";

		while (hdtr.hdr_cnt > 0 && sent > 0) {
			if (sent >= hdtr.headers->iov_len) {
				sent -= hdtr.headers->iov_len;
				hdtr.headers++;
				hdtr.hdr_cnt--;
				continue;
			} else {
				hdtr.headers->iov_base += sent;
				hdtr.headers->iov_len -= sent;
				sent = 0;
			}
		}

		if (sent > 0) {
			offset += sent;
			tosend -= sent;
		}
	}

	return NULL;
}

#endif

void
_setsockopt(int s, int level, int optname, int optval)
{
	int rc;
	rc = setsockopt(s, level, optname, &optval, sizeof(optval));
	ASSERT(rc == 0);
}
