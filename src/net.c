#ifdef NDEBUG
# undef NDEBUG
#endif

#include <assert.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

#include "net.h"

const char *
_connect(int *s, const char *host, const char *port)
{
	struct addrinfo *ai, *rp,
			hints = { 0 };
	int rc, sfd = -1, serrno;
	const char *err = NULL;

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
		err = strerror(errno);

	freeaddrinfo(ai);

	if (!err) {
		assert(sfd != -1);
		// TODO prepare socket (increase sockbuf, etc, etc)
		*s = sfd;
	}

	return err;
}

const char *
_write_all(int s, char *buf, int buflen)
{
	const char *err = NULL;
	while (buflen > 0) {
		ssize_t w;
		w = write(s, buf, buflen);
		if (w == -1) {
			err = strerror(errno);
			goto out;
		}
		buf += w;
		buflen -= w;
	}
out:
	return err;
}
