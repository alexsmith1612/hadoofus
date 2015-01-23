#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sysexits.h>

#include "util.h"

uint32_t
_be32dec(void *void_p)
{
	uint8_t *p = void_p;
	uint32_t res;

	res =
	    ((uint32_t)p[0] << 24) |
	    ((uint32_t)p[1] << 16) |
	    ((uint32_t)p[2] << 8) |
	    (uint32_t)p[3];

	return res;
}

void
_be32enc(void *void_p, uint32_t v)
{
	uint8_t *p = void_p;

	p[0] = (uint8_t)(v >> 24);
	p[1] = (uint8_t)((v >> 16) & 0xff);
	p[2] = (uint8_t)((v >> 8) & 0xff);
	p[3] = (uint8_t)(v & 0xff);
}

uint64_t
_now_ms(void)
{
	struct timespec ts;
	int rc;

	rc = clock_gettime(CLOCK_REALTIME, &ts);
	ASSERT(rc == 0);

	return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / (1000*1000);
}

void
assert_fail(const char *an, const char *fn, const char *file, unsigned line)
{

	fprintf(stderr, "ASSERTION `%s' FAILED in %s (%s:%u)\n", an, fn, file,
	    line);
	exit(EX_SOFTWARE);
}

char *
_proto_str(ProtobufCBinaryData blob)
{
	char *res;

	res = malloc(blob.len + 1);
	ASSERT(res);

	memcpy(res, blob.data, blob.len);
	res[blob.len] = '\0';

	/* No embedded NULs */
	ASSERT(strlen(res) == blob.len);
	return res;
}
