#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
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

void
assert_(bool cond, const char *an, const char *fn, const char *file,
    unsigned line)
{

	if (cond)
		return;

	fprintf(stderr, "ASSERTION `%s' FAILED in %s (%s:%u)\n", an, fn, file,
	    line);
	exit(EX_SOFTWARE);
}
