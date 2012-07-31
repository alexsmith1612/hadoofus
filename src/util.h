#ifndef _HADOOFUS_UTIL_H
#define _HADOOFUS_UTIL_H

#include <sys/types.h>

#include <stdint.h>

#define nelem(arr) (sizeof(arr) / sizeof(arr[0]))

#ifndef __DECONST
# define __DECONST(t, v) ((t)(intptr_t)(v))
#endif

static inline off_t
_min(off_t a, off_t b)
{
	if (a < b)
		return a;
	return b;
}

uint32_t	_be32dec(void *);
void		_be32enc(void *, uint32_t);

#endif
