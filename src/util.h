#ifndef _HADOOFUS_UTIL_H
#define _HADOOFUS_UTIL_H

static inline off_t
_min(off_t a, off_t b)
{
	if (a < b)
		return a;
	return b;
}

#endif
