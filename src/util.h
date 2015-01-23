#ifndef _HADOOFUS_UTIL_H
#define _HADOOFUS_UTIL_H

#include <sys/types.h>

#include <stdint.h>
#include <time.h>

#include <protobuf-c/protobuf-c.h>

#define nelem(arr) (sizeof(arr) / sizeof(arr[0]))

#ifndef __DECONST
# define __DECONST(t, v) ((t)(intptr_t)(v))
#endif

#define ASSERT(cond) do {					\
	if ((intptr_t)(cond))					\
		break;						\
								\
	assert_fail(#cond, __func__, __FILE__, __LINE__);	\
} while (false)

void	assert_fail(const char *an, const char *fn, const char *file, unsigned line)
	__attribute__((noreturn));

#ifdef NO_EXPORT_SYMS
# define EXPORT_SYM
#else
# define EXPORT_SYM __attribute__((visibility("default")))
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

uint64_t	_now_ms(void);

char *		_proto_str(ProtobufCBinaryData);

static inline void
_ms_to_tspec(uint64_t ms, struct timespec *ts)
{

	ts->tv_sec = ms / 1000;
	ts->tv_nsec = (ms % 1000) * 1000*1000;
}

#endif
