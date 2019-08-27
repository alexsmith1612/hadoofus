#ifndef _HADOOFUS_UTIL_H
#define _HADOOFUS_UTIL_H

#include <sys/types.h>

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

#include <protobuf-c/protobuf-c.h>

#include <hadoofus/lowlevel.h>

#define nelem(arr) (sizeof(arr) / sizeof(arr[0]))

#ifndef __DECONST
# define __DECONST(t, v) ((t)(intptr_t)(v))
#endif
#ifndef __predict_false
# define __predict_false(exp) __builtin_expect((exp), 0)
#endif
#ifndef __predict_true
# define __predict_true(exp) __builtin_expect((exp), 1)
#endif

#define ASSERT(cond) do {					\
	if (__predict_true((intptr_t)(cond)))			\
		break;						\
								\
	assert_fail("ASSERTION `%s' FAILED in %s (%s:%u)\n",	\
	    #cond, __func__, __FILE__, __LINE__);		\
} while (false)

#define ASSERT_ERR_RANGE(kind, n) do {				\
	if (__predict_true((n) >= -(1 << (_HE_NUM_BITS - 1)) &&	\
	    (n) <= ((1 << (_HE_NUM_BITS - 1))-1)))		\
		break;						\
	assert_fail("ASSERTION " #kind "`%d' in [%d,%d) FAILED "\
	    "in %s (%s:%u)\n", (int)(n), -(1<<29), (1<<29),	\
	    __func__, __FILE__, __LINE__);			\
} while (false)

void	assert_fail(const char *fmt, ...)
	__attribute__((noreturn)) __attribute__((format(printf, 1, 2)));

#ifdef NO_EXPORT_SYMS
# define EXPORT_SYM
#else
# define EXPORT_SYM __attribute__((visibility("default")))
#endif

#define PTR_FREE(p) do { if ((p) != NULL) free(p); (p) = NULL; } while (false)

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

static inline struct hdfs_error
error_from_errno(int error)
{
	ASSERT(error != 0);

	return (struct hdfs_error) {
		.her_kind = he_errno,
		.her_num = error,
	};
}

static inline struct hdfs_error
error_from_gai(int gaicode)
{
	ASSERT(gaicode != 0);

	return (struct hdfs_error) {
		.her_kind = he_gaierr,
		.her_num = gaicode,
	};
}

static inline struct hdfs_error
error_from_hdfs(enum hdfs_error_numeric code)
{
	ASSERT((int)code != 0);

	return (struct hdfs_error) {
		.her_kind = he_hdfserr,
		.her_num = code,
	};
}

static inline struct hdfs_error
error_from_sasl(int sasl_code)
{
	ASSERT(sasl_code != SASL_OK);

	return (struct hdfs_error) {
		.her_kind = he_saslerr,
		.her_num = sasl_code,
	};
}

#endif
