#include <errno.h>
#include <stdbool.h>

#include <hadoofus/lowlevel.h>

#include "pthread_wrappers.h"
#include "util.h"

void
_mtx_init(pthread_mutex_t *l)
{
	int rc;
	rc = pthread_mutex_init(l, NULL);
	ASSERT(rc == 0);
}

void
_mtx_destroy(pthread_mutex_t *l)
{
	int rc;
	rc = pthread_mutex_destroy(l);
	ASSERT(rc == 0);
}

void
_cond_init(pthread_cond_t *c)
{
	int rc;
	rc = pthread_cond_init(c, NULL);
	ASSERT(rc == 0);
}

void
_cond_destroy(pthread_cond_t *c)
{
	int rc;
	rc = pthread_cond_destroy(c);
	ASSERT(rc == 0);
}

void
_lock(pthread_mutex_t *l)
{
	int rc;
	rc = pthread_mutex_lock(l);
	ASSERT(rc == 0);
}

void
_unlock(pthread_mutex_t *l)
{
	int rc;
	rc = pthread_mutex_unlock(l);
	ASSERT(rc == 0);
}

void
_wait(pthread_mutex_t *l, pthread_cond_t *c)
{
	int rc;
	rc = pthread_cond_wait(c, l);
	ASSERT(rc == 0);
}

void
_waitlimit(pthread_mutex_t *l, pthread_cond_t *c, uint64_t absms)
{
	struct timespec abstime;
	int rc;

	_ms_to_tspec(absms, &abstime);
	rc = pthread_cond_timedwait(c, l, &abstime);
	ASSERT(rc == 0 || rc == ETIMEDOUT);
}

void
_notifyall(pthread_cond_t *c)
{
	int rc;
	rc = pthread_cond_broadcast(c);
	ASSERT(rc == 0);
}
