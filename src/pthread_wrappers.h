#ifndef _PTHREAD_WRAPPERS_H
#define _PTHREAD_WRAPPERS_H

#ifdef NDEBUG
# undef NDEBUG
#endif

#include <assert.h>
#include <pthread.h>


static inline void
_lock(pthread_mutex_t *l)
{
	int rc;
	rc = pthread_mutex_lock(l);
	assert(rc == 0);
}

static inline void
_unlock(pthread_mutex_t *l)
{
	int rc;
	rc = pthread_mutex_unlock(l);
	assert(rc == 0);
}

static inline void
_wait(pthread_mutex_t *l, pthread_cond_t *c)
{
	int rc;
	rc = pthread_cond_wait(c, l);
	assert(rc == 0);
}

static inline void
_notifyall(pthread_cond_t *c)
{
	int rc;
	rc = pthread_cond_broadcast(c);
	assert(rc == 0);
}

#endif
