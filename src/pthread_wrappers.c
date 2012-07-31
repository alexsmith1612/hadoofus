#include "pthread_wrappers.h"

void
_lock(pthread_mutex_t *l)
{
	int rc;
	rc = pthread_mutex_lock(l);
	assert(rc == 0);
}

void
_unlock(pthread_mutex_t *l)
{
	int rc;
	rc = pthread_mutex_unlock(l);
	assert(rc == 0);
}

void
_wait(pthread_mutex_t *l, pthread_cond_t *c)
{
	int rc;
	rc = pthread_cond_wait(c, l);
	assert(rc == 0);
}

void
_notifyall(pthread_cond_t *c)
{
	int rc;
	rc = pthread_cond_broadcast(c);
	assert(rc == 0);
}
