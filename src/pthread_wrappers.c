#include <stdbool.h>

#include "pthread_wrappers.h"
#include "util.h"

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
_notifyall(pthread_cond_t *c)
{
	int rc;
	rc = pthread_cond_broadcast(c);
	ASSERT(rc == 0);
}
