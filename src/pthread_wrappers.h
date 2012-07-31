#ifndef _PTHREAD_WRAPPERS_H
#define _PTHREAD_WRAPPERS_H

#ifdef NDEBUG
# undef NDEBUG
#endif

#include <assert.h>
#include <pthread.h>

void	_lock(pthread_mutex_t *l);
void	_unlock(pthread_mutex_t *l);
void	_wait(pthread_mutex_t *l, pthread_cond_t *c);
void	_notifyall(pthread_cond_t *c);

#endif
