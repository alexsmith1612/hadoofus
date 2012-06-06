#ifndef _HADOOFUS_NET_H
#define _HADOOFUS_NET_H

#include <sys/types.h>
#include <sys/socket.h>

#include <netdb.h>

const char *	_connect(int *s, const char *host, const char *port);
const char *	_write_all(int s, char *buf, int buflen);

#endif
