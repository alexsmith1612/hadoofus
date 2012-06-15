#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <hadoofus/highlevel.h>

#include "net.h"
#include "pthread_wrappers.h"

//
// high-level api
//

struct hdfs_datanode *
hdfs_datanode_new(struct hdfs_object *located_block, const char **error_out)
{
	const char *err = "LocatedBlock has zero datanodes";
	struct hdfs_datanode *d = malloc(sizeof *d);
	int32_t n;

	assert(d);
	assert(located_block);
	assert(located_block->ob_type == H_LOCATED_BLOCK);

	hdfs_datanode_init(d);

	n = located_block->ob_val._located_block._num_locs;
	for (int32_t i = 0; i < n; i++) {
		struct hdfs_object *di =
		    located_block->ob_val._located_block._locs[i];
		err = hdfs_datanode_connect(d,
		    di->ob_val._datanode_info._hostname,
		    di->ob_val._datanode_info._port);
		if (!err)
			return d;
	}

	hdfs_datanode_destroy(d);
	free(d);
	*error_out = err;
	return NULL;
}

void
hdfs_datanode_delete(struct hdfs_datanode *d)
{
	hdfs_datanode_destroy(d);
	free(d);
}

//
// low-level api
//

void
hdfs_datanode_init(struct hdfs_datanode *d)
{
	d->dn_lock = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
	d->dn_sock = -1;
	d->dn_used = false;
}

const char *
hdfs_datanode_connect(struct hdfs_datanode *d, const char *host, const char *port)
{
	const char *err;

	assert(d);

	_lock(&d->dn_lock);

	assert(d->dn_sock == -1);
	err = _connect(&d->dn_sock, host, port);

	_unlock(&d->dn_lock);

	return err;
}

const char *	hdfs_datanode_write(struct hdfs_datanode *, void *buf,
		size_t len, bool sendcrcs);

const char *	hdfs_datanode_write_file(struct hdfs_datanode *, int fd,
		off_t len, off_t offset, bool sendcrcs);

const char *	hdfs_datanode_read(struct hdfs_datanode *, void *buf, size_t len,
		bool verifycrc);

const char *	hdfs_datanode_read_file(struct hdfs_datanode *, int fd,
		off_t len, off_t offset, bool verifycrc);

void
hdfs_datanode_destroy(struct hdfs_datanode *d)
{
	assert(d);

	_lock(&d->dn_lock);
	if (d->dn_sock != -1)
		close(d->dn_sock);
	_unlock(&d->dn_lock);

	memset(d, 0, sizeof *d);
}
