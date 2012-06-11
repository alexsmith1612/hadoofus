#include <stdio.h>
#include <stdlib.h>

#include <hadoofus/highlevel.h>

static inline void
_assert_not_err(const char *err)
{
	if (err) {
		fprintf(stderr, "libhadoofus: Got error, bailing: %s\n", err);
		assert(!err);
	}
}

struct hdfs_namenode *
hdfs_namenode_new(const char *host, const char *port, const char *username,
	const char **error_out)
{
	const char *err;
	struct hdfs_namenode *h;

	h = malloc(sizeof *h);
	assert(h);

	hdfs_namenode_init(h);
	err = hdfs_namenode_connect(h, host, port);
	if (err)
		goto out;

	err = hdfs_namenode_authenticate(h, username);
	if (err)
		goto out;

out:
	if (err) {
		hdfs_namenode_delete(h);
		h = NULL;
		*error_out = err;
	}
	return h;
}

void
hdfs_namenode_delete(struct hdfs_namenode *h)
{
	hdfs_namenode_destroy(h, (hdfs_namenode_destroy_cb)free);
}

int64_t
hdfs_namenode_getProtocolVersion(struct hdfs_namenode *h, const char *protocol,
	int64_t client_version, struct hdfs_object **exception_out)
{
	struct hdfs_rpc_response_future future = HDFS_RPC_RESPONSE_FUTURE_INITIALIZER;
	struct hdfs_object *rpc, *object;
	const char *err;

	rpc = hdfs_rpc_invocation_new(
	    "getProtocolVersion",
	    hdfs_string_new(protocol),
	    hdfs_long_new(client_version),
	    NULL);
	err = hdfs_namenode_invoke(h, rpc, &future);
	_assert_not_err(err);

	hdfs_future_get(&future, &object);

	assert(object->ob_type == H_LONG ||
	    object->ob_type == H_PROTOCOL_EXCEPTION);

	if (object->ob_type == H_PROTOCOL_EXCEPTION) {
		*exception_out = object;
		return 0;
	} else {
		return object->ob_val._long._val;
	}
}
