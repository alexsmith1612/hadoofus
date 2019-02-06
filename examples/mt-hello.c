#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hadoofus/lowlevel.h>
#include <hadoofus/objects.h>

void *
athread(void *v)
{
	struct hdfs_namenode *nn;

	struct hdfs_object *rpc;
	struct hdfs_object *object;

	struct hdfs_error error;

	struct hdfs_rpc_response_future *futures[100];
	unsigned i;

	bool ok;

	nn = v;

	// getProtocolVersion(61)
	rpc = hdfs_rpc_invocation_new(
	    "getProtocolVersion",
	    hdfs_string_new(HADOOFUS_CLIENT_PROTOCOL_STR),
	    hdfs_long_new(61),
	    NULL);

	for (i = 0; i < 100; i++) {
		futures[i] = hdfs_rpc_response_future_alloc();
		hdfs_rpc_response_future_init(futures[i]);
		error = hdfs_namenode_invoke(nn, rpc, futures[i]);
		if (hdfs_is_error(error)) {
			warnx("namenode_invoke: %s:%s",
			    hdfs_error_str_kind(error), hdfs_error_str(error));
			goto out;
		}
	}

	for (i = 0; i < 100; i++) {
		// Get the response (should be long(61))
		ok = hdfs_future_get_timeout(futures[i], &object, 2000/*ms*/);
		if (!ok) {
			warnx("timeout waiting for result from NN server");
			continue;
		}
		hdfs_rpc_response_future_free(&futures[i]);

		if (object->ob_type != H_LONG ||
		    object->ob_val._long._val != 61L)
			printf("bad result\n");

		hdfs_object_free(object);
	}

	// N.B., this demo leaks any futures that timed out during
	// hdfs_future_get_timeout().

out:
	hdfs_object_free(rpc);
	return NULL;
}

int
main(int argc, char **argv)
{
	const char
	      *host = "localhost",
	      *port = "8020";

	struct hdfs_namenode namenode;
	struct hdfs_error error;

#define NTHR 100
	pthread_t threads[NTHR];
	unsigned i;
	int rc;

	if (argc > 1) {
		if (strcmp(argv[1], "-h") == 0) {
			printf("Usage: ./helloworld [host [port]]\n");
			exit(0);
		}
		host = argv[1];
		if (argc > 2)
			port = argv[2];
	}

	// Initialize the connection object and connect to the local namenode
	hdfs_namenode_init(&namenode, HDFS_NO_KERB);
	error = hdfs_namenode_connect(&namenode, host, port);
	if (hdfs_is_error(error))
		goto out;

	// Pretend to be the user "root"
	error = hdfs_namenode_authenticate(&namenode, "root");
	if (hdfs_is_error(error))
		goto out;

	for (i = 0; i < NTHR; i++) {
		int rc;

		rc = pthread_create(&threads[i], NULL, athread, &namenode);
		if (rc != 0) {
			errno = rc;
			err(1, "pthread_create");
		}
	}

	for (i = 0; i < NTHR; i++) {
		rc = pthread_join(threads[i], NULL);
		if (rc != 0) {
			errno = rc;
			err(1, "pthread_join");
		}
	}

out:
	if (hdfs_is_error(error))
		fprintf(stderr, "hdfs error (%s): %s\n",
		    hdfs_error_str_kind(error), hdfs_error_str(error));

	// Destroy any resources used by the connection
	hdfs_namenode_destroy(&namenode);

	return hdfs_is_error(error);
}
