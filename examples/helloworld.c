#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hadoofus/lowlevel.h>
#include <hadoofus/objects.h>

int
main(int argc, char **argv)
{
	const char
	      *host = "localhost",
	      *port = "8020";

	struct hdfs_namenode namenode;
	struct hdfs_error err;

	struct hdfs_object *rpc;
	struct hdfs_rpc_response_future *future;
	struct hdfs_object *object;

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
	err = hdfs_namenode_connect(&namenode, host, port);
	if (hdfs_is_error(err))
		goto out;

	// Pretend to be the user "mapred"
	err = hdfs_namenode_authenticate(&namenode, "mapred");
	if (hdfs_is_error(err))
		goto out;

	// Call getProtocolVersion(61)
	future = hdfs_rpc_response_future_alloc();
	hdfs_rpc_response_future_init(future);
	rpc = hdfs_rpc_invocation_new(
	    "getProtocolVersion",
	    hdfs_string_new(HADOOFUS_CLIENT_PROTOCOL_STR),
	    hdfs_long_new(61),
	    NULL);
	err = hdfs_namenode_invoke(&namenode, rpc, future);
	hdfs_object_free(rpc);
	if (hdfs_is_error(err))
		goto out;

	// Get the response (should be long(61))
	hdfs_future_get(future, &object);
	hdfs_rpc_response_future_free(&future);

	if (object->ob_type == H_LONG &&
	    object->ob_val._long._val == 61L)
		printf("success\n");
	else
		printf("bad result\n");

	hdfs_object_free(object);

out:
	if (hdfs_is_error(err))
		fprintf(stderr, "hdfs error (%s): %s\n",
		    hdfs_error_str_kind(err), hdfs_error_str(err));

	// Destroy any resources used by the connection
	hdfs_namenode_destroy(&namenode);

	return hdfs_is_error(err);
}
