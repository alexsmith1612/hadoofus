#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hadoofus/hadoofus.h>
#include <hadoofus/objects.h>

int
main(int argc, char **argv)
{
	const char *err,
	      *host = "localhost",
	      *port = "8020";

	struct hdfs_namenode namenode;

	struct hdfs_object *rpc;
	struct hdfs_rpc_response_future future;
	struct hdfs_object *object;

	if (argc > 1) {
		if (strcmp(argv[1], "-h") == 0) {
			printf("Usage: ./basic [host [port]]\n");
			exit(0);
		}
		host = argv[1];
		if (argc > 2)
			port = argv[2];
	}

	// Initialize the connection object and connect to the local namenode
	hdfs_namenode_init(&namenode);
	err = hdfs_namenode_connect(&namenode, host, port);
	if (err)
		goto out;

	// Pretend to be the user "mapred"
	err = hdfs_namenode_authenticate(&namenode, "mapred");
	if (err)
		goto out;

	// Call getProtocolVersion(61)
	future = HDFS_RPC_RESPONSE_FUTURE_INITIALIZER;
	rpc = hdfs_rpc_invocation_new(
	    "getProtocolVersion",
	    hdfs_string_new(HADOOFUS_CLIENT_PROTOCOL_STR),
	    hdfs_long_new(61),
	    NULL);
	err = hdfs_namenode_invoke(&namenode, rpc, &future);
	if (err)
		goto out;

	// Get the response (should be long(61))
	hdfs_future_get(&future, &object);

	if (object->ob_type == H_LONG &&
	    object->ob_val._long._val == 61L)
		printf("success\n");
	else
		printf("bad result\n");

out:
	if (err)
		fprintf(stderr, "hdfs error: %s\n", err);

	// Destroy any resources used by the connection
	hdfs_namenode_destroy(&namenode, NULL);

	return 0;
}
