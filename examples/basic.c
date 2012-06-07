#include <stdio.h>

#include <hadoofus/hadoofus.h>
#include <hadoofus/objects.h>

int
main(int argc, char **argv)
{
	const char *err;

	struct hdfs_namenode namenode;

	struct hdfs_object *rpc;
	struct hdfs_rpc_response_future future;
	struct hdfs_object *object;

	// Initialize the connection object and connect to the local namenode
	hdfs_namenode_init(&namenode);
	err = hdfs_namenode_connect(&namenode, "localhost", "8020");
	if (err)
		goto out;

	// Pretend to be the user "mapred"
	err = hdfs_namenode_authenticate(&namenode, "mapred");
	if (err)
		goto out;

	// Call getProtocolVersion(61)
	future = HDFS_RPC_RESPONSE_FUTURE_INITIALIZER;
	rpc = hdfs_rpc_invocation_new("getProtocolVersion", hdfs_long_new(61), NULL);
	err = hdfs_namenode_invoke(&namenode, rpc, &future);
	if (err)
		goto out;

	// Get the response (should be long(61))
	hdfs_future_get(&future, &object);

	printf("success\n");

out:
	if (err)
		fprintf(stderr, "hdfs error: %s\n", err);

	// Destroy any resources used by the connection
	hdfs_namenode_destroy(&namenode, NULL);

	return 0;
}
