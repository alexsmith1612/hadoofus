#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hadoofus/highlevel.h>

struct hdfs_namenode *h;
const char *err = NULL,
      *host = "localhost",
      *port = "8020",
      *user = "root";

int
main(int argc, char **argv)
{
	int64_t res;
	struct hdfs_object *exception = NULL;

	if (argc > 1) {
		if (strcmp(argv[1], "-h") == 0) {
			printf("Usage: ./hl-hello [host [port [user]]]\n");
			exit(0);
		}
		host = argv[1];
		if (argc > 2) {
			port = argv[2];
			if (argc > 3) {
				user = argv[3];
			}
		}
	}

	h = hdfs_namenode_new(host, port, user, &err);
	if (!h)
		goto out;

	res = hdfs_namenode_getProtocolVersion(h, HADOOFUS_CLIENT_PROTOCOL_STR, 61L,
	    &exception);
	if (exception) {
		err = exception->ob_val._exception._msg;
		goto out;
	}

	if (res != 61)
		fprintf(stderr, "protocol version != 61: %ld\n", res);
	else
		fprintf(stderr, "success\n");

out:
	if (err)
		fprintf(stderr, "hadoofus error: %s\n", err);
	if (h)
		hdfs_namenode_delete(h);
	return 0;
}
