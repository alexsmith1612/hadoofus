#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hadoofus/highlevel.h>

struct hdfs_namenode *h;
const char
      *host = "localhost",
      *port = "8020",
      *user = "root";

int
main(int argc, char **argv)
{
	int64_t res;
	int rc = 1;
	struct hdfs_error error;
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

	h = hdfs_namenode_new_version(host, port, user, HDFS_NO_KERB,
	    HDFS_NN_v1, &error);
	if (h == NULL) {
		fprintf(stderr, "Error connecting to namenode (%s): %s\n",
		    hdfs_error_str_kind(error), hdfs_error_str(error));
		goto out;
	}

	res = hdfs_getProtocolVersion(h, HADOOFUS_CLIENT_PROTOCOL_STR, 61L,
	    &exception);
	if (exception) {
		fprintf(stderr, "Exception from getProtocolVersion: %s: %s\n",
		    hdfs_etype_to_string(exception->ob_val._exception._etype),
		    exception->ob_val._exception._msg);
		goto out;
	}

	if (res != 61)
		fprintf(stderr, "protocol version != 61: %jd\n", (intmax_t)res);
	else {
		fprintf(stderr, "success\n");
		rc = 0;
	}

out:
	if (h != NULL)
		hdfs_namenode_delete(h);
	return rc;
}
