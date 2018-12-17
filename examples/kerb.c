#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hadoofus/highlevel.h>

struct hdfs_namenode *h;
const char
      *host = "localhost",
      *port = "8020",
      *user = "hdfs/host.name";

int
main(int argc, char **argv)
{
	int r, rc = 1;
	int64_t res;
	struct hdfs_object *exception = NULL, *dl;
	struct hdfs_error error;

	r = sasl_client_init(NULL);
	if (r != SASL_OK) {
		fprintf(stderr, "Error initializing sasl: %d\n", r);
		return 2;
	}

	if (argc > 1) {
		if (strcmp(argv[1], "-h") == 0) {
			printf("Usage: ./kerb [host [port [kerb_principal]]]\n");
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

	h = hdfs_namenode_new_version(host, port, user, HDFS_REQUIRE_KERB,
	    HDFS_NN_v1, &error);
	if (h == NULL) {
		fprintf(stderr, "Error connecting to namenode (%s): %s\n",
		    hdfs_error_str_kind(error), hdfs_error_str(error));
		goto out;
	}

	res = hdfs_getProtocolVersion(h, HADOOFUS_CLIENT_PROTOCOL_STR, 61L,
	    &exception);
	if (exception != NULL)
		goto out;

	if (res != 61)
		fprintf(stderr, "protocol version != 61: %jd\n", (intmax_t)res);
	else
		fprintf(stderr, "success\n");

	dl = hdfs_getListing(h, "/", NULL, &exception);
	if (exception != NULL)
		goto out;

	hdfs_object_free(dl);
	fprintf(stderr, "dl: success\n");

	rc = 0;

out:
	if (exception != NULL) {
		fprintf(stderr, "Exception raised: %s: %s\n",
		    hdfs_etype_to_string(exception->ob_val._exception._etype),
		    exception->ob_val._exception._msg);
		hdfs_object_free(exception);
	}
	if (h != NULL)
		hdfs_namenode_delete(h);
	sasl_done();
	return rc;
}
