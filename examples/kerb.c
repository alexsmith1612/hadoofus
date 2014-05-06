#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hadoofus/highlevel.h>

struct hdfs_namenode *h;
const char *err = NULL,
      *host = "localhost",
      *port = "8020",
      *user = "hdfs/host.name";

int
main(int argc, char **argv)
{
	int r;
	int64_t res;
	struct hdfs_object *exception = NULL, *dl;

	r = sasl_client_init(NULL);
	if (r != SASL_OK) {
		fprintf(stderr, "Error initializing sasl: %d\n", r);
		return -1;
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

	h = hdfs_namenode_new(host, port, user, HDFS_REQUIRE_KERB, &err);
	if (!h)
		goto out;

	res = hdfs_getProtocolVersion(h, HADOOFUS_CLIENT_PROTOCOL_STR, 61L,
	    &exception);
	if (exception) {
		err = exception->ob_val._exception._msg;
		goto out;
	}

	if (res != 61)
		fprintf(stderr, "protocol version != 61: %zd\n", (intmax_t) res);
	else
		fprintf(stderr, "success\n");

	dl = hdfs_getListing(h, "/", NULL, &exception);
	if (exception) {
		err = exception->ob_val._exception._msg;
		goto out;
	}

	hdfs_object_free(dl);
	fprintf(stderr, "dl: success\n");

out:
	if (err)
		fprintf(stderr, "hadoofus error: %s\n", err);
	if (h)
		hdfs_namenode_delete(h);
	sasl_done();
	return 0;
}
