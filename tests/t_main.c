#ifdef NDEBUG
# undef NDEBUG
#endif

#include <assert.h>
#include <err.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include <check.h>

#include <hadoofus/highlevel.h>

#include "t_main.h"

const char *H_ADDR, *H_USER = "root";

static const char *
format_error(struct hdfs_error error)
{
	static char buf[1024];

	snprintf(buf, sizeof(buf), "%s:%s", hdfs_error_str_kind(error),
	    hdfs_error_str(error));
	return buf;
}

int
main(int argc, char **argv)
{
	int64_t proto_ver;
	struct hdfs_error error;
	struct hdfs_namenode *nn;
	struct hdfs_object *exception = NULL;
	bool success = true;
	Suite *(*suites[])(void) = {
		t_hl_rpc_basics_suite,
		t_hl_rpc2_basics_suite,
		t_datanode_basics_suite,
		t_datanode2_basics_suite,
		t_unit,
	};
	int rc;

	if (!getenv(HDFS_T_ENV))
		errx(EXIT_FAILURE,
		    "Please set %s to an HDFS host before running tests!",
		    HDFS_T_ENV);

	rc = sasl_client_init(NULL);
	assert(rc == SASL_OK);

	H_ADDR = strdup(getenv(HDFS_T_ENV));
	assert(H_ADDR);

	if (getenv(HDFS_T_USER)) {
		H_USER = strdup(getenv(HDFS_T_USER));
		assert(H_USER);
	}

	// Test basic connectivity
	nn = hdfs_namenode_new_version(H_ADDR, "8020", "root", HDFS_NO_KERB,
	    HDFS_NN_v1, &error);
	if (!nn)
		errx(EXIT_FAILURE,
		    "Could not connect to namenode %s: %s",
		    H_ADDR, format_error(error));

	// And verify liveness at a protocol level
	proto_ver = hdfs_getProtocolVersion(nn, HADOOFUS_CLIENT_PROTOCOL_STR,
	    61L, &exception);
	if (exception)
		errx(EXIT_FAILURE,
		    "getProtocolVersion failed: %s",
		    hdfs_exception_get_message(exception));
	if (proto_ver != 61L)
		errx(EXIT_FAILURE,
		    "Got unexpected protocol version: %ld", proto_ver);

	hdfs_namenode_delete(nn);

	// Find and run all tests
	for (size_t i = 0; i < nelem(suites); i++) {
		Suite *s = suites[i]();
		SRunner *sr = srunner_create(s);

		srunner_run_all(sr, CK_NORMAL);

		if (srunner_ntests_failed(sr) > 0)
			success = false;

		srunner_free(sr);
	}

	if (success)
		return EXIT_SUCCESS;
	return EXIT_FAILURE;
}
