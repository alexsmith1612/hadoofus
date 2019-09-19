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

const char *H_ADDR, *H_USER = "root", *H_PORT = "8020";
enum hdfs_namenode_proto H_VER = _HDFS_NN_vLATEST; // XXX consider default version
enum hdfs_kerb H_KERB = HDFS_NO_KERB;

static const char *
format_error(struct hdfs_error error)
{
	static char buf[1024];

	snprintf(buf, sizeof(buf), "%s:%s", hdfs_error_str_kind(error),
	    hdfs_error_str(error));
	return buf;
}

int
main()
{
	struct hdfs_error error;
	struct hdfs_namenode *nn;
	char *ver, *kerb;
	bool success = true;
	Suite *(* suites[])(void) = {
		t_unit,
		t_hl_rpc_basics_suite,
		t_namenode_nb_suite,
		t_datanode_basics_suite
	};
	int rc;

#if 0
	int64_t proto_ver;
	struct hdfs_object *exception = NULL;
#endif

	if (!getenv(HDFS_T_ADDR))
		errx(EXIT_FAILURE,
		    "Please set %s to an HDFS host before running tests!",
		    HDFS_T_ADDR);

	H_ADDR = strdup(getenv(HDFS_T_ADDR));
	assert(H_ADDR);

	if (getenv(HDFS_T_PORT)) {
		H_PORT = strdup(getenv(HDFS_T_PORT));
		assert(H_PORT);
	}

	if (getenv(HDFS_T_USER)) {
		H_USER = strdup(getenv(HDFS_T_USER));
		assert(H_USER);
	}

	if ((ver = getenv(HDFS_T_VER)) != NULL) {
		if (!strcmp(ver, "1")) {
			H_VER = HDFS_NN_v1;
		} else if (!strcmp(ver, "2")) {
			H_VER = HDFS_NN_v2;
		} else if (!strcmp(ver, "2.2")){
			H_VER = HDFS_NN_v2_2;
		} else { // XXX consider just using default on unrecognized
			errx(EXIT_FAILURE, "HDFS version '%s' unsupported for testing", ver);
		}
	}

	if ((kerb = getenv(HDFS_T_KERB)) != NULL) {
		if (!strcmp(kerb, "try")) {
			H_KERB = HDFS_TRY_KERB;
		} else if (!strcmp(kerb, "require")) {
			H_KERB = HDFS_REQUIRE_KERB;
		} else if (!strcmp(kerb, "simple") || !strcmp(kerb, "none")
		    || !strcmp(kerb, "no") || !strcmp(kerb, "")) {
			H_KERB = HDFS_NO_KERB;
		} else { // XXX consider just using default on unrecognized
			errx(EXIT_FAILURE, "Kerberos setting '%s' unsupported for testing", kerb);
		}
	}

	if (H_KERB != HDFS_NO_KERB) {
		rc = sasl_client_init(NULL);
		assert(rc == SASL_OK);
	}

	// Test basic connectivity
	nn = hdfs_namenode_new_version(H_ADDR, H_PORT, H_USER, H_KERB,
	    H_VER, &error);
	if (!nn)
		errx(EXIT_FAILURE,
		    "Could not connect to %s=%s @ %s=%s (%s=%s): %s",
		    HDFS_T_USER, H_USER, HDFS_T_ADDR, H_ADDR,
		    HDFS_T_PORT, H_PORT, format_error(error));

	// XXX this only works for v1. TODO remove, have a version switch, or do
	// something version agnostic (getStats once it's implemented for v2+?)
#if 0
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
#endif

	hdfs_namenode_delete(nn);

	fprintf(stderr, "\n");
	// Find and run all tests
	for (size_t i = 0; i < nelem(suites); i++) {
		Suite *s = suites[i]();
		SRunner *sr = srunner_create(s);

		srunner_run_all(sr, CK_ENV);
		fprintf(stderr, "\n");

		if (srunner_ntests_failed(sr) > 0)
			success = false;

		srunner_free(sr);
	}

	if (H_KERB != HDFS_NO_KERB) {
		rc = sasl_client_done();
		assert(rc == SASL_OK);
	}

	free((char *)H_ADDR);
	if (getenv(HDFS_T_PORT))
		free((char *)H_PORT);
	if (getenv(HDFS_T_USER))
		free((char *)H_USER);

	if (success)
		return EXIT_SUCCESS;
	return EXIT_FAILURE;
}
