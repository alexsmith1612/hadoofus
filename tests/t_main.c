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

int
main()
{
	char *ver, *kerb;
	bool success = true;
	Suite *(* suites[])(void) = {
		t_unit,
		t_hl_rpc_basics_suite,
		t_namenode_nb_suite,
		t_datanode_basics_suite,
		t_datanode_nb_suite
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
