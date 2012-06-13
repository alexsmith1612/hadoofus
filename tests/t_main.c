#include <stdbool.h>
#include <stdlib.h>

#include <check.h>

#include "t_main.h"

#define nelem(ARR) (sizeof(ARR) / sizeof(ARR[0]))
typedef Suite *(*suite_returner)(void);

int
main(int argc, char **argv)
{
	bool success = true;
	Suite *(*suites[])(void) = {
		t_hl_rpc_basics_suite
	};

	for (int i = 0; i < nelem(suites); i++) {
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
