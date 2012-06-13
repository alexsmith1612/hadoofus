#include <check.h>

#include "t_main.h"

START_TEST(test_getProtocolVersion)
{
}
END_TEST

START_TEST(test_getBlockLocations)
{
}
END_TEST

START_TEST(test_create)
{
}
END_TEST

START_TEST(test_append)
{
}
END_TEST

START_TEST(test_setReplication)
{
}
END_TEST

START_TEST(test_setPermission)
{
}
END_TEST

START_TEST(test_setOwner)
{
}
END_TEST

START_TEST(test_abandonBlock)
{
}
END_TEST

START_TEST(test_addBlock)
{
}
END_TEST

START_TEST(test_complete)
{
}
END_TEST

START_TEST(test_rename)
{
}
END_TEST

START_TEST(test_delete)
{
}
END_TEST

START_TEST(test_mkdirs)
{
}
END_TEST

START_TEST(test_getListing)
{
}
END_TEST

START_TEST(test_renewLease)
{
}
END_TEST

START_TEST(test_getStats)
{
}
END_TEST

START_TEST(test_getPreferredBlockSize)
{
}
END_TEST

START_TEST(test_getFileInfo)
{
}
END_TEST

START_TEST(test_getContentSummary)
{
}
END_TEST

START_TEST(test_setQuota)
{
}
END_TEST

START_TEST(test_fsync)
{
}
END_TEST

START_TEST(test_setTimes)
{
}
END_TEST

START_TEST(test_recoverLease)
{
}
END_TEST

Suite *
t_hl_rpc_basics_suite()
{
	Suite *s = suite_create("High-level RPC API basic functionality");

	TCase *tc = tcase_create("basic");

	tcase_add_test(tc, test_getProtocolVersion);
	tcase_add_test(tc, test_getBlockLocations);
	tcase_add_test(tc, test_create);
	tcase_add_test(tc, test_append);
	tcase_add_test(tc, test_setReplication);
	tcase_add_test(tc, test_setPermission);
	tcase_add_test(tc, test_setOwner);
	tcase_add_test(tc, test_abandonBlock);
	tcase_add_test(tc, test_addBlock);
	tcase_add_test(tc, test_complete);
	tcase_add_test(tc, test_rename);
	tcase_add_test(tc, test_delete);
	tcase_add_test(tc, test_mkdirs);
	tcase_add_test(tc, test_getListing);
	tcase_add_test(tc, test_renewLease);
	tcase_add_test(tc, test_getStats);
	tcase_add_test(tc, test_getPreferredBlockSize);
	tcase_add_test(tc, test_getFileInfo);
	tcase_add_test(tc, test_getContentSummary);
	tcase_add_test(tc, test_setQuota);
	tcase_add_test(tc, test_fsync);
	tcase_add_test(tc, test_setTimes);
	tcase_add_test(tc, test_recoverLease);

	suite_add_tcase(s, tc);
	return s;
}
