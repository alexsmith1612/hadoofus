#include <check.h>

#include <hadoofus/highlevel.h>

#include "t_main.h"

static struct hdfs_namenode *h;

static void
setup(void)
{
	const char *err = NULL;

	h = hdfs_namenode_new(H_ADDR, "8020", H_USER, &err);
	fail_if(h == NULL, "Could not connect to %s=%s (port 8020): %s",
	    HDFS_T_ENV, H_ADDR, err);
}

static void
teardown(void)
{
	hdfs_namenode_delete(h);
	h = NULL;
}

START_TEST(test_getProtocolVersion)
{
	struct hdfs_object *e = NULL;
	int64_t pv =
	    hdfs_getProtocolVersion(h, HADOOFUS_CLIENT_PROTOCOL_STR, 61L, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(pv == 61L);
}
END_TEST

START_TEST(test_getBlockLocations)
{
	struct hdfs_object *e = NULL, *bls;
	bls = hdfs_getBlockLocations(h, "/", 0L, 1000L, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(hdfs_object_is_null(bls));
	fail_unless(hdfs_null_type(bls) == H_LOCATED_BLOCKS);
}
END_TEST

START_TEST(test_create)
{
	bool s;
	struct hdfs_object *e = NULL, *fs;
	const char *tf = "/HADOOFUS_TEST_CREATE",
	      *client = "HADOOFUS_CLIENT";

	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_if(hdfs_object_is_null(fs));

	hdfs_object_free(fs);

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(s, "delete returned false");
}
END_TEST

START_TEST(test_append)
{
	bool s;
	struct hdfs_object *e = NULL, *lb;
	const char *tf = "/HADOOFUS_TEST_APPEND",
	      *client = "HADOOFUS_CLIENT";

	// Create the file first
	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_complete(h, tf, client, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(s, "complete returned false");

	// Open for appending
	lb = hdfs_append(h, tf, client, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	hdfs_object_free(lb);

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(s, "delete returned false");
}
END_TEST

START_TEST(test_setReplication)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_SETREPLICATION",
	      *client = "HADOOFUS_CLIENT";

	// Create the file first
	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_setReplication(h, tf, 2, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(s, "setReplication returned false");

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(s, "delete returned false");
}
END_TEST

START_TEST(test_setPermission)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_SETPERMISSION",
	      *client = "HADOOFUS_CLIENT";

	// Create the file first
	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	hdfs_setPermission(h, tf, 0600, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(s, "delete returned false");
}
END_TEST

START_TEST(test_setOwner)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_SETOWNER",
	      *client = "HADOOFUS_CLIENT";

	// Create the file first
	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	hdfs_setOwner(h, tf, "daemon", "root", &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(s, "delete returned false");
}
END_TEST

START_TEST(test_abandonBlock)
{
	// TODO get this test passing.
	bool s;
	struct hdfs_object *e = NULL, *lb, *bl;
	const char *tf = "/HADOOFUS_TEST_ABANDONBLOCK",
	      *client = "HADOOFUS_CLIENT";

	// Create the file first
	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	lb = hdfs_addBlock(h, tf, client, NULL, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_if(hdfs_object_is_null(lb));

	bl = hdfs_block_from_located_block(lb);
	hdfs_object_free(lb);

	hdfs_abandonBlock(h, bl, tf, client, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	hdfs_object_free(bl);

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(s, "delete returned false");
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
	tcase_add_checked_fixture(tc, setup, teardown);

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
