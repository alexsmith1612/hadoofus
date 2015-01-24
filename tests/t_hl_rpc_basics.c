#include <check.h>

#include <hadoofus/highlevel.h>

#include "t_main.h"

static struct hdfs_namenode *h;

static void
_setup(enum hdfs_namenode_proto vers)
{
	const char *err = NULL;

	h = hdfs_namenode_new_version(H_ADDR, "8020", H_USER, HDFS_NO_KERB,
	    vers, &err);
	ck_assert_msg((intptr_t)h,
	    "Could not connect to %s=%s @ %s=%s (port 8020): %s",
	    HDFS_T_USER, H_USER, HDFS_T_ENV, H_ADDR, err);
}

static void
setup(void)
{

	_setup(HDFS_NN_v1);
}

static void
setup2(void)
{

	_setup(HDFS_NN_v2);
}

static void
setup22(void)
{

	_setup(HDFS_NN_v2_2);
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
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(pv == 61L);
}
END_TEST

START_TEST(test_getBlockLocations)
{
	struct hdfs_object *e = NULL, *bls;
	bls = hdfs_getBlockLocations(h, "/", 0L, 1000L, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(hdfs_object_is_null(bls));
	ck_assert_msg(hdfs_null_type(bls) == H_LOCATED_BLOCKS);
}
END_TEST

START_TEST(test_getBlockLocations2)
{
	struct hdfs_object *e = NULL, *e2 = NULL, *bls;
	const char *tf = "/HADOOFUS_TEST_GET_BLOCK_LOCATIONS2",
	      *client = "HADOOFUS_CLIENT";

	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	bls = hdfs_getBlockLocations(h, tf, 0L, 1000L, &e);
	hdfs_delete(h, tf, false/*recurse*/, &e2);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	if (e2)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e2));

	ck_assert_msg(bls->ob_type == H_LOCATED_BLOCKS);
}
END_TEST

START_TEST(test_create)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_CREATE",
	      *client = "HADOOFUS_CLIENT";

	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
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
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_complete(h, tf, client, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "complete returned false");

	// Open for appending
	lb = hdfs_append(h, tf, client, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	hdfs_object_free(lb);

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
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
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_setReplication(h, tf, 2, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "setReplication returned false");

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
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
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	hdfs_setPermission(h, tf, 0600, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
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
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	hdfs_setOwner(h, tf, "daemon", "wheel", &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_abandonBlock)
{
	bool s;
	struct hdfs_object *e = NULL, *lb, *bl;
	const char *tf = "/HADOOFUS_TEST_ABANDONBLOCK",
	      *client = "HADOOFUS_CLIENT";

	// Create the file first
	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	mark_point();

	lb = hdfs_addBlock(h, tf, client, NULL, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert(!hdfs_object_is_null(lb));

	mark_point();

	bl = hdfs_block_from_located_block(lb);
	hdfs_object_free(lb);

	mark_point();

	hdfs_abandonBlock(h, bl, tf, client, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	hdfs_object_free(bl);
	mark_point();

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_addBlock)
{
	bool s;
	struct hdfs_object *e = NULL, *lb;
	const char *tf = "/HADOOFUS_TEST_ADDBLOCK",
	      *client = "HADOOFUS_CLIENT";

	// Create the file first
	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	mark_point();

	lb = hdfs_addBlock(h, tf, client, NULL, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert(!hdfs_object_is_null(lb));

	hdfs_object_free(lb);
	mark_point();

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_complete)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_COMPLETE",
	      *client = "HADOOFUS_CLIENT";

	// Create the file first
	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_complete(h, tf, client, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "complete returned false");

	// Cleanup
	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_rename)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_RENAME",
	      *tf2 = "/HADOOFUS_TEST_RENAMED",
	      *client = "HADOOFUS_CLIENT";

	// Create the file first
	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_rename(h, tf, tf2, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "rename returned false");

	// Cleanup
	s = hdfs_delete(h, tf2, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_delete)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_DELETE",
	      *client = "HADOOFUS_CLIENT";

	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_mkdirs)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_MKDIRS";

	s = hdfs_mkdirs(h, tf, 0755, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "mkdirs returned false");

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_getListing)
{
	struct hdfs_object *e = NULL, *listing;

	listing = hdfs_getListing(h, "/", NULL, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert(!hdfs_object_is_null(listing));

	hdfs_object_free(listing);
}
END_TEST

START_TEST(test_renewLease)
{
	struct hdfs_object *e = NULL;

	hdfs_renewLease(h, "HADOOFUS_CLIENT", &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
}
END_TEST

START_TEST(test_getStats)
{
	struct hdfs_object *e = NULL, *stats;

	stats = hdfs_getStats(h, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	hdfs_object_free(stats);
}
END_TEST

START_TEST(test_getPreferredBlockSize)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_GETPREFERREDBLOCKSIZE",
	      *client = "HADOOFUS_CLIENT";

	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	/*bs = */hdfs_getPreferredBlockSize(h, tf, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_getFileInfo)
{
	struct hdfs_object *e = NULL, *fs;

	fs = hdfs_getFileInfo(h, "/", &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert(!hdfs_object_is_null(fs));

	hdfs_object_free(fs);
}
END_TEST

START_TEST(test_getContentSummary)
{
	bool s;
	struct hdfs_object *e = NULL, *cs;
	const char *tf = "/HADOOFUS_TEST_CSDIR";

	s = hdfs_mkdirs(h, tf, 0755, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "mkdirs returned false");

	cs = hdfs_getContentSummary(h, tf, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert(!hdfs_object_is_null(cs));

	hdfs_object_free(cs);

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_setQuota)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_SETQUOTA";

	s = hdfs_mkdirs(h, tf, 0755, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "mkdirs returned false");

	hdfs_setQuota(h, tf, -1, -1, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_fsync)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_FSYNC",
	      *client = "HADOOFUS_CLIENT";

	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	hdfs_fsync(h, tf, client, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_setTimes)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_SETTIMES",
	      *client = "HADOOFUS_CLIENT";

	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	hdfs_setTimes(h, tf, -1, -1, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_recoverLease)
{
	bool s;
	struct hdfs_object *e = NULL;
	const char *tf = "/HADOOFUS_TEST_RECOVERLEASE",
	      *client = "HADOOFUS_CLIENT",
	      *client2 = "HADOOFUS_CLIENT_2";

	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, 64*1024*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	s = hdfs_recoverLease(h, tf, client2, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(!s, "recoverLease returned true");

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_delegationTokens)
{
	struct hdfs_object *token, *e;

	e = NULL;

	token = hdfs_getDelegationToken(h, "abcde", &e);
	if (e) {
		/*
		 * "Delegation Token can be issued only with kerberos or web
		 * authentication"
		 */
#if 0
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
#else
		hdfs_object_free(e);
		e = NULL;
		token = hdfs_token_new_empty();
#endif
	}

	(void)hdfs_renewDelegationToken(h, token, &e);
	if (e) {
		/* Similar error. */
#if 0
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
#else
		hdfs_object_free(e);
		e = NULL;
#endif
	}

	hdfs_cancelDelegationToken(h, token, &e);
	if (e) {
		/* Similar error. */
#if 0
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
#else
		hdfs_object_free(e);
		e = NULL;
#endif
	}

	hdfs_object_free(token);
}
END_TEST

START_TEST(test_setSafeMode)
{
	struct hdfs_object *e;
	bool b;

	e = NULL;
	b = hdfs_setSafeMode(h, HDFS_SAFEMODE_GET, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	b = hdfs_setSafeMode(h, HDFS_SAFEMODE_ENTER, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	b = hdfs_setSafeMode(h, HDFS_SAFEMODE_LEAVE, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	(void)b;
}
END_TEST

START_TEST(test_getDatanodeReport)
{
	struct hdfs_object *e, *dns;

	e = dns = NULL;

	dns = hdfs_getDatanodeReport(h, HDFS_DNREPORT_ALL, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	else
		hdfs_object_free(dns);

	dns = hdfs_getDatanodeReport(h, HDFS_DNREPORT_LIVE, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	else
		hdfs_object_free(dns);

	dns = hdfs_getDatanodeReport(h, HDFS_DNREPORT_DEAD, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	else
		hdfs_object_free(dns);
}
END_TEST

START_TEST(test_reportBadBlocks)
{
	struct hdfs_object *e, *alb, *lb;

	e = NULL;

	lb = hdfs_located_block_new(0, 0, 0, 0);
	alb = hdfs_array_locatedblock_new();

	hdfs_array_locatedblock_append_located_block(alb, lb);
	lb = NULL;

	hdfs_reportBadBlocks(h, alb, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	hdfs_object_free(alb);
}
END_TEST

START_TEST(test_distributedUpgradeProgress)
{
	struct hdfs_object *e, *us;

	e = NULL;

	us = hdfs_distributedUpgradeProgress(h, HDFS_UPGRADEACTION_STATUS, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	hdfs_object_free(us);

	us = hdfs_distributedUpgradeProgress(h, HDFS_UPGRADEACTION_DETAILED, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
	hdfs_object_free(us);

#if 0
	us = hdfs_distributedUpgradeProgress(HDFS_UPGRADEACTION_FORCE_PROCEED);
#endif
}
END_TEST

START_TEST(test_admin_functions)
{
	struct hdfs_object *e;

	e = NULL;
	hdfs_finalizeUpgrade(h, &e);
	if (e)
#if 0
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
#else
		hdfs_object_free(e);
#endif

	hdfs_refreshNodes(h, &e);
	if (e)
#if 0
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
#else
		hdfs_object_free(e);
#endif

	hdfs_saveNamespace(h, &e);
	if (e)
#if 0
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
#else
		hdfs_object_free(e);
#endif
}
END_TEST

START_TEST(test_admin_functions2)
{
	struct hdfs_object *e, *e2;

	e = e2 = NULL;
	hdfs_metaSave(h, "/HADOOFUS_TEST_METASAVE", &e);
	(void)hdfs_delete(h, "/HADOOFUS_TEST_METASAVE", false, &e2);
	if (e2)
		hdfs_object_free(e2);
	if (e)
#if 0
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
#else
		hdfs_object_free(e);
#endif

	(void)hdfs_isFileClosed(h, "/BOGUS", &e);
	if (e)
		hdfs_object_free(e);

	hdfs_setBalancerBandwidth(h, 100000000, &e);
	if (e)
#if 0
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));
#else
		hdfs_object_free(e);
#endif
}
END_TEST

START_TEST(test_getServerDefaults)
{
	struct hdfs_object *object, *e = NULL;

	object = hdfs2_getServerDefaults(h, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	hdfs_object_free(object);
}
END_TEST

Suite *
t_hl_rpc_basics_suite()
{
	Suite *s = suite_create("rpcs");

	TCase *tc = tcase_create("basic");
	tcase_add_checked_fixture(tc, setup, teardown);

	tcase_add_test(tc, test_getProtocolVersion);
	tcase_add_test(tc, test_getBlockLocations);
	tcase_add_test(tc, test_getBlockLocations2);
	tcase_add_test(tc, test_create);
	tcase_add_test(tc, test_append);
	tcase_add_test(tc, test_setReplication);
	tcase_add_test(tc, test_setPermission);
	tcase_add_test(tc, test_setOwner);
	tcase_add_test(tc, test_complete);
	tcase_add_test(tc, test_rename);
	tcase_add_test(tc, test_delete);
	tcase_add_test(tc, test_mkdirs);
	tcase_add_test(tc, test_getListing);
	tcase_add_test(tc, test_renewLease);
	tcase_add_test(tc, test_getStats);
	tcase_add_test(tc, test_getPreferredBlockSize);
	tcase_add_test(tc, test_getFileInfo);
	tcase_add_test(tc, test_setQuota);
	tcase_add_test(tc, test_fsync);
	tcase_add_test(tc, test_setTimes);
	tcase_add_test(tc, test_recoverLease);
	tcase_add_test(tc, test_delegationTokens);
	tcase_add_test(tc, test_setSafeMode);
	tcase_add_test(tc, test_getDatanodeReport);
	tcase_add_test(tc, test_distributedUpgradeProgress);
	tcase_add_test(tc, test_admin_functions);
	tcase_add_test(tc, test_admin_functions2);

	suite_add_tcase(s, tc);

	tc = tcase_create("slow");
	tcase_add_checked_fixture(tc, setup, teardown);
	tcase_set_timeout(tc, 30./*seconds*/);
	tcase_add_test(tc, test_abandonBlock);
	tcase_add_test(tc, test_addBlock);
	tcase_add_test(tc, test_getContentSummary);

	suite_add_tcase(s, tc);

	/* My implementation of HDFS doesn't support this RPC. */
	tc = tcase_create("broken");
	tcase_add_checked_fixture(tc, setup, teardown);
	tcase_add_test(tc, test_reportBadBlocks);
#if 0
	suite_add_tcase(s, tc);
#endif
	return s;
}

Suite *
t_hl_rpc2_basics_suite()
{
	Suite *s = suite_create("rpcs2");

	TCase *tc = tcase_create("basic2");
	tcase_add_checked_fixture(tc, setup2, teardown);
	tcase_add_test(tc, test_getServerDefaults);
	tcase_add_test(tc, test_getBlockLocations2);
	tcase_add_test(tc, test_create);
	tcase_add_test(tc, test_append);
	tcase_add_test(tc, test_setReplication);
	tcase_add_test(tc, test_setPermission);
	tcase_add_test(tc, test_setOwner);
	tcase_add_test(tc, test_complete);
	tcase_add_test(tc, test_rename);
	tcase_add_test(tc, test_delete);
	tcase_add_test(tc, test_mkdirs);
	tcase_add_test(tc, test_getListing);
	tcase_add_test(tc, test_renewLease);
	tcase_add_test(tc, test_getFileInfo);
	tcase_add_test(tc, test_setQuota);
	tcase_add_test(tc, test_fsync);
	tcase_add_test(tc, test_setTimes);
	tcase_add_test(tc, test_recoverLease);
	suite_add_tcase(s, tc);

	tc = tcase_create("basic22");
	tcase_add_checked_fixture(tc, setup22, teardown);
	tcase_add_test(tc, test_getServerDefaults);
	tcase_add_test(tc, test_getBlockLocations2);
	tcase_add_test(tc, test_create);
	tcase_add_test(tc, test_append);
	tcase_add_test(tc, test_setReplication);
	tcase_add_test(tc, test_setPermission);
	tcase_add_test(tc, test_setOwner);
	tcase_add_test(tc, test_complete);
	tcase_add_test(tc, test_rename);
	tcase_add_test(tc, test_delete);
	tcase_add_test(tc, test_mkdirs);
	tcase_add_test(tc, test_getListing);
	tcase_add_test(tc, test_renewLease);
	tcase_add_test(tc, test_getFileInfo);
	tcase_add_test(tc, test_setQuota);
	tcase_add_test(tc, test_fsync);
	tcase_add_test(tc, test_setTimes);
	tcase_add_test(tc, test_recoverLease);
	suite_add_tcase(s, tc);

	tc = tcase_create("slow2");
	tcase_add_checked_fixture(tc, setup2, teardown);
	tcase_set_timeout(tc, 30./*seconds*/);
	tcase_add_test(tc, test_abandonBlock);
	tcase_add_test(tc, test_addBlock);
	tcase_add_test(tc, test_getContentSummary);
	suite_add_tcase(s, tc);

	tc = tcase_create("slow22");
	tcase_add_checked_fixture(tc, setup22, teardown);
	tcase_set_timeout(tc, 30./*seconds*/);
	tcase_add_test(tc, test_abandonBlock);
	tcase_add_test(tc, test_addBlock);
	tcase_add_test(tc, test_getContentSummary);
	suite_add_tcase(s, tc);

	return s;
}
