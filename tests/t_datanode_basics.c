#include <sys/mman.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <check.h>

#include <hadoofus/highlevel.h>

#include "t_main.h"

const int towrite = 70*1024*1024,
      blocksz = 64*1024*1024;
static struct hdfs_namenode *h;
char *buf, *rbuf;
int fd, ofd;
const char *localtf = "/tmp/HADOOFUS_TEST_WRITES",
      *localtf2 = "/tmp/HADOOFUS_TEST_WRITES.r";

static bool	filecmp(int fd1, int fd2, off_t len);

static uint64_t _now(void)
{
	int rc;
	struct timespec ts;
	rc = clock_gettime(CLOCK_MONOTONIC, &ts);
	ck_assert_msg(rc != -1, "clock_gettime: %s", strerror(errno));
	return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec/1000000;
}

static const char *
format_error(struct hdfs_error error)
{
	static char buf[1024];

	snprintf(buf, sizeof(buf), "%s:%s", hdfs_error_str_kind(error),
	    hdfs_error_str(error));
	return buf;
}

static void
_setup_buf(enum hdfs_namenode_proto proto)
{
	struct hdfs_error err;

	buf = malloc(towrite);
	ck_assert((intptr_t)buf);
	rbuf = malloc(towrite);
	ck_assert((intptr_t)rbuf);

	for (int i = 0; i < towrite; i++) {
		buf[i] = '0' + (i%10);
		rbuf[i] = 0;
	}

	h = hdfs_namenode_new_version(H_ADDR, "8020", H_USER, HDFS_NO_KERB,
	    proto, &err);
	ck_assert_msg((intptr_t)h, "Could not connect to %s=%s (port 8020): %s",
	    HDFS_T_ENV, H_ADDR, format_error(err));
}

static void
setup_buf(void)
{

	_setup_buf(HDFS_NN_v1);
}

static void
setup_buf2(void)
{

	_setup_buf(HDFS_NN_v2);
}

static void
setup_buf22(void)
{

	_setup_buf(HDFS_NN_v2_2);
}

static void
teardown_buf(void)
{
	hdfs_namenode_delete(h);
	h = NULL;

	free(buf);
	buf = NULL;
	free(rbuf);
	rbuf = NULL;
}


static void
_setup_file(enum hdfs_namenode_proto pr)
{
	int rc, written = 0;

	_setup_buf(pr);

	fd = open(localtf, O_RDWR|O_CREAT, 0600);
	ck_assert_msg(fd != -1, "open failed: %s", strerror(errno));
	ofd = open(localtf2, O_RDWR|O_CREAT, 0600);
	ck_assert_msg(fd != -1, "open failed: %s", strerror(errno));

	while (written < towrite) {
		rc = write(fd, buf + written, towrite - written);
		ck_assert_msg(rc > 0, "write failed: %s", strerror(errno));
		written += rc;
	}
}

static void
setup_file(void)
{

	_setup_file(HDFS_NN_v1);
}

static void
setup_file2(void)
{

	_setup_file(HDFS_NN_v2);
}

static void
setup_file22(void)
{

	_setup_file(HDFS_NN_v2_2);
}

static void
teardown_file(void)
{
	teardown_buf();
	close(fd);
	fd = -1;
	close(ofd);
	ofd = -1;
	unlink(localtf);
	unlink(localtf2);
}

START_TEST(test_dn_write_buf)
{
	const char *tf = "/HADOOFUS_TEST_WRITE_BUF",
	      *client = "HADOOFUS_CLIENT";
	bool s;

	struct hdfs_error err;
	struct hdfs_datanode *dn;
	struct hdfs_object *e = NULL, *bl, *fs, *bls;
	uint64_t begin, end;

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, blocksz, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	begin = _now();

	// write first block (full)
	bl = hdfs_addBlock(h, tf, client, NULL, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
	ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s (%s:%s)",
	    format_error(err),
	    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._hostname,
	    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._port);

	hdfs_object_free(bl);

	err = hdfs_datanode_write(dn, buf, blocksz, _i/*crcs*/);
	fail_if(hdfs_is_error(err), "error writing block: %s", format_error(err));

	hdfs_datanode_delete(dn);

	// write second block (partial)
	bl = hdfs_addBlock(h, tf, client, NULL, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
	ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

	hdfs_object_free(bl);

	err = hdfs_datanode_write(dn, buf+blocksz, towrite-blocksz, _i/*crcs*/);
	fail_if(hdfs_is_error(err), "error writing block: %s", format_error(err));

	hdfs_datanode_delete(dn);

	end = _now();
	fprintf(stderr, "Wrote %d MB from buf in %ld ms%s, %02g MB/s\n",
	    towrite/1024/1024, end - begin, _i? " (with crcs)":"",
	    (double)towrite/(end-begin)/1024*1000/1024);

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	ck_assert(fs->ob_val._file_status._size == towrite);
	hdfs_object_free(fs);

	s = hdfs_complete(h, tf, client, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "did not complete");

	bls = hdfs_getBlockLocations(h, tf, 0, towrite, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	begin = _now();
	for (int i = 0; i < bls->ob_val._located_blocks._num_blocks; i++) {
		struct hdfs_object *bl =
		    bls->ob_val._located_blocks._blocks[i];
		dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

		err = hdfs_datanode_read(dn, 0/*offset-in-block*/,
		    bl->ob_val._located_block._len,
		    rbuf + i*blocksz,
		    _i/*crcs*/);

		hdfs_datanode_delete(dn);

		if (err.her_kind == he_hdfserr && err.her_num == HDFS_ERR_DATANODE_NO_CRCS) {
			fprintf(stderr, "Warning: test server doesn't support "
			    "CRCs, skipping validation.\n");
			_i = 0;

			// reconnect, try again without validating CRCs (for
			// isi_hdfs_d)
			dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
			ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

			err = hdfs_datanode_read(dn, 0/*offset-in-block*/,
			    bl->ob_val._located_block._len,
			    rbuf + i*blocksz,
			    false/*crcs*/);

			hdfs_datanode_delete(dn);
		}

		fail_if(hdfs_is_error(err), "error reading block: %s", format_error(err));
	}
	end = _now();
	fprintf(stderr, "Read %d MB to buf in %ld ms%s, %02g MB/s\n",
	    towrite/1024/1024, end - begin, _i? " (with crcs)":"",
	    (double)towrite/(end-begin)/1024*1000/1024);

	hdfs_object_free(bls);
	fail_if(memcmp(buf, rbuf, towrite), "read differed from write");

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_dn_write_file)
{
	const char *tf = "/HADOOFUS_TEST_WRITE_FILE",
	      *client = "HADOOFUS_CLIENT";
	bool s;

	struct hdfs_error err;
	struct hdfs_datanode *dn;
	struct hdfs_object *e = NULL, *bl, *fs, *bls;
	uint64_t begin, end;

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, 1/*replication*/, blocksz, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	begin = _now();

	// write first block (full)
	bl = hdfs_addBlock(h, tf, client, NULL, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
	ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

	hdfs_object_free(bl);

	err = hdfs_datanode_write_file(dn, fd, blocksz, 0, _i/*crcs*/);
	fail_if(hdfs_is_error(err), "error writing block: %s", format_error(err));

	hdfs_datanode_delete(dn);

	// write second block (partial)
	bl = hdfs_addBlock(h, tf, client, NULL, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
	ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

	hdfs_object_free(bl);

	err = hdfs_datanode_write_file(dn, fd, towrite-blocksz, blocksz, _i/*crcs*/);
	fail_if(hdfs_is_error(err), "error writing block: %s", format_error(err));

	hdfs_datanode_delete(dn);

	end = _now();
	fprintf(stderr, "Wrote %d MB from file in %ld ms%s, %02g MB/s\n",
	    towrite/1024/1024, end - begin, _i? " (with crcs)":"",
	    (double)towrite/(end-begin)/1024*1000/1024);

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	ck_assert(fs->ob_val._file_status._size == towrite);
	hdfs_object_free(fs);

	s = hdfs_complete(h, tf, client, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "did not complete");

	bls = hdfs_getBlockLocations(h, tf, 0, towrite, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	begin = _now();
	for (int i = 0; i < bls->ob_val._located_blocks._num_blocks; i++) {
		struct hdfs_object *bl =
		    bls->ob_val._located_blocks._blocks[i];
		dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

		err = hdfs_datanode_read_file(dn, 0/*offset-in-block*/,
		    bl->ob_val._located_block._len,
		    ofd,
		    i*blocksz/*fd offset*/,
		    _i/*crcs*/);

		hdfs_datanode_delete(dn);

		if (err.her_kind == he_hdfserr && err.her_num == HDFS_ERR_DATANODE_NO_CRCS) {
			fprintf(stderr, "Warning: test server doesn't support "
			    "CRCs, skipping validation.\n");
			_i = 0;

			// reconnect, try again without validating CRCs (for
			// isi_hdfs_d)
			dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
			ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

			err = hdfs_datanode_read_file(dn, 0/*offset-in-block*/,
			    bl->ob_val._located_block._len,
			    ofd,
			    i*blocksz,
			    false/*crcs*/);

			hdfs_datanode_delete(dn);
		}

		fail_if(hdfs_is_error(err), "error reading block: %s", format_error(err));
	}
	end = _now();
	fprintf(stderr, "Read %d MB to file in %ld ms%s, %02g MB/s\n",
	    towrite/1024/1024, end - begin, _i? " (with crcs)":"",
	    (double)towrite/(end-begin)/1024*1000/1024);

	hdfs_object_free(bls);
	fail_if(filecmp(fd, ofd, towrite), "read differed from write");

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_short_read)
{
	struct hdfs_namenode *nn;
	struct hdfs_datanode *dn;
	struct hdfs_object *bls, *e, *bl;
	struct hdfs_error errs;

#define ABUF_LEN (128*1024)
	char *abuf = calloc(ABUF_LEN, 1);

	ck_assert_msg((bool)abuf);

	e = NULL;
	dn = NULL;

	nn = hdfs_namenode_new_version(H_ADDR, "8020", H_USER, HDFS_NO_KERB,
	    HDFS_NN_v2, &errs);
	ck_assert_msg(nn != NULL, "nn_new: %s", format_error(errs));

	bls = hdfs_getBlockLocations(nn, "/README.txt", 0, 10*1024, &e);
	if (e)
		ck_abort_msg("exception: %s", hdfs_exception_get_message(e));

	if (bls->ob_type == H_NULL ||
	    bls->ob_val._located_blocks._num_blocks == 0)
		goto out;

	bl = bls->ob_val._located_blocks._blocks[0];
	dn = hdfs_datanode_new(bl, "HADOOFUS_CLIENT", HDFS_DATANODE_AP_2_0, &errs);
	ck_assert_msg(dn != NULL, "dn_new: %s", format_error(errs));

	errs = hdfs_datanode_read(dn, 0, 1032, abuf, false/*verifycrcs*/);
	ck_assert_msg(!hdfs_is_error(errs), "dn_read: %s", format_error(errs));

#if 0
	printf("%s: '''\n%s\n'''\n", __func__, abuf);
#endif

out:
	hdfs_object_free(bls);
	hdfs_namenode_delete(nn);
}
END_TEST

START_TEST(test_short_write)
{
	const char *tf = "/HADOOFUS_TEST2_WRITE_SHORT",
	      *client = "HADOOFUS_CLIENT";
	struct hdfs_datanode *dn;
	struct hdfs_object *e, *bl, *fs;
	struct hdfs_error errs;

	e = NULL;
	dn = NULL;

	hdfs_delete(h, tf, false, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	hdfs_create(h, tf, 0644, client, true, false, 1, blocksz, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	bl = hdfs_addBlock(h, tf, client, NULL, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	dn = hdfs_datanode_new(bl, "HADOOFUS_CLIENT", HDFS_DATANODE_AP_2_0, &errs);
	ck_assert_msg(dn != NULL, "dn_new: %s", format_error(errs));

	errs = hdfs_datanode_write(dn, buf, 33128, false);
	ck_assert_msg(!hdfs_is_error(errs), "dn_read: %s", format_error(errs));

	hdfs_datanode_delete(dn);
	hdfs_object_free(bl);

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	ck_assert(fs->ob_val._file_status._size == 33128);
	hdfs_object_free(fs);

	hdfs_complete(h, tf, client, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	hdfs_delete(h, tf, false, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
}
END_TEST

Suite *
t_datanode_basics_suite()
{
	Suite *s = suite_create("datanode");

	TCase *tc = tcase_create("buf"), *tc2;
	tcase_add_unchecked_fixture(tc, setup_buf, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_buf, 0, 2);

	suite_add_tcase(s, tc);

	tc2 = tcase_create("file");
	tcase_add_unchecked_fixture(tc2, setup_file, teardown_file);
	tcase_set_timeout(tc2, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc2, test_dn_write_file, 0, 2);

	suite_add_tcase(s, tc2);

	return s;
}

Suite *
t_datanode2_basics_suite()
{
	Suite *s;
	TCase *tc;

	s = suite_create("datanode2");

	tc = tcase_create("dn2_short");
	tcase_add_unchecked_fixture(tc, setup_buf2, teardown_buf);
	tcase_add_test(tc, test_short_read);
	tcase_add_test(tc, test_short_write);
	suite_add_tcase(s, tc);

	tc = tcase_create("buf2");
	tcase_add_unchecked_fixture(tc, setup_buf2, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_buf, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);

	suite_add_tcase(s, tc);

	tc = tcase_create("buf22");
	tcase_add_unchecked_fixture(tc, setup_buf22, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_buf, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);

	suite_add_tcase(s, tc);

	tc = tcase_create("file2");
	tcase_add_unchecked_fixture(tc, setup_file2, teardown_file);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_file, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);

	suite_add_tcase(s, tc);

	tc = tcase_create("file22");
	tcase_add_unchecked_fixture(tc, setup_file22, teardown_file);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_file, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);

	suite_add_tcase(s, tc);

	return s;
}

bool
filecmp(int fd1, int fd2, off_t len)
{
	char *m1 = MAP_FAILED, *m2 = MAP_FAILED;
	bool eq = true;

	if (len == 0)
		goto out;

	m1 = mmap(NULL, len, PROT_READ, MAP_PRIVATE, fd1, 0);
	fail_if(m1 == MAP_FAILED, "mmap: %s", strerror(errno));
	m2 = mmap(NULL, len, PROT_READ, MAP_PRIVATE, fd2, 0);
	fail_if(m2 == MAP_FAILED, "mmap: %s", strerror(errno));

	if (memcmp(m1, m2, len))
		eq = false;

out:
	if (m1 != MAP_FAILED)
		fail_if(munmap(m1, len) == -1, "munmap: %s", strerror(errno));
	if (m2 != MAP_FAILED)
		fail_if(munmap(m2, len) == -1, "munmap: %s", strerror(errno));
	return !eq;
}
