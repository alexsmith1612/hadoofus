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
	fail_if(rc == -1, "clock_gettime: %s", strerror(errno));
	return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec/1000000;
}

static void
setup_buf(void)
{
	const char *err = NULL;

	buf = malloc(towrite);
	fail_unless((intptr_t)buf);
	rbuf = malloc(towrite);
	fail_unless((intptr_t)rbuf);

	for (int i = 0; i < towrite; i++) {
		buf[i] = '0' + (i%10);
		rbuf[i] = 0;
	}

	h = hdfs_namenode_new(H_ADDR, "8020", H_USER, &err);
	fail_if(h == NULL, "Could not connect to %s=%s (port 8020): %s",
	    HDFS_T_ENV, H_ADDR, err);
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
setup_file(void)
{
	int rc, written = 0;
	setup_buf();

	fd = open(localtf, O_RDWR|O_CREAT, 0600);
	fail_if(fd == -1, "open failed: %s", strerror(errno));
	ofd = open(localtf2, O_RDWR|O_CREAT, 0600);
	fail_if(fd == -1, "open failed: %s", strerror(errno));

	while (written < towrite) {
		rc = write(fd, buf + written, towrite - written);
		fail_if(rc <= 0, "write failed: %s", strerror(errno));
		written += rc;
	}
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
	      *client = "HADOOFUS_CLIENT", *err;
	bool s;

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
	fail_unless((intptr_t)dn, "error connecting to datanode: %s", err);

	hdfs_object_free(bl);

	err = hdfs_datanode_write(dn, buf, blocksz, _i/*crcs*/);
	fail_if(err, "error writing block: %s", err);

	hdfs_datanode_delete(dn);

	// write second block (partial)
	bl = hdfs_addBlock(h, tf, client, NULL, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
	fail_unless((intptr_t)dn, "error connecting to datanode: %s", err);

	hdfs_object_free(bl);

	err = hdfs_datanode_write(dn, buf+blocksz, towrite-blocksz, _i/*crcs*/);
	fail_if(err, "error writing block: %s", err);

	hdfs_datanode_delete(dn);

	end = _now();
	fprintf(stderr, "Wrote %d MB from buf in %ld ms%s, %02g MB/s\n",
	    towrite/1024/1024, end - begin, _i? " (with crcs)":"",
	    (double)towrite/(end-begin)/1024*1000/1024);

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(fs->ob_val._file_status._size == towrite);
	hdfs_object_free(fs);

	s = hdfs_complete(h, tf, client, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(s, "did not complete");

	bls = hdfs_getBlockLocations(h, tf, 0, towrite, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	begin = _now();
	for (int i = 0; i < bls->ob_val._located_blocks._num_blocks; i++) {
		struct hdfs_object *bl =
		    bls->ob_val._located_blocks._blocks[i];
		dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
		fail_unless((intptr_t)dn, "error connecting to datanode: %s", err);

		err = hdfs_datanode_read(dn, 0/*offset-in-block*/,
		    bl->ob_val._located_block._len,
		    rbuf + i*blocksz,
		    _i/*crcs*/);

		hdfs_datanode_delete(dn);

		if (err == HDFS_DATANODE_ERR_NO_CRCS) {
			fprintf(stderr, "Warning: test server doesn't support "
			    "CRCs, skipping validation.\n");
			_i = 0;

			// reconnect, try again without validating CRCs (for
			// isi_hdfs_d)
			dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
			fail_unless((intptr_t)dn, "error connecting to datanode: %s", err);

			err = hdfs_datanode_read(dn, 0/*offset-in-block*/,
			    bl->ob_val._located_block._len,
			    rbuf + i*blocksz,
			    false/*crcs*/);

			hdfs_datanode_delete(dn);
		}

		fail_if(err, "error reading block: %s", err);
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
	fail_unless(s, "delete returned false");
}
END_TEST

START_TEST(test_dn_write_file)
{
	const char *tf = "/HADOOFUS_TEST_WRITE_FILE",
	      *client = "HADOOFUS_CLIENT", *err;
	bool s;

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
	fail_unless((intptr_t)dn, "error connecting to datanode: %s", err);

	hdfs_object_free(bl);

	err = hdfs_datanode_write_file(dn, fd, blocksz, 0, _i/*crcs*/);
	fail_if(err, "error writing block: %s", err);

	hdfs_datanode_delete(dn);

	// write second block (partial)
	bl = hdfs_addBlock(h, tf, client, NULL, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
	fail_unless((intptr_t)dn, "error connecting to datanode: %s", err);

	hdfs_object_free(bl);

	err = hdfs_datanode_write_file(dn, fd, towrite-blocksz, blocksz, _i/*crcs*/);
	fail_if(err, "error writing block: %s", err);

	hdfs_datanode_delete(dn);

	end = _now();
	fprintf(stderr, "Wrote %d MB from file in %ld ms%s, %02g MB/s\n",
	    towrite/1024/1024, end - begin, _i? " (with crcs)":"",
	    (double)towrite/(end-begin)/1024*1000/1024);

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(fs->ob_val._file_status._size == towrite);
	hdfs_object_free(fs);

	s = hdfs_complete(h, tf, client, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));
	fail_unless(s, "did not complete");

	bls = hdfs_getBlockLocations(h, tf, 0, towrite, &e);
	if (e)
		fail("exception: %s", hdfs_exception_get_message(e));

	begin = _now();
	for (int i = 0; i < bls->ob_val._located_blocks._num_blocks; i++) {
		struct hdfs_object *bl =
		    bls->ob_val._located_blocks._blocks[i];
		dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
		fail_unless((intptr_t)dn, "error connecting to datanode: %s", err);

		err = hdfs_datanode_read_file(dn, 0/*offset-in-block*/,
		    bl->ob_val._located_block._len,
		    ofd,
		    i*blocksz/*fd offset*/,
		    _i/*crcs*/);

		hdfs_datanode_delete(dn);

		if (err == HDFS_DATANODE_ERR_NO_CRCS) {
			fprintf(stderr, "Warning: test server doesn't support "
			    "CRCs, skipping validation.\n");
			_i = 0;

			// reconnect, try again without validating CRCs (for
			// isi_hdfs_d)
			dn = hdfs_datanode_new(bl, client, HDFS_DATANODE_AP_1_0, &err);
			fail_unless((intptr_t)dn, "error connecting to datanode: %s", err);

			err = hdfs_datanode_read_file(dn, 0/*offset-in-block*/,
			    bl->ob_val._located_block._len,
			    ofd,
			    i*blocksz,
			    false/*crcs*/);

			hdfs_datanode_delete(dn);
		}

		fail_if(err, "error reading block: %s", err);
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
	fail_unless(s, "delete returned false");
}
END_TEST

Suite *
t_datanode_basics_suite()
{
	Suite *s = suite_create("Datanode API basic functionality");

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
