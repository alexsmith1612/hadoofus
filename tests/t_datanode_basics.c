#include <sys/mman.h>

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <check.h>

#include <hadoofus/highlevel.h>

#include "t_main.h"

const char *const csum2str[] = {
	[HDFS_CSUM_NULL] = "null",
	[HDFS_CSUM_CRC32] = "crc32",
	[HDFS_CSUM_CRC32C] = "crc32c"
};

static const int TOWRITE = 70*1024*1024,
      BLOCKSZ = 64*1024*1024;
static struct hdfs_namenode *h;
static int dn_proto;
char *buf, *rbuf;
int fd, ofd;
const char *localtf = "/tmp/HADOOFUS_TEST_WRITES",
      *localtf2 = "/tmp/HADOOFUS_TEST_WRITES.r";

static bool	filecmp(int fd1, int fd2, off_t len);

static inline off_t
_min(off_t a, off_t b)
{
	if (a < b)
		return a;
	return b;
}

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
setup_buf(void)
{
	struct hdfs_error err;

	switch (H_VER) {
	case HDFS_NN_v1:
		dn_proto = HDFS_DATANODE_AP_1_0;
		break;
	case HDFS_NN_v2:
	case HDFS_NN_v2_2:
		dn_proto = HDFS_DATANODE_AP_2_0;
		break;
	default:
		ck_abort_msg("Invalid namenode version (%d)", H_VER);
	}

	buf = malloc(TOWRITE);
	ck_assert((intptr_t)buf);
	rbuf = malloc(TOWRITE);
	ck_assert((intptr_t)rbuf);

	for (int i = 0; i < TOWRITE; i++) {
		buf[i] = '0' + (i%10);
		rbuf[i] = 0;
	}

	h = hdfs_namenode_new_version(H_ADDR, H_PORT, H_USER, H_KERB,
	    H_VER, &err);
	ck_assert_msg((intptr_t)h,
	    "Could not connect to %s=%s @ %s=%s (%s=%s): %s",
	    HDFS_T_USER, H_USER, HDFS_T_ADDR, H_ADDR,
	    HDFS_T_PORT, H_PORT, format_error(err));
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
	ck_assert_msg(fd != -1, "open failed: %s", strerror(errno));
	ofd = open(localtf2, O_RDWR|O_CREAT, 0600);
	ck_assert_msg(fd != -1, "open failed: %s", strerror(errno));

	while (written < TOWRITE) {
		rc = write(fd, buf + written, TOWRITE - written);
		ck_assert_msg(rc > 0, "write failed: %s", strerror(errno));
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
	      *client = "HADOOFUS_CLIENT";
	bool s;

	struct hdfs_error err;
	struct hdfs_datanode *dn;
	struct hdfs_object *e = NULL, *bl, *fs, *bls, *prev = NULL, *fsd;
	uint64_t begin, end;
	int replication = 1, wblk, wtot = 0, err_idx;
	ssize_t nwritten, nacked;

	if (H_VER > HDFS_NN_v1) {
		fsd = hdfs2_getServerDefaults(h, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		replication = fsd->ob_val._server_defaults._replication;
		// XXX TODO blocksize?
		hdfs_object_free(fsd);
	}

	fs = hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, replication, BLOCKSZ, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	if (fs) {
		ck_assert_int_eq(fs->ob_type, H_FILE_STATUS);
		// XXX TODO fileid?
		hdfs_object_free(fs);
	}

	// Write to the new file
	begin = _now();
	do {
		wblk = _min(TOWRITE - wtot, BLOCKSZ);

		bl = hdfs_addBlock(h, tf, client, NULL, prev, 0/*fileid?*/, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

		dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_WRITE_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s (%s:%s)",
		    format_error(err),
		    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._ipaddr,
		    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._port);

		err = hdfs_datanode_write(dn, buf + wtot, wblk, _i/*sendcrcs*/, &nwritten, &nacked, &err_idx);
		fail_if(hdfs_is_error(err), "error writing block: %s", format_error(err));
		ck_assert_int_eq(nwritten, wblk);
		ck_assert_int_eq(nacked, wblk);
		ck_assert_int_lt(err_idx, 0);

		hdfs_datanode_delete(dn);

		if (prev)
			hdfs_object_free(prev);
		prev = hdfs_block_from_located_block(bl);
		prev->ob_val._block._length += wblk;
		// XXX _generation?
		hdfs_object_free(bl);

		wtot += wblk;
	} while (wtot < TOWRITE);

	// XXX This is dubious. There's no guarantee that complete will return
	// true within the time it takes for five round trip RPC
	// request/response cycles to be completed
	for (int i = 0; i < 5; i++) {
		if (i > 0)
			fprintf(stderr, "Notice: did not complete file on attempt %d, trying again...\n", i - 1);
		s = hdfs_complete(h, tf, client, prev, 0/*fileid?*/, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		if (s) // successfully completed
			break;
	}
	ck_assert_msg(s, "did not complete");

	hdfs_object_free(prev);

	end = _now();
	fprintf(stderr, "Wrote %d MB from buf in %"PRIu64" ms (with %s csum), %02g MB/s\n",
	    TOWRITE/1024/1024, end - begin, csum2str[_i],
	    (double)TOWRITE/(end-begin)/1024*1000/1024);

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_int_eq(fs->ob_val._file_status._size, TOWRITE);
	hdfs_object_free(fs);

	// Read the file back
	bls = hdfs_getBlockLocations(h, tf, 0, TOWRITE, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	begin = _now();
	for (int i = 0; i < bls->ob_val._located_blocks._num_blocks; i++) {
		struct hdfs_object *bl =
		    bls->ob_val._located_blocks._blocks[i];
		dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_READ_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

		err = hdfs_datanode_read(dn, 0/*offset-in-block*/,
		    bl->ob_val._located_block._len, rbuf + i*BLOCKSZ, _i/*verifycrcs*/);

		hdfs_datanode_delete(dn);

		if (err.her_kind == he_hdfserr && err.her_num == HDFS_ERR_DATANODE_NO_CRCS) {
			fprintf(stderr, "Warning: test server doesn't support "
			    "CRCs, skipping validation.\n");
			_i = 0;

			// reconnect, try again without validating CRCs (for
			// isi_hdfs_d)
			dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_READ_BLOCK, &err);
			ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

			err = hdfs_datanode_read(dn, 0/*offset-in-block*/,
			    bl->ob_val._located_block._len, rbuf + i*BLOCKSZ, false/*verifycrcs*/);

			hdfs_datanode_delete(dn);
		}

		fail_if(hdfs_is_error(err), "error reading block: %s", format_error(err));
	}
	end = _now();
	fprintf(stderr, "Read %d MB to buf in %"PRIu64" ms%s, %02g MB/s\n\n",
	    TOWRITE/1024/1024, end - begin, _i? " (with csum verification)":"",
	    (double)TOWRITE/(end-begin)/1024*1000/1024);

	hdfs_object_free(bls);
	fail_if(memcmp(buf, rbuf, TOWRITE), "read differed from write");

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_dn_writev)
{
	const char *tf = "/HADOOFUS_TEST_WRITEV",
	      *client = "HADOOFUS_CLIENT";
	bool s;

	struct hdfs_error err;
	struct hdfs_datanode *dn;
	struct hdfs_object *e = NULL, *bl, *fs, *bls, *prev = NULL, *fsd;
	uint64_t begin, end;
	int replication = 1, wblk, wtot = 0, err_idx;
	ssize_t nwritten, nacked;

	if (H_VER > HDFS_NN_v1) {
		fsd = hdfs2_getServerDefaults(h, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		replication = fsd->ob_val._server_defaults._replication;
		// XXX TODO blocksize?
		hdfs_object_free(fsd);
	}

	fs = hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, replication, BLOCKSZ, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	if (fs) {
		ck_assert_int_eq(fs->ob_type, H_FILE_STATUS);
		// XXX TODO fileid?
		hdfs_object_free(fs);
	}

	// Write to the new file
	begin = _now();
	do {
		struct iovec wiov[11]; // TODO add test case where iovec ends in alignment with packet
		size_t per_wiov;

		wblk = _min(TOWRITE - wtot, BLOCKSZ);

		// split the data to be written to this block into iovecs
		per_wiov = wblk / nelem(wiov);
		for (unsigned i = 0; i < nelem(wiov); i++) {
			wiov[i].iov_base = buf + wtot + (per_wiov * i);
			wiov[i].iov_len = per_wiov;
		}
		// account for integer division discrepancies
		wiov[nelem(wiov) - 1].iov_len = wblk - (per_wiov * (nelem(wiov) - 1));

		bl = hdfs_addBlock(h, tf, client, NULL, prev, 0/*fileid?*/, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

		dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_WRITE_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s (%s:%s)",
		    format_error(err),
		    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._ipaddr,
		    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._port);

		err = hdfs_datanode_writev(dn, wiov, nelem(wiov), _i/*sendcrcs*/, &nwritten, &nacked, &err_idx);
		fail_if(hdfs_is_error(err), "error writing block: %s", format_error(err));
		ck_assert_int_eq(nwritten, wblk);
		ck_assert_int_eq(nacked, wblk);
		ck_assert_int_lt(err_idx, 0);

		hdfs_datanode_delete(dn);

		if (prev)
			hdfs_object_free(prev);
		prev = hdfs_block_from_located_block(bl);
		prev->ob_val._block._length += wblk;
		// XXX _generation?
		hdfs_object_free(bl);

		wtot += wblk;
	} while (wtot < TOWRITE);

	// XXX This is dubious. There's no guarantee that complete will return
	// true within the time it takes for five round trip RPC
	// request/response cycles to be completed
	for (int i = 0; i < 5; i++) {
		if (i > 0)
			fprintf(stderr, "Notice: did not complete file on attempt %d, trying again...\n", i - 1);
		s = hdfs_complete(h, tf, client, prev, 0/*fileid?*/, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		if (s) // successfully completed
			break;
	}
	ck_assert_msg(s, "did not complete");

	hdfs_object_free(prev);

	end = _now();
	fprintf(stderr, "Wrote %d MB from iovec arrays in %"PRIu64" ms (with %s csum), %02g MB/s\n",
	    TOWRITE/1024/1024, end - begin, csum2str[_i],
	    (double)TOWRITE/(end-begin)/1024*1000/1024);

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_int_eq(fs->ob_val._file_status._size, TOWRITE);
	hdfs_object_free(fs);

	// Read the file back
	bls = hdfs_getBlockLocations(h, tf, 0, TOWRITE, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	begin = _now();
	for (int i = 0; i < bls->ob_val._located_blocks._num_blocks; i++) {
		struct iovec riov[13]; // TODO add test case where iovec ends in alignment with packet
		size_t per_riov;
		struct hdfs_object *bl =
		    bls->ob_val._located_blocks._blocks[i];
		dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_READ_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

		// split the data to be read into iovecs
		per_riov = bl->ob_val._located_block._len / nelem(riov);
		for (unsigned j = 0; j < nelem(riov); j++) {
			riov[j].iov_base = rbuf + i*BLOCKSZ + j*per_riov;
			riov[j].iov_len = per_riov;
		}
		// account for integer division discrepancies
		riov[nelem(riov) - 1].iov_len =  bl->ob_val._located_block._len - (per_riov * (nelem(riov) - 1));

		err = hdfs_datanode_readv(dn, 0/*offset-in-block*/,
		    riov, nelem(riov), _i/*verifycrcs*/);

		hdfs_datanode_delete(dn);

		if (err.her_kind == he_hdfserr && err.her_num == HDFS_ERR_DATANODE_NO_CRCS) {
			fprintf(stderr, "Warning: test server doesn't support "
			    "CRCs, skipping validation.\n");
			_i = 0;

			// reconnect, try again without validating CRCs (for
			// isi_hdfs_d)
			dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_READ_BLOCK, &err);
			ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

			err = hdfs_datanode_readv(dn, 0/*offset-in-block*/,
			    riov, nelem(riov), false/*verifycrcs*/);

			hdfs_datanode_delete(dn);
		}

		fail_if(hdfs_is_error(err), "error reading block: %s", format_error(err));
	}
	end = _now();
	fprintf(stderr, "Read %d MB to iovec arrays in %"PRIu64" ms%s, %02g MB/s\n\n",
	    TOWRITE/1024/1024, end - begin, _i? " (with csum verification)":"",
	    (double)TOWRITE/(end-begin)/1024*1000/1024);

	hdfs_object_free(bls);
	fail_if(memcmp(buf, rbuf, TOWRITE), "read differed from write");

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_dn_append_buf)
{
	const char *tf = "/HADOOFUS_TEST_APPEND",
	      *client = "HADOOFUS_CLIENT";
	bool s;

	struct hdfs_error err;
	struct hdfs_datanode *dn;
	struct hdfs_object *e = NULL, *bl, *fs, *bls, *prev = NULL, *fsd;
	uint64_t begin, end;
	int replication = 1, wblk, wtot = 0, towrite_first, towrite_append, err_idx;
	ssize_t nwritten, nacked;
	bool first = true;

	if (H_VER > HDFS_NN_v1) {
		fsd = hdfs2_getServerDefaults(h, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		replication = fsd->ob_val._server_defaults._replication;
		hdfs_object_free(fsd);
	}

	fs = hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, replication, BLOCKSZ, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	if (fs) {
		ck_assert_int_eq(fs->ob_type, H_FILE_STATUS);
		hdfs_object_free(fs);
	}

	towrite_first = (TOWRITE / 3 - 1357) | 0x1;
	if (towrite_first % BLOCKSZ == 0)
		towrite_first -= BLOCKSZ / 3;
	towrite_append = TOWRITE - towrite_first;
	ck_assert_int_gt(towrite_first,  0);
	ck_assert_int_gt(towrite_append,  0);

	// Write the first amount
	begin = _now();
	do {
		wblk = _min(towrite_first - wtot, BLOCKSZ);

		bl = hdfs_addBlock(h, tf, client, NULL, prev, 0/*fileid?*/, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

		dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_WRITE_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s (%s:%s)",
		    format_error(err),
		    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._ipaddr,
		    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._port);

		err = hdfs_datanode_write(dn, buf + wtot, wblk, _i/*sendcrcs*/, &nwritten, &nacked, &err_idx);
		fail_if(hdfs_is_error(err), "error writing block: %s", format_error(err));
		ck_assert_int_eq(nwritten, wblk);
		ck_assert_int_eq(nacked, wblk);
		ck_assert_int_lt(err_idx, 0);

		hdfs_datanode_delete(dn);

		if (prev)
			hdfs_object_free(prev);
		prev = hdfs_block_from_located_block(bl);
		prev->ob_val._block._length += wblk;
		// XXX _generation?
		hdfs_object_free(bl);

		wtot += wblk;
	} while (wtot < towrite_first);

	// XXX This is dubious. There's no guarantee that complete will return
	// true within the time it takes for five round trip RPC
	// request/response cycles to be completed
	for (int i = 0; i < 5; i++) {
		if (i > 0)
			fprintf(stderr, "Notice: did not complete file on attempt %d, trying again...\n", i - 1);
		s = hdfs_complete(h, tf, client, prev, 0/*fileid?*/, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		if (s) // successfully completed
			break;
	}
	ck_assert_msg(s, "did not complete");

	hdfs_object_free(prev);

	end = _now();
	fprintf(stderr, "Wrote initial %d MB from buf in %"PRIu64" ms (with %s csum), %02g MB/s\n",
	    towrite_first/1024/1024, end - begin, csum2str[_i],
	    (double)towrite_first/(end-begin)/1024*1000/1024);

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_int_eq(fs->ob_val._file_status._size, towrite_first);
	hdfs_object_free(fs);

	// Append the rest of the data
	begin = _now();
	do {
		if (first) { // Only write up to a complete block
			struct hdfs_object *lbws, *ubfp_lb, *oldblock, *newblock, *nodes, *storageids;
			first = false;
			prev = NULL;
			wblk = _min(TOWRITE - wtot, BLOCKSZ - (wtot % BLOCKSZ));
			lbws = hdfs_append(h, tf, client, &e);
			ck_assert_msg(lbws, "append returned NULL");
			ck_assert_int_eq(lbws->ob_type, H_LOCATED_BLOCK_WITH_STATUS);
			bl = lbws->ob_val._located_block_with_status._block; // avoid doing a copy
			lbws->ob_val._located_block_with_status._block = NULL; // don't free the located block we just grabbed
			hdfs_object_free(lbws);

			// Note that hdfs_append() can return NULL for its located_block -- this occurs if the
			// last block in the file is already full. In that case you continue in the usual way
			// by adding a new block. We do not check for that case here since do not close the
			// file at a full block above

			dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_WRITE_BLOCK, &err);
			ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s (%s:%s)",
			    format_error(err),
			    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._ipaddr,
			    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._port);

			// Block appends require some extra pipeline setup
			oldblock = hdfs_block_from_located_block(bl);
			ubfp_lb = hdfs2_updateBlockForPipeline(h, oldblock, client, &e);
			if (e)
				fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
			// update our located block with the info received from the updateBlockForPipeline
			// RPC. This MUST happen after the call to hdfs_datanode_init() or hdfs_datanode_new()
			hdfs_located_block_update_from_update_block_for_pipeline(bl, ubfp_lb);
			hdfs_object_free(ubfp_lb);
			newblock = hdfs_block_from_located_block(bl);

			// normal append --- not recovery
			err = hdfs_datanode_write_set_append_or_recovery(dn, bl, HDFS_DN_RECOVERY_NONE,
			    -1/*maxbytesrcvd--ignored for NONE*/);
			fail_if(hdfs_is_error(err), "error setting append: %s", format_error(err));

			// setup the append pipeline
			err = hdfs_datanode_write_setup_pipeline(dn, _i/*sendcrcs*/, &err_idx);
			fail_if(hdfs_is_error(err), "error setting up append pipeline: %s", format_error(err));
			ck_assert_int_lt(err_idx, 0);

			// inform the namenode of the updated pipeline
			nodes = hdfs_array_datanode_info_from_located_block(bl);
			storageids = hdfs_storage_ids_array_string_from_located_block(bl);
			hdfs2_updatePipeline(h, client, oldblock, newblock, nodes, storageids, &e);
			if (e)
				fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

			hdfs_object_free(oldblock);
			hdfs_object_free(newblock);
			hdfs_object_free(nodes);
			hdfs_object_free(storageids);
		} else  {
			wblk = _min(TOWRITE - wtot, BLOCKSZ);
			bl = hdfs_addBlock(h, tf, client, NULL, prev, 0/*fileid?*/, &e);
			dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_WRITE_BLOCK, &err);
			ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s (%s:%s)",
			    format_error(err),
			    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._ipaddr,
			    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._port);
		}
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));


		err = hdfs_datanode_write(dn, buf + wtot, wblk, _i/*sendcrcs*/, &nwritten, &nacked, &err_idx);
		fail_if(hdfs_is_error(err), "error writing block: %s", format_error(err));
		ck_assert_int_eq(nwritten, wblk);
		ck_assert_int_eq(nacked, wblk);
		ck_assert_int_lt(err_idx, 0);

		hdfs_datanode_delete(dn);

		if (prev)
			hdfs_object_free(prev);
		prev = hdfs_block_from_located_block(bl);
		prev->ob_val._block._length += wblk;
		// XXX _generation?
		hdfs_object_free(bl);

		wtot += wblk;
	} while (wtot < TOWRITE);

	// XXX This is dubious. There's no guarantee that complete will return
	// true within the time it takes for five round trip RPC
	// request/response cycles to be completed
	for (int i = 0; i < 5; i++) {
		if (i > 0)
			fprintf(stderr, "Notice: did not complete file on attempt %d, trying again...\n", i - 1);
		s = hdfs_complete(h, tf, client, prev, 0/*fileid?*/, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		if (s) // successfully completed
			break;
	}
	ck_assert_msg(s, "did not complete");

	hdfs_object_free(prev);

	end = _now();
	fprintf(stderr, "Appended %d MB from buf in %"PRIu64" ms (with %s csum), %02g MB/s\n",
	    towrite_append/1024/1024, end - begin, csum2str[_i],
	    (double)towrite_append/(end-begin)/1024*1000/1024);

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_int_eq(fs->ob_val._file_status._size, TOWRITE);
	hdfs_object_free(fs);

	// Read the file back
	bls = hdfs_getBlockLocations(h, tf, 0, TOWRITE, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	begin = _now();
	for (int i = 0; i < bls->ob_val._located_blocks._num_blocks; i++) {
		struct hdfs_object *bl =
		    bls->ob_val._located_blocks._blocks[i];
		dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_READ_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

		err = hdfs_datanode_read(dn, 0/*offset-in-block*/,
		    bl->ob_val._located_block._len, rbuf + i*BLOCKSZ, _i/*verifycrcs*/);

		hdfs_datanode_delete(dn);

		if (err.her_kind == he_hdfserr && err.her_num == HDFS_ERR_DATANODE_NO_CRCS) {
			fprintf(stderr, "Warning: test server doesn't support "
			    "CRCs, skipping validation.\n");
			_i = 0;

			// reconnect, try again without validating CRCs (for
			// isi_hdfs_d)
			dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_READ_BLOCK, &err);
			ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

			err = hdfs_datanode_read(dn, 0/*offset-in-block*/,
			    bl->ob_val._located_block._len, rbuf + i*BLOCKSZ, false/*verifycrcs*/);

			hdfs_datanode_delete(dn);
		}

		fail_if(hdfs_is_error(err), "error reading block: %s", format_error(err));
	}
	end = _now();
	fprintf(stderr, "Read %d MB to buf in %"PRIu64" ms%s, %02g MB/s\n\n",
	    TOWRITE/1024/1024, end - begin, _i? " (with csum verification)":"",
	    (double)TOWRITE/(end-begin)/1024*1000/1024);

	hdfs_object_free(bls);
	fail_if(memcmp(buf, rbuf, TOWRITE), "read differed from write");

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
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
	struct hdfs_object *e = NULL, *bl, *fs, *bls, *prev = NULL, *fsd;
	uint64_t begin, end;
	int replication = 1, wblk, wtot = 0, err_idx;
	ssize_t nwritten, nacked;

	if (H_VER > HDFS_NN_v1) {
		fsd = hdfs2_getServerDefaults(h, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		replication = fsd->ob_val._server_defaults._replication;
		hdfs_object_free(fsd);
	}

	fs = hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, replication, BLOCKSZ, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	if (fs) {
		ck_assert_int_eq(fs->ob_type, H_FILE_STATUS);
		hdfs_object_free(fs);
	}

	begin = _now();

	// Write to the new file
	do {
		wblk = _min(TOWRITE - wtot, BLOCKSZ);

		bl = hdfs_addBlock(h, tf, client, NULL, prev, 0/*fileid?*/, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

		dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_WRITE_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s (%s:%s)",
		    format_error(err),
		    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._ipaddr,
		    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._port);

		err = hdfs_datanode_write_file(dn, fd, wblk/*len*/, wtot/*offset*/, _i/*sendcrcs*/, &nwritten, &nacked, &err_idx);
		fail_if(hdfs_is_error(err), "error writing block: %s", format_error(err));
		ck_assert_int_eq(nwritten, wblk);
		ck_assert_int_eq(nacked, wblk);
		ck_assert_int_lt(err_idx, 0);

		hdfs_datanode_delete(dn);

		if (prev)
			hdfs_object_free(prev);
		prev = hdfs_block_from_located_block(bl);
		prev->ob_val._block._length += wblk;
		// XXX _generation?
		hdfs_object_free(bl);

		wtot += wblk;
	} while (wtot < TOWRITE);

	// XXX This is dubious. There's no guarantee that complete will return
	// true within the time it takes for five round trip RPC
	// request/response cycles to be completed
	for (int i = 0; i < 5; i++) {
		if (i > 0)
			fprintf(stderr, "Notice: did not complete file on attempt %d, trying again...\n", i - 1);
		s = hdfs_complete(h, tf, client, prev, 0, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		if (s) // successfully completed
			break;
	}
	ck_assert_msg(s, "did not complete");

	hdfs_object_free(prev);

	end = _now();
	fprintf(stderr, "Wrote %d MB from file in %"PRIu64" ms (with %s csum), %02g MB/s\n",
	    TOWRITE/1024/1024, end - begin, csum2str[_i],
	    (double)TOWRITE/(end-begin)/1024*1000/1024);

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_int_eq(fs->ob_val._file_status._size, TOWRITE);
	hdfs_object_free(fs);

	// Read the file back
	bls = hdfs_getBlockLocations(h, tf, 0, TOWRITE, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	begin = _now();
	for (int i = 0; i < bls->ob_val._located_blocks._num_blocks; i++) {
		struct hdfs_object *bl =
		    bls->ob_val._located_blocks._blocks[i];
		dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_READ_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

		err = hdfs_datanode_read_file(dn, 0/*offset-in-block*/,
		    bl->ob_val._located_block._len,
		    ofd,
		    i*BLOCKSZ/*fd offset*/,
		    _i/*verifycrcs*/);

		hdfs_datanode_delete(dn);

		if (err.her_kind == he_hdfserr && err.her_num == HDFS_ERR_DATANODE_NO_CRCS) {
			fprintf(stderr, "Warning: test server doesn't support "
			    "CRCs, skipping validation.\n");
			_i = 0;

			// reconnect, try again without validating CRCs (for
			// isi_hdfs_d)
			dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_READ_BLOCK, &err);
			ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

			err = hdfs_datanode_read_file(dn, 0/*offset-in-block*/,
			    bl->ob_val._located_block._len,
			    ofd,
			    i*BLOCKSZ,
			    false/*verifycrcs*/);

			hdfs_datanode_delete(dn);
		}

		fail_if(hdfs_is_error(err), "error reading block: %s", format_error(err));
	}
	end = _now();
	fprintf(stderr, "Read %d MB to file in %"PRIu64" ms%s, %02g MB/s\n\n",
	    TOWRITE/1024/1024, end - begin, _i? " (with csum verification)":"",
	    (double)TOWRITE/(end-begin)/1024*1000/1024);

	hdfs_object_free(bls);
	fail_if(filecmp(fd, ofd, TOWRITE), "read differed from write");

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");
}
END_TEST

START_TEST(test_dn_recovery)
{
	const char *tf = "/HADOOFUS_TEST_RECOVERY",
	      *client = "HADOOFUS_CLIENT";
	bool s;

	struct hdfs_error err;
	struct hdfs_datanode *dn;
	struct hdfs_object *e = NULL, *fs, *bls, *prev = NULL, *dnr;
	uint64_t begin, end;
	int replication = 2, wtot = 0;
	struct hdfs_array_datanode_info *dnrs;

	// This test requires at least 3 datanodes to run (and replication must be at least 2)
	dnr = hdfs_getDatanodeReport(h, HDFS_DNREPORT_LIVE, &e);
	if (e)
		ck_abort_msg("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_int_eq(dnr->ob_type, H_ARRAY_DATANODE_INFO);
	dnrs = &dnr->ob_val._array_datanode_info;
	if (dnrs->_len < replication + 1) {
		fprintf(stderr, "%s: Test requires cluster with at least %d live data nodes (%d live currently). Skipping test.\n",
		    __func__, replication + 1, dnrs->_len);
		return;
	}
	hdfs_object_free(dnr);

	fs = hdfs_create(h, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, replication, BLOCKSZ, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	if (fs) {
		ck_assert_int_eq(fs->ob_type, H_FILE_STATUS);
		// XXX TODO fileid?
		hdfs_object_free(fs);
	}

	// Write to the new file
	begin = _now();
	do {
		int towrite_blk, err_idx, blk_written = 0, blk_acked = 0, maxbytesrcvd;
		ssize_t nwritten, nacked;
		struct hdfs_object *lb, *gad_lb, *ubfp_lb, *bl, *existing, *exclude,
		    *existing_sids, *transfer_lb = NULL, *oldblock, *newblock, *nodes, *storageids;
		struct hdfs_transfer_targets *ttrgs = NULL;

		towrite_blk = _min(TOWRITE - wtot, BLOCKSZ);

		lb = hdfs_addBlock(h, tf, client, NULL, prev, 0/*fileid?*/, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

		dn = hdfs_datanode_new(lb, client, dn_proto, HDFS_DN_OP_WRITE_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s (%s:%s)",
		    format_error(err),
		    lb->ob_val._located_block._locs[0]->ob_val._datanode_info._ipaddr,
		    lb->ob_val._located_block._locs[0]->ob_val._datanode_info._port);
		err = hdfs_datanode_write_nb_init(dn, _i/*sendcrcs*/);
		ck_assert_msg(!hdfs_is_error(err), "error initializing datanode write: %s", format_error(err));

		while (true) {
			int rc;
			struct pollfd pfd;
			size_t towrite_partial = towrite_blk / 2 - blk_written + 1; /*always write something*/

			err = hdfs_datanode_write_nb(dn, buf + wtot + blk_written, towrite_partial, &nwritten, &nacked, &err_idx);
			ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
			    "partial block write: %s", format_error(err));
			ck_assert_int_ge(nwritten, 0);
			ck_assert_int_ge(nacked, 0);
			ck_assert_int_lt(err_idx, 0); // XXX could actually try to handle error here

			blk_written += nwritten;
			blk_acked += nacked;

			ck_assert_int_le(blk_acked, blk_written);
			ck_assert_int_lt(blk_written, towrite_blk);

			// keep writing until we reach an arbitrary write/ack threshold
			if ((blk_written >= towrite_blk / 2) && (blk_acked >= towrite_blk / 4))
				break;

			err = hdfs_datanode_get_eventfd(dn, &pfd.fd, &pfd.events);
			ck_assert_msg(!hdfs_is_error(err), "%s", format_error(err));
			rc = poll(&pfd, 1, -1);
			ck_assert_int_eq(rc, 1);
			ck_assert_int_ne(pfd.revents, 0);
		}

		// Teardown the pipeline without finishing the block
		hdfs_datanode_delete(dn);

		// the maximum bytes received by a datanode in the pipeline includes all of that
		// data that was written
		maxbytesrcvd = lb->ob_val._located_block._len + blk_written;

		// the located block length must be incremented by the amount acked. i.e. we know
		// the block at least contains the acked data, and the located block length must be
		// minbytesrcvd
		lb->ob_val._located_block._len += blk_acked;

		fprintf(stderr, "Wrote %d bytes (%d acknowledged) to block prior to simulated pipeline failure.\n", blk_written, blk_acked);

		// 1. get a new datanode for the pipeline
		bl = hdfs_block_from_located_block(lb);

		// exclude the first datanode
		exclude = hdfs_array_datanode_info_new();
		hdfs_array_datanode_info_append_datanode_info(exclude,
		    hdfs_datanode_info_copy(lb->ob_val._located_block._locs[0]));

		// mark all of the other listed datanodes (if any) as existing
		existing = hdfs_array_datanode_info_new();
		for (int i = 1; i < lb->ob_val._located_block._num_locs; i++) {
			hdfs_array_datanode_info_append_datanode_info(existing,
			    hdfs_datanode_info_copy(lb->ob_val._located_block._locs[i]));
		}

		// grab the storage ids for the existing datanodes (if they exist)
		existing_sids = hdfs_array_string_new(0, NULL);
		if (lb->ob_val._located_block._num_storage_ids > 0) {
			ck_assert_int_eq(lb->ob_val._located_block._num_storage_ids,
			    lb->ob_val._located_block._num_locs);
			for (int i = 1; i < lb->ob_val._located_block._num_storage_ids; i++) {
				hdfs_array_string_add(existing_sids,
				    lb->ob_val._located_block._storage_ids[i]);
			}
		}

		gad_lb = hdfs2_getAdditionalDatanode(h, tf, bl, existing, exclude,
		    1/*num_additional_nodes*/, client, existing_sids, 0/*fileid?*/, &e);
		if (e)
			ck_abort_msg("exception: %s:\n%s",
			    hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		ck_assert(gad_lb);
		ck_assert_int_eq(gad_lb->ob_type, H_LOCATED_BLOCK);

		// update the real located block with the new pipeline information
		// received from getAdditionalDatanode
		hdfs_located_block_update_from_get_additional_datanode(lb, gad_lb);

		hdfs_object_free(bl);
		hdfs_object_free(exclude);
		hdfs_object_free(existing_sids);
		hdfs_object_free(gad_lb);

		// find the new datanodes and prepare the transfer targets
		// and transfer sources (transfer_lb)
		err = hdfs_get_transfer_data(lb, existing, &transfer_lb, &ttrgs);
		ck_assert_msg(!hdfs_is_error(err), "%s", format_error(err));
		ck_assert(transfer_lb);
		ck_assert(ttrgs);
		ck_assert_int_gt(transfer_lb->ob_val._located_block._num_locs, 0);
		ck_assert_int_gt(ttrgs->_num_targets, 0);

		hdfs_object_free(existing);

		// 2. transfer the data to the new datanode
		dn = hdfs_datanode_new(transfer_lb, client, dn_proto, HDFS_DN_OP_TRANSFER_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode for transfer: %s",
		    format_error(err));

		err = hdfs_datanode_transfer(dn, ttrgs);
		ck_assert_msg(!hdfs_is_error(err), "%s%s%s", format_error(err),
		    dn->dn_opresult_message ? "\n\tDatanode message: " : "",
		    dn->dn_opresult_message ? dn->dn_opresult_message : "");

		hdfs_object_free(transfer_lb);
		hdfs_transfer_targets_free(ttrgs);
		hdfs_datanode_delete(dn);

		dn = hdfs_datanode_new(lb, client, dn_proto, HDFS_DN_OP_WRITE_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s (%s:%s)",
		    format_error(err),
		    lb->ob_val._located_block._locs[0]->ob_val._datanode_info._ipaddr,
		    lb->ob_val._located_block._locs[0]->ob_val._datanode_info._port);

		// 3. updateBlockForPipeline
		oldblock = hdfs_block_from_located_block(lb);
		ubfp_lb = hdfs2_updateBlockForPipeline(h, oldblock, client, &e);
		if (e)
			ck_abort_msg("exception: %s:\n%s",
			    hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

		// update the real located block with the new token and generation
		// information received from updateBlockForPipeline. NOTE this must happen
		// AFTER the call to hdfs_datanode_init() or hdfs_datanode_new()
		hdfs_located_block_update_from_update_block_for_pipeline(lb, ubfp_lb);
		hdfs_object_free(ubfp_lb);

		// 4. setup recovery pipeline

		// configure this to be a recovery pipeline. Since we had pipeline "failure" while
		// in the middle of streaming data, we use type HDFS_DN_RECOVERY_STREAMING
		err = hdfs_datanode_write_set_append_or_recovery(dn, lb, HDFS_DN_RECOVERY_STREAMING, maxbytesrcvd);
		ck_assert_msg(!hdfs_is_error(err), "%s", format_error(err));

		// perform the actual pipeline setup
		err = hdfs_datanode_write_setup_pipeline(dn, _i/*sendcrcs*/, &err_idx);
		ck_assert_msg(!hdfs_is_error(err), "%s%s%s", format_error(err),
		    dn->dn_opresult_message ? "\n\tDatanode message: " : "",
		    dn->dn_opresult_message ? dn->dn_opresult_message : "");
		ck_assert_int_lt(err_idx, 0);

		// 5. tell the namenode about the updated pipeline (updatePipeline)
		newblock = hdfs_block_from_located_block(lb);
		nodes = hdfs_array_datanode_info_from_located_block(lb);
		storageids = hdfs_storage_ids_array_string_from_located_block(lb);
		hdfs2_updatePipeline(h, client, oldblock, newblock, nodes, storageids, &e);
		if (e)
			ck_abort_msg("exception: %s:\n%s",
			    hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

		hdfs_object_free(oldblock);
		hdfs_object_free(newblock);
		hdfs_object_free(nodes);
		hdfs_object_free(storageids);

		// 6. write to the new pipeline (starting after the last acked byte)
		err = hdfs_datanode_write(dn, buf + wtot + blk_acked, towrite_blk - blk_acked,
		    -1/*sendcrcs--ignored after setup_pipeline*/, &nwritten, &nacked, &err_idx);
		ck_assert_msg(!hdfs_is_error(err), "error writing block (recovery): %s",
		    format_error(err));
		ck_assert_int_eq(nwritten, towrite_blk - blk_acked);
		ck_assert_int_eq(nwritten, nacked);
		ck_assert_int_lt(err_idx, 0);

		hdfs_datanode_delete(dn);

		fprintf(stderr, "Recovered pipeline and wrote %zd bytes (%d new bytes). "
		    "%d total bytes written to this block\n",
		    nwritten, towrite_blk - blk_written, towrite_blk);

		if (prev)
			hdfs_object_free(prev);
		prev = hdfs_block_from_located_block(lb);
		prev->ob_val._block._length += nwritten;
		hdfs_object_free(lb);

		wtot += towrite_blk;
	} while (wtot < TOWRITE);

	// XXX This is dubious. There's no guarantee that complete will return
	// true within the time it takes for five round trip RPC
	// request/response cycles to be completed
	for (int i = 0; i < 5; i++) {
		if (i > 0)
			fprintf(stderr, "Notice: did not complete file on attempt %d, trying again...\n", i - 1);
		s = hdfs_complete(h, tf, client, prev, 0/*fileid?*/, &e);
		if (e)
			fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		if (s) // successfully completed
			break;
	}
	ck_assert_msg(s, "did not complete");

	hdfs_object_free(prev);

	end = _now();
	fprintf(stderr, "Wrote %d MB (with simulated pipeline failures and recoveries) from buf in %"PRIu64" ms (with %s csum), %02g MB/s\n",
	    TOWRITE/1024/1024, end - begin, csum2str[_i],
	    (double)TOWRITE/(end-begin)/1024*1000/1024);

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_int_eq(fs->ob_val._file_status._size, TOWRITE);
	hdfs_object_free(fs);

	// Read the file back
	bls = hdfs_getBlockLocations(h, tf, 0, TOWRITE, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	begin = _now();
	for (int i = 0; i < bls->ob_val._located_blocks._num_blocks; i++) {
		struct hdfs_object *bl =
		    bls->ob_val._located_blocks._blocks[i];
		dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_READ_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

		err = hdfs_datanode_read(dn, 0/*offset-in-block*/,
		    bl->ob_val._located_block._len, rbuf + i*BLOCKSZ, _i/*verifycrcs*/);

		hdfs_datanode_delete(dn);

		if (err.her_kind == he_hdfserr && err.her_num == HDFS_ERR_DATANODE_NO_CRCS) {
			fprintf(stderr, "Warning: test server doesn't support "
			    "CRCs, skipping validation.\n");
			_i = 0;

			// reconnect, try again without validating CRCs (for
			// isi_hdfs_d)
			dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_READ_BLOCK, &err);
			ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s", format_error(err));

			err = hdfs_datanode_read(dn, 0/*offset-in-block*/,
			    bl->ob_val._located_block._len, rbuf + i*BLOCKSZ, false/*verifycrcs*/);

			hdfs_datanode_delete(dn);
		}

		fail_if(hdfs_is_error(err), "error reading block: %s", format_error(err));
	}
	end = _now();
	fprintf(stderr, "Read %d MB to buf in %"PRIu64" ms%s, %02g MB/s\n\n",
	    TOWRITE/1024/1024, end - begin, _i? " (with csum verification)":"",
	    (double)TOWRITE/(end-begin)/1024*1000/1024);

	hdfs_object_free(bls);
	fail_if(memcmp(buf, rbuf, TOWRITE), "read differed from write");

	s = hdfs_delete(h, tf, false/*recurse*/, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
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

	nn = hdfs_namenode_new_version(H_ADDR, H_PORT, H_USER, H_KERB,
	    H_VER, &errs);
	ck_assert_msg(nn != NULL, "nn_new: %s", format_error(errs));

	bls = hdfs_getBlockLocations(nn, "/README.txt", 0, 10*1024, &e);
	if (e)
		ck_abort_msg("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	if (!bls || bls->ob_type == H_NULL ||
	    bls->ob_val._located_blocks._num_blocks == 0)
		goto out;

	bl = bls->ob_val._located_blocks._blocks[0];
	dn = hdfs_datanode_new(bl, "HADOOFUS_CLIENT", dn_proto, HDFS_DN_OP_READ_BLOCK, &errs);
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
	struct hdfs_object *e, *bl, *fs, *last;
	struct hdfs_error errs;
	int err_idx;
	ssize_t nwritten, nacked;

	e = NULL;
	dn = NULL;

	hdfs_delete(h, tf, false, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	hdfs_create(h, tf, 0644, client, true, false, 1, BLOCKSZ, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	bl = hdfs_addBlock(h, tf, client, NULL, NULL, 0, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	dn = hdfs_datanode_new(bl, "HADOOFUS_CLIENT", HDFS_DATANODE_AP_2_0, HDFS_DN_OP_WRITE_BLOCK, &errs);
	ck_assert_msg(dn != NULL, "dn_new: %s", format_error(errs));

	errs = hdfs_datanode_write(dn, buf, 33128, false/*sendcrcs*/, &nwritten, &nacked, &err_idx);
	ck_assert_msg(!hdfs_is_error(errs), "dn_read: %s", format_error(errs));
	ck_assert_int_eq(nwritten, 33128);
	ck_assert_int_eq(nacked, 33128);
	ck_assert_int_lt(err_idx, 0);

	hdfs_datanode_delete(dn);

	last = hdfs_block_from_located_block(bl);
	last->ob_val._block._length += 33128;
	hdfs_object_free(bl);

	hdfs_complete(h, tf, client, last, 0/*fileid?*/, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	fs = hdfs_getFileInfo(h, tf, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_int_eq(fs->ob_val._file_status._size, 33128);
	hdfs_object_free(fs);

	hdfs_delete(h, tf, false, &e);
	if (e)
		fail("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
}
END_TEST

static Suite *
t_datanode1_basics_suite()
{
	Suite *s;
	TCase *tc;
	s = suite_create("datanode1");

	// The test fixtures here must be checked, not unchecked, (i.e. they
	// must be run within the forked address spaces) or we must run the
	// suites in No Fork mode since otherwise RPC msgnos end up getting
	// reused between unit tests within the test case for the same namenode
	// connection, leading to test failure (hdfs raises exceptions)

	s = suite_create("datanode1");

	// v1 does not have crc32c support
	tc = tcase_create("buf1");
	tcase_add_checked_fixture(tc, setup_buf, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_buf, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32 + 1);
	suite_add_tcase(s, tc);

	tc = tcase_create("iovec1");
	tcase_add_checked_fixture(tc, setup_buf, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_writev, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32 + 1);
	suite_add_tcase(s, tc);

	tc = tcase_create("append1");
	tcase_add_checked_fixture(tc, setup_buf, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_file, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32 + 1);
	suite_add_tcase(s, tc);

	tc = tcase_create("file1");
	tcase_add_checked_fixture(tc, setup_file, teardown_file);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_file, 0, 2);
	suite_add_tcase(s, tc);

	return s;
}

static Suite *
t_datanode2_basics_suite()
{
	Suite *s;
	TCase *tc;

	// The test fixtures here must be checked, not unchecked, (i.e. they
	// must be run within the forked address spaces) or we must run the
	// suites in No Fork mode since otherwise RPC msgnos end up getting
	// reused between unit tests within the test case for the same namenode
	// connection, leading to test failure (hdfs raises exceptions)

	s = suite_create("datanode2");

	tc = tcase_create("dn2_short");
	tcase_add_checked_fixture(tc, setup_buf, teardown_buf);
	tcase_add_test(tc, test_short_read);
	tcase_add_test(tc, test_short_write);
	suite_add_tcase(s, tc);

	tc = tcase_create("buf2");
	/* tcase_add_unchecked_fixture(tc, setup_buf, teardown_buf); */
	tcase_add_checked_fixture(tc, setup_buf, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_buf, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);
	suite_add_tcase(s, tc);

	tc = tcase_create("iovec2");
	/* tcase_add_unchecked_fixture(tc, setup_buf, teardown_buf); */
	tcase_add_checked_fixture(tc, setup_buf, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_writev, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);
	suite_add_tcase(s, tc);

	tc = tcase_create("append2");
	/* tcase_add_unchecked_fixture(tc, setup_buf, teardown_buf); */
	tcase_add_checked_fixture(tc, setup_buf, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_append_buf, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);
	suite_add_tcase(s, tc);

	tc = tcase_create("file2");
	tcase_add_checked_fixture(tc, setup_file, teardown_file);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_file, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);
	suite_add_tcase(s, tc);

	return s;
}

static Suite *
t_datanode22_basics_suite()
{
	Suite *s;
	TCase *tc;

	// The test fixtures here must be checked, not unchecked, (i.e. they
	// must be run within the forked address spaces) or we must run the
	// suites in No Fork mode since otherwise RPC msgnos end up getting
	// reused between unit tests within the test case for the same namenode
	// connection, leading to test failure (hdfs raises exceptions)

	s = suite_create("datanode22");

	tc = tcase_create("buf22");
	tcase_add_checked_fixture(tc, setup_buf, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_buf, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);
	suite_add_tcase(s, tc);

	tc = tcase_create("iovec22");
	tcase_add_checked_fixture(tc, setup_buf, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_writev, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);
	suite_add_tcase(s, tc);

	tc = tcase_create("append22");
	tcase_add_checked_fixture(tc, setup_buf, teardown_buf);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_append_buf, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);
	suite_add_tcase(s, tc);

	tc = tcase_create("file22");
	tcase_add_checked_fixture(tc, setup_file, teardown_file);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_file, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);

	suite_add_tcase(s, tc);

	tc = tcase_create("recovery22");
	tcase_add_checked_fixture(tc, setup_file, teardown_file);
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_recovery, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);

	suite_add_tcase(s, tc);

	return s;
}

Suite *
t_datanode_basics_suite()
{
	switch (H_VER) {
	case HDFS_NN_v1:
		return t_datanode1_basics_suite();
	case HDFS_NN_v2:
		return t_datanode2_basics_suite();
	case HDFS_NN_v2_2:
		return t_datanode22_basics_suite();
	default:
		assert(false);
	}
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
