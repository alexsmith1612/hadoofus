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

static const int TOWRITE = 70*1024*1024,
      BLOCKSZ = 64*1024*1024;

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

START_TEST(test_dn_nb_misc)
{
	const char *client = "HADOOFUS_CLIENT";
	const char *tf = "/HADOOFUS_TEST_DN_MISC";
	struct hdfs_error err = HDFS_SUCCESS;
	struct hdfs_namenode *nn;
	struct hdfs_datanode *dn;
	struct hdfs_object *fsd, *fs, *e = NULL, *bl, *last, *bls;
	int dn_proto, replication = 1, err_idx, rc;
	size_t wblk = 0, ablk = 0;
	ssize_t nwritten, nacked;
	const char wbuf[] = "01234567890123456789";
	char rbuf[sizeof(wbuf)];
	struct pollfd pfd;
	bool s;

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

	nn = hdfs_namenode_new_version(H_ADDR, H_PORT, H_USER, H_KERB,
	    H_VER, &err);
	ck_assert_msg(nn,
	    "Could not connect to %s=%s @ %s=%s (%s=%s): error (%s): %s",
	    HDFS_T_USER, H_USER, HDFS_T_ADDR, H_ADDR,
	    HDFS_T_PORT, H_PORT, hdfs_error_str_kind(err), hdfs_error_str(err));

	if (H_VER > HDFS_NN_v1) {
		fsd = hdfs2_getServerDefaults(nn, &e);
		if (e)
			ck_abort_msg("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		replication = fsd->ob_val._server_defaults._replication;
		// XXX TODO blocksize?
		hdfs_object_free(fsd);
	}

	fs = hdfs_create(nn, tf, 0644, client, true/*overwrite*/,
	    false/*createparent*/, replication, BLOCKSZ, &e);
	if (e)
		ck_abort_msg("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	if (fs) {
		ck_assert_int_eq(fs->ob_type, H_FILE_STATUS);
		// XXX TODO fileid?
		hdfs_object_free(fs);
	}

	bl = hdfs_addBlock(nn, tf, client, NULL/*excluded*/, NULL/*prev*/, 0/*fileid?*/, &e);
	if (e)
		ck_abort_msg("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_WRITE_BLOCK, &err);
	ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s:%s (%s:%s)",
	    hdfs_error_str_kind(err), hdfs_error_str(err),
	    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._ipaddr,
	    bl->ob_val._located_block._locs[0]->ob_val._datanode_info._port);

	err = hdfs_datanode_write_nb_init(dn, false/*sendcrcs*/);
	ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
	    hdfs_error_str_kind(err), hdfs_error_str(err));

	// Write the first segment
	while (true) {
		err = hdfs_datanode_write_nb(dn, wbuf + wblk, strlen(wbuf)/2 - wblk, &nwritten, &nacked, &err_idx);
		wblk += nwritten;
		ablk += nacked;
		if (!hdfs_is_again(err)) {
			break;
		}
		err = hdfs_datanode_get_eventfd(dn, &pfd.fd, &pfd.events);
		ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
		    hdfs_error_str_kind(err), hdfs_error_str(err));
		rc = poll(&pfd, 1, -1);
		ck_assert_int_eq(rc, 1);
		ck_assert_int_ne(pfd.revents, 0);
	}
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s%s%s%s%s",
	    hdfs_error_str_kind(err),
	    hdfs_error_str(err),
	    dn->dn_opresult_message ? "\n\tDatanode message: " : "",
	    dn->dn_opresult_message ? dn->dn_opresult_message : "",
	    dn->dn_unexpected_firstbadlink ? "\n\tUnexpected firstbadlink: " : "",
	    dn->dn_unexpected_firstbadlink ? dn->dn_unexpected_firstbadlink : "");
	ck_assert_int_eq(wblk, strlen(wbuf)/2);

	// Send a heartbeat packet
	while (true) {
		err = hdfs_datanode_send_heartbeat(dn, &nacked, &err_idx);
		ablk += nacked;
		if (!hdfs_is_again(err)) {
			break;
		}
		err = hdfs_datanode_get_eventfd(dn, &pfd.fd, &pfd.events);
		ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
		    hdfs_error_str_kind(err), hdfs_error_str(err));
		rc = poll(&pfd, 1, -1);
		ck_assert_int_eq(rc, 1);
		ck_assert_int_ne(pfd.revents, 0);
	}
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s%s%s%s%s",
	    hdfs_error_str_kind(err),
	    hdfs_error_str(err),
	    dn->dn_opresult_message ? "\n\tDatanode message: " : "",
	    dn->dn_opresult_message ? dn->dn_opresult_message : "",
	    dn->dn_unexpected_firstbadlink ? "\n\tUnexpected firstbadlink: " : "",
	    dn->dn_unexpected_firstbadlink ? dn->dn_unexpected_firstbadlink : "");

	// Test manual ack checking
	err = hdfs_datanode_check_acks(dn, &nacked, &err_idx);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s%s%s%s%s",
	    hdfs_error_str_kind(err),
	    hdfs_error_str(err),
	    dn->dn_opresult_message ? "\n\tDatanode message: " : "",
	    dn->dn_opresult_message ? dn->dn_opresult_message : "",
	    dn->dn_unexpected_firstbadlink ? "\n\tUnexpected firstbadlink: " : "",
	    dn->dn_unexpected_firstbadlink ? dn->dn_unexpected_firstbadlink : "");
	ablk += nacked;

	// Write the second segment
	while (true) {
		err = hdfs_datanode_write_nb(dn, wbuf + wblk, strlen(wbuf) - wblk, &nwritten, &nacked, &err_idx);
		wblk += nwritten;
		ablk += nacked;
		if (!hdfs_is_again(err)) {
			break;
		}
		err = hdfs_datanode_get_eventfd(dn, &pfd.fd, &pfd.events);
		ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
		    hdfs_error_str_kind(err), hdfs_error_str(err));
		rc = poll(&pfd, 1, -1);
		ck_assert_int_eq(rc, 1);
		ck_assert_int_ne(pfd.revents, 0);
	}
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s%s%s%s%s",
	    hdfs_error_str_kind(err),
	    hdfs_error_str(err),
	    dn->dn_opresult_message ? "\n\tDatanode message: " : "",
	    dn->dn_opresult_message ? dn->dn_opresult_message : "",
	    dn->dn_unexpected_firstbadlink ? "\n\tUnexpected firstbadlink: " : "",
	    dn->dn_unexpected_firstbadlink ? dn->dn_unexpected_firstbadlink : "");
	ck_assert_int_eq(wblk, strlen(wbuf));

	// Finish the block
	while (true) {
		err = hdfs_datanode_finish_block(dn, &nacked, &err_idx);
		ablk += nacked;
		if (!hdfs_is_again(err)) {
			break;
		}
		err = hdfs_datanode_get_eventfd(dn, &pfd.fd, &pfd.events);
		ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
		    hdfs_error_str_kind(err), hdfs_error_str(err));
		rc = poll(&pfd, 1, -1);
		ck_assert_int_eq(rc, 1);
		ck_assert_int_ne(pfd.revents, 0);
	}
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s%s%s%s%s",
	    hdfs_error_str_kind(err),
	    hdfs_error_str(err),
	    dn->dn_opresult_message ? "\n\tDatanode message: " : "",
	    dn->dn_opresult_message ? dn->dn_opresult_message : "",
	    dn->dn_unexpected_firstbadlink ? "\n\tUnexpected firstbadlink: " : "",
	    dn->dn_unexpected_firstbadlink ? dn->dn_unexpected_firstbadlink : "");
	ck_assert_int_eq(ablk, wblk);

	hdfs_datanode_delete(dn);

	last = hdfs_block_from_located_block(bl);
	last->ob_val._block._length += ablk;
	hdfs_object_free(bl);

	for (int i = 0; i < 5; i++) {
		if (i > 0) {
			fprintf(stderr, "Notice: did not complete file on attempt %d, delaying for 1s and trying again...\n", i - 1);
			usleep(1000000); // sleep for 1s before retrying
		}
		s = hdfs_complete(nn, tf, client, last, 0/*fileid?*/, &e);
		if (e)
			ck_abort_msg("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		if (s) // successfully completed
			break;
	}
	ck_assert_msg(s, "did not complete");
	hdfs_object_free(last);

	fs = hdfs_getFileInfo(nn, tf, &e);
	if (e)
		ck_abort_msg("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_int_eq(fs->ob_val._file_status._size, strlen(wbuf));
	hdfs_object_free(fs);


	// Read the file back
	bls = hdfs_getBlockLocations(nn, tf, 0, strlen(wbuf), &e);
	if (e)
		ck_abort_msg("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));

	for (int i = 0; i < bls->ob_val._located_blocks._num_blocks; i++) {
		struct hdfs_object *bl =
		    bls->ob_val._located_blocks._blocks[i];
		dn = hdfs_datanode_new(bl, client, dn_proto, HDFS_DN_OP_READ_BLOCK, &err);
		ck_assert_msg((intptr_t)dn, "error connecting to datanode: %s:%s",
		    hdfs_error_str_kind(err), hdfs_error_str(err));

		err = hdfs_datanode_read(dn, 0/*offset-in-block*/,
		    bl->ob_val._located_block._len, rbuf + i*BLOCKSZ, false/*verifycrcs*/);

		hdfs_datanode_delete(dn);

		ck_assert_msg(!hdfs_is_error(err), "error reading block: %s:%s",
		    hdfs_error_str_kind(err), hdfs_error_str(err));
	}

	hdfs_object_free(bls);
	ck_assert_msg(!memcmp(wbuf, rbuf, strlen(wbuf)), "read differed from write");

	s = hdfs_delete(nn, tf, false/*recurse*/, &e);
	if (e)
		ck_abort_msg("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
	ck_assert_msg(s, "delete returned false");

	hdfs_namenode_delete(nn);
}
END_TEST

START_TEST(test_dn_write_nb)
{
	const char *client = "HADOOFUS_CLIENT";
	struct hdfs_error err = HDFS_SUCCESS;
	struct hdfs_namenode nn = { 0 };
	struct hdfs_object *obj, *fsd, *e = NULL;
	int dn_proto, rc, replication = 1, nrpcs = 0, n_wr_end = 0, n_rd_end = 0;
	int64_t mn_recv;
	bool finished, wr_started = false, rd_started = false;
	char *wbuf = NULL, *rbuf = NULL;
	const int rbufsz = 48 * 1024; // Something smaller than a packet size for testing purposes
	uint64_t tstart_tot, tend_tot, tstart_wr, tstart_rd;

	struct fctx {
		const char *tf;
		enum { ST_CR, ST_AB, ST_WR, ST_CM, ST_GI, ST_GB, ST_RD, ST_DL, ST_FN } st;
		int64_t mn;
		uint64_t tstart,
			 tend,
			 cmpl_retry_time;
		struct hdfs_object *prev,
				   *bls;
		struct hdfs_datanode dn;
		int wtot,
		    atot,
		    ablk,
		    cmpl_attempts,
		    blkidx,
		    rblk,
		    rtot;
		bool cmpl_retry;
	} fctxs[] = {
		{ .tf = "/HADOOFUS_TEST_WRITE_NB_0" },
		{ .tf = "/HADOOFUS_TEST_WRITE_NB_1" },
		{ .tf = "/HADOOFUS_TEST_WRITE_NB_2" },
		{ .tf = "/HADOOFUS_TEST_WRITE_NB_3" },
		{ .tf = "/HADOOFUS_TEST_WRITE_NB_4" },
		{ .tf = "/HADOOFUS_TEST_WRITE_NB_5" },
		{ .tf = "/HADOOFUS_TEST_WRITE_NB_6" },
		{ .tf = "/HADOOFUS_TEST_WRITE_NB_7" },
	};

	struct pollfd pfd[1 + nelem(fctxs)];

	// XXX TODO This test could easily (and should) get refactored
	// into separate functions for readability

	// Initialize the buffers
	wbuf = malloc(TOWRITE);
	ck_assert(wbuf);
	for (int i = 0; i < TOWRITE; i++) {
		wbuf[i] = '0' + (i % 10);
	}
	rbuf = malloc(rbufsz);
	ck_assert(rbuf);

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

	tstart_tot = _now();

	hdfs_namenode_init_ver(&nn, H_KERB, H_VER);
	hdfs_namenode_auth_nb_init(&nn, H_USER);

	// Connect and authenticate with the namenode
	err = hdfs_namenode_connect_init(&nn, H_ADDR, H_PORT, false/*numerichost*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	do {
		if (hdfs_is_again(err)) {
			err = hdfs_namenode_get_eventfd(&nn, &pfd[0].fd, &pfd[0].events);
			ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
			    hdfs_error_str_kind(err), hdfs_error_str(err));
			ck_assert_int_ne(pfd[0].events, 0);

			rc = poll(pfd, 1, -1);
			ck_assert_int_eq(rc, 1);
			ck_assert_int_ne(pfd[0].revents, 0);
		}

		err = hdfs_namenode_connauth_nb(&nn);
	} while (hdfs_is_again(err));
	ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
	    hdfs_error_str_kind(err), hdfs_error_str(err));

	if (H_VER > HDFS_NN_v1) {
		// We can use the blocking hl API when ther are no pending RPCs
		fsd = hdfs2_getServerDefaults(&nn, &e);
		if (e)
			ck_abort_msg("exception: %s:\n%s", hdfs_exception_get_type_str(e), hdfs_exception_get_message(e));
		replication = fsd->ob_val._server_defaults._replication;
		// XXX TODO blocksize?
		hdfs_object_free(fsd);
	}

	// Send the create RPCs to begin with
	for (unsigned i = 0; i < nelem(fctxs); i++) {
		err = hdfs_create_nb(&nn, fctxs[i].tf, 0644, client, true/*overwrite*/,
		    false/*create_parent*/, replication, BLOCKSZ, &fctxs[i].mn, &fctxs[i] /*userdata*/);
		ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
		    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
		nrpcs++;
	}

	do {
		uint64_t tnow;

		// pfd[0] holds the namenode fd
		err = hdfs_namenode_get_eventfd(&nn, &pfd[0].fd, &pfd[0].events);
		ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
		    hdfs_error_str_kind(err), hdfs_error_str(err));

		// the rest of the array hold the datanode fds (with the pfd idx one plus the fctxs idx)
		for (unsigned i = 0; i < nelem(fctxs); i++) {
			if (fctxs[i].st == ST_WR || fctxs[i].st == ST_RD) {
				err = hdfs_datanode_get_eventfd(&fctxs[i].dn,
				    &pfd[i + 1].fd, &pfd[i + 1].events);
				ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
				    hdfs_error_str_kind(err), hdfs_error_str(err));
			} else {
				pfd[i + 1].fd = -1;
				pfd[i + 1].events = 0;
			}
		}

		rc = poll(pfd, nelem(pfd), 1000);
		ck_assert_msg(rc >= 0, "poll error: %s", strerror(errno));

		// Check if we need to retry any complete RPCs
		tnow = _now();
		for (unsigned i = 0; i < nelem(fctxs); i++) {
			if (fctxs[i].st != ST_CM || !fctxs[i].cmpl_retry
			    || fctxs[i].cmpl_retry_time > tnow)
				continue;

			// Retry the complete RPC
			err = hdfs_complete_nb(&nn, fctxs[i].tf, client,
			    fctxs[i].prev, 0/*fileid?*/, &fctxs[i].mn, &fctxs[i]);
			ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
			    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
			fctxs[i].cmpl_attempts++;
			fctxs[i].cmpl_retry = false;
			nrpcs++;
		}

		if (rc == 0) // No fds are ready
			continue;

		if (pfd[0].revents & POLLOUT) {
			err = hdfs_namenode_invoke_continue(&nn);
			ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
			    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
		}

		// Process RPC responses
		do {
			void *ud_recv;
			struct fctx *fctxp;
			struct hdfs_object *lb;

			if (nrpcs == 0 || !(pfd[0].revents & POLLIN))
				break;

			err = hdfs_namenode_recv(&nn, &obj, &mn_recv, &ud_recv);
			ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
			    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
			if (hdfs_is_again(err))
				break;

			// We received a response
			ck_assert_msg(ud_recv, "NULL userdata returned from hdfs_namenode_recv()");
			fctxp = ud_recv;
			ck_assert_int_eq(fctxp->mn, mn_recv);
			nrpcs--;
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION,
			    "exception (file %s, state %d): %s:\n%s",
			    fctxp->tf, fctxp->st,
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));

			switch (fctxp->st) {
			case ST_CR:
				ck_assert_int_eq(obj->ob_type, H_FILE_STATUS);
				hdfs_object_free(obj);
				fctxp->tstart = _now();
				if (!wr_started) {
					wr_started = true;
					tstart_wr = fctxp->tstart;
				}
				err = hdfs_addBlock_nb(&nn, fctxp->tf, client, NULL,
				    fctxp->prev, 0/*fileid*/, &fctxp->mn, fctxp);
				ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
				    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
				nrpcs++;
				fctxp->st = ST_AB;
				break;

			case ST_AB:
				ck_assert_int_eq(obj->ob_type, H_LOCATED_BLOCK);
				err = hdfs_datanode_init(&fctxp->dn, obj, client, dn_proto, HDFS_DN_OP_WRITE_BLOCK);
				ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
				    hdfs_error_str_kind(err), hdfs_error_str(err));
				err = hdfs_datanode_write_nb_init(&fctxp->dn, _i/*sendcrcs*/);
				ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
				    hdfs_error_str_kind(err), hdfs_error_str(err));
				// first write happens below, prior to next poll()
				fctxp->prev = hdfs_block_from_located_block(obj);
				hdfs_object_free(obj);
				fctxp->st = ST_WR;
				break;

			case ST_CM:
				ck_assert_int_eq(obj->ob_type, H_BOOLEAN);
				if (!obj->ob_val._boolean._val) {
					hdfs_object_free(obj);
					if (fctxp->cmpl_attempts == 5)
						ck_abort_msg("Unable to complete file %s", fctxp->tf);
					fprintf(stderr,
					    "Notice: did not complete file %s on attempt %d, delaying for 1s and trying again...\n",
					    fctxp->tf, fctxp->cmpl_attempts);
					fctxp->cmpl_retry = true;
					fctxp->cmpl_retry_time = tnow + 1000; // Delay 1000ms before retrying
					break;
				}
				hdfs_object_free(obj);
				hdfs_object_free(fctxp->prev);
				fctxp->tend = _now();
				fprintf(stderr, "(Non-blocking: %s) Wrote %d MB from buf in %"PRIu64" ms (with %s csum), %02g MB/s\n",
				    fctxp->tf, fctxp->wtot/1024/1024, fctxp->tend - fctxp->tstart,
				    csum2str[_i],
				    ((double)fctxp->wtot/1024/1024) / ((double)(fctxp->tend - fctxp->tstart)/1000));
				if (++n_wr_end == nelem(fctxs)) {
					fprintf(stderr, "(Non-blocking) %ju files, each %d MB (%ju MB total) "
					    "written in %"PRIu64" ms (with %s csum), %02g MB/s\n",
					    nelem(fctxs), TOWRITE/1024/1024, TOWRITE*nelem(fctxs)/1024/1024,
					    fctxp->tend - tstart_wr, csum2str[_i],
					    ((double)TOWRITE*nelem(fctxs)/1024/1024) / ((double)(fctxp->tend - tstart_wr)/1000));
				}
				err = hdfs_getFileInfo_nb(&nn, fctxp->tf, &fctxp->mn, fctxp);
				ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
				    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
				nrpcs++;
				fctxp->st = ST_GI;
				break;

			case ST_GI:
				ck_assert_int_eq(obj->ob_type, H_FILE_STATUS);
				ck_assert_msg(fctxp->wtot == obj->ob_val._file_status._size,
				    "File '%s' size does not match: expected %d, got %"PRIi64,
				    fctxp->tf, fctxp->wtot, obj->ob_val._file_status._size);
				hdfs_object_free(obj);
				err = hdfs_getBlockLocations_nb(&nn, fctxp->tf,
				    0/*offset*/, TOWRITE/*length*/, &fctxp->mn, fctxp);
				ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
				    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
				nrpcs++;
				fctxp->st = ST_GB;
				break;

			case ST_GB:
				ck_assert_int_eq(obj->ob_type, H_LOCATED_BLOCKS);
				ck_assert_int_gt(obj->ob_val._located_blocks._num_blocks, 0);
				fctxp->tstart = _now();
				if (!rd_started) {
					rd_started = true;
					tstart_rd = fctxp->tstart;
				}
				fctxp->bls = obj;
				lb = fctxp->bls->ob_val._located_blocks._blocks[fctxp->blkidx];
				err = hdfs_datanode_init(&fctxp->dn, lb, client, dn_proto, HDFS_DN_OP_READ_BLOCK);
				ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
				    hdfs_error_str_kind(err), hdfs_error_str(err));
				err = hdfs_datanode_read_nb_init(&fctxp->dn, 0/*block offset*/,
				    lb->ob_val._located_block._len, _i/*verifycrcs*/);
				ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
				    hdfs_error_str_kind(err), hdfs_error_str(err));
				// first read happens below prior to next poll()
				fctxp->st = ST_RD;
				break;

			case ST_DL:
				ck_assert_int_eq(obj->ob_type, H_BOOLEAN);
				ck_assert_msg(obj->ob_val._boolean._val, "delete returned false");
				hdfs_object_free(obj);
				fctxp->st = ST_FN;
				break;

			case ST_WR:
			case ST_RD:
			case ST_FN:
				ck_abort_msg("Received rpc response in state %d for file %s",
				    fctxp->st, fctxp->tf);
			default:
				ck_abort_msg("Unknown state %d for file %s", fctxp->st, fctxp->tf);
			}
		} while (nrpcs > 0); // Proces RPC responses

		// Process the datanodes
		for (unsigned i = 0; i < nelem(fctxs); i++) {
			if ((pfd[i + 1].fd >= 0 && !pfd[i + 1].revents)
			    || (pfd[i + 1].fd < 0 && fctxs[i].st != ST_WR && fctxs[i].st != ST_RD))
				continue;

			if (fctxs[i].st == ST_WR) { // Writing the files
				size_t wlen;
				ssize_t nwritten, nacked;
				int erridx;

				ck_assert_int_le(fctxs[i].prev->ob_val._block._length, BLOCKSZ);

				if (fctxs[i].wtot < TOWRITE && fctxs[i].prev->ob_val._block._length < BLOCKSZ) {
					// more to write
					wlen = _min(TOWRITE - fctxs[i].wtot,
					    BLOCKSZ - fctxs[i].prev->ob_val._block._length);
					err = hdfs_datanode_write_nb(&fctxs[i].dn, wbuf + fctxs[i].wtot,
					    wlen, &nwritten, &nacked, &erridx);
					ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
					    "error (%s): %s%s%s%s%s",
					    hdfs_error_str_kind(err),
					    hdfs_error_str(err),
					    fctxs[i].dn.dn_opresult_message ? "\n\tDatanode message: " : "",
					    fctxs[i].dn.dn_opresult_message ? fctxs[i].dn.dn_opresult_message : "",
					    fctxs[i].dn.dn_unexpected_firstbadlink ? "\n\tUnexpected firstbadlink: " : "",
					    fctxs[i].dn.dn_unexpected_firstbadlink ? fctxs[i].dn.dn_unexpected_firstbadlink : "");
					fctxs[i].wtot += nwritten;
					fctxs[i].prev->ob_val._block._length += nwritten;
					fctxs[i].atot += nacked;
					fctxs[i].ablk += nacked;
				}

				if (fctxs[i].wtot == TOWRITE || fctxs[i].prev->ob_val._block._length == BLOCKSZ) {
					// no more data to write, but must finalize block
					err = hdfs_datanode_finish_block(&fctxs[i].dn, &nacked, &erridx);
					ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
					    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
					fctxs[i].atot += nacked;
					fctxs[i].ablk += nacked;
					if (!hdfs_is_error(err)) {
						// finished block
						hdfs_datanode_clean(&fctxs[i].dn);
						if (fctxs[i].wtot < TOWRITE) {
							ck_assert_int_eq(fctxs[i].prev->ob_val._block._length, BLOCKSZ);
							ck_assert_int_eq(fctxs[i].ablk, BLOCKSZ);
							fctxs[i].ablk = 0;
							err = hdfs_addBlock_nb(&nn, fctxs[i].tf, client, NULL,
							    fctxs[i].prev, 0/*fileid*/, &fctxs[i].mn, &fctxs[i]);
							ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
							    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
							nrpcs++;
							hdfs_object_free(fctxs[i].prev);
							fctxs[i].st = ST_AB;
						} else {
							ck_assert_int_eq(fctxs[i].prev->ob_val._block._length, TOWRITE % BLOCKSZ);
							ck_assert_int_eq(fctxs[i].atot, TOWRITE);
							err = hdfs_complete_nb(&nn, fctxs[i].tf, client,
							    fctxs[i].prev, 0/*fileid*/, &fctxs[i].mn, &fctxs[i]);
							ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
							    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
							nrpcs++;
							fctxs[i].st = ST_CM;
						}
					}
				}
			} else if (fctxs[i].st == ST_RD) { // Reading back the files
				bool has_next_block;

				do {
					ssize_t nread;

					has_next_block = false;
					do {
						err = hdfs_datanode_read_nb(&fctxs[i].dn, rbufsz, rbuf, &nread);
						// TODO handle servers that don't support crcs (do those still exist?)
						ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
						    "error (%s): %s%s%s", hdfs_error_str_kind(err), hdfs_error_str(err),
						    fctxs[i].dn.dn_opresult_message ? "\n\tDatanode message: " : "",
						    fctxs[i].dn.dn_opresult_message ? fctxs[i].dn.dn_opresult_message : "");
						if (nread > 0) {
							ck_assert_msg(memcmp(wbuf + fctxs[i].rtot, rbuf, nread) == 0,
							    "Read differed from write for '%s'", fctxs[i].tf);
							fctxs[i].rblk += nread;
							fctxs[i].rtot += nread;
						}
						// Tight loop while there is more data left in the block that we want
						// to read (i.e. HDFS_AGAIN) and our buffer has been completely filled
						// (i.e. there may be more data available immediately)
					} while (hdfs_is_again(err) && nread == rbufsz);

					if (!hdfs_is_error(err)) {
						struct hdfs_object *lb = fctxs[i].bls->ob_val._located_blocks._blocks[fctxs[i].blkidx];
						// Finished with our read operation
						if (fctxs[i].rtot == TOWRITE) {
							// Fully read the file
							fctxs[i].tend = _now();
							fprintf(stderr, "(Non-blocking: %s) Read and compared %d MB from buf in %"PRIu64" ms%s, %02g MB/s\n",
							    fctxs[i].tf, fctxs[i].rtot/1024/1024, fctxs[i].tend - fctxs[i].tstart,
							    _i ? " (with csum verification)" : "",
							    ((double)fctxs[i].rtot/1024/1024) / ((double)(fctxs[i].tend - fctxs[i].tstart)/1000));
							if (++n_rd_end == nelem(fctxs)) {
								fprintf(stderr, "(Non-blocking) %ju files, each %d MB (%ju MB total) "
								    "read and compared in %"PRIu64" ms%s, %02g MB/s\n",
								    nelem(fctxs), TOWRITE/1024/1024, TOWRITE*nelem(fctxs)/1024/1024,
								    fctxs[i].tend - tstart_rd, _i ? " (with csum verification)" : "",
								    ((double)TOWRITE*nelem(fctxs)/1024/1024) / ((double)(fctxs[i].tend - tstart_rd)/1000));
							}
							hdfs_datanode_destroy(&fctxs[i].dn);
							hdfs_object_free(fctxs[i].bls);
							err = hdfs_delete_nb(&nn, fctxs[i].tf, false/*can_recurse*/,
							    &fctxs[i].mn, &fctxs[i]);
							ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
							    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
							nrpcs++;
							fctxs[i].st = ST_DL;
						} else if (fctxs[i].rblk == lb->ob_val._located_block._len) {
							// Fully read this block
							hdfs_datanode_clean(&fctxs[i].dn);
							fctxs[i].blkidx++;
							lb = fctxs[i].bls->ob_val._located_blocks._blocks[fctxs[i].blkidx];
							err = hdfs_datanode_init(&fctxs[i].dn, lb, client, dn_proto, HDFS_DN_OP_READ_BLOCK);
							ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
							    hdfs_error_str_kind(err), hdfs_error_str(err));
							err = hdfs_datanode_read_nb_init(&fctxs[i].dn, 0/*block offset*/,
							    lb->ob_val._located_block._len, _i/*verifycrcs*/);
							ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
							    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));

							// Loop over this whole read processing again for the
							// new block so that we proceed to a state where we
							// are waiting on resources such that we can call
							// hdfs_datanode_get_eventfd() again
							has_next_block = true;
						} else
							ck_abort_msg("Finished read operation without finishing desired total or block size");
					}
				} while (has_next_block);
			} else
				ck_abort_msg("Datanode being processed in unexpected state %d", fctxs[i].st);
		} // Process datanodes

		finished = true;
		for (unsigned i = 0; i < nelem(fctxs); i++) {
			if (fctxs[i].st != ST_FN) {
				finished = false;
				break;
			}
		}
	} while (!finished);

	tend_tot = _now();
	fprintf(stderr, "(Non-blocking) %ju files, each %d MB (%ju MB total) "
	    "created, written, read, compared, and deleted in %"PRIu64" ms (with %s csum). Average of %02g MB/s I/O\n\n",
	    nelem(fctxs), TOWRITE/1024/1024, TOWRITE*nelem(fctxs)/1024/1024,
	    tend_tot - tstart_tot, csum2str[_i],
	    ((double)TOWRITE*nelem(fctxs)/1024/1024) / ((double)(tend_tot - tstart_tot)/1000));

	hdfs_namenode_destroy(&nn);
	free(wbuf);
	free(rbuf);
}
END_TEST

static Suite *
t_datanode1_nb_suite()
{
	Suite *s;
	TCase *tc;

	s = suite_create("datanode1_nb");

	tc = tcase_create("datanode_nb_misc1");
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	tcase_add_test(tc, test_dn_nb_misc);
	suite_add_tcase(s, tc);

	tc = tcase_create("multi_file1");
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_nb, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32 + 1);
	suite_add_tcase(s, tc);

	return s;
}

static Suite *
t_datanode2_nb_suite()
{
	Suite *s;
	TCase *tc;

	s = suite_create("datanode2_nb");

	tc = tcase_create("datanode_nb_misc2");
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	tcase_add_test(tc, test_dn_nb_misc);
	suite_add_tcase(s, tc);

	tc = tcase_create("multi_file2");
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_nb, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);
	suite_add_tcase(s, tc);

	return s;
}

static Suite *
t_datanode22_nb_suite()
{
	Suite *s;
	TCase *tc;

	s = suite_create("datanode22_nb");

	tc = tcase_create("datanode_nb_misc22");
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	tcase_add_test(tc, test_dn_nb_misc);
	suite_add_tcase(s, tc);

	tc = tcase_create("multi_file22");
	tcase_set_timeout(tc, 2*60/*2 minutes*/);
	// Loop each test to send or not send crcs
	tcase_add_loop_test(tc, test_dn_write_nb, HDFS_CSUM_NULL,
	    HDFS_CSUM_CRC32C + 1);
	suite_add_tcase(s, tc);

	return s;
}

Suite *
t_datanode_nb_suite()
{
	switch (H_VER) {
	case HDFS_NN_v1:
		return t_datanode1_nb_suite();
	case HDFS_NN_v2:
		return t_datanode2_nb_suite();
	case HDFS_NN_v2_2:
		return t_datanode22_nb_suite();
	default:
		assert(false);
	}
}
