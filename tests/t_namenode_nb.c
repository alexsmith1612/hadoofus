#include <inttypes.h>
#include <poll.h>
#include <stdio.h>

#include <check.h>

#include <hadoofus/highlevel.h>

#include "t_main.h"

START_TEST(test_nb_multi_rpcs)
{
	const char *client = "HADOOFUS_CLIENT",
	    *tf1 = "/HADOOFUS_TEST_MULTI_1",
	    *tf2 = "/HADOOFUS_TEST_MULTI_2",
	    *tf3 = "/HADOOFUS_TEST_MULTI_3",
	    *tf4 = "/HADOOFUS_TEST_MULTI_4";
	struct hdfs_error err = HDFS_SUCCESS, ret;
	struct hdfs_namenode nn = { 0 };
	struct pollfd pfd = { 0 };
	int rc, nrpcs;
	int64_t mn_recv,
	    mn_cr1, mn_cr2, mn_cr3, mn_cr4,
	    mn_gfi1, mn_gfi2, mn_gfi3, mn_gfi4,
	    mn_del1, mn_del2, mn_del3, mn_del4;
	struct hdfs_object *obj;
	void *ud_recv;

	hdfs_namenode_init_ver(&nn, H_KERB, H_VER);
	hdfs_namenode_auth_nb_init(&nn, H_USER);

	// Connect and authenticate with the namenode
	err = hdfs_namenode_connect_init(&nn, H_ADDR, H_PORT, false/*numerichost*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	do {
		if (hdfs_is_again(err)) {
			err = hdfs_namenode_get_eventfd(&nn, &pfd.fd, &pfd.events);
			ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
			    hdfs_error_str_kind(err), hdfs_error_str(err));
			ck_assert_int_ne(pfd.events, 0);

			rc = poll(&pfd, 1, -1);
			ck_assert_int_eq(rc, 1);
			ck_assert_int_ne(pfd.revents, 0);
		}

		err = hdfs_namenode_connauth_nb(&nn);
	} while (hdfs_is_again(err));
	ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
	    hdfs_error_str_kind(err), hdfs_error_str(err));

	// Send the first wave of RPCs
	nrpcs = 0;
	err = hdfs_create_nb(&nn, tf1, 0644, client, true/*overwrite*/,
	    false/*create_parent*/, 1/*replication*/, 64*1024*1024/*blocksize*/,
	    &mn_cr1, &mn_cr1/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;
	err = hdfs_create_nb(&nn, tf2, 0644, client, true/*overwrite*/,
	    false/*create_parent*/, 1/*replication*/, 64*1024*1024/*blocksize*/,
	    &mn_cr2, &mn_cr2/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;
	err = hdfs_create_nb(&nn, tf3, 0644, client, true/*overwrite*/,
	    false/*create_parent*/, 1/*replication*/, 64*1024*1024/*blocksize*/,
	    &mn_cr3, &mn_cr3/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;
	err = hdfs_create_nb(&nn, tf4, 0644, client, true/*overwrite*/,
	    false/*create_parent*/, 1/*replication*/, 64*1024*1024/*blocksize*/,
	    &mn_cr4, &mn_cr4/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;

	fprintf(stderr, "Received msgnos in order: {");

	// Loop over the first wave of RPC responses
	for (int i = 0; i < nrpcs; i++) {
		// Receive a single response
		do {
			err = hdfs_namenode_recv(&nn, &obj, &mn_recv, &ud_recv/*userdata*/);
			if (hdfs_is_again(err)) {
				ret = hdfs_namenode_get_eventfd(&nn, &pfd.fd, &pfd.events);
				ck_assert_msg(!hdfs_is_error(ret), "error (%s): %s",
				    hdfs_error_str_kind(ret), hdfs_error_str(ret));
				ck_assert_int_ne(pfd.events, 0);
				rc = poll(&pfd, 1, -1);
				ck_assert_int_eq(rc, 1);
				ck_assert_int_ne(pfd.revents, 0);
				// Send any additional data if necessary
				if (pfd.revents & POLLOUT) {
					ret = hdfs_namenode_invoke_continue(&nn);
					ck_assert_msg(!hdfs_is_error(ret) || hdfs_is_again(ret),
					    "error (%s): %s", hdfs_error_str_kind(ret), hdfs_error_str(ret));
				}
			}
		} while (hdfs_is_again(err));
		ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
		    hdfs_error_str_kind(err), hdfs_error_str(err));

		fprintf(stderr, " %"PRIu64, mn_recv);
		// Match the response with the request
		if (mn_recv == mn_cr1) {
			mn_cr1 = -1;
			ck_assert_msg(ud_recv == &mn_cr1, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_cr1);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			ck_assert_int_eq(obj->ob_type, H_FILE_STATUS);
		} else if (mn_recv == mn_cr2) {
			mn_cr2 = -1;
			ck_assert_msg(ud_recv == &mn_cr2, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_cr2);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			ck_assert_int_eq(obj->ob_type, H_FILE_STATUS);
		} else if (mn_recv == mn_cr3) {
			mn_cr3 = -1;
			ck_assert_msg(ud_recv == &mn_cr3, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_cr3);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			ck_assert_int_eq(obj->ob_type, H_FILE_STATUS);
		} else if (mn_recv == mn_cr4) {
			mn_cr4 = -1;
			ck_assert_msg(ud_recv == &mn_cr4, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_cr4);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			ck_assert_int_eq(obj->ob_type, H_FILE_STATUS);
		} else {
			ck_abort_msg("Unexpected msgno %" PRIu64, mn_recv);
		}

		hdfs_object_free(obj);
	}

	// Send the second wave of RPCs
	nrpcs = 0;
	err = hdfs_getFileInfo_nb(&nn, tf1, &mn_gfi1, &mn_gfi1/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;
	err = hdfs_getFileInfo_nb(&nn, tf2, &mn_gfi2, &mn_gfi2/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;
	err = hdfs_getFileInfo_nb(&nn, tf3, &mn_gfi3, &mn_gfi3/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;
	err = hdfs_getFileInfo_nb(&nn, tf4, &mn_gfi4, &mn_gfi4/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;

	// Loop over the second wave of RPC responses
	for (int i = 0; i < nrpcs; i++) {
		// Receive a single response
		do {
			err = hdfs_namenode_recv(&nn, &obj, &mn_recv, &ud_recv/*userdata*/);
			if (hdfs_is_again(err)) {
				ret = hdfs_namenode_get_eventfd(&nn, &pfd.fd, &pfd.events);
				ck_assert_msg(!hdfs_is_error(ret), "error (%s): %s",
				    hdfs_error_str_kind(ret), hdfs_error_str(ret));
				ck_assert_int_ne(pfd.events, 0);
				rc = poll(&pfd, 1, -1);
				ck_assert_int_eq(rc, 1);
				ck_assert_int_ne(pfd.revents, 0);
				// Send any additional data if necessary
				if (pfd.revents & POLLOUT) {
					ret = hdfs_namenode_invoke_continue(&nn);
					ck_assert_msg(!hdfs_is_error(ret) || hdfs_is_again(ret),
					    "error (%s): %s", hdfs_error_str_kind(ret), hdfs_error_str(ret));
				}
			}
		} while (hdfs_is_again(err));
		ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
		    hdfs_error_str_kind(err), hdfs_error_str(err));

		fprintf(stderr, " %"PRIu64, mn_recv);
		// Match the response with the request
		if (mn_recv == mn_gfi1) {
			mn_gfi1 = -1;
			ck_assert_msg(ud_recv == &mn_gfi1, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_gfi1);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			if (H_VER == HDFS_NN_v1) {
				ck_assert_int_eq(obj->ob_type, H_NULL);
				ck_assert_int_eq(obj->ob_val._null._type, H_FILE_STATUS);
			} else
				ck_assert_int_eq(obj->ob_type, H_FILE_STATUS);
		} else if (mn_recv == mn_gfi2) {
			mn_gfi2 = -1;
			ck_assert_msg(ud_recv == &mn_gfi2, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_gfi2);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			if (H_VER == HDFS_NN_v1) {
				ck_assert_int_eq(obj->ob_type, H_NULL);
				ck_assert_int_eq(obj->ob_val._null._type, H_FILE_STATUS);
			} else
				ck_assert_int_eq(obj->ob_type, H_FILE_STATUS);
		} else if (mn_recv == mn_gfi3) {
			mn_gfi3 = -1;
			ck_assert_msg(ud_recv == &mn_gfi3, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_gfi3);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			if (H_VER == HDFS_NN_v1) {
				ck_assert_int_eq(obj->ob_type, H_NULL);
				ck_assert_int_eq(obj->ob_val._null._type, H_FILE_STATUS);
			} else
				ck_assert_int_eq(obj->ob_type, H_FILE_STATUS);
		} else if (mn_recv == mn_gfi4) {
			mn_gfi4 = -1;
			ck_assert_msg(ud_recv == &mn_gfi4, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_gfi4);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			if (H_VER == HDFS_NN_v1) {
				ck_assert_int_eq(obj->ob_type, H_NULL);
				ck_assert_int_eq(obj->ob_val._null._type, H_FILE_STATUS);
			} else
				ck_assert_int_eq(obj->ob_type, H_FILE_STATUS);
		} else {
			ck_abort_msg("Unexpected msgno %" PRIu64, mn_recv);
		}

		hdfs_object_free(obj);
	}

	// Send the last wave of RPCs
	nrpcs = 0;
	err = hdfs_delete_nb(&nn, tf1, false/*can_recurse*/, &mn_del1, &mn_del1/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;
	err = hdfs_delete_nb(&nn, tf2, false/*can_recurse*/, &mn_del2, &mn_del2/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;
	err = hdfs_delete_nb(&nn, tf3, false/*can_recurse*/, &mn_del3, &mn_del3/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;
	err = hdfs_delete_nb(&nn, tf4, false/*can_recurse*/, &mn_del4, &mn_del4/*userdata*/);
	ck_assert_msg(!hdfs_is_error(err) || hdfs_is_again(err),
	    "error (%s): %s", hdfs_error_str_kind(err), hdfs_error_str(err));
	nrpcs++;

	// Loop over the second wave of RPC responses
	for (int i = 0; i < nrpcs; i++) {
		// Receive a single response
		do {
			err = hdfs_namenode_recv(&nn, &obj, &mn_recv, &ud_recv/*userdata*/);
			if (hdfs_is_again(err)) {
				ret = hdfs_namenode_get_eventfd(&nn, &pfd.fd, &pfd.events);
				ck_assert_msg(!hdfs_is_error(ret), "error (%s): %s",
				    hdfs_error_str_kind(ret), hdfs_error_str(ret));
				ck_assert_int_ne(pfd.events, 0);
				rc = poll(&pfd, 1, -1);
				ck_assert_int_eq(rc, 1);
				ck_assert_int_ne(pfd.revents, 0);
				// Send any additional data if necessary
				if (pfd.revents & POLLOUT) {
					ret = hdfs_namenode_invoke_continue(&nn);
					ck_assert_msg(!hdfs_is_error(ret) || hdfs_is_again(ret),
					    "error (%s): %s", hdfs_error_str_kind(ret), hdfs_error_str(ret));
				}
			}
		} while (hdfs_is_again(err));
		ck_assert_msg(!hdfs_is_error(err), "error (%s): %s",
		    hdfs_error_str_kind(err), hdfs_error_str(err));

		fprintf(stderr, " %"PRIu64, mn_recv);
		// Match the response with the request
		if (mn_recv == mn_del1) {
			mn_del1 = -1;
			ck_assert_msg(ud_recv == &mn_del1, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_del1);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			ck_assert_int_eq(obj->ob_type, H_BOOLEAN);
			ck_assert_msg(obj->ob_val._boolean._val, "delete1 returned false");
		} else if (mn_recv == mn_del2) {
			mn_del2 = -1;
			ck_assert_msg(ud_recv == &mn_del2, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_del2);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			ck_assert_int_eq(obj->ob_type, H_BOOLEAN);
			ck_assert_msg(obj->ob_val._boolean._val, "delete2 returned false");
		} else if (mn_recv == mn_del3) {
			mn_del3 = -1;
			ck_assert_msg(ud_recv == &mn_del3, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_del3);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			ck_assert_int_eq(obj->ob_type, H_BOOLEAN);
			ck_assert_msg(obj->ob_val._boolean._val, "delete3 returned false");
		} else if (mn_recv == mn_del4) {
			mn_del4 = -1;
			ck_assert_msg(ud_recv == &mn_del4, "user data pointer did not match"
			    " (got %p, expected %p)", ud_recv, &mn_del4);
			ck_assert_msg(obj->ob_type != H_PROTOCOL_EXCEPTION, "exception: %s:\n%s",
			    hdfs_exception_get_type_str(obj), hdfs_exception_get_message(obj));
			ck_assert_int_eq(obj->ob_type, H_BOOLEAN);
			ck_assert_msg(obj->ob_val._boolean._val, "delete4 returned false");
		} else {
			ck_abort_msg("Unexpected msgno %" PRIu64, mn_recv);
		}

		hdfs_object_free(obj);
	}
	fprintf(stderr, " }\n");

	hdfs_namenode_destroy(&nn);
}
END_TEST

Suite *
t_namenode_nb_suite()
{
	Suite *s = suite_create("namenode_nb");

	TCase *tc = tcase_create("multi_rpc");
	tcase_add_test(tc, test_nb_multi_rpcs);
	suite_add_tcase(s, tc);

	return s;
}
