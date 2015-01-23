#include <check.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../src/heapbuf.h"

#include "t_main.h"

static void
print_hex(size_t n, const uint8_t *bytes)
{
	size_t i;

	for (i = 0; i < n; i++)
		printf("%02x", (unsigned)bytes[i]);
}

#define _ck_assert_mem_eq(B1, S1, B2, S2)	do {			\
	if ((size_t)(S1) != (size_t)(S2)) {				\
		printf("_ck_assert_mem_eq: Lengths don't match (%zu != %zu) (\"",	\
		    (size_t)(S1), (size_t)(S2));			\
		goto printhex;						\
	}								\
	if (memcmp((B1), (B2), (size_t)(S1)) == 0)			\
		break;							\
	printf("_ck_assert_mem_eq: Contents don't match (\"");		\
printhex:								\
	print_hex((size_t)(S1), (const uint8_t *)(B1));			\
	printf("\" != \"");						\
	print_hex((size_t)(S2), (const uint8_t *)(B2));			\
	printf("\")\n");						\
	ck_abort_msg("_ck_assert_mem_eq: Contents didn't match; see "	\
	    "CK_FORK=no for debugging output");				\
} while (0)

START_TEST(test_vlint_encode)
{
	struct {
		int64_t testcase;
		const char *exp;
		size_t explen;
	} *it, cases[] = {
		{ 0, "\0", 1, },
		{ 0x7f, "\x7f", 1, },
		{ 0x80, "\x80\x01", 2, },
		{ 0x3fff, "\xff\x7f", 2, },
		{ 0x4000, "\x80\x80\x01", 3, },
		{ 0xffffffff, "\xff\xff\xff\xff\x0f", 5, },
		{ 0xffffffffffffffffL, "\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01", 10, },
		{ 0, NULL, 0, },
	};
	struct hdfs_heap_buf buf = { 0 };

	for (it = cases; it->exp; it++) {
		_bappend_vlint(&buf, it->testcase);

		ck_assert_msg(buf.used == (int)it->explen,
		    "Expected %zu-byte encoding, got %d bytes", it->explen,
		    buf.used);
		_ck_assert_mem_eq(it->exp, it->explen, buf.buf, buf.used);

		buf.used = 0;
	}
	free(buf.buf);
}
END_TEST

START_TEST(test_vlint_decode)
{
	struct {
		char *test;
		size_t testlen;
		int64_t exp;
	} *it, cases[] = {
		{ "\x01", 1, 1, },
		{ "\xac\x02", 2, 300, },
		{ "\xff\xff\xff\xff\x0f", 5, 0xffffffff, },
		{ "\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01", 10, 0xffffffffffffffffL, },
		{ NULL, 0, 0, },
	};
	struct hdfs_heap_buf rbuf;
	int64_t val;

	for (it = cases; it->exp; it++) {
		rbuf.used = 0;
		rbuf.buf = it->test;
		rbuf.size = (int)it->testlen;

		val = _bslurp_vlint(&rbuf);
		ck_assert_msg(rbuf.used >= 0, "got error parsing good vlint");
		ck_assert_msg(rbuf.used == rbuf.size, "short read?");
		ck_assert_msg(val == it->exp, "expected:%jd != actual:%jd",
		    (intmax_t)it->exp, (intmax_t)val);
	}
}
END_TEST

START_TEST(test_vlint_roundtrip)
{
	struct hdfs_heap_buf buf = { 0 };
	int64_t val, act;
	unsigned i;
	ssize_t rd;
	int fd;

	fd = open("/dev/urandom", O_RDONLY);
	if (fd < 0) {
		printf("Missing /dev/urandom??\n\n");
		return;
	}

	for (i = 0; i < 1000; i++) {
		rd = read(fd, &val, sizeof(val));
		ck_assert_msg(rd >= 0, "read of urandom: %d (%s)", errno,
		    strerror(errno));
		ck_assert_msg(rd == sizeof(val), "short read of urandom: %d < %d",
		    (int)rd, (int)sizeof(val));

		_bappend_vlint(&buf, val);

		/* Configure buf for reading: */
		buf.size = buf.used;
		buf.used = 0;

		act = _bslurp_vlint(&buf);
		ck_assert_msg(buf.used >= 0, "got error parsing vlint");
		ck_assert_msg(buf.used == buf.size, "short read?");
		ck_assert_msg(act == val, "expected:%jd != actual:%jd",
		    (intmax_t)val, (intmax_t)act);

		/* Configure buf for serializing again: */
		buf.used = 0;
	}

	close(fd);
}
END_TEST

Suite *
t_unit(void)
{
	Suite *s = suite_create("unit");

	TCase *tc = tcase_create("vlint");
	tcase_add_test(tc, test_vlint_encode);
	tcase_add_test(tc, test_vlint_decode);
	tcase_add_test(tc, test_vlint_roundtrip);

	suite_add_tcase(s, tc);

	return s;
}
