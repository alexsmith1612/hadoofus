#include <check.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../src/crc32c.h"
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

	free(buf.buf);
	close(fd);
}
END_TEST

START_TEST(test_crc32c)
{
	struct {
		char *test;
		size_t testlen;
		uint32_t exp;
	} cases[] = {
		{ "\x4b\x82\xc5\x8f\x6e\x54\xd6\xe2\x09\xe2\xed\xf6\x8c\x5a\xe2\x5e"
		  "\x34\xbd\xb8\x28\x06\x40\x84\xe3\xc5\x25\xdd\x75\xc5\x22\x5b\xf2",
		  32, 0x2d916611
		},
		{ "\xf6\x78\x10\x23\x8e\xf8\xaa\x23\xb5\xc1\x49\x00\x72\x64\x59\xbf"
		  "\xf8\x82\x32\xc0\x92\xfa\x54\x65\xfe\xe8\x03\x72\x7f\x50\x82\x48"
		  "\x5b\x74\xb9\xde\x4b\x5c\x03\x04\xa7\xe2\x89\xef\x2c\x07\x9d\x18"
		  "\x57\xfc\x5d\x4f\x23\x44\x9d\xf9\x52\x0c\x3a\x6b\xba\x95\xac\x9b"
		  "\xfe\x17\xe8\x66\xbc\xe6\xee\x19\xca\xaa\x85\x44\x70\x49\xad\xb6"
		  "\xf5\xa2\xd9\x43\x5e\x2a\xd3\xdf\x6b\xaa\xe6\x58\x9f\x80\xb1\x3d"
		  "\x5f\x29\x9a\x6e\x2f\x42\xc2\xb8\x9f\x02\x86\x8a\x30\x13\x2d\x58"
		  "\xc9\xc4\x8f\xba\x4a\xc8\x5f\x2c\xe3\x18\x34",
		  123, 0xbc6bbefe
		},
		{ "\xf2\x8b\xad\xad\xc6\x57\xd7\xe2\x95\x78\xfc\x2d\x18\xaa\x21\x6c"
		  "\x39\x9e\xc6\xe3\x6c\x9d\xf6\xb6\x0b\x14\x66\xc5\x67\x4a\x40\x6e"
		  "\x85\x1d\x91\x19\x85\x53\x8c\xe3\xa7\x3a\xd8\x8d\xff\x4c\x84\xd5"
		  "\x67\xff\x2c\x87\x37\xff\xe6\x50\x45\xb5\x87\x9f\xb2\x8b\xc2\xb3"
		  "\x2b\x2d\xf5\x2e\xfc\xac\x31\xa4\x88\xe2\x16\xd9\xeb\xf0\xa3\xb0"
		  "\x95\x61\x09\xf4\x95\xcb\xae\x72\x2c\x99\x60\x2a\x38\xa1\x5c\x8d"
		  "\x87\x0f\x15\x62\x27\xa1\xc7\xfd\xc1\xb1\xca\x60\x1c\xef\xd0\x38"
		  "\xf5\x35\xdc\x2c\x93\x4a\xfe\x3c\xa8\xd9\xa1\xb3\x51\xdd\x80\xf3"
		  "\x5a\x56\xd2\x0e\xbb\x4a\xfb\xc3\x90\x72\x6e\x50\x62\x60\x5b\xd6"
		  "\x79\xcf\x40\xe5\xf3\xff\x27\x21\x1e\xcb\x9e\x45\x22\xd0\x65\x4e"
		  "\x5b\x86\x18\xb5\x71\x38\xb4\x81\x6d\x8f\xbc\x62\x61\xc6\xc9\x12"
		  "\x27\xe0\xf3\x3e\xdb\x44\x47\xfc\xfc\x11\x60\xf7\x14\x93\xc5\x26"
		  "\x66\x2d\xad\x89\x2d\xe3\xa6\xf8\xeb\xf1\xb6\x8e\x54\x65\x41\x31"
		  "\x2c\x8d\xb7\xf5\x03\xb6\x2b\xf4\x51\x9a\x6d\xa7\xb7\xd0\x25\xa8"
		  "\xdc\xbe\x8b\x6f\xd1\x0d\x60\xa8\x53\x3f\xcd\xfe\xa0\xe7\x67\xe9"
		  "\x95\xe1\xa7\x8e\x93\x93\x52\x36\xe9\x9a\x46\x05\xbe\xcf\x66\x5f"
		  "\x3e\xeb\xe6\x9b\xdd\x80\x44\x1f\x03\xf0\x80\x97\x16\xdb\xb9\x4d"
		  "\x11\xed\xa2\x7d\xec\x5d\xd8\x46\xb0\xbb\x1f\x7c\x55\x39\x8a\xd1"
		  "\xa8\x28\x49\x5a\x94\x68\xa9\xa7\x44\xfd\xb3\xa3\xba\x76\x44\xe0"
		  "\xc3\xc2\xf4\x1d\xad\xf7\x35\x1d\x2f\x94\x07\xdd\x3d\xf5\x5a\xac"
		  "\x62\x4a\x9b\xfe\x86\x47\x1e\xf1\x73\x94\x04\x74\xe6\x29\x1a\x75"
		  "\x4a\x17\x62\xfb\xcc\x23\xae\x45\x1b\x4c\x61\x43\xd8\xef\x6b\x18"
		  "\xf8\xb3\x13\x2c\x22\x77\xf9\x7c\xf9\x57\x01\x9c\x32\xae\x5a\x2b"
		  "\x02\xc5\x5e\x70\x4c\x01\x27\x38\xbc\x90\x51\xa0\xa6\x95\x1a\x7d"
		  "\x6c\xc3\x1e\x10\xc5\x25\xfc\x9c\xe1\x98\x7a\xdd\xc8\x2c\xa3\x40"
		  "\x0e\x00\xe0\xb3\x05\xb6\xd4\x99\xe6\x1e\xc4\x88\x34\xda\xc2\x64"
		  "\x01\x8c\xc7\x85\x0d\x7d\x9b\x50\x93\x7b\x50\x81\x0d\xc0\xbd\xfe"
		  "\xea\x86\xda\xe4\x9a\x13\x1d\x51\x19\x2c\xbd\xaa\x96\x15\xd1\x98"
		  "\x87\x30\xe7\x9a\x62\xfc\xf6\xc2\xe7\xea\xbd\xdc\xd6\xb9\xae\xa0"
		  "\xb8\x08\xa4\xa8\x63\xf8\x3f\xbd\x66\xb1\x74\xea\xda\x15\x7c\xbc"
		  "\xab\x23\x94\x0a\x94\xbc\x90\xde\x7a\xba\xb2\x52\xbb\x0b\x11\xc0"
		  "\xd1\xd7\x38\x23\x68\xf8\x48\x2b\x5e\x5a\x15\xdc\x5d\xa0\xee\xcb",
		  512, 0x81b3b4b2
		},
	};

	for (size_t i = 0; i < nelem(cases); i++) {
		uint32_t crc, crc_sw;

		// Test the ifunc version (which will resolve to a
		// hardware-accelerated implementation if available).
		crc = crc32c(0, cases[i].test, cases[i].testlen);
		ck_assert_int_eq(cases[i].exp, crc);

		// Explicitly test the software implementation since that is
		// always available
		crc_sw = crc32c(0, cases[i].test, cases[i].testlen);
		ck_assert_int_eq(cases[i].exp, crc_sw);
	}
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

	tc = tcase_create("crc32c");
	tcase_add_test(tc, test_crc32c);
	suite_add_tcase(s, tc);

	return s;
}
