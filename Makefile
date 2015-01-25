PREFIX=/usr/local
DISTDIR=
LIB = src/libhadoofus.so

all: build all-wrappers all-examples

all-wrappers: wrappers $(LIB)
	gmake -C wrappers all

all-examples: examples $(LIB)
	gmake -C examples all

all-test: tests $(LIB)
	gmake -C tests all

build: src
	gmake -C src all

test: all-test
	gmake -C tests check

clean: cov-clean
	gmake -C src clean
	gmake -C examples clean
	gmake -C tests clean
	gmake -C wrappers clean

cov-clean:
	rm -rf hadoofus-coverage
	gmake -C src cov-clean

cov-html: clean cov-clean
	CFLAGS="$(CFLAGS) -fprofile-arcs -ftest-coverage" \
		  LDFLAGS="-lgcov" \
		  gmake -C . build
	LDFLAGS="-lgcov" gmake -C . all-test
	lcov --directory src --zerocounters
	CK_FORK=no gmake -C tests check
	lcov --directory src --capture --output-file hadoofus.info
	genhtml --output-directory hadoofus-coverage hadoofus.info
	gmake -C src clean cov-clean
	gmake -C tests clean
	rm -f hadoofus.info

$(LIB): build

install:
	[ -d "$(DISTDIR)$(PREFIX)" ] || exit 1
	gmake -C src install
	gmake -C include install
	gmake -C examples install
	gmake -C wrappers install
