PREFIX=/usr/local
DISTDIR=
LIB = src/libhadoofus.so
CC = gcc

all: build all-wrappers all-examples

all-wrappers: wrappers $(LIB)
	make -C wrappers all

all-examples: examples $(LIB)
	make -C examples all

all-test: tests $(LIB)
	make -C tests all

build: src
	make -C src all

test: all-test
	make -C tests check

clean: cov-clean
	make -C src clean
	make -C examples clean
	make -C tests clean
	make -C wrappers clean

cov-clean:
	rm -rf hadoofus-coverage
	make -C src cov-clean

cov-html: clean cov-clean
	CFLAGS="$(CFLAGS) -fprofile-arcs -ftest-coverage" \
		  LDFLAGS="-lgcov" \
		  make -C . build
	LDFLAGS="-lgcov" make -C . all-test
	lcov --directory src --zerocounters
	CK_FORK=no make -C tests check
	lcov --directory src --capture --output-file hadoofus.info
	genhtml --output-directory hadoofus-coverage hadoofus.info
	make -C src clean cov-clean
	make -C tests clean
	rm -f hadoofus.info

$(LIB): build

install:
	[ -d "$(DISTDIR)$(PREFIX)" ] || exit 1
	make -C src install
	make -C include install
	make -C examples install
	make -C wrappers install
