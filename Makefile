PREFIX=/usr/local
DISTDIR=

all: build all-wrappers

all-wrappers: wrappers
	make -C wrappers all

build: src/libhadoofus.so all-wrappers

test: test-build
	make -C tests check

test-build: tests/check_hadoofus

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
	LDFLAGS="-lgcov" make -C . test-build
	lcov --directory src --zerocounters
	CK_FORK=no make -C tests check
	lcov --directory src --capture --output-file hadoofus.info
	genhtml --output-directory hadoofus-coverage hadoofus.info
	make -C src clean cov-clean
	make -C tests clean
	rm -f hadoofus.info


tests/check_hadoofus: src/libhadoofus.so tests/*.c tests/*.h
	make -C tests check_hadoofus

src/libhadoofus.so: src/*.c
	make -C src

examples/helloworld: src/libhadoofus.so examples/helloworld.c
	make -C examples helloworld

examples/hl-hello: src/libhadoofus.so examples/hl-hello.c
	make -C examples hl-hello


install:
	[ -d "$(DISTDIR)$(PREFIX)" ] || exit 1
	make -C src install
	make -C include install
	make -C examples install
	make -C wrappers install
