PREFIX=/usr/local
DISTDIR=
LIB = src/libhadoofus.so

all: build all-examples all-test

all-examples: examples $(LIB)
	$(MAKE) -C examples all

all-test: tests $(LIB)
	$(MAKE) -C tests all

build: src
	$(MAKE) -C src all

test: all-test
	$(MAKE) -C tests check

clean: cov-clean
	$(MAKE) -C src clean
	$(MAKE) -C examples clean
	$(MAKE) -C tests clean

cov-clean:
	rm -rf hadoofus-coverage
	$(MAKE) -C src cov-clean

cov-html: clean cov-clean
	CFLAGS="$(CFLAGS) -fprofile-arcs -ftest-coverage" \
		  LDFLAGS="-lgcov" \
		  $(MAKE) -C . build
	LDFLAGS="-lgcov" $(MAKE) -C . all-test
	lcov --directory src --zerocounters
	CK_FORK=no $(MAKE) -C tests check
	lcov --directory src --capture --output-file hadoofus.info
	genhtml --output-directory hadoofus-coverage hadoofus.info
	$(MAKE) -C src clean cov-clean
	$(MAKE) -C tests clean
	rm -f hadoofus.info

$(LIB): build

install:
	[ -d "$(DISTDIR)$(PREFIX)" ] || exit 1
	$(MAKE) -C src install
	$(MAKE) -C include install
	$(MAKE) -C examples install
