src/libhadoofus.so: src/*.c
	make -C src

all: src/libhadoofus.so

examples/helloworld: src/libhadoofus.so examples/helloworld.c
	make -C examples helloworld

examples/hl-hello: src/libhadoofus.so examples/hl-hello.c
	make -C examples hl-hello

test: test-build
	make -C tests check

test-build: tests/check_hadoofus
	make -C tests check_hadoofus

clean:
	make -C src clean
	make -C examples clean
