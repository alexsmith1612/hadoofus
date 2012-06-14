all: build

build: src/libhadoofus.so

test: test-build
	make -C tests check

test-build: tests/check_hadoofus

clean:
	make -C src clean
	make -C examples clean



tests/check_hadoofus: src/libhadoofus.so tests/*.c tests/*.h
	make -C tests check_hadoofus

src/libhadoofus.so: src/*.c
	make -C src

examples/helloworld: src/libhadoofus.so examples/helloworld.c
	make -C examples helloworld

examples/hl-hello: src/libhadoofus.so examples/hl-hello.c
	make -C examples hl-hello
