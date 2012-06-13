src/libhadoofus.so:
	make -C src

all: src/libhadoofus.so

examples/helloworld: src/libhadoofus.so examples/helloworld.c
	make -C examples helloworld

examples/hl-hello: src/libhadoofus.so examples/hl-hello.c
	make -C examples hl-hello

test:
	make -C tests check

clean:
	make -C src clean
	make -C examples clean
