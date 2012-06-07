src/libhadoofus.so:
	make -C src

all: src/libhadoofus.so

examples/basic: src/libhadoofus.so
	make -C examples basic

clean:
	make -C src clean
	make -C examples clean
