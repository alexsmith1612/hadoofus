src/libhadoofus.so:
	make -C src

all: src/libhadoofus.so

examples/helloworld: src/libhadoofus.so
	make -C examples helloworld

clean:
	make -C src clean
	make -C examples clean
