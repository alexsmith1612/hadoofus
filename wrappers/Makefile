all: python libhdfs

python: py
	make -C py all

libhdfs: c
	make -C c all

clean:
	make -C py clean
	make -C c clean


install: all
	make -C c install
	make -C py install
