all: python libhdfs

python: py
	gmake -C py all

libhdfs: c
	gmake -C c all

clean:
	gmake -C py clean
	gmake -C c clean


install: all
	gmake -C c install
	gmake -C py install
