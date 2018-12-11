all: python libhdfs

python: py
	$(MAKE) -C py all

libhdfs: c
	$(MAKE) -C c all

clean:
	$(MAKE) -C py clean
	$(MAKE) -C c clean


install: all
	$(MAKE) -C c install
	$(MAKE) -C py install
