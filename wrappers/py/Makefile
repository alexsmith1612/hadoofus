NAME = cpydoofus
LIB = $(NAME).so
SRCS = \
	   chighlevel.pxd \
	   clowlevel.pxd \
	   cobjects.pxd \
	   cpydoofus.pyx

FLAGS = $(CFLAGS) -I../../include
LINKFLAGS = $(LDFLAGS) -L../../src

all: $(LIB)

$(LIB): $(SRCS) setup.py
	CFLAGS="$(FLAGS)" LDFLAGS="$(LINKFLAGS)" \
		   python setup.py build_ext -i

clean:
	rm -f $(LIB) $(NAME).c
	rm -rf build
