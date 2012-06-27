NAME = hadoofus
LIB = $(NAME).so
SRCS = \
	   chighlevel.pxd \
	   clowlevel.pxd \
	   cobjects.pxd \
	   $(NAME).pyx

# Note: -fno-strict-aliasing is NECESSARY for ensuring correct behavior of
# Python API code. (The Python object model is built on a system of type-
# punning; it is unsafe for the compiler to make strict aliasing assumptions.)
FLAGS = $(CFLAGS) -I../../include -fno-strict-aliasing
LINKFLAGS = $(LDFLAGS) -L../../src

all: $(LIB)

$(LIB): $(SRCS) setup.py
	CFLAGS="$(FLAGS)" LDFLAGS="$(LINKFLAGS)" \
		   python setup.py build_ext -i

clean:
	rm -f $(LIB) $(NAME).c
	rm -rf build
