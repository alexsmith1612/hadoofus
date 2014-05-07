# Python reads only the CFLAGS from PY_CFLAGS
PY_CFLAGS=-std=gnu99
PY_LDFLAGS=
PY_CPPFLAGS=

CFLAGS?=-O2 -g -pipe -Wall -fexceptions -fstack-protector --param=ssp-buffer-size=4 \
		-mtune=generic $(PY_CFLAGS)

OBJS=\
	 datanode.o \
	 heapbuf.o \
	 heapbufobjs.o \
	 highlevel.o \
	 namenode.o \
	 net.o \
	 objects.o \
	 pthread_wrappers.o \
	 util.o \

WARNS = -Wall -Wmissing-prototypes -Wpointer-arith -Wstrict-prototypes \
		-Wformat -Werror -Wextra -Wno-missing-field-initializers \
		-Wno-unused-parameter -Wstrict-prototypes -Wpointer-arith \
		-Wreturn-type -Wcast-qual -Wwrite-strings -Wswitch -Wshadow \
		-Wcast-align -Wunused-parameter -Wchar-subscripts -Winline \
		-Wnested-externs -Wredundant-decls
FLAGS = $(WARNS) $(CFLAGS) -fPIC -g -fvisibility=hidden
LIBFLAGS = $(FLAGS) -shared $(LDFLAGS) -lsasl2 -lz
LIB = libhadoofus.so
LIBDIR = $(PREFIX)/lib

CC = gcc

$(LIB): $(OBJS)
	$(CC) -o $@ $(LIBFLAGS) $(OBJS)

all: $(LIB)

%.o: %.c
	$(CC) -I../include -std=gnu99 $(FLAGS) -c $<

clean:
	rm -f *.o $(LIB)

cov-clean:
	rm -f *.gcda *.gcno


install: all
	mkdir -p -m 755 $(DISTDIR)$(LIBDIR)
	install -m 755 $(LIB) $(DISTDIR)$(LIBDIR)/
