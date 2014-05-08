CFLAGS ?= -O2 -g -pipe -Wall -fexceptions -fstack-protector \
		  --param=ssp-buffer-size=4 -mtune=generic

OBJS = datanode.o \
	 heapbuf.o \
	 heapbufobjs.o \
	 highlevel.o \
	 namenode.o \
	 net.o \
	 objects.o \
	 pthread_wrappers.o \
	 util.o

STATIC_OBJS = $(patsubst %.o,%_static.o,$(OBJS))

WARNS = -Wall -Wmissing-prototypes -Wpointer-arith -Wstrict-prototypes \
		-Wformat -Werror -Wextra -Wno-missing-field-initializers \
		-Wno-unused-parameter -Wstrict-prototypes -Wpointer-arith \
		-Wreturn-type -Wcast-qual -Wwrite-strings -Wswitch -Wshadow \
		-Wcast-align -Wunused-parameter -Wchar-subscripts -Winline \
		-Wnested-externs -Wredundant-decls
FLAGS = $(WARNS) -fPIC $(CFLAGS) -g -fvisibility=hidden
LIBFLAGS = $(FLAGS) -shared $(LDFLAGS) -lsasl2 -lz
LIB = libhadoofus.so
SLIB = libhadoofus.a
LIBDIR = $(PREFIX)/lib

CC = gcc

$(LIB): $(OBJS)
	$(CC) -o $@ $(LIBFLAGS) $(OBJS)

$(SLIB): $(STATIC_OBJS)
	ar rcs $@ $^

all: $(LIB) $(SLIB)

%_static.o: %.c
	$(CC) -I../include -std=gnu99 $(FLAGS) -DNO_EXPORT_SYMS -o $@ -c $<

%.o: %.c
	$(CC) -I../include -std=gnu99 $(FLAGS) -c $<

clean:
	rm -f *.o $(LIB) $(SLIB) $(STATIC_OBJS)

cov-clean:
	rm -f *.gcda *.gcno


install: all
	mkdir -p -m 755 $(DISTDIR)$(LIBDIR)
	install -m 755 $(LIB) $(DISTDIR)$(LIBDIR)/
	install -m 644 $(SLIB) $(DISTDIR)$(LIBDIR)/
