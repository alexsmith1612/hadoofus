CFLAGS?=\
		-O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector --param=ssp-buffer-size=4 \
		-mtune=generic

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
		-Wformat -Werror -Wno-missing-field-initializers -Wextra \
		-Wno-unused-parameter -Wstrict-prototypes -Wpointer-arith \
		-Wreturn-type -Wcast-qual -Wwrite-strings -Wswitch -Wshadow \
		-Wcast-align -Wunused-parameter -Wchar-subscripts -Winline \
		-Wnested-externs -Wredundant-decls
FLAGS = $(WARNS) -fPIC $(CFLAGS) -g
LIBFLAGS = $(FLAGS) -shared $(LDFLAGS) -lsasl2
LIB = libhadoofus.so
LIBDIR = $(PREFIX)/lib

$(LIB): libhadoofus.version $(OBJS)
	gcc -o $@ $(LIBFLAGS) -Wl,--version-script=libhadoofus.version $(OBJS)

all: $(LIB)

%.o: %.c
	gcc -I../include -std=gnu99 $(FLAGS) -c $<

clean:
	rm -f *.o $(LIB)

cov-clean:
	rm -f *.gcda *.gcno


install: all
	mkdir -p -m 755 $(DISTDIR)$(LIBDIR)
	install -m 755 $(LIB) $(DISTDIR)$(LIBDIR)/
