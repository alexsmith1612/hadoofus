NAME = libhdfs_hadoofus
SLIB = $(NAME).a
LIB = $(NAME).so
SRCS = \
	   hdfs.c
HDR = hdfs.h
OBJS = $(SRCS:%.c=%.o)

LIBDIR = $(PREFIX)/lib
INCLUDEDIR = $(PREFIX)/include

FLAGS = $(CFLAGS) -I../../include
LINKFLAGS = $(LDFLAGS) -L../../src -lhadoofus -lz

LIBFLAGS = $(FLAGS) -shared $(LDFLAGS)

all: $(LIB) $(SLIB)

$(LIB): $(OBJS)
	gcc -o $@ $(LIBFLAGS) $(OBJS)

$(SLIB): $(OBJS)
	ar rcs $@ $(OBJS)

%.o: %.c $(HDR)
	gcc $(FLAGS) -fPIC -std=gnu99 -c $<

clean:
	rm -f $(LIB) $(OBJS)

test: hdfs_test
	LD_LIBRARY_PATH="$$LD_LIBRARY_PATH:../../src" ./hdfs_test

hdfs_test: hdfs_test.c $(SLIB)
	gcc $(FLAGS) -I. -o $@ -std=gnu99 $(LINKFLAGS) $< $(SLIB)


install: all
	mkdir -p -m 755 $(DISTDIR)$(LIBDIR)
	mkdir -p -m 755 $(DISTDIR)$(INCLUDEDIR)/hadoofus
	install -m 755 $(LIB) $(DISTDIR)$(LIBDIR)/
	install -m 644 $(SLIB) $(DISTDIR)$(LIBDIR)/
	install -m 644 $(HDR) $(DISTDIR)$(INCLUDEDIR)/hadoofus/
