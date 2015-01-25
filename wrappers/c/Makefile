NAME = libhdfs_hadoofus
SLIB = $(NAME).a
LIB = $(NAME).so
SRCS = \
	   hdfs.c
HDR = hdfs.h
OBJS = $(SRCS:%.c=%.o)


LIBDIR = $(PREFIX)/lib
INCLUDEDIR = $(PREFIX)/include

FLAGS = $(CFLAGS) -I../../include -I/usr/local/include
LINKFLAGS = $(LDFLAGS) -L../../src -lhadoofus `pkg-config --libs 'libprotobuf-c >= 1.0.0'` -lz -lrt

LIBFLAGS = $(FLAGS) -shared $(LDFLAGS)

all: $(LIB) $(SLIB)

$(LIB): $(OBJS)
	$(CC) -o $@ $(LIBFLAGS) $(LINKFLAGS) $(OBJS)

$(SLIB): $(OBJS)
	ar rcs $@ $(OBJS)

%.o: %.c $(HDR)
	$(CC) $(FLAGS) -fPIC -std=gnu99 -c $<

clean:
	rm -f $(LIB) $(OBJS)

test: hdfs_test
	LD_LIBRARY_PATH="$$LD_LIBRARY_PATH:../../src" ./hdfs_test

hdfs_test: hdfs_test.c $(SLIB)
	$(CC) $(FLAGS) -I. -o $@ -std=gnu99 $(LINKFLAGS) $< $(SLIB)


install: all
	mkdir -p -m 755 $(DISTDIR)$(LIBDIR)
	mkdir -p -m 755 $(DISTDIR)$(INCLUDEDIR)/hadoofus
	install -m 755 $(LIB) $(DISTDIR)$(LIBDIR)/
	install -m 644 $(SLIB) $(DISTDIR)$(LIBDIR)/
	install -m 644 $(HDR) $(DISTDIR)$(INCLUDEDIR)/hadoofus/
