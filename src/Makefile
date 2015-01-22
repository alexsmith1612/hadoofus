# Python reads only the CFLAGS from PY_CFLAGS
PY_CFLAGS=-std=gnu99
PY_LDFLAGS=
PY_CPPFLAGS=

CFLAGS=-O2 -g -pipe -Wall -fexceptions -fstack-protector --param=ssp-buffer-size=4 \
		-mtune=generic $(PY_CFLAGS)

OBJS = datanode.o \
	 heapbuf.o \
	 heapbufobjs.o \
	 highlevel.o \
	 namenode.o \
	 net.o \
	 objects.o \
	 pthread_wrappers.o \
	 rpc2.o \
	 util.o

STATIC_OBJS = $(patsubst %.o,%_static.o,$(OBJS))

PB_OBJS = \
		ClientNamenodeProtocol.pb-c.o \
		datatransfer.pb-c.o \
		hadoop_rpc.pb-c.o \
		hdfs.pb-c.o \
		IpcConnectionContext.pb-c.o \
		ProtobufRpcEngine.pb-c.o \
		RpcPayloadHeader.pb-c.o \
		Rpc2_2Header.pb-c.o \
		Security.pb-c.o

OBJS += $(PB_OBJS)

PB_C = $(patsubst %.o,%.c,$(PB_OBJS))
PB_H = $(patsubst %.o,%.h,$(PB_OBJS))

WARNS = -Wall -Wmissing-prototypes -Wpointer-arith -Wstrict-prototypes \
		-Wformat -Werror -Wextra -Wno-missing-field-initializers \
		-Wno-unused-parameter -Wstrict-prototypes -Wpointer-arith \
		-Wreturn-type -Wcast-qual -Wwrite-strings -Wswitch -Wshadow \
		-Wcast-align -Wunused-parameter -Wchar-subscripts -Winline \
		-Wnested-externs -Wredundant-decls
FLAGS = $(WARNS) $(CFLAGS) -fPIC -g -fvisibility=hidden \
		$$(pkg-config --cflags 'libprotobuf-c >= 1.0.0')
LIBFLAGS = $(FLAGS) -shared $(LDFLAGS) -lsasl2 -lz -lrt
LIB = libhadoofus.so
SLIB = libhadoofus.a
LIBDIR = $(PREFIX)/lib

CC = gcc

$(LIB): $(OBJS)
	$(CC) -o $@ $(LIBFLAGS) $(OBJS)

$(SLIB): $(STATIC_OBJS)
	ar rcs $@ $^

all: $(LIB) $(SLIB)

%.pb-c.c %.pb-c.h: proto/%.proto
	( cd proto && protoc-c --c_out=.. $(subst proto/,,$<) )

$(OBJS): $(PB_H)

%_static.o: %.c
	$(CC) -I../include -std=gnu99 $(FLAGS) -DNO_EXPORT_SYMS -o $@ -c $<

%.o: %.c
	$(CC) -I../include -std=gnu99 $(FLAGS) -c $<

clean:
	rm -f *.o $(LIB) $(SLIB) $(STATIC_OBJS) $(PB_C) $(PB_H)

cov-clean:
	rm -f *.gcda *.gcno


install: all
	mkdir -p -m 755 $(DISTDIR)$(LIBDIR)
	install -m 755 $(LIB) $(DISTDIR)$(LIBDIR)/
	install -m 644 $(SLIB) $(DISTDIR)$(LIBDIR)/
