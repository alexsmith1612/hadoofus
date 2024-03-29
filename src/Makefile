OBJS = crc32c.o \
	 crc32c_armv8.o \
	 crc32c_sse42.o \
	 crc32c_sw.o \
	 datanode.o \
	 heapbuf.o \
	 heapbufobjs.o \
	 highlevel.o \
	 namenode.o \
	 net.o \
	 objects.o \
	 rpc2.o \
	 util.o

STATIC_OBJS = $(patsubst %.o,%_static.o,$(OBJS))

PB_OBJS = \
		acl.o \
		ClientNamenodeProtocol.o \
		datatransfer.o \
		encryption.o \
		erasurecoding.o \
		hadoop_rpc2_0.o \
		HAServiceProtocol.o \
		hdfs.o \
		inotify.o \
		IpcConnectionContext.o \
		ProtobufRpcEngine.o \
		RpcHeader.o \
		RpcPayloadHeader2_0.o \
		Security.o \
		xattr.o

OBJS += $(PB_OBJS)

PB_C = $(patsubst %.o,%.pb-c.c,$(PB_OBJS))
PB_H = $(patsubst %.o,%.pb-c.h,$(PB_OBJS))

WARNS = -Wall -Wextra \
	-Wcast-align -Wcast-qual -Wchar-subscripts -Wformat=2 -Winit-self \
	-Winline -Wmissing-prototypes -Wnested-externs -Wpointer-arith \
	-Wredundant-decls -Wreturn-type -Wshadow -Wstrict-aliasing=1 \
	-Wstrict-prototypes -Wswitch -Wswitch-enum -Wundef -Wunused-parameter \
	-Wvla -Wwrite-strings \
	-Werror \
	-Wno-error=unused-parameter -Wno-error=missing-field-initializers \
	-Wno-error=inline

# Warnings too new for Travis-CI, which runs an ancient version of Ubuntu:
WARNS_NEW ?= -Wnull-dereference

# Allow users to specify their own libprotobuf cflags/ldflags to break
# hard build dependency on pkg-config
PROTOBUF_CFLAGS ?= $(shell pkg-config --cflags 'libprotobuf-c >= 1.0.0')
PROTOBUF_LDFLAGS ?= $(shell pkg-config --libs 'libprotobuf-c >= 1.0.0')

FLAGS = $(WARNS) $(WARNS_NEW) -O2 -g -pipe -fexceptions -fstack-protector \
	--param=ssp-buffer-size=4 $(CFLAGS) -fPIC -fvisibility=hidden \
	-I/usr/local/include $(PROTOBUF_CFLAGS)
LIBFLAGS = $(FLAGS) -shared $(LDFLAGS) -lz -lsasl2 $(PROTOBUF_LDFLAGS)
ifneq ($(shell uname -s), Darwin)
	LIBFLAGS += -lrt
endif


LIB = libhadoofus.so
SLIB = libhadoofus.a
LIBDIR = $(PREFIX)/lib


shared: $(LIB)
$(LIB): $(OBJS)
	$(CC) -o $@ $(LIBFLAGS) $(OBJS)

static: $(SLIB)
$(SLIB): $(STATIC_OBJS)
	ar rcs $@ $^

all: shared static

%.pb-c.c %.pb-c.h: proto/%.proto
	( cd proto && protoc-c --c_out=.. $(subst proto/,,$<) )

$(OBJS): $(PB_H) $(PB_C)
$(STATIC_OBJS): $(PB_H) $(PB_C)

# This is a little ugly, but I have not found anything I like better.  We
# intentionally use a different stem (%_static) from the prequisite (%) to
# avoid the %.o: %.c rule/recipe.
%_static.o: %.c
	$(CC) -I../include $(FLAGS) -DNO_EXPORT_SYMS -o $@ -c $<

%_static.o: %.S
	$(CC) -I../include $(FLAGS) -DNO_EXPORT_SYMS -o $@ -c $<

# Similarly, we use a different stem (%) from prequisite (%.pb-c) to disable
# strict aliasing optimizations (and warnings) from protobuf-c generated code,
# which (ab)uses pointer aliasing. It may also generate cast-align warnings
# which are false positives due to the alignment guarantees of malloc.
%_static.o: %.pb-c.c
	$(CC) -I../include $(FLAGS) -fno-strict-aliasing -Wno-cast-align -o $@ -c $<

%.o: %.c
	$(CC) -I../include $(FLAGS) -c $<

%.o: %.S
	$(CC) -I../include $(FLAGS) -c $<

%.o: %.pb-c.c
	$(CC) -I../include $(FLAGS) -fno-strict-aliasing -o $@ -c $<

clean:
	rm -f *.o $(LIB) $(SLIB) $(STATIC_OBJS) $(PB_C) $(PB_H)

cov-clean:
	rm -f *.gcda *.gcno


install: all
	mkdir -p -m 755 $(DISTDIR)$(LIBDIR)
	install -m 755 $(LIB) $(DISTDIR)$(LIBDIR)/
	install -m 644 $(SLIB) $(DISTDIR)$(LIBDIR)/
