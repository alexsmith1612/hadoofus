HEADERS = \
		  ../include/lowlevel.h \
		  ../include/objects.h \
		  ../include/highlevel.h

TEST_SRCS = \
			t_main.c \
			t_unit.c \
			t_hl_rpc_basics.c \
			t_namenode_nb.c \
			t_datanode_basics.c \
			t_datanode_nb.c

PRIV_OBJS = \
			../src/crc32c.o \
			../src/crc32c_armv8.o \
			../src/crc32c_sse42.o \
			../src/crc32c_sw.o \
			../src/heapbuf.o \
			../src/util.o

LIB = ../src/libhadoofus.so
TEST_OBJS = $(TEST_SRCS:%.c=%.o)
TEST_PRGM = check_hadoofus

# Allow users to specify their own libcheck and libprotobuf
# cflags/ldflags to break hard build dependency on pkg-config
PROTOBUF_LDFLAGS ?= $(shell pkg-config --libs 'libprotobuf-c >= 1.0.0')
CHECK_CFLAGS ?= $(shell pkg-config --cflags check)
CHECK_LDFLAGS ?= $(shell pkg-config --libs --static check)

LINK_FLAGS = $(LDFLAGS) -L/usr/local/lib -L../src -lhadoofus \
	     $(CHECK_LDFLAGS) $(PROTOBUF_LDFLAGS) \
	      -lz -lsasl2 -lpthread -lm
ifneq ($(shell uname -s), Darwin)
	LINK_FLAGS += -lrt
endif
ifeq ($(shell uname -s),FreeBSD)
	LINK_FLAGS += -lexecinfo
endif
FLAGS = -Wall -Wextra -Werror -Wno-error=unused-parameter \
	-Wno-error=missing-field-initializers \
	-fPIC -g $(CFLAGS) $(CHECK_CFLAGS)

ifeq ($(shell uname -s), Darwin)
	LD_PATH = DYLD_LIBRARY_PATH="$$DYLD_LIBRARY_PATH:../src"
else
	LD_PATH = LD_LIBRARY_PATH="$$LD_LIBRARY_PATH:../src"
endif

ifeq ($(HDFS_TEST_DEBUGGER),)
	DEBUG =
else
	DEBUG = CK_FORK=no $(HDFS_TEST_DEBUGGER)
endif

all: $(TEST_PRGM)

$(TEST_PRGM): $(PRIV_OBJS) $(TEST_OBJS) $(LIB) $(HEADERS) t_main.h
	$(CC) -o $@ $(FLAGS) $(TEST_OBJS) $(PRIV_OBJS) $(LINK_FLAGS)

check: $(TEST_PRGM)
	$(LD_PATH) $(DEBUG) ./$(TEST_PRGM)

%.o: %.c
	$(CC) $(FLAGS) -I../include -std=gnu99 -c $<

%.o: %.S
	$(CC) $(FLAGS) -I../include -std=gnu99 -c $<

clean:
	rm -f $(TEST_PRGM) *.o
