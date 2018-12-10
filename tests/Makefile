HEADERS = \
		  ../include/lowlevel.h \
		  ../include/objects.h \
		  ../include/highlevel.h

TEST_SRCS = \
			t_main.c \
			t_datanode_basics.c \
			t_hl_rpc_basics.c \
			t_unit.c \

PRIV_OBJS = \
			../src/heapbuf.o \
			../src/util.o \

LIB = ../src/libhadoofus.so
TEST_OBJS = $(TEST_SRCS:%.c=%.o)
TEST_PRGM = check_hadoofus
LINK_FLAGS = $(LDFLAGS) -L/usr/local/lib -lcheck -L../src -lhadoofus \
	     `pkg-config --libs 'libprotobuf-c >= 1.0.0'` -lz -lrt -lsasl2 \
	     -lpthread -lm
ifeq ($(shell uname -s),FreeBSD)
	LINK_FLAGS += -lexecinfo
endif
FLAGS = -Wall -Werror -fPIC -g $(CFLAGS) -I/usr/local/include

all: $(TEST_PRGM)

$(TEST_PRGM): $(PRIV_OBJS) $(TEST_OBJS) $(LIB) $(HEADERS) t_main.h
	$(CC) -o $@ $(FLAGS) $(TEST_OBJS) $(PRIV_OBJS) $(LINK_FLAGS)

check: $(TEST_PRGM)
	LD_LIBRARY_PATH="$$LD_LIBRARY_PATH:../src" ./$(TEST_PRGM)

%.o: %.c
	$(CC) $(FLAGS) -I../include -std=gnu99 -c $<

clean:
	rm -f $(TEST_PRGM) *.o
