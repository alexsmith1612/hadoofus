HEADERS = \
		  ../include/lowlevel.h \
		  ../include/objects.h \
		  ../include/highlevel.h

TEST_SRCS = \
			t_main.c \
			t_datanode_basics.c \
			t_hl_rpc_basics.c

LIB = ../src/libhadoofus.so
TEST_OBJS = $(TEST_SRCS:%.c=%.o)
TEST_PRGM = check_hadoofus
LINK_FLAGS = $(LDFLAGS) -lcheck -L../src -lhadoofus -lz -lrt -lsasl2
FLAGS = -Wall -Werror -fPIC -g $(CFLAGS)

all: $(TEST_PRGM)

$(TEST_PRGM): $(TEST_OBJS) $(LIB) $(HEADERS) t_main.h
	$(CC) -o $@ $(FLAGS) $(TEST_OBJS) $(LINK_FLAGS)

check: $(TEST_PRGM)
	LD_LIBRARY_PATH="$$LD_LIBRARY_PATH:../src" ./$(TEST_PRGM)

%.o: %.c
	$(CC) $(FLAGS) -I../include -std=gnu99 -c $<

clean:
	rm -f $(TEST_PRGM) *.o
