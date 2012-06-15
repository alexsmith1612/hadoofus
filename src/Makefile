OBJS=\
	 datanode.o \
	 heapbuf.o \
	 heapbufobjs.o \
	 highlevel.o \
	 namenode.o \
	 net.o \
	 objects.o

FLAGS = -Wall -Werror -fPIC $(CFLAGS) -g
LIBFLAGS = $(FLAGS) -shared $(LDFLAGS)
LIB = libhadoofus.so

$(LIB): libhadoofus.version $(OBJS)
	gcc -o $@ $(LIBFLAGS) -Wl,--version-script=libhadoofus.version $(OBJS)

all: $(LIB)

%.o: %.c
	gcc -I../include -std=gnu99 $(FLAGS) -c $<

clean:
	rm -f *.o $(LIB)

cov-clean:
	rm -f *.gcda *.gcno
