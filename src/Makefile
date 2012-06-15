OBJS=\
	 datanode.o \
	 heapbuf.o \
	 heapbufobjs.o \
	 highlevel.o \
	 namenode.o \
	 net.o \
	 objects.o

libhadoofus.so: libhadoofus.version $(OBJS)
	gcc -Wall -Werror -fPIC -g -shared -o libhadoofus.so $(OBJS) \
		$(LDFLAGS) -Wl,--version-script=libhadoofus.version

all: libhadoofus.so

%.o: %.c
	gcc -Wall -Werror -fPIC $(CFLAGS) -g -I../include -std=gnu99 -c $<

clean:
	rm -f *.o libhadoofus.so

cov-clean:
	rm -f *.gcda *.gcno
