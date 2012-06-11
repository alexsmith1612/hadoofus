OBJS=\
	 heapbuf.o \
	 heapbufobjs.o \
	 highlevel.o \
	 namenode.o \
	 net.o \
	 objects.o

libhadoofus.so: libhadoofus.version $(OBJS)
	gcc -fPIC -g -shared -o libhadoofus.so $(OBJS) -Wl,--version-script=libhadoofus.version

all: libhadoofus.so

%.o: %.c
	gcc -Wall -fPIC $(CFLAGS) -g -I../include -std=gnu99 -c $<

clean:
	rm -f *.o libhadoofus.so
