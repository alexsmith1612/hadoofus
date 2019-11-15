LIBS = -L../src -lhadoofus `pkg-config --libs 'libprotobuf-c >= 1.0.0'` -lpthread -lz -lsasl2
ifneq ($(shell uname -s), Darwin)
	LIBS += -lrt
endif
ifeq ($(shell uname -s),FreeBSD)
	LIBS += -lexecinfo
endif
FLAGS = -Wall -Wextra -Werror $(CFLAGS) -g -I../include -I/usr/local/include
BIN = \
	  helloworld \
	  hl-hello \
	  kerb \

LIB = ../src/libhadoofus.so

all: $(BIN)

helloworld: helloworld.c $(LIB)
	$(CC) $(FLAGS) $< -o $@ $(LIBS)

hl-hello: hl-hello.c $(LIB)
	$(CC) $(FLAGS) $< -o $@ $(LIBS)

kerb: kerb.c $(LIB)
	$(CC) $(FLAGS) $< -o $@ $(LIBS) -lsasl2

clean:
	rm -f $(BIN)


install:
	:
