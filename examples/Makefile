LIBS = -L../src -lhadoofus -lpthread -lz -lsasl2
FLAGS = -Wall -Werror $(CFLAGS) -g -I../include
BIN = \
	  helloworld \
	  hl-hello \
	  kerb \

LIB = ../src/libhadoofus.so
CC = gcc

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
