LIBS = -L../src -lhadoofus -lpthread -lz
FLAGS = -Wall -Werror $(CFLAGS) -g -I../include
BIN = \
	  helloworld \
	  hl-hello \
	  kerb \

LIB = ../src/libhadoofus.so

all: $(BIN)

helloworld: helloworld.c $(LIB)
	gcc $(FLAGS) $< -o $@ $(LIBS)

hl-hello: hl-hello.c $(LIB)
	gcc $(FLAGS) $< -o $@ $(LIBS)

kerb: kerb.c $(LIB)
	gcc $(FLAGS) $< -o $@ $(LIBS) -lsasl2

clean:
	rm -f $(BIN)


install:
	:
