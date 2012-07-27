LIBS = -L../src -lhadoofus -lpthread -lz
FLAGS = -Wall -Werror $(CFLAGS) -g -I../include
BIN = \
	  helloworld \
	  hl-hello

all: $(BIN)

helloworld: helloworld.c
	gcc $(FLAGS) $^ -o $@ $(LIBS)

hl-hello: hl-hello.c
	gcc $(FLAGS) $^ -o $@ $(LIBS)

clean:
	rm -f $(BIN)


install:
	:
