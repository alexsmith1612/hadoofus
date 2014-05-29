NAME = hadoofus
LIB = $(NAME).so

all: $(LIB)

$(LIB): $(SRCS) setup.py
	python setup.py build

clean:
	rm -f $(LIB) $(NAME).c
	rm -rf build

install: all setup.py
	python setup.py install --root="$(DISTDIR)/" --prefix="$(PREFIX)"
