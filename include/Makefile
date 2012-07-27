INCLUDEDIR = $(PREFIX)/include

HDRS = \
	   highlevel.h \
	   lowlevel.h \
	   objects.h

install:
	mkdir -p -m 755 $(DISTDIR)$(INCLUDEDIR)/hadoofus
	for f in $(HDRS) ; do \
		install -m 644 "$$f" $(DISTDIR)$(INCLUDEDIR)/hadoofus/ ; \
	done
