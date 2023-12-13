BINARY=cffi-test
SOURCES=cffi-test.c
INCPATHS=kernel.h
LIBPATHS=../target/debug
LDFLAGS=-ldeltakernel #`pkg-config --libs arrow-glib`
CFLAGS=-c -Wall #`pkg-config --cflags arrow-glib`
CC=gcc

OBJECTS=$(SOURCES:.c=.o)
#INCFLAGS=$(foreach TMP,$(INCPATHS),-I$(TMP))
LIBFLAGS=$(foreach TMP,$(LIBPATHS),-L$(TMP))

all: $(SOURCES) $(BINARY)

$(BINARY): $(OBJECTS)
	$(CC) $(LIBFLAGS) $(OBJECTS) $(LDFLAGS) -o $@
.c.o:
	$(CC) $(INCFLAGS) $(CFLAGS) -fPIC $< -o $@

run:
	LD_LIBRARY_PATH=$(LIBPATHS) ./$(BINARY) $(table)

distclean: clean
	rm -f $(BINARY)

clean:
	rm -f $(OBJECTS)
