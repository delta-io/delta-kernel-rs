BINARY=cffi-test
SOURCES=cffi-test.c
INCPATHS=../target/ffi-headers
LIBPATHS=../target/debug
LDFLAGS=-ldelta_kernel_ffi #`pkg-config --libs arrow-glib`
CFLAGS=-c -Wall -DDEFINE_DEFAULT_CLIENT #`pkg-config --cflags arrow-glib`
CC=gcc

OBJECTS=$(SOURCES:.c=.o)
INCFLAGS=$(foreach TMP,$(INCPATHS),-I$(TMP))
LIBFLAGS=$(foreach TMP,$(LIBPATHS),-L$(TMP))

all: $(SOURCES) $(BINARY)

$(BINARY): $(OBJECTS)
	$(CC) $(LIBFLAGS) $(OBJECTS) $(LDFLAGS) -o $@
.c.o:
	$(CC) $(INCFLAGS) $(CFLAGS) -fPIC $< -o $@

run: $(BINARY)
	LD_LIBRARY_PATH=$(LIBPATHS) ./$(BINARY) $(table)

test: $(BINARY)
	LD_LIBRARY_PATH=$(LIBPATHS) ./$(BINARY) ../kernel/tests/data/basic_partitioned

distclean: clean
	rm -f $(BINARY)

clean:
	rm -f $(OBJECTS) $(BINARY)
