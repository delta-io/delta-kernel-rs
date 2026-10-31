read table
==========

Simple example to show how to read and dump the data of a table using kernel's cffi, and arrow-glib.

# Building

This example is built with [cmake]. Instructions below assume you start in the directory containing this README.

Note that prior to building these examples you must build `delta_kernel_ffi` (see [the FFI readme] for details). TLDR:
```bash
# from repo root
$ cargo build -p delta_kernel_ffi [--release] --features tracing
# from ffi/ dir
$ cargo build [--release] --features tracing
```

Available CMake options include:
```bash
# turn on VERBOSE mode (default is off) - print more diagnostics
$ cmake -DVERBOSE=yes ..
# turn off PRINT_DATA (default is on) - see below
$ cmake -DPRINT_DATA=no ..
```

## Linux / MacOS

Most likely something like this should work:
```
$ mkdir build
$ cd build
$ cmake ..
$ make
$ ./read_table [path/to/table]
```

## Windows

For windows, assuming you already have a working cmake + c toolchain:
```
PS mkdir build
PS cd build
PS cmake -G "Visual Studio 17 2022" ..
PS cmake --build .
PS .\Debug\read_table.exe [path\to\table]
```

If running on windows you should also run `chcp.exe 65001` to set the codepage to utf-8, or things
won't print out correctly.

## Arrow GLib
This example uses the `arrow-glib (c)` component from arrow to print out data. This requires
_installing_ that component which can be non-trivial. Please see
[here](https://arrow.apache.org/install/) to find installation instructions for your system.

For macOS and homebrew this should be as easy as:
```
brew install apache-arrow-glib
```

If you don't want to have to install this, you can run `ccmake ..` (`cmake-gui.exe ..` on windows)
from the `build` directory, and turn `OFF`/uncheckmark `PRINT_DATA`. Then "configure" and
"generate" and follow the above instructions again.

## UDT annotation tests

The UDT schema round-trip test requires both the Rust `udt-in-dev` feature and CMake's
`UDT_IN_DEV=ON` option. The CMake option registers the test; it does not enable the Rust feature.
Run these commands from the repository root:

```sh
cargo build -p delta_kernel_ffi --features tracing,udt-in-dev
mkdir -p ffi/examples/read-table/build
cd ffi/examples/read-table/build
cmake .. -DUDT_IN_DEV=ON -DPRINT_DATA=OFF
cmake --build .
ctest -R '^udt_annotation_' --output-on-failure
```

`PRINT_DATA=OFF` avoids the Arrow GLib dependency for these schema tests. The C example owns
annotation keys and values as C strings, with a null value pointer representing JSON `null`.
Embedded NUL bytes in annotation strings are rejected. The tests check that rejection and the
schema round trip, including annotation lifetimes after the source Rust schemas are released.

## Snapshot metadata tests

`read_table -m path/to/table` prints all the information from the table's metadata, then exits
without scanning.
`-m` can't be combined with `-a`, `-c`, or `-s`. Because no data files are read, the test doesn't
need Arrow GLib. Run these commands from the repository root:

```sh
cargo build -p delta_kernel_ffi --features tracing
mkdir -p ffi/examples/read-table/build
cd ffi/examples/read-table/build
cmake .. -DPRINT_DATA=OFF
cmake --build . --target read_table
ctest -R '^snapshot_visit_metadata$' --output-on-failure
```

[cmake]: https://cmake.org/
