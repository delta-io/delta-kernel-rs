read table
==========

Simple example to show how to read and dump the data of a table using kernel's cffi, and arrow-glib.

# Building

This example is built with [cmake]. Instructions below assume you start in the directory containing this README.

Note that prior to building these examples you must build `delta_kernel_ffi` (see [the FFI readme] for details). TLDR:
```bash
# from repo root
$ cargo build -p delta_kernel_ffi [--release] [--features default-engine, tracing]
# from ffi/ dir
$ cargo build [--release] [--features default-engine, tracing]
```

There are two configurations that can currently be configured in cmake:
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

[cmake]: https://cmake.org/
