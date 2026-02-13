uc catalog example
==========

Simple example to show how to use the uc-catalog ffi features

# Building

This example is built with [cmake]. Instructions below assume you start in the directory containing this README.

Note that prior to building these examples you must build `delta_kernel_ffi` with all feature enabled (see [the FFI readme] for details). TLDR:
```bash
# from repo root
$ cargo build -p delta_kernel_ffi [--release] --all-features
# from ffi/ dir
$ cargo build [--release] --all-features
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
$ ./uc_catalog_example [path/to/table]
```
