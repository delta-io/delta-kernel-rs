# delta-kernel-rs ffi

This crate provides a c foreign function internface (ffi) for delta-kernel-rs.

## Building

### Building Kernel and Headers
You can build static and shared-libraries, as well as the include headers by simply running:

```sh
cargo build [--release] [--features default-engine]
```

This will place libraries in the root `target` dir (`../target/[debug,release]` from the directory containing this README), and headers in `../target/ffi-headers`. In that directory there will be a `delta_kernel_ffi.h` file, which is the C header, and a `delta_kernel_ffi.hpp` which is the C++ header.

### Building example
To build and run the excample C program which exercises FFI (after building the ffi as above):

```sh
cd examples/read-table
mkdir build
cd build
cmake ..
make
./read_table ../../../../kernel/tests/data/table-with-dv-small
```

By default this has a dependency on
[`arrow-glib`](https://github.com/apache/arrow/blob/main/c_glib/README.md). You can read install
instructions for your platform [here](https://arrow.apache.org/install/).

If you don't want to install `arrow-glib` you can run the above `cmake` command as:

```sh
cmake -DPRINT_DATA=no ..
```

and the example will only print out the schema of the table, not the data.

### C/C++ Extension (VSCode)

By default the VSCode C/C++ Extension does not use any defines flags. You can open `settings.json` and set the following line:
```
    "C_Cpp.default.defines": [
        "DEFINE_DEFAULT_ENGINE",
        "DEFINE_SYNC_ENGINE"
    ]
```
