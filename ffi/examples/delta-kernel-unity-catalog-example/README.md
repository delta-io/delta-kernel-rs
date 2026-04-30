delta-kernel-unity-catalog example
===================================

Simple example showing how to use the delta-kernel-unity-catalog FFI surface -- namely
`get_uc_commit_client`, `get_uc_committer`, and `transaction_with_committer` -- to run a
commit against a catalog-managed table using a custom commit callback.

# Building

This example is built with [cmake]. Instructions below assume you start in the directory
containing this README.

Before building this example, you must build `delta_kernel_ffi` with **all features** enabled
so that the Unity Catalog bindings are present in `delta_kernel_ffi.h`. See
[the FFI readme](../../README.md) for details. TLDR:

```bash
# from repo root
$ cargo build -p delta_kernel_ffi [--release] --all-features
# or, from the ffi/ dir
$ cargo build [--release] --all-features
```

## Linux / MacOS

```
$ mkdir build
$ cd build
$ cmake ..
$ make
$ ./delta_kernel_unity_catalog_example [path/to/catalog-managed-table]
```

The included ctest (`test_delta_kernel_unity_catalog_ffi`) runs the binary against the
catalog-managed fixture at
`delta-kernel-unity-catalog/tests/data/catalog_managed_0`, so you generally don't need to
supply your own table to see it work:

```
$ ctest --output-on-failure
```

[cmake]: https://cmake.org/
