# Custom I/O engine example

This example shows how a connector supplies **all** storage, JSON, and parquet I/O
through a single [`make_custom_io_engine`](../../src/custom_io_engine/mod.rs) call.
Evaluation always uses the kernel's default Arrow handler.

## Contract

- All three vtable pointers in [`CustomIOCallbacks`](../../src/custom_io_engine/mod.rs) must be
  non-null. There is no partial override: use `get_default_engine` when the built-in object store
  is sufficient.
- JSON read batches use the column-materialization protocol (`materialize_columns`); the kernel
  converts them to Arrow before evaluation.
- Parquet read batches cross the boundary as [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
  structs (`FFI_ArrowArray` + `FFI_ArrowSchema`).
- JSON and parquet writes receive Arrow batches exported by the kernel.

## Build

```bash
# From the repo root (after building delta_kernel_ffi):
cargo build -p delta_kernel_ffi --features default-engine-rustls,arrow
cd ffi/examples/custom-io-engine
cmake -B build && cmake --build build
./build/custom_io_engine
```

The example calls `test_drive_custom_io_engine` with stub callbacks to verify the factory accepts a
fully-populated vtable bundle.
