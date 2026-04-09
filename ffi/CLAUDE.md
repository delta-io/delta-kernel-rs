# FFI Layer

The `delta_kernel_ffi` crate exposes the kernel to C/C++ via a stable FFI boundary using
cbindgen-generated headers (`.h` and `.hpp`).

## Handle System

Objects crossing the FFI boundary may be wrapped in **handles** -- opaque pointers with
ownership semantics:
- **Mutable handles** (`Box`-like) -- exclusive ownership, neither `Copy` nor `Clone`
- **Shared handles** (`Arc`-like) -- shared ownership via reference counting

A handle is needed when a value might outlive the function call that passes it across the
FFI boundary, or when the type is not representable in C/C++ (dyn trait references, slices,
options, etc.). Short-lived "plain old data" types like `ExternResult`, `KernelError`,
`KernelStringSlice`, and `EngineIterator` do not need handles.

Every handle has a corresponding `free_*` function (e.g. `free_engine`, `free_snapshot`).

## Error Handling

Fallible functions return `ExternResult` (tagged union of Ok/Err). The caller provides an
`allocate_error` callback when creating the engine; kernel calls this to allocate errors in
the caller's memory space.

## Key Files

- `src/lib.rs` -- main FFI entry points and type definitions
- `src/handle.rs` -- opaque handle system for passing Rust objects across FFI
- `src/scan.rs` -- scan FFI interface
- `src/schema_visitor.rs` -- visitor pattern for schema traversal

## Read Flow

```
get_default_engine() -> get_snapshot_builder() -> snapshot_builder_build() -> scan() -> scan_metadata() -> read + transform
```

Snapshot builder API (`ffi/src/lib.rs`):
- `get_snapshot_builder(path, engine)` -- fresh snapshot from a table path
- `get_snapshot_builder_from(old_snapshot, engine)` -- incremental update reusing an existing snapshot (avoids re-reading the log)
- `snapshot_builder_set_version(builder, version)` -- optional: pin to a specific version
- `snapshot_builder_set_log_tail(builder, log_tail)` -- optional: set log tail (for catalog-managed tables)
- `snapshot_builder_build(builder)` -- consume the builder and produce a `SharedSnapshot`
- `free_snapshot_builder(builder)` -- discard without building (e.g. on error paths)

The caller owns the returned builder handle and must call either `snapshot_builder_build` or `free_snapshot_builder`.

## Write Flow

```
get_default_engine() -> transaction() -> with_engine_info() -> add_files() -> commit()
```

## Building

```bash
cargo build -p delta_kernel_ffi --release
# Headers written to target/ffi-headers/
```

Feature flags:
- `default-engine-rustls` (default)
- `default-engine-native-tls`
- `arrow` (default; currently maps to `arrow-58`)
- `arrow-58`, `arrow-57`
- `delta-kernel-unity-catalog`
- `tracing`
