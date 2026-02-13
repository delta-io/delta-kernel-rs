# FFI Layer

The `delta_kernel_ffi` crate exposes the kernel to C/C++ via a stable FFI boundary using
cbindgen-generated headers (`.h` and `.hpp`).

## Handle System

Objects crossing the FFI boundary are wrapped in **handles** -- opaque pointers with
ownership semantics:
- **Mutable handles** (`Box`-like) -- exclusive ownership, neither `Copy` nor `Clone`
- **Shared handles** (`Arc`-like) -- shared ownership via reference counting

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
get_default_engine() -> snapshot() -> scan() -> scan_metadata() -> read + transform
```

## Write Flow

```
get_default_engine() -> transaction() -> with_engine_info() -> add_files() -> commit()
```

## Building

```bash
cargo build -p delta_kernel_ffi --release
# Headers written to target/ffi-headers/
```

Feature flags: `default-engine-rustls` (default), `default-engine-native-tls`,
`catalog-managed`, `tracing`.
