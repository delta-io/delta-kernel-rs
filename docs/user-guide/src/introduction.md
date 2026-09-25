# Delta Kernel Rust User Guide

<span style="display:block;text-align:center">
  <img src="images/ferris-delta-small.png" alt="The Delta Kernel Logo"/>
 </span>

Delta Kernel is a Rust library for building Delta Lake connectors. It handles the
Delta protocol so you don't have to. Connectors read and write Delta tables through
Kernel's API without needing to understand the protocol internals. When the protocol
evolves, updating your Kernel dependency is all that's needed to pick up new features.

Kernel is query-engine agnostic. It provides a native Rust API and a C/C++ FFI layer,
making it usable from virtually any language.

> [!NOTE]
> This guide is a work in progress.

## Architecture at a glance

```text
     ┌──────────────────────────────────────────┐
     │                Connectors                │
     │  (Query Engines, Analytics Tools, etc.)  │
     └───────────┬──────────────────────┬───────┘
                 │                      │
     ┌───────────▼─────────┐   ┌────────▼───────┐
     │    Rust Bindings    │   │  FFI Bindings  │
     │  (Native Rust API)  │   │  (C/C++ API)   │
     └────────────────┬────┘   └─┬──────────────┘
                      │          │
                  ┌───▼──────────▼─┐
                  │  Delta Kernel  │
                  │  (core logic)  │
                  └───────┬────────┘
                          │ calls into
                  ┌───────▼────────┐
                  │  Engine trait   │
                  │  (abstraction)  │
                  └───────┬────────┘
                          │ implemented by
                  ┌───────▼────────┐
                  │  DefaultEngine │
                  │  (or custom)   │
                  └───────┬────────┘
                          │
                  ┌───────▼────────┐
                  │  Delta Table   │
                  │  (storage)     │
                  └────────────────┘
```

The **Engine trait** is the boundary between Kernel and your connector. Kernel defines
_what_ needs to happen; the engine defines _how_. A ready-to-use `DefaultEngine` covers common
Arrow and object-store connectors. [Architecture overview](./concepts/architecture.md) explains
the boundary, while [rustdoc] defines the exact public APIs.

[rustdoc]: https://docs.rs/delta_kernel/latest/delta_kernel/

## FFI layer

The `delta_kernel_ffi` crate exposes the full Kernel API to C and C++ via a stable FFI
boundary. Headers (`.h` and `.hpp`) are generated automatically at build time using
cbindgen. Rust objects cross the boundary as opaque **handles** with clear ownership
semantics, and every fallible function returns a structured error type.

This means you can build a Delta connector in C, C++, or any language with a C FFI
without writing any Rust. See the [FFI overview](./ffi/overview.md) for details.

## Getting started

For Rust projects, add to `Cargo.toml`:

```toml
delta_kernel = "0.28.0"
delta_kernel_default_engine = { version = "0.28.0", features = ["rustls"] }
```

For C/C++ projects, build the FFI crate and link against it. See the
[FFI overview](./ffi/overview.md).

Then follow the quick starts to see Kernel in action.

## What's next

- [Quick Start: Reading a Table](./getting_started/quick_start_read.md)
- [Quick Start: Writing a Table](./getting_started/quick_start_write.md)
- [Architecture Overview](./concepts/architecture.md)
