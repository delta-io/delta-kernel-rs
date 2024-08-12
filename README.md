# delta-kernel-rs

Delta-kernel-rs is an experimental [Delta][delta] implementation focused on
interoperability with a wide range of query engines. It currently only supports
reads.

The Delta Kernel project is a Rust and C library for building Delta connectors that can read (and
soon, write) Delta tables without needing to understand the Delta [protocol
details][delta-protocol]. This is the Rust/C equivalent of [Java Delta Kernel][java-kernel].

## Crates

Delta-kernel-rs is split into a few different crates:

- kernel: The actual core kernel crate
- acceptance: Acceptance tests that validate correctness  via the [Delta Acceptance Tests][dat]
- derive-macros: A crate for our [derive-macros] to live in
- ffi: Functionallity that enables delta-kernel-rs to be used from `C` or `C++` See the [ffi](ffi)
  directory for more information.

## Building
By default we build only the `kernel` and `acceptance` crates, which will also build `derive-macros`
as a dependency.

To get started, install Rust via [rustup], clone the repository, and then run:

```sh
cargo test
```

This will build the kernel, run all unit tests, fetch the [Delta Acceptance Tests][dat] data and run
the acceptance tests against it.

As it is a library, in general you will want to depend on `delta-kernel-rs` by adding it as a
dependency to your `Cargo.toml`. For example:

```toml
delta_kernel = "0.3"
```

### Versions and Api Stability
We intend to follow [Semantic Versioning](https://semver.org/). However, in the `0.x` line, the APIs
are still unstable. We therefore may break APIs within minor releases (that is, `0.1` -> `0.2`), but
we will not break APIs in patch releases (`0.1.0` -> `0.1.1`).

## Documentation

- [API Docs](https://docs.rs/delta_kernel/latest/delta_kernel/)
- [arcitecture.md](doc/architecture.md) document describing the kernel architecture (currently wip)

## Examples

There are some example programs showing how `delta-kernel-rs` can be used to interact with delta
tables. They live in the [`kernel/examples`](kernel/examples) directory.

## Development

delta-kernel-rs is still under heavy development but follows conventions adopted by most Rust
projects.

### Concepts

There are a few key concepts that will help in understanding kernel:

1. The `Engine` trait encapsulates all the functionality and engine or connector needs to provide to
   the Delta Kernel in order to read the Delta table.
2. The `DefaultEngine` is our default implementation of the the above trait. It lives in
   `engine/default`, and provides a reference implementation for all `Engine`
   functionality. `DefaultEngine` uses [arrow](https://docs.rs/arrow/latest/arrow/) as its in-memory
   data format.
3. A `Scan` is the entrypoint for reading data from a table.

### Design Principles

Some design principles which should be considered:

- async should live only in the `Engine` implementation. The core kernel does not use async at
  all. We do not wish to impose the need for an entire async runtime on an engine or connector. The
  `DefaultEngine` _does_ use async quite heavily. It doesn't depend on a particular runtime however,
  and implementations could provide an "executor" based on tokio, smol, async-std, or whatever might
  be needed. Currently only a `tokio` based executor is provided.
- Minimal `Table` API. The kernel intentionally exposes the concept of immutable versions of tables
  through the snapshot API. This encourages users to think about the Delta table state more
  accurately.
- Prefer builder style APIs over object oriented ones.
- "Simple" set of default-features enabled to provide the basic functionality with the least
  necessary amount of dependencies possible. Putting more complex optimizations or APIs behind
  feature flags
- API conventions to make it clear which operations involve I/O, e.g. fetch or retrieve type
  verbiage in method signatures.

### Tips

- When developing, `rust-analyzer` is your friend. `rustup component add rust-analyzer`
- If using `emacs`, both [eglot](https://github.com/joaotavora/eglot) and
  [lsp-mode](https://github.com/emacs-lsp/lsp-mode) provide excellent integration with
  `rust-analyzer`. [rustic](https://github.com/brotzeit/rustic) is a nice mode as well.
- When also developing in vscode its sometimes convenient to configure rust-analyzer in
  `.vscode/settings.json`.

```json
{
  "editor.formatOnSave": true,
  "rust-analyzer.cargo.features": ["default-engine", "acceptance"]
}
```

- The crate's documentation can be easily reviewed with: `cargo docs --open`
- Code coverage is available on codecov via [cargo-llvm-cov]. See their docs for instructions to install/run locally.

[delta]: https://delta.io
[delta-protocol]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
[delta-github]: https://github.com/delta-io/delta
[java-kernel]: https://github.com/delta-io/delta/tree/master/kernel
[rustup]: https://rustup.rs
[architecture.md]: https://github.com/delta-incubator/delta-kernel-rs/tree/master/architecture.md
[dat]: https://github.com/delta-incubator/dat
[derive-macros]: https://doc.rust-lang.org/reference/procedural-macros.html
[API Docs]: https://docs.rs/delta_kernel/latest/delta_kernel/
[cargo-llvm-cov]: https://github.com/taiki-e/cargo-llvm-cov