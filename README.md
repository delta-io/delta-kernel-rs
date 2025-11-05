# Delta Kernel (rust) &emsp; [![build-status]][actions] [![latest-version]][crates.io] [![docs]][docs.rs] [![rustc-version-1.84+]][rustc]

[build-status]: https://img.shields.io/github/actions/workflow/status/delta-io/delta-kernel-rs/build.yml?branch=main
[actions]: https://github.com/delta-io/delta-kernel-rs/actions/workflows/build.yml?query=branch%3Amain
[latest-version]: https://img.shields.io/crates/v/delta_kernel.svg
[crates.io]: https://crates.io/crates/delta\_kernel
[rustc-version-1.84+]: https://img.shields.io/badge/rustc-1.84+-lightgray.svg
[rustc]: https://blog.rust-lang.org/2025/01/09/Rust-1.84.0/
[docs]: https://img.shields.io/docsrs/delta_kernel
[docs.rs]: https://docs.rs/delta_kernel/latest/delta_kernel/

Delta-kernel-rs is an experimental [Delta][delta] implementation focused on interoperability with a
wide range of query engines. It currently supports reads and (experimental) writes. Only blind
appends are currently supported in the write path.

The Delta Kernel project is a Rust and C library for building Delta connectors that can read and
write Delta tables without needing to understand the Delta [protocol details][delta-protocol]. This
is the Rust/C equivalent of [Java Delta Kernel][java-kernel].

## Crates

Delta-kernel-rs is split into a few different crates:

- kernel: The actual core kernel crate
- acceptance: Acceptance tests that validate correctness  via the [Delta Acceptance Tests][dat]
- derive-macros: A crate for our [derive-macros] to live in
- ffi: Functionality that enables delta-kernel-rs to be used from `C` or `C++` See the [ffi](ffi)
  directory for more information.

## Building
By default we build only the `kernel` and `acceptance` crates, which will also build `derive-macros`
as a dependency.

To get started, install Rust via [rustup], clone the repository, and then run:

```sh
cargo test --all-features
```

This will build the kernel, run all unit tests, fetch the [Delta Acceptance Tests][dat] data and run
the acceptance tests against it.

In general, you will want to depend on `delta-kernel-rs` by adding it as a dependency to your
`Cargo.toml`, (that is, for rust projects using cargo) for other projects please see the [FFI]
module. The core kernel includes facilities for reading and writing delta tables, and allows the
consumer to implement their own `Engine` trait in order to build engine-specific implementations of
the various `Engine` APIs that the kernel relies on (e.g. implement an engine-specific
`read_json_files()` using the native engine JSON reader). If there is no need to implement the
consumer's own `Engine` trait, the kernel has a feature flag to enable a default, asynchronous
`Engine` implementation built with [Arrow] and [Tokio].

```toml
# fewer dependencies, requires consumer to implement Engine trait.
# allows consumers to implement their own in-memory format
delta_kernel = "0.16.0"

# or turn on the default engine, based on arrow
delta_kernel = { version = "0.16.0", features = ["default-engine", "arrow-56"] }
```

### Feature flags
There are more feature flags in addition to the `default-engine` flag shown above. Relevant flags
include:

| Feature flag  | Description   |
| ------------- | ------------- |
| `default-engine`    | Turn on the 'default' engine: async, arrow-based `Engine` implementation  |
| `arrow-conversion`  | Conversion utilities for arrow/kernel schema interoperation |
| `arrow-expression`  | Expression system implementation for arrow |

### Versions and Api Stability
We intend to follow [Semantic Versioning](https://semver.org/). However, in the `0.x` line, the APIs
are still unstable. We therefore may break APIs within minor releases (that is, `0.1` -> `0.2`), but
we will not break APIs in patch releases (`0.1.0` -> `0.1.1`).

## Arrow versioning
If you enable the `default-engine` feature, you get an implementation of the `Engine` trait that
uses [Arrow] as its data format.

The [`arrow crate`](https://docs.rs/arrow/latest/arrow/) tends to release new major versions rather
frequently. To enable engines that already integrate arrow to also integrate kernel and not force
them to track a specific version of arrow that kernel depends on, we take as broad dependency on
arrow versions as we can.

We allow selecting the version of arrow to use via feature flags. Currently we support the following
flags:

- `arrow-55`: Use arrow version 55
- `arrow-56`: Use arrow version 56
- `arrow`: Use the latest arrow version. Note that this is an _unstable_ flag: we will bump this to
  the latest arrow version at every arrow version release. Only removing old arrow versions will
  cause a breaking change for kernel. If you require a specific version N of arrow, you should
  specify it directly with `arrow-N`, e.g. `arrow-56`.

Note that if more than one `arrow-x` feature is enabled, kernel will use the _highest_ (latest)
specified flag. This also means that if you use `--all-features` you will get the latest version of
arrow that kernel supports.

### Object Store
You may also need to patch the `object_store` version used if the version of `parquet` you depend on
depends on a different version of `object_store`. This can be done by including `object_store` in
the patch list with the required version. You can find this out by checking the `parquet` [docs.rs
page](https://docs.rs/parquet/52.2.0/parquet/index.html), switching to the version you want to use,
and then checking what version of `object_store` it depends on.

## Documentation

- [API Docs](https://docs.rs/delta_kernel/latest/delta_kernel/)
- [architecture.md](doc/architecture.md) document describing the kernel architecture (currently wip)

## Examples

There are some example programs showing how `delta-kernel-rs` can be used to interact with delta
tables. They live in the [`kernel/examples`](kernel/examples) directory.

## Development

delta-kernel-rs is still under heavy development but follows conventions adopted by most Rust
projects.

### Concepts

There are a few key concepts that will help in understanding kernel:

1. The `Engine` trait encapsulates all the functionality an engine or connector needs to provide to
   the Delta Kernel in order to read/write the Delta table.
2. The `DefaultEngine` is our default implementation of the above trait. It lives in
   `engine/default`, and provides a reference implementation for all `Engine`
   functionality. `DefaultEngine` uses [arrow](https://docs.rs/arrow/latest/arrow/) as its in-memory
   data format.
3. A `Scan` is the entrypoint for reading data from a table.
4. A `Transaction` is the entrypoint for writing data to a table.

### Design Principles

Some design principles which should be considered:

- async should live only in the `Engine` implementation. The core kernel does not use async at
  all. We do not wish to impose the need for an entire async runtime on an engine or connector. The
  `DefaultEngine` _does_ use async quite heavily. It doesn't depend on a particular runtime however,
  and implementations could provide an "executor" based on tokio, smol, async-std, or whatever might
  be needed. Currently only a `tokio` based executor is provided.
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
- When also developing in VS Code it's convenient to add rust-analyzer to your workspace.

- The crate's documentation can be easily reviewed with: `cargo docs --open`
- Code coverage is available on codecov via [cargo-llvm-cov]. See their docs for instructions to install/run locally.

[delta]: https://delta.io
[delta-protocol]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
[delta-github]: https://github.com/delta-io/delta
[java-kernel]: https://github.com/delta-io/delta/tree/master/kernel
[rustup]: https://rustup.rs
[architecture.md]: https://github.com/delta-io/delta-kernel-rs/tree/master/architecture.md
[dat]: https://github.com/delta-incubator/dat
[derive-macros]: https://doc.rust-lang.org/reference/procedural-macros.html
[API Docs]: https://docs.rs/delta_kernel/latest/delta_kernel/
[cargo-llvm-cov]: https://github.com/taiki-e/cargo-llvm-cov
[FFI]: ffi/
[Arrow]: https://arrow.apache.org/rust/arrow/index.html
[Tokio]: https://tokio.rs/