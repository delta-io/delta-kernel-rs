# delta-kernel-rs

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
- ffi: Functionallity that enables delta-kernel-rs to be used from `C` or `C++` See the [ffi](ffi)
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
delta_kernel = "0.6"

# or turn on the default engine, based on arrow
delta_kernel = { version = "0.6", features = ["default-engine"] }
```

### Feature flags
There are more feature flags in addition to the `default-engine` flag shown above. Relevant flags
include:

| Feature flag  | Description   |
| ------------- | ------------- |
| `default-engine`    | Turn on the 'default' engine: async, arrow-based `Engine` implementation  |
| `sync-engine`       | Turn on the 'sync' engine: synchronous, arrow-based `Engine` implementation. Only supports local storage! |
| `arrow-conversion`  | Conversion utilities for arrow/kernel schema interoperation |
| `arrow-expression`  | Expression system implementation for arrow |

### Versions and Api Stability
We intend to follow [Semantic Versioning](https://semver.org/). However, in the `0.x` line, the APIs
are still unstable. We therefore may break APIs within minor releases (that is, `0.1` -> `0.2`), but
we will not break APIs in patch releases (`0.1.0` -> `0.1.1`).

## Arrow versioning
If you enable the `default-engine` or `sync-engine` features, you get an implemenation of the
`Engine` trait that uses [Arrow] as its data format.

The [`arrow crate`](https://docs.rs/arrow/latest/arrow/) tends to release new major versions rather
quickly. To enable engines that already integrate arrow to also integrate kernel and not force them
to track a specific version of arrow that kernel depends on, we take as broad dependecy on arrow
versions as we can.

This means you can force kernel to rely on the specific arrow version that your engine already uses,
as long as it falls in that range. You can see the range in the `Cargo.toml` in the same folder as
this `README.md`.

For example, although arrow 53.1.0 has been released, you can force kernel to compile on 53.0 by
putting the following in your project's `Cargo.toml`:

```toml
[patch.crates-io]
arrow = "53.0"
arrow-arith = "53.0"
arrow-array = "53.0"
arrow-buffer = "53.0"
arrow-cast = "53.0"
arrow-data = "53.0"
arrow-ord = "53.0"
arrow-json = "53.0"
arrow-select = "53.0"
arrow-schema = "53.0"
parquet = "53.0"
```

Note that unfortunatly patching in `cargo` requires that _exactly one_ version matches your
specification. If only arrow "53.0.0" had been released the above will work, but if "53.0.1" where
to be released, the specification will break and you will need to provide a more restrictive
specification like `"=53.0.0"`.

### Object Store
You may also need to patch the `object_store` version used if the version of `parquet` you depend on
depends on a different version of `object_store`. This can be done by including `object_store` in
the patch list with the required version. You can find this out by checking the `parquet` [docs.rs
page](https://docs.rs/parquet/52.2.0/parquet/index.html), switching to the version you want to use,
and then checking what version of `object_store` it depends on.

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
   the Delta Kernel in order to read/write the Delta table.
2. The `DefaultEngine` is our default implementation of the the above trait. It lives in
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
[architecture.md]: https://github.com/delta-io/delta-kernel-rs/tree/master/architecture.md
[dat]: https://github.com/delta-incubator/dat
[derive-macros]: https://doc.rust-lang.org/reference/procedural-macros.html
[API Docs]: https://docs.rs/delta_kernel/latest/delta_kernel/
[cargo-llvm-cov]: https://github.com/taiki-e/cargo-llvm-cov
[FFI]: ffi/
[Arrow]: https://arrow.apache.org/rust/arrow/index.html
[Tokio]: https://tokio.rs/