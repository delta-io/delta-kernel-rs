# delta-kernel-rs

Delta-kernel-rs is an experimental [Delta][delta] implementation focused on
interoperability with a wide range of query engines. It currently only supports
reads.

Delta is an open-source [storage format][delta-protocol] with a [reference
implementation][delta-github] that is effectively a Spark library. For the reference implementation,
JVM/Spark are required to interact with Delta tables. Delta-kernel-rs, by contrast,
aims to democratize interacting with Delta tables by providing a means of
'teaching' any query engine how to read (and in the future, write) Delta
tables.

## Building

To get started, install Rust via [rustup], and [just] as a command runner - then clone the repository.
Before running tests, we need to load reference data from the [Delta Acceptance Tests][dat] via:

```sh
just load-dat
```

Then we can build and run tests via

```sh
cargo test --features acceptance,default-client
```

or using just again:

```sh
just test
```

## Development

delta-kernel-rs is still under heavy development but follows conventions
adopted by most Rust projects.

### Architecture

See [doc/architecture.md] document (currently wip)

Some design principles which should be considered:

- async all the things, but ideally no runtime. Basically delta-core should
  have async traits and implementations, but strive to avoid the need of a
  runtime so that implementers can use tokio, smol, async-std, or whatever
  might be needed. Essentially, we want to strive to design without actually
  evaluating futures and leave that to the implementers on top of possible
- No `DeltaTable`. The initial prototype intentionally exposes the concept of
  immutable versions of tables through the snapshot API. This encourages users
  to think about the Delta table state more accurately.
- Consistently opinionated. In #delta-rs we have some object-oriented APIs and
  some builder APIs. delta-kernel-rs uses builder-style APIs and that we should
  adopt consistently through delta-core-rs
- "Simple" set of default-features enabled to provide the basic functionality
  with the least necessary amount of dependencies possible. Putting more
  complex optimizations or APIs behind feature flags
- API conventions to make it clear which operations involve I/O, e.g. fetch or
  retrieve type verbiage in method signatures.

### Tips

- When developing, `rust-analyzer` is your friend. `rustup component add rust-analyzer`
- When also developing in vscode its sometimes convenient to configure rust-anmalyzer
  in `.vscode/settings.json`.

```json
{
  "editor.formatOnSave": true,
  "rust-analyzer.cargo.features": ["default-client", "acceptance"]
}
```

- The crate's documentation can be easily reviewed with: `cargo docs --open`

[delta]: https://delta.io
[delta-protocol]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
[delta-github]: https://github.com/delta-io/delta
[rustup]: https://rustup.rs
[architecture.md]: https://github.com/delta-incubator/delta-kernel-rs/tree/master/architecture.md
[dat]: https://github.com/delta-incubator/dat
[just]: https://github.com/casey/just
