delta-kernel-rs
===============

Delta-kernel-rs is an experimental [Delta][delta] implementation focused on
interoperability with a wide range of query engines. It currently only supports
reads.

Delta's (the open-source [storage format][delta-protocol]) [reference
implementation][delta-github] is effectively a Spark library. That is, one must
use the JVM and Spark to interact with Delta tables. Delta-kernel-rs, by contrast,
aims to democratize interacting with Delta tables by providing a means of
'teaching' any query engine how to read (and in the future, write) Delta
tables.

architecture
------------

see [architecture.md] document (currently wip)

building
--------

Install rust via [rustup]. Then clone the repository and build/run tests via
`cargo test` in the root of the project.

development
-----------

Rust-analyzer is your friend. `rustup component add rust-analyzer`

### documetation

`cargo docs --open`

[delta]: https://delta.io
[delta-protocol]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
[delta-github]: https://github.com/delta-io/delta
[rustup]: https://rustup.rs
[architecture.md]: https://github.com/delta-incubator/delta-kernel-rs/tree/master/architecture.md
