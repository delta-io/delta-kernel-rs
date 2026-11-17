# delta kernel mdbook

This book teaches developers how to build Delta Lake connectors with Kernel. Follow
[documentation ownership](../../AGENTS.md#documentation-ownership) when deciding what belongs here
and the [writing standards](CLAUDE.md) when editing guide pages.

## Maintenance

Update a guide page in the same PR as a user-visible change to the workflow it teaches. Fix examples
affected by API changes in that PR. Before a release, review the changes since the commit recorded
in [LAST_VALIDATED.md](LAST_VALIDATED.md) and update affected guide pages. Advance that record only
after reviewing the full range.

## prerequisites

This book is built with [`mdbook`]. Install the latest version with `cargo install mdbook` or whatever
their latest docs say. If running into issues with the `mermaid` preprocessor (e.g. "Unable to run the preprocessor `mermaid`"), run `cargo install mdbook-mermaid`.

Testing the Rust examples with `make test` also requires Python 3.

## building
The book is built in CI and deployed to [docs.delta.io/kernel/rust](https://docs.delta.io/kernel/rust/). When working on
the book locally you can preview changes with mdbook local server:

```bash
mdbook serve # from docs/user-guide to serve the book on localhost:3000
```

[`mdbook`]: https://github.com/rust-lang/mdBook
