# delta kernel mdbook

This book teaches connector workflows and explains how Kernel APIs fit together. Exact public API
contracts live in [rustdoc], while repository workflows and conventions live in the agent docs.

Update a page in the same PR as a user-visible change to the workflow it teaches. Release-time
validation is a backstop, not the guide's update cadence.

## prerequisites

This book is built with [`mdbook`]. Install the latest version with `cargo install mdbook` or whatever
their latest docs say. If running into issues with the `mermaid` preprocessor (e.g. "Unable to run the preprocessor `mermaid`"), run `cargo install mdbook-mermaid`.

## building
The book is built in CI and deployed to [docs.delta.io/kernel/rust](https://docs.delta.io/kernel/rust/). When working on
the book locally you can preview changes with mdbook local server:

```bash
mdbook serve # from docs/user-guide to serve the book on localhost:3000
```

[`mdbook`]: https://github.com/rust-lang/mdBook
[rustdoc]: https://docs.rs/delta_kernel/latest/delta_kernel/
