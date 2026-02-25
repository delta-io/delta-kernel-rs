# delta kernel mdbook

This book serves as a user guide + developer guide + overall docs for the kernel. In addition to our
docs.rs and top-level README, the three provide comprehensive documentation for the project.

## prerequisites
This book is built with [`mdbook`]. Install the latest version with `cargo install mdbook` or whatever
their latest docs say.


## building
The book is built in CI and deployed to github pages. TODO link to the page here. When working on
the book locally you can preview changes with mdbook local server:

```bash
mdbook serve # from doc/delta-kernel-book to serve the book on localhost:3000
```

[`mdbook`]: https://github.com/rust-lang/mdBook