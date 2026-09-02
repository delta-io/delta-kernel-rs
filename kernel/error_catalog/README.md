# Delta error catalog

`delta-error-classes.json` is copied verbatim from the Delta repository and is the sole catalog
source for generated condition names, SQLSTATEs, parameters, and message templates.
`source.json` records its upstream repository, exact commit, source path, and content checksum.
The update command maintains both files together.

The generated Rust metadata is checked in so normal kernel builds do not run code generation.
To update the catalog from a local checkout of <https://github.com/delta-io/delta>, run:

```shell
cargo xtask update-delta-error-catalog --delta-repo ../delta --revision <revision>
```

Run `cargo xtask generate-delta-error-conditions --check` to verify that the generated files are
current. Catalog additions, removals, and metadata changes flow directly into the generated Rust,
so review that diff for API changes. Condition strings, not Rust enum discriminants, are the stable
identities exposed to connectors.
