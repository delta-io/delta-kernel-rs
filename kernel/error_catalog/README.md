# Delta error catalog

`cargo xtask generate-delta-error-conditions` generates
`kernel/src/error/delta_error_condition/generated.rs` from these checked-in inputs:

- `delta-error-classes.json`: a verbatim snapshot of [Delta's error catalog][upstream-catalog].
- `source.json`: the delta-io/delta repository, full commit SHA, source path, and catalog SHA-256.
- `selected-conditions.json`: the allowlist of exact leaf condition names to expose in Kernel.
  Subclasses use `PARENT.SUB`; selecting a parent does not select its children.

Add a condition to `selected-conditions.json` when Kernel code needs to use it. Preserve upstream
names, SQLSTATEs, message templates, and parameter names. Duplicate selections, unknown names, and
selected names that map to the same Rust variant are rejected.

To download the latest upstream catalog, run `cargo xtask update-delta-error-catalog` (requires
`git`, `curl`, and network access). It resolves delta-io/delta master to a commit, downloads the
catalog at that commit, and updates the snapshot and source metadata. It leaves the allowlist and
generated Rust untouched.

To generate Rust, edit `selected-conditions.json` as needed and run
`cargo xtask generate-delta-error-conditions`. Generation always uses local inputs and verifies the
catalog hash. Review and commit the inputs and generated Rust together. Normal builds compile the
generated Rust directly.

For a manual freshness check, run `cargo xtask generate-delta-error-conditions --check`.
It fails if generated Rust differs from the local inputs, without writing files.
Do not edit the snapshot's individual entries or the generated Rust by hand.

Removing or renaming a selected condition removes a `DeltaErrorCondition` variant, which is a
breaking Kernel API change. Unselected upstream conditions do not become public Rust variants.

[upstream-catalog]:
  https://github.com/delta-io/delta/blob/master/spark/src/main/resources/error/delta-error-classes.json
