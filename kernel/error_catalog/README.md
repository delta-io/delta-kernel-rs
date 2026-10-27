# Delta error catalog

`cargo xtask update-delta-error-catalog` generates
`kernel/src/error/delta_error_condition/generated.rs` from these checked-in inputs:

- `delta-error-classes.json`: a verbatim snapshot of Delta's
  `spark/src/main/resources/error/delta-error-classes.json`.
- `source.json`: the delta-io/delta repository, full commit SHA, source path, and catalog SHA-256.
- `selected-conditions.json`: the allowlist of exact leaf condition names to expose in Kernel.
  Subclasses use `PARENT.SUB`; selecting a parent does not select its children.

Add conditions alongside the factories and call sites that need them. Preserve upstream names,
SQLSTATEs, message templates, and parameter names. Duplicate selections, unknown names, and
selected names that map to the same Rust variant are rejected.

To add a condition already in the pinned catalog, edit `selected-conditions.json` and run
`cargo xtask update-delta-error-catalog --offline`. Review and commit the allowlist and generated
Rust together. Normal builds compile the generated Rust directly.

Generation always uses local inputs; `--offline` is accepted to make that intent explicit.
`cargo xtask update-delta-error-catalog --check` verifies the catalog hash and fails if the generated
Rust differs, without writing files. CI runs this check against the checked-in inputs.

Updating the upstream snapshot is a separate, explicit change. Copy the catalog verbatim from a
specific delta-io/delta commit, update `source.json` with that full commit SHA and the file's SHA-256
(`sha256sum kernel/error_catalog/delta-error-classes.json`), then regenerate and review the diff.
Do not edit the snapshot's individual entries or the generated Rust by hand.

Removing or renaming a selected condition removes a `DeltaErrorCondition` variant, which is a
breaking Kernel API change. Unselected upstream conditions do not become public Rust variants.
