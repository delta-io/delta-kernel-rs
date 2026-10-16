# Delta error catalog

These files are inputs to `cargo xtask update-delta-error-catalog`, which generates
`kernel/src/error/delta_error_condition/generated.rs`. Never edit them by hand.

- `delta-error-classes.json`: a verbatim copy of Delta's
  `spark/src/main/resources/error/delta-error-classes.json`.
- `source.json`: the delta-io/delta commit it was copied from and its SHA-256.

Commands:

- Bump to the latest delta-io/delta master (needs `git` and `curl`):
  `cargo xtask update-delta-error-catalog`
- Regenerate after a generator-only change: `cargo xtask update-delta-error-catalog --offline`
- Verify the catalog and generated code are current:
  `cargo xtask update-delta-error-catalog --check`

Review the generated diff on every bump: removing or renaming an upstream condition removes a
`DeltaErrorCondition` variant, which is a breaking kernel API change. Upstream has removed
conditions before, for example `DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE`.
