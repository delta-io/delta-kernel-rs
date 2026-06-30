# Unity Catalog integration guidelines

## Where this sits in the UC stack

Three crates layer, and lower layers know nothing of higher ones:

1. `unity-catalog-delta-client-api` -- transport-agnostic traits (`GetCommitsClient`,
   `CommitClient`) and request/response + credential models. No HTTP.
2. `unity-catalog-delta-rest-client` -- concrete HTTP implementation of those traits.
3. `delta-kernel-unity-catalog` (this crate) -- the kernel-facing glue: `UCKernelClient` loads a
   snapshot from UC-reported commits, and `UCCommitter` implements kernel's `Committer` for
   catalog-managed tables.

When changing the trait surface, edit `*-client-api` and let the REST client and this crate
follow; do not duplicate models downstream. Catalog-managed table background is in
`CLAUDE/architecture.md`.

## Invariants to uphold

- **Catalog-managed tables must commit through `UCCommitter`**, never `FileSystemCommitter`. The
  committer is the only thing that knows the stage/ratify/publish protocol.
- **Version 0 publishes directly; version >= 1 stages then ratifies.** v0 (table creation) writes
  the commit file to the published log path. v>=1 writes a staged commit and calls the UC commit
  API to ratify it. Preserve this split -- it is the catalog-managed write protocol.
- **The commit log tail from UC must be contiguous** (no version gaps), and bounded by the
  max-ratified version UC reports. Validate, don't assume.
- **Required protocol properties are enforced centrally** (the `get_*_required_properties_*`
  helpers). A catalog-managed table's feature set (e.g. `catalogManaged`, `inCommitTimestamp`,
  `vacuumProtocolCheck`) must be consistent; route checks through these helpers rather than
  hand-listing features at call sites.
- **Commit runs in async context** and assumes a multi-threaded Tokio runtime (it drives kernel
  operations that block); don't call it from a single-threaded runtime.
