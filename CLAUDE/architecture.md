# Source navigation

This file helps contributors find implementation boundaries. It does not define public API
contracts or retell the connector workflow. Use the
[user guide](https://docs.delta.io/kernel/rust/) for cross-API explanations and
[rustdoc](https://docs.rs/delta_kernel/latest/delta_kernel/) for exact public behavior.

## Find the owning module

| Task | Start here | Related implementation |
|------|------------|------------------------|
| Load or refresh a snapshot | `kernel/src/snapshot/` | `log_segment/`, `log_reader/`, `crc/` |
| Build or execute a read | `kernel/src/scan/` | `log_replay/`, `actions/deletion_vector.rs` |
| Read changes between versions | `kernel/src/incremental_scan/`, `kernel/src/table_changes/` | `log_replay/` |
| Create or modify a table | `kernel/src/transaction/` | `committer/`, `partition/` |
| Write checkpoints or CRCs | `kernel/src/checkpoint/`, `kernel/src/crc/` | `actions/` |
| Implement engine capabilities | `kernel/src/lib.rs`, `kernel/src/engine/` | `default-engine/src/` |
| Change schemas or expressions | `kernel/src/schema/`, `kernel/src/expressions/` | `transforms/` |
| Change protocol feature checks | `kernel/src/table_features/` | `table_configuration.rs`, `actions/` |
| Work on catalog-managed tables | `kernel/src/committer/` | `delta-kernel-unity-catalog/` |

Search for the public type or operation before assuming these entry points are exhaustive. Module
layouts change more often than their ownership boundaries.

## Boundaries that matter in implementation work

- Kernel describes I/O and computation through `Engine`; engine implementations perform them.
- `EngineData` is opaque to kernel production code. Inspect rows through the visitor APIs, and
  handle every batch returned for a file.
- Public API behavior belongs in rustdoc. Implementation comments should explain hidden invariants
  and design constraints.
- Delta protocol behavior must match the
  [protocol specification](https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md).
- Connector workflows that cross module boundaries belong in the user guide, not in this map.
