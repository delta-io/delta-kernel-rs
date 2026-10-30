# Checkpoint protection

To write to a table with the `checkpointProtection` feature, enable the `delta_kernel` Cargo
feature `checkpoint-protection`. Without it, Kernel rejects transactions and checkpoint writes
on these tables; reads remain supported.

See [Appending data](./append.md) for the transaction write flow.

## Connector requirements

The `checkpointProtection` table feature protects checkpoints and version history across
table feature removal. Enable the Cargo feature only if your connector satisfies all
of the feature's [writer requirements].

These requirements cover preserving protected checkpoints, validating the protocol at the
version being checkpointed, and safely cleaning up version history. Kernel does not perform
metadata cleanup; your connector is responsible for making its cleanup comply with the feature.
Skipping metadata cleanup satisfies the cleanup requirements.

Kernel's [checkpoint APIs](../maintenance/checkpointing.md) validate the protocol at the snapshot
version being checkpointed.

[writer requirements]: https://github.com/delta-io/delta/blob/master/protocol_rfcs/checkpoint-protection.md#writer-requirements-for-checkpoint-protection

## What's next

- [Appending data](./append.md): write files and commit a transaction
- [Checkpointing](../maintenance/checkpointing.md): write checkpoints
