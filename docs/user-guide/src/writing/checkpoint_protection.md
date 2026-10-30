# Checkpoint protection

To write to a table with the `checkpointProtection` feature, enable the `delta_kernel` Cargo
feature `checkpoint-protection`. Without it, Kernel rejects transactions and checkpoint writes
on these tables; reads remain supported.

See [Appending data](./append.md) for the transaction write flow.

## What's next

- [Appending data](./append.md): write files and commit a transaction
- [Checkpointing](../maintenance/checkpointing.md): write checkpoints
