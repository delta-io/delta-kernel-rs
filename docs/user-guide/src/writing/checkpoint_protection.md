# Checkpoint protection

The `checkpointProtection` table feature restricts cleanup of checkpoints and JSON transaction logs.
Kernel doesn't support metadata cleanup. If your connector performs metadata cleanup itself, it must
follow the feature's [writer requirements].

See [Appending data](./append.md) for the transaction write flow.

[writer requirements]: https://github.com/delta-io/delta/blob/master/protocol_rfcs/checkpoint-protection.md#writer-requirements-for-checkpoint-protection

## What's next

- [Appending data](./append.md): write files and commit a transaction
- [Checkpointing](../maintenance/checkpointing.md): write checkpoints
