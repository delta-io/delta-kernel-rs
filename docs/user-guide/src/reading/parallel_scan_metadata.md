# Distributed log replay with parallel_scan_metadata()

To distribute Delta log replay across multiple threads or machines, use
`parallel_scan_metadata()`. The standard `scan_metadata()` method processes the entire log
sequentially on a single node. For large tables with V2 checkpoints (which use sidecars or
multi-part checkpoint files), that sequential replay can become a bottleneck.
`parallel_scan_metadata()` splits replay into two phases so that the expensive checkpoint
processing can run in parallel.

## Why two phases?

A Delta table's transaction log consists of:

- **Commit files** (JSON), which must be processed sequentially in version order
- **Checkpoint files**, which can be large but whose constituent parts (sidecars or
  multi-part checkpoint files) can be processed independently

`parallel_scan_metadata()` exploits this structure. The sequential phase handles commits and
the checkpoint manifest. The parallel phase then distributes the checkpoint leaf files
across workers.

## Sequential phase

The sequential phase processes commits and the checkpoint manifest. It returns an iterator
of `ScanMetadata` (the same type that `scan_metadata()` yields):

```rust,ignore
use delta_kernel::scan::{
    AfterSequentialScanMetadata, ParallelScanMetadata, ParallelState,
    SequentialScanMetadata,
};

let scan = snapshot.scan_builder().build()?;
let mut sequential = scan.parallel_scan_metadata(engine.clone())?;

// Process the sequential phase: commits and checkpoint manifest
for result in sequential.by_ref() {
    let scan_metadata = result?;
    // Process scan metadata (same as scan_metadata())...
}
```

After exhausting the iterator, call `finish()` to find out whether a parallel phase is
needed:

```rust,ignore
match sequential.finish()? {
    AfterSequentialScanMetadata::Done => {
        // All log replay completed in the sequential phase.
        // No checkpoint sidecars or multi-part files to process.
    }
    AfterSequentialScanMetadata::Parallel { state, files } => {
        // Parallel phase needed. `files` contains the checkpoint leaf files
        // (sidecars or multi-part checkpoint parts) to process in parallel.
    }
}
```

## Parallel phase

If `finish()` returns `Parallel`, partition the `files` across workers and create a
`ParallelScanMetadata` iterator per partition:

```rust,ignore
AfterSequentialScanMetadata::Parallel { state, files } => {
    // Unbox and wrap in Arc for sharing across workers
    let state = Arc::new(*state);

    // Distribute files across workers (one file per worker, or batched)
    for file in files {
        let parallel = ParallelScanMetadata::try_new(
            engine.clone(),
            state.clone(),
            vec![file],
        )?;
        for result in parallel {
            let scan_metadata = result?;
            // Process scan metadata (same as the sequential phase)...
        }
    }
}
```

Each `ParallelScanMetadata` reads its assigned checkpoint files and processes them through
the shared `ParallelState`, which handles deduplication (filtering out files already seen in
the sequential phase).

## Serializing state across the network

For distributed engines where workers run on different machines, the `ParallelState` can be
serialized to bytes and shipped over the network:

```rust,ignore
AfterSequentialScanMetadata::Parallel { state, files } => {
    // On the driver: serialize the state to bytes after the sequential phase
    let serialized_bytes = state.into_bytes()?;

    // Ship `serialized_bytes` and `files` to remote workers...

    // On each worker: reconstruct state from bytes and create a parallel iterator
    let state = Arc::new(ParallelState::from_bytes(engine.as_ref(), &serialized_bytes)?);
    let parallel = ParallelScanMetadata::try_new(engine.clone(), state, my_files)?;
}
```

> [!WARNING]
> The serialized state may only be deserialized by the same binary version of
> `delta-kernel-rs`. Using different versions for serialization and deserialization leads to
> undefined behavior.

## When to use this

Use `parallel_scan_metadata()` instead of `scan_metadata()` when:

- The table uses V2 checkpoints with sidecars or multi-part checkpoint files
- Log replay is a bottleneck (large tables with many files)
- Your engine can distribute work across multiple threads or machines

For most use cases, `scan_metadata()` is sufficient and simpler.

## What's next

- [Advanced reads with scan_metadata()](./scan_metadata.md) for the standard sequential API
- [Building a Scan](./building_a_scan.md) for the basics of building and executing a scan
