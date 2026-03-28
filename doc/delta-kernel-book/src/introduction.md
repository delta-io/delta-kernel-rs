# Welcome to Delta Kernel

<div class="warning">

Warning!

This is heavily work-in-progress documentation for the delta kernel.

</div>

Delta kernel is a query-engine agnostic library to provide Delta support in any engine.

There are currently two implementations of the Delta kernel: a native (rust) implementation (with a
C/C++ FFI): [delta-kernel-rs] (this project!) and a Java implementation [delta-kernel-java]. This
book is only concerned with the rust implementation.

```
           Engine Trait
     ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
       ┌───────────────────┐  │
     │ │ EvaluationHandler │
       └───────────────────┘  │
     │ ┌─────────────────┐
       │  ParquetHandler │    │
     │ └─────────────────┘
     │ ┌────────────────┐
       │  JsonHandler   │     │
     │ └────────────────┘
     │ ┌──────────────────┐
       │  StorageHandler  │   │
     │ └──────────────────┘
      ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
```


The read path follows the following (rough) pattern:
1. Kernel resolves metadata
2. ships `ScanMetadata` to engine which includes files to scan, transforms
3. engine reads + applies transforms

Write path is largely the reverse:
1. Kernel resolves metadata during transaction creation
2. ships `WriteMetadata` to engine which includes files to write, transform
3. engine applies transforms and writes files
4. engine calls `Kernel::commit` to finalize the transaction


[delta-kernel-rs]: https://github.com/delta-io/delta-kernel-rs
[delta-kernel-java]: https://github.com/delta-io/delta/tree/master/kernel