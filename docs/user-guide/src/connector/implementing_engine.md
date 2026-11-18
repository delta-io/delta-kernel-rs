# Implementing the Engine trait

To integrate Kernel with your connector's storage, data format, and runtime, implement the
capabilities required by `Engine` and assemble them behind one Engine value. Read
[The Engine trait](../concepts/engine_trait.md) first for the role of each capability.

The [`Engine` rustdoc] defines the current trait. Follow its links for every handler's exact
parameters, return values, errors, and ordering requirements. This page focuses on implementation
sequence and design choices.

[`Engine` rustdoc]: https://docs.rs/delta_kernel/latest/delta_kernel/trait.Engine.html

## Choose what to replace

Start from the data and I/O boundaries your connector already owns. A custom Engine does not imply
four unrelated implementations from scratch.

1. Decide which columnar representation crosses the Engine boundary.
2. Reuse default handlers that already produce and consume that representation.
3. Replace only the handlers that must integrate with connector-native services.
4. Wrap the chosen handlers in one Engine implementation.

If your connector uses Arrow and `object_store`, prefer `DefaultEngine`. If it uses Arrow with a
custom filesystem client, a custom storage handler may be enough. A non-Arrow connector usually
needs matching JSON, Parquet, and evaluation handlers because those handlers exchange
`EngineData`.

## Define the EngineData boundary

Implement `EngineData` before handlers that produce it. Kernel must be able to:

- visit typed row values;
- append computed or partition columns; and
- apply a selection vector without assuming a concrete batch type.

Keep the concrete columnar type inside your Engine implementation. Kernel production code does not
downcast `EngineData`. See [The EngineData trait](engine_data.md) for the visitor workflow and the
[`EngineData` rustdoc] for exact method contracts.

[`EngineData` rustdoc]: https://docs.rs/delta_kernel/latest/delta_kernel/engine_data/trait.EngineData.html

## Implement storage access

Your `StorageHandler` connects Kernel's object-level operations to the connector's filesystem or
object-store client. Decide how the handler will preserve listing order, represent byte ranges, map
not-found and already-exists conditions, and provide atomic publication where Kernel requires it.

Do not translate these rules from examples on this page. Implement against the current
[`StorageHandler` rustdoc], then test the behavior through Kernel operations such as snapshot load
and commit publication.

[`StorageHandler` rustdoc]: https://docs.rs/delta_kernel/latest/delta_kernel/trait.StorageHandler.html

## Implement JSON handling

Your `JsonHandler` converts Delta log data between JSON and your `EngineData` representation. Reuse
the storage client from the previous step rather than creating a second I/O path with different
authentication or retry behavior.

Test parsing and file reads with projected schemas, nullable fields, multiple input files, and
predicates the implementation cannot evaluate. Test writes with null fields and conflicting
destinations. The [`JsonHandler` rustdoc] defines the required ordering and serialization behavior.

[`JsonHandler` rustdoc]: https://docs.rs/delta_kernel/latest/delta_kernel/trait.JsonHandler.html

## Implement Parquet handling

The `ParquetHandler` is usually the performance-critical boundary because scans use it for table
data and checkpoints. Integrate the connector's native reader when avoiding conversion or sharing
its I/O scheduler matters.

Validate column projection, missing nullable columns, physical field IDs, metadata columns,
multiple batches per file, and conservative predicate handling. Preserve the file and row ordering
defined by the [`ParquetHandler` rustdoc].

[`ParquetHandler` rustdoc]: https://docs.rs/delta_kernel/latest/delta_kernel/trait.ParquetHandler.html

## Implement expression evaluation

Your `EvaluationHandler` turns Kernel expressions and predicates into reusable evaluators over the
chosen `EngineData` representation. Translate Kernel's schema and scalar types at evaluator
construction time so repeated batch evaluation stays cheap.

Treat unsupported expressions as an explicit compatibility decision. A predicate used as a
best-effort hint must remain conservative: uncertainty keeps data instead of dropping possible
matches. The [`EvaluationHandler` rustdoc] and evaluator trait links define their output contracts.

[`EvaluationHandler` rustdoc]: https://docs.rs/delta_kernel/latest/delta_kernel/trait.EvaluationHandler.html

## Support cancellation

Kernel provides cancellation-aware variants for storage, JSON, and Parquet reads. The defaults
check before calling a plain handler and before iterator pulls. Override them if your runtime can
also interrupt a request waiting on I/O.

Use the runtime's notification primitive to implement `CancellationToken::cancelled_future`.
Avoid polling `is_cancelled` in a loop. Race the I/O future against cancellation, stop initiating
new requests after cancellation wins, and allow already-completed work to finish normally.

The [Engine cancellation contract] owns the exact semantics. Add tests for cancellation before a
read, during an in-flight read, and between iterator pulls.

[Engine cancellation contract]: https://docs.rs/delta_kernel/latest/delta_kernel/cancellation/index.html#engine-operation-contract

## Assemble the Engine

Store one shared instance of each handler in your Engine value. Returning the same handler instance
lets evaluators, caches, clients, and runtime resources retain connector-defined lifetimes.

Keep cross-handler configuration at this assembly boundary. For example, pass one storage client
to the JSON and Parquet handlers instead of making each handler discover credentials independently.
This avoids inconsistent retries, endpoints, and observability labels.

If you reuse parts of `DefaultEngine`, make the ownership boundary visible in the Engine's
constructor. Callers should configure the connector once and receive an Engine ready for Kernel
operations.

## Validate through public workflows

Handler unit tests catch local contract violations. Add integration tests that exercise the Engine
through public Kernel APIs:

- load the latest snapshot and a specific historical version;
- scan projected columns with supported and unsupported predicates;
- read files that produce more than one `EngineData` batch;
- apply partition values, column mapping, and deletion vectors;
- write and reload a commit; and
- cancel work at each supported I/O boundary.

For cloud storage, run the write path against S3, Azure, and GCS. Object stores differ in conditional
writes, listing behavior, and error mapping even when one client library abstracts them.

## What's next

- [The EngineData trait](engine_data.md) explains row visitors and selection vectors.
- [Building a scan](../reading/building_a_scan.md) uses the Engine on the read path.
- [Configuring storage](../storage/configuring_storage.md) covers the default engine's backends.
