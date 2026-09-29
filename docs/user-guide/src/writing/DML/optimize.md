# OPTIMIZE

`OPTIMIZE` rewrites existing data into a different file layout, such as combining small files into
larger ones. Your connector chooses the source files and produces the replacement files. Kernel
commits the replacements and removals together.

Before reading this page, make sure you understand [Appending data](./append.md) and
[Removing data](./remove.md).

Use `with_data_change(false)` because an `OPTIMIZE` transaction rearranges data without changing
the table's rows.

## OPTIMIZE without row tracking

To rewrite files on a partitioned table without row tracking enabled:

1. Load a `Snapshot` and use it for both your scan and your transaction.
2. Use `scan_metadata()` to select source files, retaining their metadata for `remove_files()`.
   Read all live rows from those files, including duplicates. Apply the scan's selection vectors,
   transforms, and deletion vectors (which mark deleted rows) so you don't restore deleted data.
3. Create a transaction with `with_data_change(false)` and obtain its write state.
4. Group rows by partition and bind a write context for each partition as described in
   [Writing to partitioned tables](../partitioned_writes.md).
   Write replacement files containing exactly the selected files' live rows.
5. Register replacement file metadata with `add_files()` and selected source file metadata with
   `remove_files()`. Commit both in the same transaction.

The following pseudocode uses `DefaultEngine` for writes. You implement the `connector_*`
functions to select files, read their live rows, and regroup those rows into replacement files.
The hidden definitions are placeholders so the Kernel API calls can be compile-checked.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::collections::HashMap;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::arrow_data::ArrowEngineData;
# use delta_kernel::expressions::Scalar;
# use delta_kernel::scan::{Scan, ScanMetadata};
# use delta_kernel::transaction::CommitResult;
# use delta_kernel::{DeltaResult, Engine, Error, Snapshot};
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# fn connector_select_files(metadata: ScanMetadata) -> DeltaResult<ScanMetadata> {
#     Err(Error::unsupported("Implement file selection in your connector"))
# }
# fn connector_read_live_rows(scan: &Scan, files: &[ScanMetadata], engine: &dyn Engine)
#     -> DeltaResult<Vec<ArrowEngineData>> {
#     Err(Error::unsupported("Implement source-file reads in your connector"))
# }
# fn connector_regroup_by_partition(rows: Vec<ArrowEngineData>, partition_columns: &[String])
#     -> DeltaResult<Vec<(HashMap<String, Scalar>, Vec<ArrowEngineData>)>> {
#     Err(Error::unsupported("Implement partition grouping in your connector"))
# }
# async fn optimize() -> DeltaResult<CommitResult> {
# let url = delta_kernel::try_parse_uri("/tmp/partitioned_table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
let snapshot = Snapshot::builder_for(url).build(&engine)?;
let scan = snapshot.clone().scan_builder().build()?;

// Connector-defined: choose source files by narrowing the scan's selection vectors.
let source_files = scan
    .scan_metadata(&engine)?
    .map(|metadata| connector_select_files(metadata?))
    .collect::<DeltaResult<Vec<_>>>()?;

// Connector-defined: read only selected files; apply transforms and deletion vectors.
let rows = connector_read_live_rows(&scan, &source_files, &engine)?;

let mut txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_operation("OPTIMIZE".to_string())
    .with_data_change(false);
let write_state = txn.write_state()?;

// Connector-defined: group by typed partition values, preserving every live row.
// Remove partition columns from the output batches to match logical_data_schema().
let partitions = connector_regroup_by_partition(rows, txn.logical_partition_columns())?;
for (partition_values, batches) in partitions {
    let write_context = write_state
        .write_context_builder()
        .with_partition_values(partition_values)
        .build()?;

    for batch in batches {
        let file_metadata = engine.write_parquet(&batch, &write_context).await?;
        txn.add_files(file_metadata);
    }
}

for metadata in source_files {
    txn.remove_files(metadata.scan_files);
}
let result = txn.commit(&engine)?;
# Ok(result)
# }
```

Don't mark the transaction as a blind append: it depends on existing files and removes them.
Handle the [commit result](./append.md#committing); if another writer conflicts, load a new
snapshot and re-evaluate the rewrite before committing again.

Don't split the additions and removals into separate transactions: readers could observe
duplicated or missing rows between commits.
Other feature restrictions still apply. For example, Kernel rejects file removals when
`icebergCompatV3` is enabled.

## OPTIMIZE with row tracking

**Row tracking** associates each row with a stable identifier and the commit version that last
inserted or updated it. When `delta.enableRowTracking = true`, your rewrite must preserve both
values for every copied row. The steps below mark the additions to the flow without row tracking:

1. Load a `Snapshot` and use it for both your scan and your transaction.
2. Select source files with `scan_metadata()` and read their live rows as above.
   **Row tracking addition:** request the `MetadataColumnSpec::RowId` and
   `MetadataColumnSpec::RowCommitVersion`
   [metadata columns](../../reading/column_selection.md#metadata-columns) in the scan schema.
   Use the resolved values from the scan transforms.
3. Create a transaction with `with_data_change(false)` and obtain its write state.
4. Group rows by partition and write replacement files.
   **Row tracking addition:** keep each row's ID and commit version attached when regrouping or
   sorting, and bind both columns with `with_row_tracking_columns(...)` in each write context.
5. Register additions and removals, then commit them together.
   **Row tracking addition:** call `ack_row_tracking_preservation()` before committing.

This pseudocode uses the same connector-defined helpers. The `row_id` and `row_commit_version`
names are aliases you choose for the metadata columns; they must not conflict with data columns.
Comments beginning with `Row tracking:` mark the additional requirements.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::collections::HashMap;
# use std::sync::Arc;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::arrow_data::ArrowEngineData;
# use delta_kernel::expressions::Scalar;
# use delta_kernel::scan::{Scan, ScanMetadata};
use delta_kernel::schema::MetadataColumnSpec;
use delta_kernel::transaction::RowTrackingMetadataColumns;
# use delta_kernel::transaction::CommitResult;
# use delta_kernel::{DeltaResult, Engine, Error, Snapshot};
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# fn connector_select_files(metadata: ScanMetadata) -> DeltaResult<ScanMetadata> {
#     Err(Error::unsupported("Implement file selection in your connector"))
# }
# fn connector_read_live_rows(scan: &Scan, files: &[ScanMetadata], engine: &dyn Engine)
#     -> DeltaResult<Vec<ArrowEngineData>> {
#     Err(Error::unsupported("Implement source-file reads in your connector"))
# }
# fn connector_regroup_by_partition(rows: Vec<ArrowEngineData>, partition_columns: &[String])
#     -> DeltaResult<Vec<(HashMap<String, Scalar>, Vec<ArrowEngineData>)>> {
#     Err(Error::unsupported("Implement partition grouping in your connector"))
# }
# async fn optimize() -> DeltaResult<CommitResult> {
# let url = delta_kernel::try_parse_uri("/tmp/partitioned_table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// Row tracking: request both stable metadata values alongside the table's data columns.
let schema = snapshot
    .schema()
    .add_metadata_column("row_id", MetadataColumnSpec::RowId)?
    .add_metadata_column("row_commit_version", MetadataColumnSpec::RowCommitVersion)?;

let scan = snapshot
    .clone()
    .scan_builder()
    .with_schema(Arc::new(schema))
    .build()?;

// Connector-defined: choose source files by narrowing the scan's selection vectors.
let source_files = scan
    .scan_metadata(&engine)?
    .map(|metadata| connector_select_files(metadata?))
    .collect::<DeltaResult<Vec<_>>>()?;

// Connector-defined: read only selected files; apply transforms and deletion vectors.
// Row tracking: use the resolved IDs and commit versions, not new row numbers.
let rows = connector_read_live_rows(&scan, &source_files, &engine)?;

let mut txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_operation("OPTIMIZE".to_string())
    .with_data_change(false);
let write_state = txn.write_state()?;

// Connector-defined: group by typed partition values and remove partition columns.
// Row tracking: keep row_id and row_commit_version attached to their original rows.
let partitions = connector_regroup_by_partition(rows, txn.logical_partition_columns())?;
for (partition_values, batches) in partitions {
    let write_context = write_state
        .write_context_builder()
        .with_partition_values(partition_values)
        // Row tracking: write both preserved values into the replacement files.
        .with_row_tracking_columns(RowTrackingMetadataColumns {
            row_id_col_name: Some("row_id"),
            row_commit_version_col_name: Some("row_commit_version"),
        })
        .build()?;

    for batch in batches {
        let file_metadata = engine.write_parquet(&batch, &write_context).await?;
        txn.add_files(file_metadata);
    }
}

for metadata in source_files {
    txn.remove_files(metadata.scan_files);
}
// Row tracking: acknowledge preservation before committing.
txn.ack_row_tracking_preservation();
let result = txn.commit(&engine)?;
# Ok(result)
# }
```

Your input must match `write_context.logical_data_schema()`. Kernel maps the supplied metadata
columns to the table's configured physical column names. Writing these values into replacement
Parquet files **materializes** them so they survive a change in file layout.

Kernel rejects removals and deletion-vector updates on a row-tracking-enabled table without this
acknowledgment. It doesn't inspect the rewritten rows to verify preservation. The acknowledgment
is a promise by your connector, not a request for Kernel to populate the columns. See the
[row tracking writer requirements](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-row-tracking)
for the complete contract.

## What's next

- [Advanced reads with scan_metadata()](../../reading/scan_metadata.md) explains the file metadata,
  deletion vectors, and transforms used when reading source files.
- [Writing to partitioned tables](../partitioned_writes.md) explains how to bind partition values
  for replacement files.
