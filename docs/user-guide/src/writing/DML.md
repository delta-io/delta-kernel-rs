# Data manipulation (DML)

**Data manipulation language (DML)** describes operations that insert, update, or delete table
rows, such as `INSERT`, `UPDATE`, `DELETE`, and `MERGE`. File-rewrite operations such as `OPTIMIZE`
use the same write APIs, but reorganize existing rows without changing the table's logical contents.

Before reading this page, make sure you understand [Appending data](./append.md) and
[Removing data](./removing_files.md).

Kernel provides APIs to read rows, write replacement files, and commit file additions and removals.
It doesn't execute SQL commands or implement all DML semantics. `OPTIMIZE` is the rewrite operation
covered here. Other DML operations and their interactions with table features aren't fully covered
by Kernel's write validation. If you implement them, follow the
[Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) requirements for every
feature enabled on the table. A successful commit doesn't prove that your operation preserves those
requirements.

For example, **Change Data Feed (CDF)** exposes row-level changes between table versions. Kernel
doesn't support writing change data files. On a CDF-enabled table, it rejects data-changing
transactions that combine file additions with removals or deletion-vector updates. Setting
`with_operation("MERGE".to_string())` records an operation name; it doesn't implement `MERGE` or
enable otherwise unsupported writes.

## OPTIMIZE

`OPTIMIZE` rewrites existing data into a different file layout, such as combining small files into
larger ones. Your connector chooses the source files and produces the replacement files. Kernel
commits the replacements and removals together.

Use `with_data_change(false)` because an `OPTIMIZE` transaction preserves all live rows, including
duplicate rows. Kernel applies this flag to the added and removed files. It doesn't compare the
old and new rows for you. A **deletion vector** marks rows in a file as deleted; exclude those rows
when producing replacements so that the rewrite doesn't restore deleted data.

> [!WARNING]
> Don't use `with_data_change(false)` for an operation that changes row values or adds or deletes
> live rows. Readers such as CDF consumers can skip these actions and miss the changes.

### OPTIMIZE without row tracking

To rewrite files on a table without row tracking enabled:

1. Load a `Snapshot` and use it for both your scan and your transaction.
2. Use `scan_metadata()` to select source files, retaining their metadata for `remove_files()`.
   Read all live rows from those files, applying the scan's selection vectors and transforms.
3. Create a transaction with `with_data_change(false)` and obtain its write context.
4. Write replacement files containing exactly those rows. For a partitioned table, group rows by
   partition and bind a write context for each partition as described in
   [Writing to partitioned tables](./partitioned_writes.md).
5. Register replacement file metadata with `add_files()` and selected source file metadata with
   `remove_files()`. Commit both in the same transaction.

For an unpartitioned table, the transaction setup is:

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::{DeltaResult, Engine, SnapshotRef};
# fn prepare_optimize(snapshot: SnapshotRef, engine: &dyn Engine) -> DeltaResult<()> {
let mut txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), engine)?
    .with_operation("OPTIMIZE".to_string())
    .with_data_change(false);

let write_context = txn.write_state()?.write_context_builder().build()?;
# Ok(())
# }
```

Use the [Parquet writing flow](./append.md#writing-parquet-files) with this context. Don't mark the
transaction as a blind append: it depends on existing files and removes them. Handle the
[commit result](./append.md#committing); if another writer conflicts, load a new snapshot and
re-evaluate the rewrite before committing again.

CDF-enabled tables allow this non-data-changing rewrite. Don't split the additions and removals
into separate transactions: readers could observe duplicated or missing rows between commits.
Other feature restrictions still apply. For example, Kernel rejects file removals when
`icebergCompatV3` is enabled.

### OPTIMIZE with row tracking

**Row tracking** associates each row with a stable identifier and the commit version that last
inserted or updated it. When `delta.enableRowTracking = true`, your rewrite must preserve both
values for every copied row. Follow the same `OPTIMIZE` flow above, with the additional read and
write steps below.

Request both [metadata columns](../reading/column_selection.md#metadata-columns) in the scan schema.
Choose names that don't conflict with your table's data columns:

```rust,no_run
# extern crate delta_kernel;
# use std::sync::Arc;
use delta_kernel::schema::MetadataColumnSpec;
# use delta_kernel::{DeltaResult, SnapshotRef};
# fn build_optimize_scan(snapshot: SnapshotRef) -> DeltaResult<()> {
let schema = snapshot
    .schema()
    .add_metadata_column("row_id", MetadataColumnSpec::RowId)?
    .add_metadata_column("row_commit_version", MetadataColumnSpec::RowCommitVersion)?;

let scan = snapshot
    .scan_builder()
    .with_schema(Arc::new(schema))
    .build()?;
# Ok(())
# }
```

Use the resolved values produced by the scan, not new row numbers computed after filtering or
reordering. If you read files through your own execution path, apply the
[scan transforms and deletion vectors](../reading/scan_metadata.md) to obtain those values.
Keep both metadata columns attached to their rows while combining or sorting the data.

Configure the write context to include the preserved values in the replacement files:

```rust,no_run
# extern crate delta_kernel;
use delta_kernel::transaction::RowTrackingMetadataColumns;
# use delta_kernel::transaction::Transaction;
# use delta_kernel::DeltaResult;
# fn configure_row_tracking_write(txn: &Transaction) -> DeltaResult<()> {
let write_context = txn
    .write_state()?
    .write_context_builder()
    .with_row_tracking_columns(RowTrackingMetadataColumns {
        row_id_col_name: Some("row_id"),
        row_commit_version_col_name: Some("row_commit_version"),
    })
    .build()?;
# Ok(())
# }
```

This example assumes an unpartitioned table. For partitioned tables, also supply
`with_partition_values(...)`. Your input must match `write_context.logical_data_schema()`.
Kernel maps the supplied metadata columns to the table's configured physical column names.
Writing these values into the replacement Parquet files **materializes** them so they survive a
change in file layout. Kernel still assigns the new files' default row-tracking metadata; those
defaults don't replace your responsibility to preserve the existing stable values.

Before committing the additions and removals, acknowledge that your connector implements this
preservation:

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::transaction::Transaction;
# fn acknowledge_preservation(txn: &mut Transaction) {
txn.ack_row_tracking_preservation();
# }
```

Kernel rejects removals and deletion-vector updates on a row-tracking-enabled table without this
acknowledgment. It doesn't inspect the rewritten rows to verify preservation. The acknowledgment
is a promise by your connector, not a request for Kernel to populate the columns. See the
[row tracking writer requirements](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-row-tracking)
for the complete contract.

## What's next

- [Advanced reads with scan_metadata()](../reading/scan_metadata.md) explains the file metadata,
  deletion vectors, and transforms used when reading source files.
- [Writing to partitioned tables](./partitioned_writes.md) explains how to bind partition values
  for replacement files.
