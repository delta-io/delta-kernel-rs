# Removing data

To remove data files from an existing Delta table, you scan the table's file
metadata, select which files to remove, and commit those removals as a
[Transaction](../concepts/architecture.md#transaction). Kernel tracks removes at
file-level granularity. Each removed file produces a **remove action** in the
Delta **transaction log**.

Before reading this page, make sure you understand
[Appending Data](./append.md) and
[Advanced Reads with scan_metadata()](../reading/scan_metadata.md).

## The remove flow

Removing files follows these steps:

1. Get a `Snapshot` of the table
2. Create a `Transaction` from the snapshot
3. Build a `Scan` and call `scan_metadata()` to get file-level metadata
4. Modify the selection vector to mark files for removal
5. Pass the modified `FilteredEngineData` to `Transaction::remove_files()`
6. Commit the transaction

```text
Snapshot ──> Transaction
                │
Snapshot ──> Scan ──> scan_metadata()
                          │
                    ScanMetadata { scan_files: FilteredEngineData, ... }
                          │
                    Modify selection vector
                          │
                    txn.remove_files(modified_scan_files)
                          │
                    txn.commit(engine)
```

## Getting file metadata with scan_metadata()

`Scan::scan_metadata()` returns an iterator of `ScanMetadata`. Each
`ScanMetadata` contains a `scan_files` field of type `FilteredEngineData`.
This `FilteredEngineData` holds one row per file in the table, along with a
selection vector that indicates which rows (files) are active.

The underlying data conforms to the schema returned by `scan_row_schema()`:

```text
{
   path: string,
   size: long,
   modificationTime: long,
   stats: string,
   deletionVector: {
     storageType: string,
     pathOrInlineDv: string,
     offset: int,
     sizeInBytes: int,
     cardinality: long,
   },
   fileConstantValues: {
     partitionValues: map<string, string>,
     tags: map<string, string>,
     baseRowId: long,
     defaultRowCommitVersion: long,
     clusteringProvider: string,
   }
}
```

You don't need to construct this data yourself. The scan produces it for you.
For full details on working with `scan_metadata()`, see
[Advanced Reads with scan_metadata()](../reading/scan_metadata.md).

> [!NOTE]
> If your scan was built with a partition predicate (`Scan::with_predicate`), the
> scan-row schema also includes a `partitionValues_parsed` field. Kernel drops that
> field internally when transforming the rows for `remove_files()`, so you don't need
> to handle it.

## Selecting files for removal

`FilteredEngineData` pairs engine data with a boolean selection vector. Each
`true` entry marks a row (file) as selected. When you pass a
`FilteredEngineData` to `remove_files()`, Kernel removes every file whose
corresponding selection vector entry is `true`.

To control which files are removed, decompose the `FilteredEngineData` with
`into_parts()`, modify the selection vector, and reassemble with
`FilteredEngineData::try_new()`:

```rust,ignore
let (data, mut selection_vector) = scan_files.into_parts();

// Example: deselect all files, then select only the ones you want to remove
for entry in selection_vector.iter_mut() {
    *entry = false;
}
// Mark specific files for removal (e.g., based on your own logic)
selection_vector[0] = true;

let files_to_remove = FilteredEngineData::try_new(data, selection_vector)?;
```

## Calling remove_files()

Once you have a `FilteredEngineData` with the correct selection vector, pass
it to the transaction:

```rust,ignore
txn.remove_files(files_to_remove);
```

The signature is:

```rust,ignore
pub fn remove_files(&mut self, remove_metadata: FilteredEngineData)
```

You can call `remove_files()` multiple times to remove files from different
`scan_metadata()` batches. Each call appends to the transaction's list of
pending removals.

> [!NOTE]
> `remove_files()` is available on transaction states that produce data files (gated by
> the `SupportsDataFiles` trait bound). Metadata-only transaction states cannot register
> file removals.

## Full example

This example removes the first file from a filesystem-backed table:

```rust,no_run
# extern crate delta_kernel;
# extern crate tokio;
# use std::sync::Arc;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::engine_data::FilteredEngineData;
# use delta_kernel::transaction::CommitResult;
# use delta_kernel::{DeltaResult, Snapshot};
# #[tokio::main]
# async fn main() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/my_table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
// 1. Get a snapshot
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// 2. Create a transaction
let mut txn = snapshot
    .clone()
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_operation("DELETE".to_string());

// 3. Build a scan and get file metadata
let scan = snapshot.scan_builder().build()?;

for metadata in scan.scan_metadata(&engine)? {
    let metadata = metadata?;

    // 4. Modify the selection vector to pick files for removal
    let (data, mut selection_vector) = metadata.scan_files.into_parts();

    // Deselect everything, then select only the first file
    for entry in selection_vector.iter_mut() {
        *entry = false;
    }
    if !selection_vector.is_empty() {
        selection_vector[0] = true;
    }

    let files_to_remove = FilteredEngineData::try_new(data, selection_vector)?;

    // 5. Register the files for removal
    txn.remove_files(files_to_remove);
}

// 6. Commit the transaction
match txn.commit(&engine)? {
    CommitResult::CommittedTransaction(committed) => {
        println!("Committed version {}", committed.commit_version());
    }
    _ => eprintln!("commit did not succeed"),
}
# Ok(())
# }
```

> [!TIP]
> In practice, you would inspect file statistics or partition values to decide
> which files to remove rather than selecting by index. Use a visitor on the
> `FilteredEngineData` to read per-file metadata before modifying the
> selection vector.

## Change Data Feed restriction

> [!WARNING]
> If the table has Change Data Feed enabled
> (`delta.enableChangeDataFeed = true`), you cannot add _and_ remove files in
> the same transaction. Kernel does not yet support writing the CDC files that
> Delta requires for DML operations that both add and remove data. If you
> attempt this, `commit()` returns an error. Use separate transactions: one to
> add files, another to remove files.

## Blind appends and remove_files()

A transaction marked with `with_blind_append()` cannot remove files. Blind
appends are optimized for the append-only case and reject any removals at
commit time. If your transaction removes files, do not call
`with_blind_append()`.

## What's next

- [Idempotent Writes](./idempotent_writes.md) covers how to make writes
  safely repeatable.
- [Domain Metadata](./domain_metadata.md) explains how to attach custom
  metadata to a commit.
- [Advanced Reads with scan_metadata()](../reading/scan_metadata.md) covers
  the full `scan_metadata()` API used to obtain `FilteredEngineData`.
