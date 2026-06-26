# Domain metadata

To store application-specific configuration alongside a Delta table, you use
**domain metadata**. Each domain is a named key-value pair persisted in the
Delta transaction log. Your connector can write, read, and remove domain
metadata through the Kernel API without affecting table data.

Domain metadata is useful for tracking connector-specific state, feature
flags, or custom configuration that should travel with the table and survive
across sessions.

Before reading this page, make sure you understand
[Appending Data](./append.md) and how transactions work.

## Writing domain metadata

To attach domain metadata to a commit, call `with_domain_metadata()` on the
transaction. This method is available on both create-table and existing-table
transactions.

```rust,no_run
# extern crate delta_kernel;
# extern crate tokio;
# use std::sync::Arc;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# #[tokio::main]
# async fn main() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_domain_metadata(
        "myConnector.settings".to_string(),
        r#"{"version": 1, "compress": true}"#.to_string(),
    )
    .with_operation("UPDATE METADATA".to_string());

txn.commit(&engine)?;
# Ok(())
# }
```

The `with_domain_metadata` signature takes two `String` arguments:

```rust,ignore
pub fn with_domain_metadata(self, domain: String, configuration: String) -> Self
```

The `domain` identifies the metadata namespace and `configuration` holds the
value. The configuration is an opaque string. You can store JSON, plain text,
or any format your connector understands.

You can set metadata for multiple distinct domains in the same transaction by
calling `with_domain_metadata` more than once with different domain names.

## Removing domain metadata

To remove a domain from an existing table, call `with_domain_metadata_removed()`
on an existing-table transaction. This method is not available on create-table
transactions because there is no metadata to remove from a table that does not
exist yet.

```rust,no_run
# extern crate delta_kernel;
# extern crate tokio;
# use std::sync::Arc;
# use delta_kernel::committer::FileSystemCommitter;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# #[tokio::main]
# async fn main() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
# let snapshot = Snapshot::builder_for(url).build(&engine)?;
let txn = snapshot
    .transaction(Box::new(FileSystemCommitter::new()), &engine)?
    .with_domain_metadata_removed("myConnector.settings".to_string())
    .with_operation("REMOVE METADATA".to_string());

txn.commit(&engine)?;
# Ok(())
# }
```

If the domain does not exist in the log, the removal is a no-op. Kernel
handles this gracefully during commit.

## Reading domain metadata

To read domain metadata from a table, call `get_domain_metadata()` on a
`Snapshot`. 

> [!TIP]
> Reading domain data from a snapshot is efficient if a CRC file was present when loading
> the snapshot. Otherwise determining domain metadata requires performing a log replay.
> Connectors that heavily rely on `get_domain_metadata()` should ensure checksum CRC files are
> written with each commit.


```rust,no_run
# extern crate delta_kernel;
# extern crate tokio;
# use std::sync::Arc;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# #[tokio::main]
# async fn main() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
let snapshot = Snapshot::builder_for(url).build(&engine)?;

// Returns Ok(Some(config)) if the domain exists, Ok(None) if it does not
let config: Option<String> = snapshot.get_domain_metadata("myConnector.settings", &engine)?;

match config {
    Some(value) => println!("Domain config: {value}"),
    None => println!("No metadata found for this domain"),
}
# Ok(())
# }
```

> [!WARNING]
> `get_domain_metadata()` rejects domain names that start with `delta.` and
> returns an error. The `delta.*` namespace is reserved for Kernel's internal
> use (row tracking, clustering). You can only read user-defined domains
> through this API.

## Constraints and validation

Kernel validates domain metadata operations at commit time. The following rules
apply:

- **One domain per transaction.** Each domain name can appear at most once per
  transaction. You cannot set and remove the same domain in a single commit,
  and you cannot set the same domain twice. If you include a duplicate domain,
  the commit fails with an error.

- **Reserved prefix.** Domain names starting with `delta.` are reserved for
  Kernel's internal use (e.g., clustering metadata). Attempting to read, write,
  or remove a `delta.` domain through the public API returns an error.

- **Feature requirement.** Domain metadata operations require the
  `domainMetadata` writer feature to be enabled on the table (writer version 7).
  If the feature is not enabled, the commit fails.

- **No removals on create-table.** The `with_domain_metadata_removed()` method
  is only available on existing-table transactions. The Rust type system
  prevents calling it on a create-table transaction at compile time.

- **Multiple domains allowed.** Although each domain can appear only once, you
  can set or remove metadata for multiple _distinct_ domains in the same
  transaction.

> [!NOTE]
> Validation is deferred until `commit()`. The builder methods
> `with_domain_metadata()` and `with_domain_metadata_removed()` do not check
> for duplicates or reserved prefixes eagerly. Errors surface when you call
> `commit()`.

## What's next

- [Idempotent Writes](./idempotent_writes.md) covers `SetTransaction` actions
  for at-most-once write guarantees
- [Appending Data](./append.md) explains the full write flow for adding data
  files to a table
- [Creating a Table](./create_table.md) shows how to set domain metadata during
  table creation
