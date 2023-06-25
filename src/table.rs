use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::ObjectStore;

use crate::error::Error;
use crate::snapshot::Snapshot;
use crate::{version_from_path, DeltaResult, Version};

/// In-memory representation of a Delta table, which acts as an immutable root entity for reading
/// the different versions (see [Snapshot]) of the table located in storage.
///
/// A [Table] holds a reference to an [ObjectStore] which for example, can point to an S3 bukcet
/// where multiple Delta tables are located. The [Path] then is used to identify a specific table
/// within that storage.
///
/// ```rust
/// # use object_store::memory::InMemory;
/// # use object_store::path::Path;
/// # use std::sync::Arc;
/// # use deltakernel::{DeltaResult, Table};
/// # fn main() -> DeltaResult<()> {
/// // Consult the `object_store` crate documentation for constructing AmazonS3, MicrosoftAzure,
/// // etc `ObjectStore` implementations
/// let store = Arc::new(InMemory::new());
///
/// let access_logs = Table::with_store(store.clone())
///                             .at("logs/nginx_access")
///                             .build()?;
/// let error_logs = Table::with_store(store.clone())
///                             .at("logs/nginx_error")
///                             .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Table {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl Table {
    /// Create a new table builder with the given store
    pub fn with_store(store: Arc<dyn ObjectStore>) -> TableBuilder {
        TableBuilder::default().with_store(store)
    }

    /// Create a new table builder with the given path
    pub fn at(path: Path) -> TableBuilder {
        TableBuilder::default().at(path)
    }

    /// Retrieve the configured relative path for the table
    pub fn get_path(&self) -> &Path {
        &self.path
    }

    /// Create a new Delta table with the given parameters
    fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self { store, path }
    }

    /// Get the latest snapshot of the table. Requires a storage client to read table metadata.
    pub async fn get_latest_snapshot(&self) -> DeltaResult<Snapshot> {
        Snapshot::try_new(
            self.path.clone(),
            self.get_latest_version()
                .await?
                .ok_or(Error::MissingVersion)?,
            self.store.clone(),
        )
        .await
    }

    /// get a snapshot corresponding to `version` for the table. Requires a storage client to read
    /// table metadata.
    pub async fn get_snapshot(&self, version: Version) -> DeltaResult<Snapshot> {
        Snapshot::try_new(self.path.clone(), version, self.store.clone()).await
    }

    // FIXME listing _delta_log should likely be moved to delta_log module?
    async fn get_latest_version(&self) -> DeltaResult<Option<Version>> {
        // NOTE duplicate work: we list files here but we could keep them for use in the snapshot.
        let path = self.path.child("_delta_log");
        // XXX: Error handling on list
        let list_stream = self.store.list(Some(&path)).await?;
        let mut paths = list_stream
            .map_ok(|res| res.location)
            .try_collect::<Vec<_>>()
            .await?;
        paths.sort();

        // XXX: This is not handling checkpoint files efficiently
        if let Some(last_commit) = paths.iter().rev().find(|p| p.extension() == Some("json")) {
            Ok(Some(version_from_path(last_commit)?))
        } else {
            Ok(None)
        }
    }
}

/// The DeltaTableBuilder provides a fluid interface for constructing new and validated
/// [DeltaTable] instances.
///
/// ```rust
/// # use object_store::memory::InMemory;
/// # use object_store::path::Path;
/// # use std::sync::Arc;
/// # use deltakernel::Table;
/// let store = Arc::new(InMemory::new());
/// let relative_table_path = "path/to/table";
/// let result = Table::with_store(store).at(relative_table_path).build();
/// let table = result.expect("Failed to build a table");
/// ```
#[derive(Clone, Debug, Default)]
pub struct TableBuilder {
    store: Option<Arc<dyn ObjectStore>>,
    path: Option<Path>,
}

impl TableBuilder {
    /// Add the given [ObjectStore] to the builder
    ///
    /// This can be called repeatedly but only the last invocation applies
    pub fn with_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Add the given [Path] compatible object as a path
    ///
    /// This can be called repeatedly but only the last invocation applies
    pub fn at(mut self, path: impl Into<Path>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Attempt to construct the [Table] with the configured options
    ///
    /// This will return errors if the table cannot be built correctly
    pub fn build(self) -> DeltaResult<Table> {
        if self.store.is_none() {
            return Err(Error::Generic("Cannot build table without a store".into()));
        }

        if self.path.is_none() {
            return Err(Error::Generic("Cannot build table without a path".into()));
        }
        Ok(Table::new(self.store.unwrap().clone(), self.path.unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[test]
    fn construct_simple_delta_table() {
        let store = Arc::new(InMemory::new());
        let _ = Table::new(store, "/tmp/delta-table".into());
    }

    #[test]
    fn build_invalid_table_withoout_store() {
        let result = TableBuilder::default().build();
        assert!(result.is_err());
    }

    #[test]
    fn build_invalid_table_without_path() {
        let store = Arc::new(InMemory::new());
        let result = Table::with_store(store).build();
        assert!(result.is_err());
    }
}
