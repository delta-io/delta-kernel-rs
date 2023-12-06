use std::sync::Arc;

use url::Url;

use crate::snapshot::Snapshot;
use crate::{DeltaResult, TableClient, Version};

/// In-memory representation of a Delta table, which acts as an immutable root entity for reading
/// the different versions (see [`Snapshot`]) of the table located in storage.
#[derive(Clone)]
pub struct Table<JRC: Send, PRC: Send + Sync> {
    table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
    location: Url,
}

impl<JRC: Send, PRC: Send + Sync> std::fmt::Debug for Table<JRC, PRC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Table")
            .field("location", &self.location)
            .finish()
    }
}

impl<JRC: Send, PRC: Send + Sync> Table<JRC, PRC> {
    /// Create a new Delta table with the given parameters
    pub fn new(
        location: Url,
        table_client: Arc<dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>>,
    ) -> Self {
        Self {
            location,
            table_client,
        }
    }

    /// Fully qualified location of the Delta table.
    pub fn location(&self) -> &Url {
        &self.location
    }

    /// Create a [`Snapshot`] of the table corresponding to `version`.
    ///
    /// If no version is supplied, a snapshot for the latest version will be created.
    pub fn snapshot(&self, version: Option<Version>) -> DeltaResult<Arc<Snapshot<JRC, PRC>>> {
        Snapshot::try_new(self.location.clone(), self.table_client.clone(), version)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::*;
    use crate::client::DefaultTableClient;
    use crate::executor::tokio::TokioBackgroundExecutor;

    #[test]
    fn test_table() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let table_client = Arc::new(
            DefaultTableClient::try_new(
                &url,
                HashMap::<String, String>::new(),
                Arc::new(TokioBackgroundExecutor::new()),
            )
            .unwrap(),
        );

        let table = Table::new(url, table_client);
        let snapshot = table.snapshot(None).unwrap();
        assert_eq!(snapshot.version(), 1)
    }
}
