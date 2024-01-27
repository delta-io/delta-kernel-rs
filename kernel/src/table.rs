use std::sync::Arc;

use url::Url;

use crate::snapshot::Snapshot;
use crate::{DeltaResult, EngineClient, Version};

/// In-memory representation of a Delta table, which acts as an immutable root entity for reading
/// the different versions (see [`Snapshot`]) of the table located in storage.
#[derive(Clone)]
pub struct Table {
    location: Url,
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Table")
            .field("location", &self.location)
            .finish()
    }
}

impl Table {
    /// Create a new Delta table with the given parameters
    pub fn new(location: Url) -> Self {
        Self { location }
    }

    /// Fully qualified location of the Delta table.
    pub fn location(&self) -> &Url {
        &self.location
    }

    /// Create a [`Snapshot`] of the table corresponding to `version`.
    ///
    /// If no version is supplied, a snapshot for the latest version will be created.
    pub fn snapshot(
        &self,
        engine_client: &dyn EngineClient,
        version: Option<Version>,
    ) -> DeltaResult<Arc<Snapshot>> {
        Snapshot::try_new(self.location.clone(), engine_client, version)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::simple_client::SimpleClient;

    #[test]
    fn test_table() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine_client = SimpleClient::new();
        let table = Table::new(url);
        let snapshot = table.snapshot(&engine_client, None).unwrap();
        assert_eq!(snapshot.version(), 1)
    }
}
