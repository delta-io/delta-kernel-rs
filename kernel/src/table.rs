//! In-memory representation of a Delta table, which acts as an immutable root entity for reading
//! the different versions

use std::borrow::Cow;
use std::ops::Deref;
use std::path::PathBuf;

use url::Url;

use crate::checkpoint::CheckpointBuilder;
use crate::snapshot::Snapshot;
use crate::table_changes::TableChanges;
use crate::transaction::Transaction;
use crate::{DeltaResult, Engine, Error, Version};

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

    /// Try to create a new table from a string uri. This will do it's best to handle things like
    /// `/local/paths`, and even `../relative/paths`.
    pub fn try_from_uri(uri: impl AsRef<str>) -> DeltaResult<Self> {
        let uri = uri.as_ref();
        let uri_type = resolve_uri_type(uri)?;
        let url = match uri_type {
            UriType::LocalPath(path) => {
                if !path.exists() {
                    // When we support writes, create a directory if we can
                    return Err(Error::InvalidTableLocation(format!(
                        "Path does not exist: {path:?}"
                    )));
                }
                if !path.is_dir() {
                    return Err(Error::InvalidTableLocation(format!(
                        "{path:?} is not a directory"
                    )));
                }
                let path = std::fs::canonicalize(path).map_err(|err| {
                    let msg = format!("Invalid table location: {} Error: {:?}", uri, err);
                    Error::InvalidTableLocation(msg)
                })?;
                Url::from_directory_path(path.clone()).map_err(|_| {
                    let msg = format!(
                        "Could not construct a URL from canonicalized path: {:?}.\n\
                         Something must be very wrong with the table path.",
                        path
                    );
                    Error::InvalidTableLocation(msg)
                })?
            }
            UriType::Url(url) => url,
        };
        Ok(Self::new(url))
    }

    /// Fully qualified location of the Delta table.
    pub fn location(&self) -> &Url {
        &self.location
    }

    /// Create a [`Snapshot`] of the table corresponding to `version`.
    ///
    /// If no version is supplied, a snapshot for the latest version will be created.
    pub fn snapshot(&self, engine: &dyn Engine, version: Option<Version>) -> DeltaResult<Snapshot> {
        Snapshot::try_new(self.location.clone(), engine, version)
    }

    /// Create a [`TableChanges`] to get a change data feed for the table between `start_version`,
    /// and `end_version`. If no `end_version` is supplied, the latest version will be used as the
    /// `end_version`.
    pub fn table_changes(
        &self,
        engine: &dyn Engine,
        start_version: Version,
        end_version: impl Into<Option<Version>>,
    ) -> DeltaResult<TableChanges> {
        TableChanges::try_new(
            self.location.clone(),
            engine,
            start_version,
            end_version.into(),
        )
    }

    /// Creates a [`CheckpointBuilder`] for generating table checkpoints.
    ///
    /// Checkpoints are compact representations of the table state that improve reading performance
    /// by providing a consolidated view without requiring full log replay.
    ///
    /// # Checkpoint Types
    ///
    /// The type of checkpoint created depends on table features and builder configuration:
    ///
    /// 1. Classic V1 Checkpoint: Created automatically for tables without v2Checkpoints feature support.
    ///    - Uses classic naming format (`<version>.checkpoint.parquet`)
    ///    - Created regardless of `with_classic_naming` setting
    ///
    /// 2. Classic V2 Checkpoint* Created when tables support v2Checkpoints feature AND
    ///    `with_classic_naming(true)` is specified.
    ///    - Uses classic naming format (`<version>.checkpoint.parquet`)
    ///    - Includes additional V2 metadata
    ///
    /// 3. **UUID V2 Checkpoint**: Created when tables support v2Checkpoints feature AND
    ///    `with_classic_naming(false)` is used (default).
    ///    - Uses UUID naming format (`<version>.<uuid>.checkpoint.parquet`)
    ///    - Includes additional V2 metadata
    ///    - Recommended for most tables that support v2Checkpoints
    pub fn checkpoint(
        &self,
        engine: &dyn Engine,
        version: Option<Version>,
    ) -> DeltaResult<CheckpointBuilder> {
        Ok(CheckpointBuilder::new(self.snapshot(engine, version)?))
    }

    /// Create a new write transaction for this table.
    pub fn new_transaction(&self, engine: &dyn Engine) -> DeltaResult<Transaction> {
        Transaction::try_new(self.snapshot(engine, None)?)
    }
}

#[derive(Debug)]
enum UriType {
    LocalPath(PathBuf),
    Url(Url),
}

/// Utility function to figure out whether string representation of the path is either local path or
/// some kind or URL.
///
/// Will return an error if the path is not valid.
fn resolve_uri_type(table_uri: impl AsRef<str>) -> DeltaResult<UriType> {
    let table_uri = table_uri.as_ref();
    let table_uri = if table_uri.ends_with('/') {
        Cow::Borrowed(table_uri)
    } else {
        Cow::Owned(format!("{table_uri}/"))
    };
    if let Ok(url) = Url::parse(&table_uri) {
        let scheme = url.scheme().to_string();
        if url.scheme() == "file" {
            Ok(UriType::LocalPath(
                url.to_file_path()
                    .map_err(|_| Error::invalid_table_location(table_uri))?,
            ))
        } else if scheme.len() == 1 {
            // NOTE this check is required to support absolute windows paths which may properly
            // parse as url we assume here that a single character scheme is a windows drive letter
            Ok(UriType::LocalPath(PathBuf::from(table_uri.as_ref())))
        } else {
            Ok(UriType::Url(url))
        }
    } else {
        Ok(UriType::LocalPath(table_uri.deref().into()))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::engine::sync::SyncEngine;

    #[test]
    fn test_table() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let table = Table::new(url);
        let snapshot = table.snapshot(&engine, None).unwrap();
        assert_eq!(snapshot.version(), 1)
    }

    #[test]
    fn test_path_parsing() {
        for x in [
            // windows parsing of file:/// is... odd
            #[cfg(not(windows))]
            "file:///foo/bar",
            #[cfg(not(windows))]
            "file:///foo/bar/",
            "/foo/bar",
            "/foo/bar/",
            "../foo/bar",
            "../foo/bar/",
            "c:/foo/bar",
            "c:/",
            "file:///C:/",
        ] {
            match resolve_uri_type(x) {
                Ok(UriType::LocalPath(_)) => {}
                x => panic!("Should have parsed as a local path {x:?}"),
            }
        }

        for x in [
            "s3://foo/bar",
            "s3a://foo/bar",
            "memory://foo/bar",
            "gs://foo/bar",
            "https://foo/bar/",
            "unknown://foo/bar",
            "s2://foo/bar",
        ] {
            match resolve_uri_type(x) {
                Ok(UriType::Url(_)) => {}
                x => panic!("Should have parsed as a url {x:?}"),
            }
        }

        #[cfg(not(windows))]
        resolve_uri_type("file://foo/bar").expect_err("file://foo/bar should not have parsed");
    }

    #[test]
    fn try_from_uri_without_trailing_slash() {
        let location = "s3://foo/__unitystorage/catalogs/cid/tables/tid";
        let table = Table::try_from_uri(location).unwrap();

        assert_eq!(
            table.location.join("_delta_log/").unwrap().as_str(),
            "s3://foo/__unitystorage/catalogs/cid/tables/tid/_delta_log/"
        );
    }
}
