//! Various utility functions/macros used throughout the kernel
use std::borrow::Cow;
use std::ops::Deref;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use url::Url;

use crate::{DeltaResult, Error};
use delta_kernel_derive::internal_api;

/// convenient way to return an error if a condition isn't true
macro_rules! require {
    ( $cond:expr, $err:expr ) => {
        if !($cond) {
            return Err($err);
        }
    };
}

pub(crate) use require;

/// Try to parse string uri into a URL for a table path. This will do it's best to handle things
/// like `/local/paths`, and even `../relative/paths`.
#[allow(unused)]
#[internal_api]
pub(crate) fn try_parse_uri(uri: impl AsRef<str>) -> DeltaResult<Url> {
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
                let msg = format!("Invalid table location: {uri} Error: {err:?}");
                Error::InvalidTableLocation(msg)
            })?;
            Url::from_directory_path(path.clone()).map_err(|_| {
                let msg = format!(
                    "Could not construct a URL from canonicalized path: {path:?}.\n\
                     Something must be very wrong with the table path."
                );
                Error::InvalidTableLocation(msg)
            })?
        }
        UriType::Url(url) => url,
    };
    Ok(url)
}

#[allow(unused)]
#[derive(Debug)]
enum UriType {
    LocalPath(PathBuf),
    Url(Url),
}

/// Utility function to figure out whether string representation of the path is either local path or
/// some kind or URL.
///
/// Will return an error if the path is not valid.
#[allow(unused)]
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

/// Returns the current time as a Duration since Unix epoch.
pub(crate) fn current_time_duration() -> DeltaResult<Duration> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::generic(format!("System time before Unix epoch: {}", e)))
}

/// Returns the current time in milliseconds since Unix epoch.
pub(crate) fn current_time_ms() -> DeltaResult<i64> {
    let duration = current_time_duration()?;
    i64::try_from(duration.as_millis())
        .map_err(|_| Error::generic("Current timestamp exceeds i64 millisecond range"))
}

// Extension trait for Cow<'_, T>
pub(crate) trait CowExt<T: ToOwned + ?Sized> {
    /// The owned type that corresopnds to Self
    type Owned;

    /// Propagate the results of nested transforms. If the nested transform made no change (borrowed
    /// `self`), then return a borrowed result `s` as well. Otherwise, invoke the provided mapping
    /// function `f` to convert the owned nested result into an owned result.
    fn map_owned_or_else<S: Clone>(self, s: &S, f: impl FnOnce(Self::Owned) -> S) -> Cow<'_, S>;
}

// Basic implementation for a single Cow value
impl<T: ToOwned + ?Sized> CowExt<T> for Cow<'_, T> {
    type Owned = T::Owned;

    fn map_owned_or_else<S: Clone>(self, s: &S, f: impl FnOnce(T::Owned) -> S) -> Cow<'_, S> {
        match self {
            Cow::Owned(v) => Cow::Owned(f(v)),
            Cow::Borrowed(_) => Cow::Borrowed(s),
        }
    }
}

// Additional implementation for a pair of Cow values
impl<'a, T: ToOwned + ?Sized> CowExt<(Cow<'a, T>, Cow<'a, T>)> for (Cow<'a, T>, Cow<'a, T>) {
    type Owned = (T::Owned, T::Owned);

    fn map_owned_or_else<S: Clone>(self, s: &S, f: impl FnOnce(Self::Owned) -> S) -> Cow<'_, S> {
        match self {
            (Cow::Borrowed(_), Cow::Borrowed(_)) => Cow::Borrowed(s),
            (left, right) => Cow::Owned(f((left.into_owned(), right.into_owned()))),
        }
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use crate::actions::{
        get_all_actions_schema, Add, Cdc, CommitInfo, Metadata, Protocol, Remove,
    };
    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    use crate::Engine;
    use crate::EngineData;

    use itertools::Itertools;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use serde::Serialize;
    use std::{path::Path, sync::Arc};
    use tempfile::TempDir;
    use test_utils::delta_path_for_version;

    #[derive(Serialize)]
    pub(crate) enum Action {
        #[serde(rename = "add")]
        Add(Add),
        #[serde(rename = "remove")]
        Remove(Remove),
        #[serde(rename = "cdc")]
        Cdc(Cdc),
        #[serde(rename = "metaData")]
        Metadata(Metadata),
        #[serde(rename = "protocol")]
        Protocol(Protocol),
        #[allow(unused)]
        #[serde(rename = "commitInfo")]
        CommitInfo(CommitInfo),
    }

    /// A mock table that writes commits to a local temporary delta log. This can be used to
    /// construct a delta log used for testing.
    pub(crate) struct LocalMockTable {
        commit_num: u64,
        store: Arc<LocalFileSystem>,
        dir: TempDir,
    }

    impl LocalMockTable {
        pub(crate) fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
            Self {
                commit_num: 0,
                store,
                dir,
            }
        }
        /// Writes all `actions` to a new commit in the log
        pub(crate) async fn commit(&mut self, actions: impl IntoIterator<Item = Action>) {
            let data = actions
                .into_iter()
                .map(|action| serde_json::to_string(&action).unwrap())
                .join("\n");

            let path = delta_path_for_version(self.commit_num, "json");
            self.commit_num += 1;

            self.store
                .put(&path, data.into())
                .await
                .expect("put log file in store");
        }

        /// Get the path to the root of the table.
        pub(crate) fn table_root(&self) -> &Path {
            self.dir.path()
        }
    }

    /// Try to convert an `EngineData` into a `RecordBatch`. Panics if not using `ArrowEngineData` from
    /// the default module
    fn into_record_batch(engine_data: Box<dyn EngineData>) -> RecordBatch {
        ArrowEngineData::try_from_engine_data(engine_data)
            .unwrap()
            .into()
    }

    /// Checks that two `EngineData` objects are equal by converting them to `RecordBatch` and comparing
    pub(crate) fn assert_batch_matches(actual: Box<dyn EngineData>, expected: Box<dyn EngineData>) {
        assert_eq!(into_record_batch(actual), into_record_batch(expected));
    }

    pub(crate) fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(ArrowEngineData::new(batch))
    }

    pub(crate) fn parse_json_batch(json_strings: StringArray) -> Box<dyn EngineData> {
        let engine = SyncEngine::new();
        let json_handler = engine.json_handler();
        let output_schema = get_all_actions_schema().clone();
        json_handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap()
    }

    pub(crate) fn action_batch() -> Box<dyn EngineData> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"remove":{"path":"part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","deletionTimestamp":1670892998135,"dataChange":true,"partitionValues":{"c1":"4","c2":"c"},"size":452}}"#,
            r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none", "delta.enableChangeDataFeed":"true"},"createdTime":1677811175819}}"#,
            r#"{"cdc":{"path":"_change_data/age=21/cdc-00000-93f7fceb-281a-446a-b221-07b88132d203.c000.snappy.parquet","partitionValues":{"age":"21"},"size":1033,"dataChange":false}}"#,
            r#"{"sidecar":{"path":"016ae953-37a9-438e-8683-9a9a4a79a395.parquet","sizeInBytes":9268,"modificationTime":1714496113961,"tags":{"tag_foo":"tag_bar"}}}"#,
            r#"{"txn":{"appId":"myApp","version": 3}}"#,
            r#"{"checkpointMetadata":{"version":2, "tags":{"tag_foo":"tag_bar"}}}"#,
        ]
        .into();
        parse_json_batch(json_strings)
    }

    // TODO: allow tests to pass in context (issue#1133)
    pub(crate) fn assert_result_error_with_message<T, E: ToString>(
        res: Result<T, E>,
        message: &str,
    ) {
        match res {
            Ok(_) => panic!("Expected error, but got Ok result"),
            Err(error) => {
                let error_str = error.to_string();
                assert!(
                    error_str.contains(message),
                    "Error message does not contain the expected message.\nExpected message:\t{message}\nActual message:\t\t{error_str}"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let url = try_parse_uri(location).unwrap();

        assert_eq!(
            url.to_string(),
            "s3://foo/__unitystorage/catalogs/cid/tables/tid/"
        );
    }
}
