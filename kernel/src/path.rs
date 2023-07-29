//! Utilities to make working with directory and file paths easier

use lazy_static::lazy_static;
use regex::Regex;
use url::Url;

use crate::{DeltaResult, Version};

/// The delimiter to separate object namespaces, creating a directory structure.
const DELIMITER: &str = "/";

lazy_static! {
    static ref CHECKPOINT_FILE_PATTERN: Regex =
        Regex::new(r#"\d+\.checkpoint(\.\d+\.\d+)?\.parquet"#).unwrap();
    static ref DELTA_FILE_PATTERN: Regex = Regex::new(r#"\d+\.json"#).unwrap();
}

#[derive(Debug)]
pub(crate) struct LogPath<'a>(pub(crate) &'a Url);

impl<'a> LogPath<'a> {
    pub(crate) fn child(&self, path: impl AsRef<str>) -> DeltaResult<Url> {
        Ok(self.0.join(path.as_ref())?)
    }

    /// Returns the last path segment containing the filename stored in this [`LogPath`]
    pub(crate) fn filename(&self) -> Option<&str> {
        match self.0.path().is_empty() || self.0.path().ends_with('/') {
            true => None,
            false => self.0.path().split(DELIMITER).last(),
        }
    }

    /// Returns the extension of the file stored in this [`LogPath`], if any
    #[allow(unused)]
    pub(crate) fn extension(&self) -> Option<&str> {
        self.filename()
            .and_then(|f| f.rsplit_once('.'))
            .and_then(|(_, extension)| {
                if extension.is_empty() {
                    None
                } else {
                    Some(extension)
                }
            })
    }

    pub(crate) fn is_checkpoint_file(&self) -> bool {
        self.filename()
            .map(|name| CHECKPOINT_FILE_PATTERN.captures(name).is_some())
            .unwrap_or(false)
    }

    pub(crate) fn is_commit_file(&self) -> bool {
        self.filename()
            .map(|name| DELTA_FILE_PATTERN.captures(name).is_some())
            .unwrap_or(false)
    }

    /// Parse the version number assuming a commit json or checkpoint parquet file
    pub(crate) fn commit_version(&self) -> Option<Version> {
        self.filename()
            .and_then(|f| f.split_once('.'))
            .and_then(|(name, _)| name.parse().ok())
    }
}

impl<'a> AsRef<Url> for LogPath<'a> {
    fn as_ref(&self) -> &Url {
        self.0
    }
}

impl<'a> AsRef<str> for LogPath<'a> {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn table_url() -> Url {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        url::Url::from_file_path(path).unwrap()
    }

    #[test]
    fn test_file_patterns() {
        let table_url = table_url();
        let log_path = LogPath(&table_url)
            .child("_delta_log/00000000000000000000.json")
            .unwrap();
        let log_path = LogPath(&log_path);

        assert_eq!("00000000000000000000.json", log_path.filename().unwrap());
        assert_eq!("json", log_path.extension().unwrap());
        assert!(log_path.is_commit_file());
        assert!(!log_path.is_checkpoint_file());
        assert_eq!(log_path.commit_version(), Some(0));

        let log_path = log_path.child("00000000000000000005.json").unwrap();
        let log_path = LogPath(&log_path);

        assert_eq!(log_path.commit_version(), Some(5));

        let log_path = log_path
            .child("00000000000000000002.checkpoint.parquet")
            .unwrap();
        let log_path = LogPath(&log_path);

        assert_eq!(
            "00000000000000000002.checkpoint.parquet",
            log_path.filename().unwrap()
        );
        assert_eq!("parquet", log_path.extension().unwrap());
        assert!(!log_path.is_commit_file());
        assert!(log_path.is_checkpoint_file());
        assert_eq!(log_path.commit_version(), Some(2));
    }
}
