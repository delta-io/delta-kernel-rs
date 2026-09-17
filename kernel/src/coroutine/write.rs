//! Storage-write requests.
//!
//! Complete objects use one request. JSON writes carry a generator so the connector can pull row
//! batches while it owns the write.

use bytes::Bytes;
use derive_more::Constructor;
use url::Url;

use super::GeneratorTask;
use crate::FilteredEngineData;

/// Controls how a file sink handles an existing destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileWriteMode {
    /// Fail if the destination already exists.
    CreateNew,
    /// Replace an existing destination.
    Overwrite,
}

impl FileWriteMode {
    pub(crate) fn overwrite(self) -> bool {
        self == Self::Overwrite
    }
}

/// Writes newline-delimited JSON rows to one file.
#[derive(Constructor)]
pub struct WriteJsonFile {
    /// Destination URL.
    pub url: Url,
    /// Existing-destination behavior.
    pub mode: FileWriteMode,
    /// Row batches to serialize in generator order.
    pub input: GeneratorTask<FilteredEngineData>,
}

/// Write the bytes of one complete storage object to the specified destination URL.
#[derive(Constructor)]
pub struct WriteBytes {
    /// Destination URL.
    pub url: Url,
    /// Complete object contents.
    pub data: Bytes,
    /// Whether to replace an existing destination.
    ///
    /// If false, an existing destination must produce [`crate::Error::FileAlreadyExists`].
    pub overwrite: bool,
}

/// Atomically copy one immutable object to a destination that must not already exist.
#[derive(Constructor)]
pub struct CopyAtomic {
    /// Existing source object.
    pub source: Url,
    /// New destination object.
    pub destination: Url,
}
