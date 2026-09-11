//! Storage-write requests.

use bytes::Bytes;
use delta_kernel_derive::internal_api;
use derive_more::Constructor;
use url::Url;

use super::core::{Pending, Step};
use super::{Cursor, SinkRequest};
use crate::{DeltaResult, FileMeta, FilteredEngineData};

/// Describes one connector-managed sink.
pub trait SinkOperation: Send + Sized + 'static {
    /// Data accepted by each write to the sink.
    type Input: Send + 'static;

    /// Value produced when the sink finishes.
    type Output: Send + 'static;
}

/// A suspended phase of a connector-managed sink.
#[internal_api]
pub(crate) enum PendingSinkRequest<Op: SinkOperation> {
    /// Start the sink.
    Start(Pending<Op, Cursor<Op>>),
    /// Append a batch to the sink.
    Write(Pending<SinkWrite<Op>, Cursor<Op>>),
    /// Finish the sink.
    Finish(Pending<Cursor<Op>, Op::Output>),
}

/// Data supplied when appending to a connector-managed sink.
#[internal_api]
#[derive(Constructor)]
pub(crate) struct SinkWrite<Op: SinkOperation> {
    sink: Cursor<Op>,
    data: Op::Input,
}

impl<Op: SinkOperation> PendingSinkRequest<Op> {
    /// Return whether the coroutine still owns this sink exchange.
    #[internal_api]
    pub(crate) fn is_live(&self) -> bool {
        match self {
            Self::Start(exchange) => exchange.is_live(),
            Self::Write(exchange) => exchange.is_live(),
            Self::Finish(exchange) => exchange.is_live(),
        }
    }

    /// Convert this exchange into a connector-facing sink request.
    #[internal_api]
    pub(crate) fn into_request<N: Send + 'static>(
        self,
        step: impl Step<N>,
    ) -> DeltaResult<SinkRequest<N, Op>> {
        match self {
            Self::Start(exchange) => exchange.into_request(step, SinkRequest::Start),
            Self::Write(exchange) => exchange.into_request(step, |write, resume| {
                SinkRequest::Write(write.sink, write.data, resume)
            }),
            Self::Finish(exchange) => exchange.into_request(step, SinkRequest::Finish),
        }
    }
}

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
}

impl SinkOperation for WriteJsonFile {
    type Input = FilteredEngineData;
    type Output = FileMeta;
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
