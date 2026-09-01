//! Storage-write requests.

use bytes::Bytes;
use url::Url;

use super::core::PendingRequest;
use super::Channel;
use crate::DeltaResult;

/// Write the bytes of one complete storage object to the specified destination URL.
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

impl Channel {
    /// Write `data` to `url`, replacing an existing object only when `overwrite` is true.
    pub(crate) async fn write_bytes(
        &self,
        url: Url,
        data: Bytes,
        overwrite: bool,
    ) -> DeltaResult<()> {
        let op = WriteBytes {
            url,
            data,
            overwrite,
        };
        self.exchange(op, PendingRequest::WriteBytes).await
    }
}
