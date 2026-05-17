//! `MetadataProtocolReader` — consumer KDF that extracts the table's
//! [`Protocol`] and [`Metadata`] actions from a log / checkpoint scan.
//!
//! Uses the existing [`Protocol::try_new_from_data`] and
//! [`Metadata::try_new_from_data`] helpers, which guarantee atomic
//! extraction — either a full valid action is returned or `None`. The
//! reader stops once both have been seen.
//!
//! Newest commits must be fed first (the SM partitions + sorts upstream
//! by `version DESC`), so the first `Some(Protocol)` / `Some(Metadata)`
//! the reader captures is the effective one.

use crate::actions::{Metadata, Protocol};
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::kdf::{ConsumerKdf, ConsumerKdfId, KdfControl, KdfOutput};
use crate::{delta_error, DeltaResult, EngineData};

/// Captures the first `Some(Protocol)` / `Some(Metadata)` seen under the
/// declared row ordering.
#[derive(Debug, Clone, Default)]
pub struct MetadataProtocolReader {
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
}

impl MetadataProtocolReader {
    pub fn new() -> Self {
        Self::default()
    }

    fn is_complete(&self) -> bool {
        self.protocol.is_some() && self.metadata.is_some()
    }
}

impl ConsumerKdf for MetadataProtocolReader {
    fn kdf_id(&self) -> ConsumerKdfId {
        ConsumerKdfId::MetadataProtocol
    }

    fn finish(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        Box::new(*self)
    }

    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        if self.protocol.is_none() {
            if let Some(p) = Protocol::try_new_from_data(batch)? {
                self.protocol = Some(p);
            }
        }
        if self.metadata.is_none() {
            if let Some(m) = Metadata::try_new_from_data(batch)? {
                self.metadata = Some(m);
            }
        }
        if self.is_complete() {
            Ok(KdfControl::Break)
        } else {
            Ok(KdfControl::Continue)
        }
    }
}

impl KdfOutput for MetadataProtocolReader {
    type Output = (Protocol, Metadata);

    fn into_output(self) -> Result<Self::Output, DeltaError> {
        let missing = |what: &str| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "metadata_protocol.into_output: missing {what} after scan completed",
            )
        };
        let protocol = self.protocol.ok_or_else(|| missing("protocol"))?;
        let metadata = self.metadata.ok_or_else(|| missing("metadata"))?;
        Ok((protocol, metadata))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kdf_id_is_stable() {
        let r = MetadataProtocolReader::new();
        assert_eq!(
            crate::plans::kdf::ConsumerKdf::kdf_id(&r),
            ConsumerKdfId::MetadataProtocol
        );
    }

    #[test]
    fn missing_protocol_errors() {
        let r = MetadataProtocolReader::new();
        let err = r.into_output().unwrap_err();
        assert!(format!("{err}").contains("missing protocol"), "got: {err}");
    }
}
