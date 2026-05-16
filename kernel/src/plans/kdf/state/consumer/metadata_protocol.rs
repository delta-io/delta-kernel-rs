//! `MetadataProtocolReader` — consumer KDF that extracts the table's
//! [`Protocol`] and [`Metadata`] actions from a log / checkpoint scan.
//!
//! Uses the existing [`Protocol::try_new_from_data`] and
//! [`Metadata::try_new_from_data`] helpers, which guarantee atomic
//! extraction — either a full valid action is returned or `None`. The
//! reader stops once both have been seen.
//!
//! Required ordering is `version DESC`: the SM feeds newer commits first,
//! so the first `Some(Protocol)` / `Some(Metadata)` the reader sees is the
//! effective one. Classic checkpoint files that mix per-row P&M from
//! multiple versions require the SM to partition + sort upstream; this
//! consumer doesn't re-derive ordering itself.

use crate::actions::{Metadata, Protocol};
use crate::plans::errors::DeltaError;
use crate::plans::ir::nodes::OrderingSpec;
use crate::plans::kdf::{take_single, ConsumerKdf, KdfControl, KdfOutput, KdfStateToken};
use crate::schema::column_name;
use crate::{DeltaResult, EngineData};

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

impl_kdf!(MetadataProtocolReader, "consumer.metadata_protocol");

impl ConsumerKdf for MetadataProtocolReader {
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
        Ok(if self.is_complete() {
            KdfControl::Break
        } else {
            KdfControl::Continue
        })
    }

    fn required_ordering(&self) -> Option<OrderingSpec> {
        // Newest commits first so the first P&M captured is the effective one.
        Some(OrderingSpec::desc(column_name!("version")))
    }
}

impl KdfOutput for MetadataProtocolReader {
    type Output = (Protocol, Metadata);

    fn into_output(parts: Vec<Self>) -> Result<Self::Output, DeltaError> {
        let token = KdfStateToken::new("consumer.metadata_protocol");
        let single = take_single(parts, &token)?;
        let protocol = single.protocol.ok_or_else(|| missing("protocol", &token))?;
        let metadata = single.metadata.ok_or_else(|| missing("metadata", &token))?;
        Ok((protocol, metadata))
    }
}

fn missing(what: &str, token: &KdfStateToken) -> DeltaError {
    crate::delta_error!(
        crate::plans::errors::DeltaErrorCode::DeltaCommandInvariantViolation,
        "metadata_protocol.into_output: token `{token}`: missing {what} after scan completed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kdf_id_is_stable() {
        let r = MetadataProtocolReader::new();
        assert_eq!(
            crate::plans::kdf::Kdf::kdf_id(&r),
            "consumer.metadata_protocol"
        );
    }

    #[test]
    fn required_ordering_is_version_desc() {
        let r = MetadataProtocolReader::new();
        let ord = r.required_ordering().expect("must declare an ordering");
        assert_eq!(ord.column, column_name!("version"));
        assert!(ord.descending);
    }

    #[test]
    fn missing_protocol_errors() {
        let r = MetadataProtocolReader::new();
        let err = MetadataProtocolReader::into_output(vec![r]).unwrap_err();
        assert!(format!("{err}").contains("missing protocol"), "got: {err}");
    }
}
