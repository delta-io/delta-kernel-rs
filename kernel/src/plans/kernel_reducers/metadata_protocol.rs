//! `MetadataProtocolReader` -- reducer KDF that extracts the table's
//! [`Protocol`] and [`Metadata`] actions from a log / checkpoint scan.
//!
//! Uses the existing [`Protocol::try_new_from_data`] and
//! [`Metadata::try_new_from_data`] helpers, which guarantee atomic
//! extraction -- either a full valid action is returned or `None`. The
//! reader stops once both have been seen.
//!
//! # Caller invariant: newest-first ordering
//!
//! The reader captures the FIRST `Some(Protocol)` / `Some(Metadata)` it sees and ignores
//! subsequent ones, so the caller MUST feed batches in `version DESC` order (newer commits
//! first, then checkpoints, oldest last). Mirrors the contract that
//! [`crate::log_segment::LogSegment::replay_for_pm`] establishes for the visitor path. If
//! batches are fed in chronological order, the reader will silently return the wrong
//! (oldest, not newest) action -- which is why we surface the contract here AND assert it
//! via `debug_assert!` on every batch when the reducer can prove the order.

use crate::actions::{Metadata, Protocol};
use crate::plans::errors::DeltaError;
use crate::plans::kernel_reducers::{
    KdfControl, KernelReducer, KernelReducerKind, KernelReducerOutput,
};
use crate::{DeltaResult, EngineData};

/// Captures the first `Some(Protocol)` / `Some(Metadata)` seen under the
/// declared row ordering.
#[derive(Debug, Clone, Default)]
pub struct MetadataProtocolReader {
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
}

impl KernelReducer for MetadataProtocolReader {
    fn kind(&self) -> KernelReducerKind {
        KernelReducerKind::MetadataProtocol
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
        if self.protocol.is_some() && self.metadata.is_some() {
            Ok(KdfControl::Break)
        } else {
            Ok(KdfControl::Continue)
        }
    }
}

impl KernelReducerOutput for MetadataProtocolReader {
    /// `(Metadata, Protocol)` matches the existing kernel convention used by
    /// `LogSegment::read_protocol_metadata` and its underlying `replay_for_pm` helper.
    type Output = (Metadata, Protocol);

    fn into_output(self) -> Result<Self::Output, DeltaError> {
        let missing = |what: &str| {
            DeltaError::invariant(format!(
                "metadata_protocol.into_output: missing {what} after scan completed"
            ))
        };
        let protocol = self.protocol.ok_or_else(|| missing("protocol"))?;
        let metadata = self.metadata.ok_or_else(|| missing("metadata"))?;
        Ok((metadata, protocol))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::actions::Format;
    use crate::table_features::TableFeature;
    use crate::table_properties::{
        COLUMN_MAPPING_MODE, ENABLE_CHANGE_DATA_FEED, ENABLE_DELETION_VECTORS,
    };
    use crate::utils::test_utils::action_batch;

    /// The full `Protocol` carried by `crate::utils::test_utils::action_batch()`. Kept in sync
    /// with the JSON fixture inline below so a fixture drift is loud rather than silent.
    fn expected_action_batch_protocol() -> Protocol {
        Protocol::try_new(
            3,
            7,
            Some([TableFeature::DeletionVectors]),
            Some([TableFeature::DeletionVectors]),
        )
        .expect("test fixture protocol should be valid")
    }

    /// The full `Metadata` carried by `crate::utils::test_utils::action_batch()`. Mirrors
    /// `actions::visitors::tests::test_parse_metadata`'s expected struct so any drift in the
    /// shared fixture surfaces in both tests.
    fn expected_action_batch_metadata() -> Metadata {
        let configuration = HashMap::from_iter([
            (ENABLE_DELETION_VECTORS.to_string(), "true".to_string()),
            (COLUMN_MAPPING_MODE.to_string(), "none".to_string()),
            (ENABLE_CHANGE_DATA_FEED.to_string(), "true".to_string()),
        ]);
        Metadata::new_unchecked(
            "testId",
            None,
            None,
            Format {
                provider: "parquet".into(),
                options: HashMap::new(),
            },
            r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
            Vec::new(),
            Some(1677811175819),
            configuration,
        )
    }

    #[test]
    fn kind_is_stable() {
        let r = MetadataProtocolReader::default();
        assert_eq!(KernelReducer::kind(&r), KernelReducerKind::MetadataProtocol);
    }

    #[test]
    fn missing_protocol_errors() {
        let r = MetadataProtocolReader::default();
        let err = r.into_output().unwrap_err();
        // Detail now lives in the source (the message is static per code).
        let src = std::error::Error::source(&err).expect("detail in source");
        assert!(src.to_string().contains("missing protocol"), "got: {src}");
    }

    #[test]
    fn apply_extracts_protocol_and_metadata_then_breaks() {
        // `action_batch()` is the canonical test fixture used by the visitor unit tests; it
        // contains a single batch with both Protocol and Metadata actions, plus some adds.
        let batch = action_batch();
        let mut r = MetadataProtocolReader::default();
        assert_eq!(
            r.apply(batch.as_ref()).unwrap(),
            KdfControl::Break,
            "both actions present -> Break on first batch"
        );

        let (metadata, protocol) = r.into_output().unwrap();
        // Compare against the full expected structs, not just one field each: this catches
        // silent drift in any of the version, feature, schema, partition, configuration, or
        // created-time fields produced by the reducer.
        assert_eq!(protocol, expected_action_batch_protocol());
        assert_eq!(metadata, expected_action_batch_metadata());
    }

    #[test]
    fn apply_ignores_subsequent_actions_per_newest_first_contract() {
        // The reader captures the FIRST Protocol/Metadata it sees and silently ignores any
        // subsequent ones (because the caller is expected to feed newest-first). Pin that
        // contract here so a regression that started overwriting on later batches surfaces.
        let first = action_batch();
        let second = action_batch();

        let mut r = MetadataProtocolReader::default();
        assert_eq!(r.apply(first.as_ref()).unwrap(), KdfControl::Break);
        let snapshot = (r.protocol.clone(), r.metadata.clone());
        // Subsequent apply is a no-op for state; result is still Break.
        assert_eq!(r.apply(second.as_ref()).unwrap(), KdfControl::Break);
        assert_eq!((r.protocol, r.metadata), snapshot);
    }
}
