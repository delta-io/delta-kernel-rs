//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use tracing::instrument;

use super::LogSegment;
use crate::actions::{get_commit_schema, Metadata, Protocol, METADATA_NAME, PROTOCOL_NAME};
use crate::log_replay::ActionsBatch;
use crate::metrics::events::PROTOCOL_METADATA_LOADED_SPAN;
use crate::metrics::MetricId;
use crate::{DeltaResult, Engine, Error};

impl LogSegment {
    /// Read the latest Protocol and Metadata from this log segment via full log replay.
    /// Returns an error if either is missing.
    ///
    /// This is the checked variant of [`Self::read_protocol_metadata_opt`], used for fresh
    /// snapshot creation where both Protocol and Metadata must exist.
    #[instrument(name = PROTOCOL_METADATA_LOADED_SPAN, fields(report, operation_id = %operation_id), skip(engine))]
    pub(crate) fn read_protocol_metadata(
        &self,
        engine: &dyn Engine,
        operation_id: MetricId,
    ) -> DeltaResult<(Metadata, Protocol)> {
        match self.read_protocol_metadata_opt(engine)? {
            (Some(m), Some(p)) => Ok((m, p)),
            (None, Some(_)) => Err(Error::MissingMetadata),
            (Some(_), None) => Err(Error::MissingProtocol),
            (None, None) => Err(Error::MissingMetadataAndProtocol),
        }
    }

    /// Read the latest Protocol and Metadata from this log segment via full log replay,
    /// stopping early once both are found. Returns `None` for either if not found.
    ///
    /// This is the unchecked variant of [`Self::read_protocol_metadata`], used for incremental
    /// snapshot updates where the caller can fall back to an existing snapshot's Protocol and
    /// Metadata.
    #[instrument(name = "log_seg.load_p_m", skip_all, err)]
    pub(crate) fn read_protocol_metadata_opt(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        let mut metadata_opt = None;
        let mut protocol_opt = None;
        for actions_batch in self.read_pm_batches(engine)? {
            let actions = actions_batch?.actions;
            if metadata_opt.is_none() {
                metadata_opt = Metadata::try_new_from_data(actions.as_ref())?;
            }
            if protocol_opt.is_none() {
                protocol_opt = Protocol::try_new_from_data(actions.as_ref())?;
            }
            if metadata_opt.is_some() && protocol_opt.is_some() {
                break;
            }
        }
        Ok((metadata_opt, protocol_opt))
    }

    // Replay the commit log, projecting rows to only contain Protocol and Metadata action columns.
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        let schema = get_commit_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
        self.read_actions(engine, schema)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use itertools::Itertools;
    use test_log::test;

    use crate::engine::sync::SyncEngine;
    use crate::Snapshot;

    // NOTE: In addition to testing the meta-predicate for metadata replay, this test also verifies
    // that the parquet reader properly infers nullcount = rowcount for missing columns. The two
    // checkpoint part files that contain transaction app ids have truncated schemas that would
    // otherwise fail skipping due to their missing nullcount stat:
    //
    // Row group 0:  count: 1  total(compressed): 111 B total(uncompressed):107 B
    // --------------------------------------------------------------------------------
    //              type    nulls  min / max
    // txn.appId    BINARY  0      "3ae45b72-24e1-865a-a211-3..." / "3ae45b72-24e1-865a-a211-3..."
    // txn.version  INT64   0      "4390" / "4390"
    #[test]
    fn test_replay_for_metadata() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let data: Vec<_> = snapshot
            .log_segment()
            .read_pm_batches(&engine)
            .unwrap()
            .try_collect()
            .unwrap();

        // The checkpoint has five parts, each containing one action:
        // 1. txn (physically missing P&M columns)
        // 2. metaData
        // 3. protocol
        // 4. add
        // 5. txn (physically missing P&M columns)
        //
        // The parquet reader should skip parts 1, 3, and 5. Note that the actual `read_metadata`
        // always skips parts 4 and 5 because it terminates the iteration after finding both P&M.
        //
        // NOTE: Each checkpoint part is a single-row file -- guaranteed to produce one row group.
        //
        // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 -- We currently
        // read parts 1 and 5 (4 in all instead of 2) because row group skipping is disabled for
        // missing columns, but can still skip part 3 because has valid nullcount stats for P&M.
        assert_eq!(data.len(), 4);
    }
}
