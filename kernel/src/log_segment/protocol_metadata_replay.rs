//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::{Arc, LazyLock};

use crate::actions::{get_commit_schema, Metadata, Protocol, METADATA_NAME, PROTOCOL_NAME};
use crate::crc::{CrcLoadResult, LazyCrc};
use crate::log_replay::ActionsBatch;
use crate::{DeltaResult, Engine, Error, Expression, Predicate, PredicateRef};

use tracing::{info, instrument};

use super::LogSegment;

impl LogSegment {
    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// Returns an error if either is missing.
    ///
    /// This is the checked variant of [`Self::read_protocol_metadata_unchecked`], used for
    /// fresh snapshot creation where both Protocol and Metadata must exist.
    pub(crate) fn read_protocol_metadata_checked(
        &self,
        engine: &dyn Engine,
        lazy_crc: &LazyCrc,
    ) -> DeltaResult<(Metadata, Protocol)> {
        match self.read_protocol_metadata_unchecked(engine, lazy_crc)? {
            (Some(m), Some(p)) => Ok((m, p)),
            (None, Some(_)) => Err(Error::MissingMetadata),
            (Some(_), None) => Err(Error::MissingProtocol),
            (None, None) => Err(Error::MissingMetadataAndProtocol),
        }
    }

    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// Returns `None` for either if not found.
    ///
    /// This is the unchecked variant of [`Self::read_protocol_metadata_checked`], used for
    /// incremental snapshot updates where the caller can fall back to an existing snapshot's
    /// Protocol and Metadata.
    ///
    /// The `lazy_crc` parameter allows the CRC to be loaded at most once and shared for
    /// future use (domain metadata, in-commit timestamp, etc.).
    #[instrument(name = "log_seg.load_p_m", skip_all, err)]
    pub(crate) fn read_protocol_metadata_unchecked(
        &self,
        engine: &dyn Engine,
        lazy_crc: &LazyCrc,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        let crc_version = lazy_crc.crc_version();

        // Case 1: If CRC at target version, use it directly and exit early.
        if crc_version == Some(self.end_version) {
            if let CrcLoadResult::Loaded(crc) = lazy_crc.get_or_load(engine) {
                info!("P&M from CRC at target version {}", self.end_version);
                return Ok((Some(crc.metadata.clone()), Some(crc.protocol.clone())));
            }
            info!(
                "CRC at target version {} failed to load, falling back to log replay",
                self.end_version
            );
        }

        // We didn't return above, so we need to do log replay to find P&M.
        //
        // Case 2: CRC exists at an earlier version => Prune the log segment to only replay
        //         commits *after* the CRC version.
        //   (a) If we find new P&M in the pruned replay, return it.
        //   (b) If we don't find new P&M, fall back to the CRC.
        //   (c) If the CRC also fails, fall back to replaying the remaining segment
        //       (checkpoint + commits up through the CRC version).
        //
        // Case 3: CRC at target version failed to load => Full P&M log replay.
        //
        // Case 4: No CRC exists at all => Full P&M log replay.

        if let Some(crc_v) = crc_version.filter(|&v| v < self.end_version) {
            // Case 2(a): Replay only commits after CRC version
            info!("Pruning log segment to commits after CRC version {}", crc_v);
            let pruned = self.pruned_after(crc_v);
            let (metadata_opt, protocol_opt) = pruned.replay_for_pm(engine, None, None)?;

            if metadata_opt.is_some() && protocol_opt.is_some() {
                info!("Found P&M from pruned log replay");
                return Ok((metadata_opt, protocol_opt));
            }

            // Case 2(b): P&M incomplete from pruned replay, try CRC.
            // Use `or_else` so any newer P or M found in the pruned replay takes priority
            // over the (older) CRC values.
            if let CrcLoadResult::Loaded(crc) = lazy_crc.get_or_load(engine) {
                info!("P&M fallback to CRC (no P&M changes after CRC version)");
                return Ok((
                    metadata_opt.or_else(|| Some(crc.metadata.clone())),
                    protocol_opt.or_else(|| Some(crc.protocol.clone())),
                ));
            }

            // Case 2(c): CRC failed to load. Replay the remaining segment (checkpoint +
            // commits up through CRC version), carrying forward any partial results from the
            // pruned replay above.
            info!(
                "CRC at version {} failed to load, replaying remaining segment",
                crc_v
            );
            let remaining = self.pruned_through(crc_v);
            return remaining.replay_for_pm(engine, metadata_opt, protocol_opt);
        }

        // Case 3 / Case 4: Full P&M log replay.
        self.replay_for_pm(engine, None, None)
    }

    /// Replays the log segment for Protocol and Metadata, merging with any already-found values.
    /// Stops early once both are found.
    fn replay_for_pm(
        &self,
        engine: &dyn Engine,
        mut metadata_opt: Option<Metadata>,
        mut protocol_opt: Option<Protocol>,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
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
        // filter out log files that do not contain metadata or protocol information
        static META_PREDICATE: LazyLock<Option<PredicateRef>> = LazyLock::new(|| {
            Some(Arc::new(Predicate::or(
                Expression::column([METADATA_NAME, "id"]).is_not_null(),
                Expression::column([PROTOCOL_NAME, "minReaderVersion"]).is_not_null(),
            )))
        });
        // read the same protocol and metadata schema for both commits and checkpoints
        self.read_actions(engine, schema, META_PREDICATE.clone())
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
