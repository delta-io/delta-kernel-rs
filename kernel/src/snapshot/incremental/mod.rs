//! Advances an existing snapshot to a requested version or to the latest available version.
//!
//! The builder normalizes its options into [`SnapshotUpdate`], which coordinates the update:
//! 1. [`LogUpdate::discover`] merges storage listings with the catalog log tail and selects the
//!    retained files. Its [`StateBaseline`] says whether those files can extend the existing state.
//! 2. [`prepare_incremental`] resolves protocol, metadata, and CRC from that state. If an adopted
//!    checkpoint is ahead of the existing snapshot, the fresh-build resolver supplies state
//!    instead.
//! 3. The resolved state and selected files become the returned snapshot.
//!
//! Retained history and state replay have different starting versions. For a snapshot at v10,
//! adopting a checkpoint at v8 keeps commits from v9 onward for subsequent scans. Protocol and
//! metadata can still start from the existing v10 state. CRC replay has a separate baseline and
//! budget, described in [`resolve_incremental_state`].

use std::sync::Arc;
use std::time::Instant;

use log_update::{LogUpdate, StateBaseline};
use tracing::{error, instrument};

use super::builder::SnapshotTarget;
use super::{IncrementalReplay, PreparedSnapshot, Snapshot};
use crate::crc::Crc;
use crate::log_segment::{LogSegment, PmResolution};
use crate::log_segment_files::CheckpointHandling;
use crate::metrics::{
    SnapshotLoadMetricContext, emit_log_segment_load, emit_log_segment_load_failure,
    emit_protocol_metadata_load, emit_protocol_metadata_load_failure,
};
use crate::path::ParsedLogPath;
use crate::table_configuration::TableConfiguration;
use crate::{Engine, KernelError, KernelResult};

mod log_update;

#[cfg(test)]
mod legacy_tests;
#[cfg(test)]
mod tests;

/// An incremental build request after the builder validates catalog version and log-tail inputs.
pub(super) struct SnapshotUpdate {
    /// Supplies resolved table state and retained log paths, including unpublished staged commits.
    pub(super) existing: Arc<Snapshot>,
    pub(super) target: SnapshotTarget,
    /// Catalog-supplied commit paths; may start after the existing snapshot's version.
    pub(super) log_tail: Vec<ParsedLogPath>,
    /// Whether discovery may replace the existing checkpoint and discard the history it covers.
    pub(super) checkpoint_handling: CheckpointHandling,
    /// Budget for advancing a stale CRC. Protocol and metadata replay runs as needed.
    pub(super) crc_replay: IncrementalReplay,
}

impl SnapshotUpdate {
    /// Builds the requested snapshot, reusing the input state whenever the selected files allow it.
    ///
    /// `TimeTravel` and `CatalogLatest` return the input `Arc` unchanged when their version equals
    /// the existing version, and reject older versions. Neither path performs I/O. `Latest` still
    /// discovers files and may refresh the checkpoint or build flags without advancing the version.
    #[instrument(name = "try_new_from", skip_all, fields(path = %self.existing.table_root(), enable_call_frame, version, operation_id = %metric_context.operation_id, correlation_id = metric_context.correlation_id.as_deref().unwrap_or(""), incremental_replay = ?self.crc_replay, built_as_latest = self.target.is_latest()))]
    pub(super) fn execute(
        self,
        engine: &dyn Engine,
        metric_context: SnapshotLoadMetricContext,
    ) -> KernelResult<Arc<Snapshot>> {
        let existing = &self.existing;
        let target_version = self.target.version();
        tracing::Span::current().record("version", target_version.unwrap_or(existing.version()));

        // Borrow each segment while preparing state so failure diagnostics retain the actual files.
        let report_failure = |error: &KernelError, segment: &LogSegment| {
            error!(
                %error,
                existing_snapshot = ?existing,
                log_segment = ?segment,
                ?metric_context,
                incremental_replay = ?self.crc_replay,
                checkpoint_handling = ?self.checkpoint_handling,
                built_as_latest = self.target.is_latest(),
                "failed to update snapshot"
            );
        };

        if let Some(target) = target_version {
            if target == existing.version() {
                return Ok(existing.clone());
            }
            if target < existing.version() {
                let error = KernelError::Generic(format!(
                    "Requested snapshot version {target} is older than snapshot \
                     hint version {}",
                    existing.version()
                ));
                report_failure(&error, &existing.log_segment);
                return Err(error);
            }
        }

        // Select retained history before resolving state: checkpoint adoption decides whether
        // every commit after the existing snapshot remains available for incremental replay.
        let start = Instant::now();
        let update = LogUpdate::discover(
            &existing.log_segment,
            self.log_tail,
            target_version,
            self.checkpoint_handling,
            engine.storage_handler().as_ref(),
        )
        .inspect_err(|error| {
            emit_log_segment_load_failure(&metric_context);
            report_failure(error, &existing.log_segment);
        })?;
        let Some(LogUpdate { segment, baseline }) = update else {
            // An unchanged file set can still record a latest-version request or checkpoint policy.
            return reuse_with_build_metadata(
                existing,
                self.target.is_latest(),
                self.checkpoint_handling == CheckpointHandling::Ignore,
            )
            .inspect_err(|error| report_failure(error, &existing.log_segment));
        };
        emit_log_segment_load(&metric_context, &segment, start.elapsed());

        let prepared = match baseline {
            StateBaseline::ExistingSnapshot => {
                prepare_incremental(existing, &segment, engine, self.crc_replay, &metric_context)
            }
            StateBaseline::NewCheckpoint => Snapshot::prepare_new_from_log_segment(
                existing.table_root(),
                &segment,
                engine,
                &metric_context,
                self.crc_replay,
                self.target.is_latest(),
            ),
        }
        .inspect_err(|error| report_failure(error, &segment))?;

        let PreparedSnapshot {
            table_configuration,
            crc,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            captured_checkpoint_action,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit,
        } = prepared;
        tracing::Span::current().record("version", table_configuration.version());
        Ok(Arc::new(Snapshot::new_with_validated_crc(
            segment,
            table_configuration,
            crc,
            self.target.is_latest(),
            self.checkpoint_handling == CheckpointHandling::Ignore,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            captured_checkpoint_action,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            last_manifest_commit,
        )))
    }
}

/// Resolves state at `segment.end_version` while reusing the existing table configuration.
///
/// The segment's checkpoint must be at or below `existing.version()`, with every later commit
/// retained. State resolution supplies protocol/metadata replacements and an optional CRC;
/// configuration and CRC validation must succeed before the caller can construct the snapshot.
fn prepare_incremental(
    existing: &Snapshot,
    segment: &LogSegment,
    engine: &dyn Engine,
    crc_replay: IncrementalReplay,
    metric_context: &SnapshotLoadMetricContext,
) -> KernelResult<PreparedSnapshot> {
    let start = Instant::now();
    let existing_config = existing.table_configuration();
    let (resolution, crc) = resolve_incremental_state(existing, segment, engine, crc_replay)
        .inspect_err(|_| emit_protocol_metadata_load_failure(metric_context))?;
    emit_protocol_metadata_load(metric_context, resolution.source, start.elapsed());

    let table_configuration = TableConfiguration::try_new_from(
        existing_config,
        resolution.metadata,
        resolution.protocol,
        segment.end_version,
    )?;
    tracing::Span::current().record("version", table_configuration.version());
    let crc = Snapshot::validate_configuration_and_crc(segment, &table_configuration, crc)?;
    Ok(PreparedSnapshot {
        table_configuration,
        crc,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        captured_checkpoint_action: resolution.captured_checkpoint_action,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        last_manifest_commit: resolution.last_manifest_commit,
    })
}

/// Resolves protocol/metadata changes and the CRC to retain for the updated snapshot.
///
/// CRC replay starts from the newest usable disk or in-memory CRC and measures its budget from
/// that CRC's version, which may predate the snapshot. Without a target CRC, protocol and metadata
/// start from the existing snapshot or a newer CRC. A stale CRC may still be retained for future
/// updates; it cannot replace newer protocol or metadata.
fn resolve_incremental_state(
    existing: &Snapshot,
    segment: &LogSegment,
    engine: &dyn Engine,
    crc_replay: IncrementalReplay,
) -> KernelResult<(PmResolution, Option<Arc<Crc>>)> {
    let base_crc = segment.pick_latest_base_crc(engine, existing.base_crc())?;
    let crc_at_version =
        segment.try_build_crc_within_budget(engine, base_crc.as_ref(), crc_replay)?;
    let resolution = match &crc_at_version {
        Some((crc, source)) => {
            let mut resolution = PmResolution::from_crc(crc, *source);
            // None preserves the existing configuration, including its parsed schema
            // and properties.
            let existing_config = existing.table_configuration();
            resolution
                .metadata
                .take_if(|metadata| metadata == existing_config.metadata());
            resolution
                .protocol
                .take_if(|protocol| protocol == existing_config.protocol());
            resolution
        }
        None => {
            let newer_crc = base_crc
                .as_ref()
                .filter(|crc| crc.version > existing.version());
            match newer_crc {
                Some(crc) => segment.read_protocol_metadata_opt(engine, Some(crc))?,
                None => segment.read_protocol_metadata_after(engine, existing.version())?,
            }
        }
    };
    Ok((resolution, crc_at_version.map(|(crc, _)| crc).or(base_crc)))
}

/// Reuses unchanged table state, returning the same `Arc` when the build flags also match.
///
/// A latest-version request can set `built_as_latest` but cannot clear it. The checkpoint flag
/// records the current request's policy even when discovery found no new files.
fn reuse_with_build_metadata(
    existing: &Arc<Snapshot>,
    built_as_latest: bool,
    skipped_new_checkpoints: bool,
) -> KernelResult<Arc<Snapshot>> {
    let built_as_latest = existing.built_as_latest || built_as_latest;
    if existing.built_as_latest == built_as_latest
        && existing.skipped_new_checkpoints == skipped_new_checkpoints
    {
        return Ok(existing.clone());
    }
    Ok(Arc::new(Snapshot::new_with_crc(
        existing.log_segment.clone(),
        existing.table_configuration.clone(),
        existing.base_crc().cloned(),
        built_as_latest,
        skipped_new_checkpoints,
    )?))
}
