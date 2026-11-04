//! Updates an existing snapshot from newly discovered log files.
//!
//! [`SnapshotUpdate`] receives the builder's resolved target and validated catalog log tail.
//! [`LogUpdate`] selects the retained files and determines whether the existing snapshot can
//! supply the table state. A checkpoint at or below the existing version changes the retained
//! files but leaves that state usable. A checkpoint ahead of it requires rebuilding state.
//!
//! CRC replay is an optional optimization within state resolution. A stale CRC may be retained
//! for future updates, but only a CRC newer than the existing snapshot may replace its metadata.

use std::sync::Arc;
use std::time::Instant;

use log_update::{LogUpdate, StateBaseline};
use tracing::{error, instrument};

use super::{IncrementalReplay, PreparedSnapshot, Snapshot};
use crate::log_segment::{LogSegment, PmResolution};
use crate::log_segment_files::CheckpointHandling;
use crate::metrics::{
    SnapshotLoadMetricContext, emit_log_segment_load, emit_log_segment_load_failure,
    emit_protocol_metadata_load, emit_protocol_metadata_load_failure,
};
use crate::path::ParsedLogPath;
use crate::table_configuration::TableConfiguration;
use crate::{Engine, KernelError, KernelResult, Version};

mod log_update;

#[cfg(test)]
mod legacy_tests;
#[cfg(test)]
mod tests;

/// Inputs for an incremental build after the builder validates catalog version and tail inputs.
pub(super) struct SnapshotUpdate {
    pub(super) existing: Arc<Snapshot>,
    pub(super) target: UpdateTarget,
    /// Catalog-supplied commits merged with storage listings, which also supply checkpoints/CRCs.
    pub(super) log_tail: Vec<ParsedLogPath>,
    /// Whether discovery may replace the existing checkpoint and discard the history it covers.
    pub(super) checkpoint_handling: CheckpointHandling,
    /// Budget for advancing a CRC; does not limit protocol/metadata replay needed for correctness.
    pub(super) crc_replay: IncrementalReplay,
}

/// A version requirement and its meaning for the snapshot's `built_as_latest` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UpdateTarget {
    /// Time travel to this exact version, without claiming it is latest.
    Version(Version),
    /// Discover the latest version from storage.
    Latest,
    /// Build at the catalog's latest ratified version, even if storage contains later commits.
    CatalogLatest(Version),
}

impl SnapshotUpdate {
    /// Builds the requested snapshot, reusing the input state whenever the selected files allow it.
    ///
    /// An equal target returns the input `Arc` without I/O or build-flag changes; an older target
    /// is rejected. An unbounded latest request may refresh files and build flags at the same
    /// version.
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

impl UpdateTarget {
    /// The required end version; `None` lets discovery determine the latest version.
    fn version(self) -> Option<Version> {
        match self {
            Self::Version(version) | Self::CatalogLatest(version) => Some(version),
            Self::Latest => None,
        }
    }

    fn is_latest(self) -> bool {
        !matches!(self, Self::Version(_))
    }
}

/// Resolves state above `existing` using a segment whose checkpoint is at or below its version.
///
/// CRC construction uses the newest eligible CRC, even if it predates `existing`. Protocol and
/// metadata instead start from `existing` or a newer CRC so an older CRC cannot rewind table state.
fn prepare_incremental(
    existing: &Snapshot,
    segment: &LogSegment,
    engine: &dyn Engine,
    crc_replay: IncrementalReplay,
    metric_context: &SnapshotLoadMetricContext,
) -> KernelResult<PreparedSnapshot> {
    let start = Instant::now();
    let existing_config = existing.table_configuration();
    let (resolution, crc) = (|| {
        let base_crc = segment.pick_latest_base_crc(engine, existing.base_crc())?;
        let crc_at_version =
            segment.try_build_crc_within_budget(engine, base_crc.as_ref(), crc_replay)?;
        let resolution = match &crc_at_version {
            Some((crc, source)) => {
                let mut resolution = PmResolution::from_crc(crc, *source);
                // Unchanged values let TableConfiguration reuse its derived schema and properties.
                resolution
                    .metadata
                    .take_if(|m| m == existing_config.metadata());
                resolution
                    .protocol
                    .take_if(|p| p == existing_config.protocol());
                resolution
            }
            None => {
                let newer_crc = base_crc
                    .as_ref()
                    .filter(|crc| crc.version > existing.version());
                segment
                    .segment_after_version(existing.version())
                    .read_protocol_metadata_opt(engine, newer_crc)?
            }
        };
        Ok((resolution, crc_at_version.map(|(crc, _)| crc).or(base_crc)))
    })()
    .inspect_err(|_: &KernelError| emit_protocol_metadata_load_failure(metric_context))?;
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

/// Records a latest-version refresh and checkpoint policy without changing table state.
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
