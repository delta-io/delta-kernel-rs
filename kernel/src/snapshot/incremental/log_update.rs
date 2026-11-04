//! Selects the files retained by an updated snapshot and the state baseline they support.
//!
//! Discovery merges storage listings with the catalog tail. Planning and file assembly use only
//! those paths and the existing segment; they neither read actions nor resolve CRC contents.

use crate::log_segment::LogSegment;
use crate::log_segment_files::{CheckpointHandling, LogSegmentFiles};
use crate::path::ParsedLogPath;
use crate::{KernelError, KernelResult, StorageHandler, Version};

/// The files for an update, and whether their retained history can extend the existing state.
#[derive(Debug)]
pub(super) struct LogUpdate {
    pub(super) segment: LogSegment,
    pub(super) baseline: StateBaseline,
}

/// Which state remains usable after checkpoint selection discards covered commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StateBaseline {
    ExistingSnapshot,
    /// A checkpoint ahead of the snapshot replaces commits needed to extend the existing state.
    NewCheckpoint,
}

impl LogUpdate {
    /// Discovers files and plans an update after the caller has rejected equal or older targets.
    /// Returns `None` when the existing files can be reused unchanged.
    pub(super) fn discover(
        existing: &LogSegment,
        log_tail: Vec<ParsedLogPath>,
        target: Option<Version>,
        checkpoint_handling: CheckpointHandling,
        storage: &dyn StorageHandler,
    ) -> KernelResult<Option<Self>> {
        let Some(start) = listing_start(existing, checkpoint_handling) else {
            return Ok(None);
        };
        let listed = LogSegmentFiles::list_with_checkpoint_handling(
            storage,
            &existing.log_root,
            log_tail,
            Some(start),
            target,
            checkpoint_handling,
        )?;
        Self::from_listing(existing, listed, target)
    }

    /// Assembles discovered paths without I/O. `target`, when set, must exceed the existing
    /// version.
    ///
    /// Returns `None` to preserve the existing segment. An empty listing permits this only for an
    /// unbounded latest request. A checkpoint at or below the existing version can change retained
    /// files even when the version stays equal.
    pub(super) fn from_listing(
        existing: &LogSegment,
        listed: LogSegmentFiles,
        target: Option<Version>,
    ) -> KernelResult<Option<Self>> {
        if listed.ascending_commit_files().is_empty() && listed.checkpoint_parts().is_empty() {
            return match target {
                Some(_) => Err(KernelError::MissingVersion(existing.end_version + 1)),
                None => Ok(None),
            };
        }
        let discovered = LogSegment::try_new(listed, existing.log_root.clone(), target, None)?;
        if discovered.end_version < existing.end_version {
            return Err(KernelError::invalid_log_segment(format!(
                "Unexpected state: the newest version in the log {} is \
                 older than the existing snapshot version {}",
                discovered.end_version, existing.end_version
            )));
        }
        if discovered
            .checkpoint_version
            .is_some_and(|v| v > existing.end_version)
        {
            return Ok(Some(Self {
                segment: discovered,
                baseline: StateBaseline::NewCheckpoint,
            }));
        }
        if discovered.end_version == existing.end_version && discovered.checkpoint_version.is_none()
        {
            return Ok(None);
        }
        Ok(Some(Self {
            segment: merge_segments(existing, discovered, target)?,
            baseline: StateBaseline::ExistingSnapshot,
        }))
    }
}

/// Adopt searches for checkpoints after the retained checkpoint. Ignore needs only newer commits.
/// `None` means the retained starting version is `Version::MAX` and cannot be advanced.
pub(super) fn listing_start(
    existing: &LogSegment,
    checkpoint_handling: CheckpointHandling,
) -> Option<Version> {
    let after = match checkpoint_handling {
        CheckpointHandling::Adopt => existing.checkpoint_version.unwrap_or(0),
        CheckpointHandling::Ignore => existing.end_version,
    };
    after.checked_add(1)
}

/// Keeps existing paths above the adopted checkpoint and appends only newly discovered versions.
///
/// Existing paths win in the overlap, including staged commits that have since been published.
/// The publication watermark still advances from discovery independently of those retained paths.
fn merge_segments(
    existing: &LogSegment,
    mut discovered: LogSegment,
    target: Option<Version>,
) -> KernelResult<LogSegment> {
    let checkpoint = discovered.checkpoint_version;
    let crc_file = resolve_crc_file(&discovered, existing);
    let mut commits = files_after_checkpoint(&existing.listed.ascending_commit_files, checkpoint);
    commits.extend(
        discovered
            .listed
            .ascending_commit_files
            .into_iter()
            .filter(|path| path.version > existing.end_version),
    );
    let mut compactions =
        files_after_checkpoint(&existing.listed.ascending_compaction_files, checkpoint);
    // Compactions crossing the existing version cannot replace just the new portion of history.
    compactions.extend(
        discovered
            .listed
            .ascending_compaction_files
            .into_iter()
            .filter(|path| path.version > existing.end_version),
    );
    let checkpoint_hint = if checkpoint.is_some() {
        // Discovery does not read _last_checkpoint, so the adopted checkpoint has no cached hint.
        discovered.last_checkpoint_metadata
    } else {
        discovered.listed.checkpoint_parts = existing.listed.checkpoint_parts.clone();
        existing.last_checkpoint_metadata.clone()
    };
    LogSegment::try_new(
        LogSegmentFiles {
            ascending_commit_files: commits,
            ascending_compaction_files: compactions,
            checkpoint_parts: discovered.listed.checkpoint_parts,
            latest_crc_file: crc_file,
            latest_commit_file: discovered.listed.latest_commit_file,
            max_published_version: discovered
                .listed
                .max_published_version
                .max(existing.listed.max_published_version),
        },
        existing.log_root.clone(),
        target,
        checkpoint_hint,
    )
}

/// Prefers the discovered CRC file, retaining the existing one only if the checkpoint permits it.
/// This selects a file path; state resolution separately selects the in-memory CRC baseline.
fn resolve_crc_file(discovered: &LogSegment, existing: &LogSegment) -> Option<ParsedLogPath> {
    discovered.listed.latest_crc_file.clone().or_else(|| {
        existing.listed.latest_crc_file.clone().filter(|crc| {
            discovered
                .checkpoint_version
                .is_none_or(|checkpoint| crc.version >= checkpoint)
        })
    })
}

fn files_after_checkpoint(
    files: &[ParsedLogPath],
    checkpoint: Option<Version>,
) -> Vec<ParsedLogPath> {
    files
        .iter()
        .filter(|file| checkpoint.is_none_or(|v| file.version > v))
        .cloned()
        .collect()
}
