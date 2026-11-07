//! Reconstruction of unchanged state exported by a validated snapshot.

use std::sync::Arc;

use delta_kernel_derive::internal_api;
use url::Url;

use super::{PublicationWatermark, Snapshot, SnapshotHintFreshness, SnapshotRef};
use crate::actions::{Metadata, Protocol};
use crate::crc::Crc;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::log_segment::LogSegment;
use crate::log_segment_files::{CheckpointHandling, LogSegmentFiles};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::schema::SchemaRef;
use crate::table_configuration::TableConfiguration;
use crate::{KernelResult, LogPath, Version};

/// Unchanged components exported by one validated snapshot from the same Kernel build.
///
/// The metadata includes its original schema string; `logical_schema` supplies the decoded schema.
/// This input is not suitable for connector-authored hints.
#[internal_api]
pub(crate) struct SourceSnapshotState {
    pub table_root: Url,
    pub version: Version,
    pub publication_watermark: PublicationWatermark,
    pub log_paths: Vec<LogPath>,
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub logical_schema: SchemaRef,
    pub last_checkpoint: Option<LastCheckpointHint>,
    pub crc: Option<Arc<Crc>>,
    pub freshness: SnapshotHintFreshness,
}

/// Reconstructs an ordinary snapshot without repeating source-proven log-shape checks.
///
/// Configuration and snapshot-load checks still run. The returned snapshot owns the reconstructed
/// state, and ordinary scans retain their own reference to it.
#[internal_api]
pub(crate) fn snapshot_from_source(state: SourceSnapshotState) -> KernelResult<SnapshotRef> {
    let segment = source_log_segment(
        &state.table_root,
        state.version,
        state.publication_watermark,
        state.log_paths,
        state.last_checkpoint,
    )?;
    let configuration = TableConfiguration::try_new_from_schema(
        state.metadata,
        state.protocol,
        state.table_root,
        state.version,
        state.logical_schema,
    )?;
    Snapshot::new_with_crc(
        segment,
        configuration,
        state.crc,
        state.freshness == SnapshotHintFreshness::Latest,
        false,
    )
    .map(Arc::new)
}

fn source_log_segment(
    table_root: &Url,
    version: Version,
    watermark: PublicationWatermark,
    paths: Vec<LogPath>,
    last_checkpoint: Option<LastCheckpointHint>,
) -> KernelResult<LogSegment> {
    let mut commits = Vec::with_capacity(paths.len());
    let mut other = Vec::new();
    for path in paths {
        let path = ParsedLogPath::from(path);
        if path.is_commit() {
            commits.push(path);
        } else {
            other.push(path);
        }
    }
    // Published and staged paths are exported by URL, which need not be version order.
    commits.sort_unstable_by_key(|path| path.version);
    let mut listed = LogSegmentFiles::build_log_segment_files(
        other.into_iter().map(Ok),
        Vec::new(),
        0,
        None,
        CheckpointHandling::Adopt,
    )?;
    let checkpoint_version = listed.checkpoint_parts.first().map(|path| path.version);
    listed.max_published_version = match watermark {
        PublicationWatermark::InferFromLogPaths => commits
            .iter()
            .filter(|path| path.file_type == LogPathFileType::Commit)
            .map(|path| path.version)
            .max(),
        PublicationWatermark::NoPublishedCommits => None,
        PublicationWatermark::PublishedThrough(version) => Some(version),
    };
    listed.latest_commit_file = commits
        .iter()
        .rev()
        .find(|path| checkpoint_version.is_none_or(|version| path.version >= version))
        .cloned();
    commits.retain(|path| checkpoint_version.is_none_or(|version| path.version > version));
    listed.ascending_commit_files = commits;
    Ok(LogSegment {
        end_version: version,
        checkpoint_version,
        log_root: table_root.join("_delta_log/")?,
        listed,
        last_checkpoint_metadata: last_checkpoint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileMeta;

    #[rstest::rstest]
    #[case::inferred(PublicationWatermark::InferFromLogPaths, Some(2))]
    #[case::absent(PublicationWatermark::NoPublishedCommits, None)]
    #[case::explicit(PublicationWatermark::PublishedThrough(0), Some(0))]
    fn mixed_commit_paths_are_sorted_by_version(
        #[case] watermark: PublicationWatermark,
        #[case] expected_published: Option<Version>,
    ) {
        let root = Url::parse("memory:///table/").unwrap();
        let log_root = root.join("_delta_log/").unwrap();
        let paths = [
            "00000000000000000000.checkpoint.parquet",
            "00000000000000000002.json",
            "_staged_commits/00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
            "_staged_commits/00000000000000000003.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
        ]
        .into_iter()
        .map(|name| {
            LogPath::try_new(FileMeta {
                location: log_root.join(name).unwrap(),
                last_modified: 0,
                size: 1,
            })
            .unwrap()
        })
        .collect();
        let segment = source_log_segment(&root, 3, watermark, paths, None).unwrap();
        assert_eq!(segment.checkpoint_version, Some(0));
        assert_eq!(segment.listed.max_published_version, expected_published);
        assert_eq!(
            segment
                .listed
                .ascending_commit_files
                .iter()
                .map(|path| path.version)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(segment.listed.latest_commit_file.unwrap().version, 3);
    }
}
