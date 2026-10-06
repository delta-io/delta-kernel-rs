//! Scan input validation with bounded commit-path scratch space.

use super::*;
use crate::error::SnapshotHintError;
use crate::log_segment_files::CheckpointHandling;
use crate::snapshot::{SnapshotLogPathIterator, SnapshotScanState};

impl LogSegment {
    /// Select checkpoints in a first pass, then validate commits directly into plan output.
    /// The returned segment supplies checkpoint information only; commit inputs live in `files`.
    /// Both passes read the same immutable, ordered connector state.
    pub(crate) fn stream_scan_inputs(
        state: &dyn SnapshotScanState,
        paths: SnapshotLogPathIterator<'_>,
    ) -> KernelResult<(Self, Vec<ScanFile>)> {
        let result = (|| {
            let paths = checked_paths(paths).filter_map(|result| match result {
                Ok(path) if path.is_commit() => None,
                other => Some(other),
            });
            let mut listed = LogSegmentFiles::build_log_segment_files(
                paths,
                Vec::new(),
                0,
                None,
                CheckpointHandling::Adopt,
            )?;
            validate_log_path_fields(&listed)?;
            validate_compaction_files(&listed.ascending_compaction_files)?;
            validate_checkpoint_parts(&listed.checkpoint_parts)?;
            let checkpoint_version = listed.checkpoint_parts.first().map(|p| p.version);
            let paths = state.ordered_log_paths()?.ok_or_else(|| {
                KernelError::generic(
                    "Connector removed its ordered log path source during planning",
                )
            })?;
            let mut previous: Option<ParsedLogPath> = None;
            let mut files = Vec::new();
            for path in checked_paths(paths) {
                let path = path?;
                if path.file_type == LogPathFileType::Commit {
                    listed.max_published_version =
                        listed.max_published_version.max(Some(path.version));
                }
                if !path.is_commit() {
                    continue;
                }
                if checkpoint_version.is_some_and(|v| path.version < v) {
                    continue;
                }
                listed.latest_commit_file = Some(path.clone());
                if checkpoint_version == Some(path.version) {
                    continue;
                }
                validate_commit_file_types(std::slice::from_ref(&path))?;
                if let Some(previous) = previous.take() {
                    let pair = [previous, path.clone()];
                    validate_commit_files_sorted(&pair)?;
                    validate_commit_files_contiguous(&pair)?;
                } else {
                    validate_checkpoint_commit_gap(
                        checkpoint_version,
                        std::slice::from_ref(&path),
                    )?;
                }
                let version = path.version_as_i64()?;
                previous = Some(path.clone());
                files.push(ScanFile {
                    meta: path.location,
                    file_constants: vec![Scalar::Long(version)],
                });
            }
            let effective_version = validate_end_version(
                previous.as_slice(),
                &listed.checkpoint_parts,
                Some(state.version()),
            )?;
            validate_latest_commit_file(&listed, effective_version)?;
            validate_crc(
                listed.latest_crc_file.as_ref(),
                checkpoint_version,
                effective_version,
            )?;
            // Commit-cover planning reads newest first. No second copy of the input list is needed.
            files.reverse();
            Ok((
                Self {
                    end_version: effective_version,
                    checkpoint_version,
                    log_root: state.table_root().join("_delta_log/")?,
                    last_checkpoint_metadata: state.last_checkpoint()?,
                    listed,
                },
                files,
            ))
        })();
        result.map_err(|source| {
            SnapshotHintError::LogSegment {
                source: Box::new(source),
            }
            .into()
        })
    }
}

fn checked_paths(
    paths: SnapshotLogPathIterator<'_>,
) -> impl Iterator<Item = KernelResult<ParsedLogPath>> + '_ {
    let mut previous = None;
    paths.map(move |path| {
        let path: ParsedLogPath = path?.into();
        require!(
            !matches!(path.file_type, LogPathFileType::CompactedCommit { .. }),
            SnapshotHintError::LogCompaction.into()
        );
        let key = (path.version, path.filename.clone());
        require!(
            previous.as_ref().is_none_or(|p| p <= &key),
            KernelError::invalid_log_segment(
                "Connector log paths are not ordered by version and filename"
            )
        );
        previous = Some(key);
        let reparsed = ParsedLogPath::try_from(path.location.clone())?
            .ok_or_else(|| KernelError::invalid_log_path(path.location.location.as_str()))?;
        require!(
            reparsed == path,
            KernelError::invalid_log_path("Parsed log path fields do not match its location")
        );
        Ok(path)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::{Metadata, Protocol};
    use crate::log_path::LogPath;
    use crate::snapshot::{log_segment_from_state, SnapshotLogState};

    struct State {
        root: Url,
        version: Version,
        paths: Vec<LogPath>,
    }

    impl SnapshotLogState for State {
        fn table_root(&self) -> &Url {
            &self.root
        }
        fn version(&self) -> Version {
            self.version
        }
        fn is_latest(&self) -> bool {
            false
        }
        fn last_checkpoint(&self) -> KernelResult<Option<LastCheckpointHint>> {
            Ok(None)
        }
        fn visit_log_paths(
            &self,
            visitor: &mut dyn FnMut(&[LogPath]) -> KernelResult<()>,
        ) -> KernelResult<()> {
            visitor(&self.paths)
        }
        fn ordered_log_paths(&self) -> KernelResult<Option<SnapshotLogPathIterator<'_>>> {
            Ok(Some(Box::new(self.paths.iter().cloned().map(Ok))))
        }
    }
    impl SnapshotScanState for State {
        fn protocol(&self) -> KernelResult<Protocol> {
            Err(KernelError::generic("not used"))
        }
        fn metadata(&self) -> KernelResult<Metadata> {
            Err(KernelError::generic("not used"))
        }
        fn logical_schema(&self) -> KernelResult<SchemaRef> {
            Err(KernelError::generic("not used"))
        }
    }

    fn state(names: &[String], version: Version) -> State {
        let root = Url::parse("memory:///table/").unwrap();
        let mut names = names.to_vec();
        names.sort();
        let paths = names
            .iter()
            .map(|name| {
                LogPath::try_new(FileMeta {
                    location: root.join(&format!("_delta_log/{name}")).unwrap(),
                    size: 100,
                    last_modified: 0,
                })
                .unwrap()
            })
            .collect();
        State {
            root,
            version,
            paths,
        }
    }

    fn compare(state: &State) {
        let baseline = log_segment_from_state(state);
        let streamed =
            LogSegment::stream_scan_inputs(state, state.ordered_log_paths().unwrap().unwrap());
        match (baseline, streamed) {
            (Ok(baseline), Ok((checkpoint, files))) => {
                assert!(checkpoint.listed.ascending_commit_files.is_empty());
                assert_eq!(checkpoint.end_version, baseline.end_version);
                assert_eq!(checkpoint.checkpoint_version, baseline.checkpoint_version);
                assert_eq!(
                    checkpoint.listed.checkpoint_parts,
                    baseline.listed.checkpoint_parts
                );
                assert_eq!(
                    checkpoint.listed.latest_crc_file,
                    baseline.listed.latest_crc_file
                );
                assert_eq!(
                    checkpoint.listed.latest_commit_file,
                    baseline.listed.latest_commit_file
                );
                assert_eq!(
                    files,
                    baseline.commit_cover_version_tagged_scan_files().unwrap()
                );
            }
            (Err(_), Err(_)) => {}
            (baseline, streamed) => panic!("baseline={baseline:?}, streamed={streamed:?}"),
        }
    }

    #[test]
    fn streamed_log_inputs_match_materialized_validation_and_selection() {
        for names in [
            vec![],
            vec!["00000000000000000000.json"],
            vec!["00000000000000000000.json", "00000000000000000002.json"],
            vec!["00000000000000000002.json", "00000000000000000002.json"],
            vec![
                "00000000000000000000.checkpoint.parquet",
                "00000000000000000002.json",
            ],
            vec![
                "00000000000000000001.checkpoint.parquet",
                "00000000000000000002.json",
            ],
            vec![
                "00000000000000000002.checkpoint.parquet",
                "00000000000000000002.json",
            ],
            vec!["00000000000000000002.checkpoint.0000000001.0000000002.parquet"],
            vec![
                "00000000000000000002.checkpoint.parquet",
                "00000000000000000003.crc",
            ],
            vec!["00000000000000000000.00000000000000000002.compacted.json"],
        ] {
            let names: Vec<_> = names.into_iter().map(String::from).collect();
            for version in [0, 1, 2, 3] {
                compare(&state(&names, version));
            }
        }
        let mut names: Vec<_> = (1..=1000).map(|v| format!("{v:020}.json")).collect();
        names.push("00000000000000000000.checkpoint.parquet".into());
        names.push("00000000000000000500.checkpoint.parquet".into());
        compare(&state(&names, 1000));
    }
}
