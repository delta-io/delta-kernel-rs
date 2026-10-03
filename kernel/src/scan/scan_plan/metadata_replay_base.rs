use crate::checkpoint::CheckpointShape;
use crate::scan::Scan;
use crate::{Engine, Result};

/// Reconciled table state from which metadata replay starts.
pub(in crate::scan) enum MetadataReplayBase {
    /// Complete live file state from a CRC newer than the checkpoint.
    Crc { version: crate::Version },
    /// Latest checkpoint, including its resolved file-action topology.
    Checkpoint(CheckpointShape),
}

impl MetadataReplayBase {
    /// Select the newest eligible metadata base and resolve checkpoint shape only when needed.
    ///
    /// # Errors
    ///
    /// Returns an error when checkpoint inspection requires an unavailable plan executor, or when
    /// checkpoint shape resolution fails.
    pub(in crate::scan) fn try_new(scan: &Scan, engine: &dyn Engine) -> Result<Self> {
        let checkpoint_version = scan.snapshot.log_segment().checkpoint_version;
        let newer_crc = scan.snapshot.base_crc_all_files().filter(|(version, _)| {
            checkpoint_version.is_none_or(|checkpoint| *version > checkpoint)
        });
        if let Some((version, _)) = newer_crc {
            return Ok(Self::Crc { version });
        }

        let plan_executor = engine.require_plan_executor()?;
        let needs_leaf_schema = scan.state_info.physical_stats_read_schema().is_some()
            || scan.state_info.physical_partition_schema.is_some();
        let shape = if needs_leaf_schema {
            CheckpointShape::try_new_with_leaf_schema(plan_executor.as_ref(), &scan.snapshot)?
        } else {
            CheckpointShape::try_new(plan_executor.as_ref(), &scan.snapshot)?
        };
        Ok(Self::Checkpoint(shape))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::super::tests::{
        add, checkpoint_path, log_root, log_segment, mock_snapshot_with_crc,
    };
    use super::*;
    use crate::crc::Crc;
    use crate::engine::sync::SyncEngine;
    use crate::engine::test_delegating::DelegatingEngine;
    use crate::plans::ir::nodes::{FileType, Operator};
    use crate::unit_test_utils::load_test_table;
    use crate::KernelError;

    #[rstest]
    #[case::crc_without_checkpoint(4, None, true)]
    #[case::crc_equal_to_checkpoint(5, Some(5), false)]
    fn selects_crc_without_executor_only_when_newer_than_checkpoint(
        #[case] version: crate::Version,
        #[case] checkpoint_version: Option<crate::Version>,
        #[case] crc_wins: bool,
    ) -> Result<()> {
        let (engine, latest, _tempdir) =
            load_test_table("v1-multi-part-partitioned-struct-stats-only")?;
        let snapshot = crate::Snapshot::builder_for(latest.table_root().clone())
            .at_version(version)
            .build(engine.as_ref())?;
        assert_eq!(
            snapshot.log_segment().checkpoint_version,
            checkpoint_version
        );
        assert_eq!(
            snapshot.base_crc_all_files().map(|(version, _)| version),
            Some(version)
        );
        let scan = snapshot.scan_builder().build()?;

        let no_plan_engine = DelegatingEngine::new(engine).without_plan_executor();
        let base = MetadataReplayBase::try_new(&scan, &no_plan_engine);
        if crc_wins {
            assert!(matches!(
                base?,
                MetadataReplayBase::Crc { version: base } if base == version
            ));
        } else {
            assert!(matches!(base, Err(KernelError::Unsupported(_))));
        }
        Ok(())
    }

    #[test]
    fn crc_all_files_bounds_commit_replay() -> Result<()> {
        let segment = log_segment(
            log_root(),
            &[
                "file:///_delta_log/00000000000000000001.json",
                "file:///_delta_log/00000000000000000002.json",
            ],
            Some(checkpoint_path(FileType::Parquet)),
        );
        let snapshot = mock_snapshot_with_crc(
            segment,
            Some(Crc {
                version: 1,
                all_files: Some(vec![add("a.parquet")]),
                ..Default::default()
            }),
        )?;
        let scan = snapshot.scan_builder().build()?;
        let no_plan_engine =
            DelegatingEngine::new(Arc::new(SyncEngine::new())).without_plan_executor();
        let base = MetadataReplayBase::try_new(&scan, &no_plan_engine)?;

        let plan = scan.build_metadata_scan_plan(&base)?.expect("non-empty");
        let json_paths: Vec<_> = plan
            .nodes
            .iter()
            .filter_map(|node| match &node.op {
                Operator::ScanJson(scan) => Some(&scan.files),
                _ => None,
            })
            .flatten()
            .map(|file| file.meta.location.path())
            .collect();
        assert_eq!(json_paths, ["/_delta_log/00000000000000000002.json"]);
        Ok(())
    }
}
