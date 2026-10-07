use super::MetadataPlanner;
use crate::checkpoint::CheckpointShape;
use crate::{Engine, KernelResult, Snapshot, Version};

/// Reconciled table state from which metadata replay starts.
pub(in crate::scan) enum MetadataReplayBase {
    /// Complete live file state from a CRC at least as new as the checkpoint.
    Crc { version: Version },
    /// Checkpoint replay base, or an empty base when no checkpoint exists.
    Checkpoint {
        version: Option<Version>,
        shape: CheckpointShape,
    },
}

impl MetadataReplayBase {
    /// Select the newest eligible metadata base and resolve checkpoint shape only when needed.
    pub(in crate::scan) fn try_new(
        snapshot: &Snapshot,
        engine: &dyn Engine,
        planner: &MetadataPlanner<'_>,
    ) -> KernelResult<Self> {
        let checkpoint_version = snapshot.log_segment().checkpoint_version;
        // Keep replay eligibility independent of SnapshotCrc's validation.
        if let Some((version, _)) = snapshot.base_crc_all_files().filter(|(version, _)| {
            checkpoint_version.is_none_or(|checkpoint| *version >= checkpoint)
        }) {
            return Ok(Self::Crc { version });
        }

        let plan_executor = engine.require_plan_executor()?;
        let shape = if planner.requires_checkpoint_add_schema() {
            CheckpointShape::try_new_with_leaf_schema(plan_executor.as_ref(), snapshot)?
        } else {
            CheckpointShape::try_new(plan_executor.as_ref(), snapshot)?
        };
        Ok(Self::Checkpoint {
            version: checkpoint_version,
            shape,
        })
    }

    pub(super) fn version(&self) -> Option<Version> {
        match self {
            Self::Crc { version } => Some(*version),
            Self::Checkpoint { version, .. } => *version,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::crc::Crc;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::test_delegating::DelegatingEngine;
    use crate::plans::ir::nodes::Operator;
    use crate::plans::Operation as PlanOperation;
    use crate::scan::scan_plan::tests::log_segment;
    use crate::unit_test_utils::load_test_table;

    #[rstest]
    #[case::without_checkpoint(4, None)]
    #[case::equal_to_checkpoint(5, Some(5))]
    fn crc_not_older_than_checkpoint_is_selected_without_executor(
        #[case] version: Version,
        #[case] checkpoint_version: Option<Version>,
    ) -> KernelResult<()> {
        let (engine, latest, _tempdir) =
            load_test_table("v1-multi-part-partitioned-struct-stats-only")?;
        let snapshot = Snapshot::builder_for(latest.table_root().clone())
            .at_version(version)
            .build(engine.as_ref())?;
        assert_eq!(
            snapshot.log_segment().checkpoint_version,
            checkpoint_version
        );
        let scan = snapshot.scan_builder().build()?;
        let planner = MetadataPlanner::try_new(&scan)?;
        let no_plan_engine = DelegatingEngine::new(engine).without_plan_executor();

        assert!(matches!(
            MetadataReplayBase::try_new(&scan.snapshot, &no_plan_engine, &planner)?,
            MetadataReplayBase::Crc { version: selected } if selected == version
        ));
        Ok(())
    }

    #[test]
    fn crc_without_all_files_falls_back_to_checkpoint() -> KernelResult<()> {
        let (engine, latest, _tempdir) =
            load_test_table("v1-multi-part-partitioned-struct-stats-only")?;
        let crc = Crc {
            version: 5,
            all_files: None,
            ..Default::default()
        };
        let snapshot = Arc::new(Snapshot::new_with_crc(
            latest.log_segment().clone(),
            latest.table_configuration().clone(),
            Some(Arc::new(crc)),
            false,
            false,
        )?);
        assert_eq!(snapshot.log_segment().checkpoint_version, Some(5));
        let scan = snapshot.scan_builder().build()?;
        let planner = MetadataPlanner::try_new(&scan)?;

        assert!(matches!(
            MetadataReplayBase::try_new(&scan.snapshot, engine.as_ref(), &planner)?,
            MetadataReplayBase::Checkpoint {
                version: Some(5),
                ..
            }
        ));
        Ok(())
    }

    #[rstest]
    #[case::base_only(4, false, 4)]
    #[case::base_plus_commit(5, false, 5)]
    #[case::empty_base_only(4, true, 0)]
    #[case::empty_base_plus_commit(5, true, 1)]
    fn crc_bounds_replay_to_newer_commits(
        #[case] target_version: Version,
        #[case] empty_crc: bool,
        #[case] expected_rows: usize,
    ) -> KernelResult<()> {
        let (engine, latest, _tempdir) =
            load_test_table("v1-multi-part-partitioned-struct-stats-only")?;
        let target = Snapshot::builder_for(latest.table_root().clone())
            .at_version(target_version)
            .build(engine.as_ref())?;
        let base = Snapshot::builder_for(latest.table_root().clone())
            .at_version(4)
            .build(engine.as_ref())?;
        let (version, files) = base.base_crc_all_files().expect("version 4 CRC allFiles");
        let crc = Crc {
            version,
            all_files: Some(if empty_crc { vec![] } else { files.to_vec() }),
            ..Default::default()
        };
        let log_root = latest.log_segment().log_root.clone();
        let commits: Vec<_> = (0..=target_version)
            .map(|version| {
                log_root
                    .join(&format!("{version:020}.json"))
                    .unwrap()
                    .to_string()
            })
            .collect();
        let snapshot = Arc::new(Snapshot::new_with_crc(
            log_segment(
                log_root,
                &commits.iter().map(String::as_str).collect::<Vec<_>>(),
                None,
            ),
            target.table_configuration().clone(),
            Some(Arc::new(crc)),
            false,
            false,
        )?);
        let scan = snapshot.scan_builder().build()?;
        let plan = scan.declarative_metadata_scan_plan(
            &DelegatingEngine::new(engine.clone()).without_plan_executor(),
        )?;
        if expected_rows == 0 {
            assert!(plan.is_none());
            return Ok(());
        }
        let plan = plan.expect("non-empty");

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
        assert_eq!(json_paths.len(), (target_version - version) as usize);
        assert!(json_paths
            .iter()
            .all(|path| path.ends_with("00000000000000000005.json")));

        let row_count = engine
            .plan_executor()
            .expect("plan executor")
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?
            .try_fold(0, |count, batch| {
                Ok::<_, crate::KernelError>(count + batch?.try_into_record_batch()?.num_rows())
            })?;
        assert_eq!(row_count, expected_rows);
        Ok(())
    }
}
