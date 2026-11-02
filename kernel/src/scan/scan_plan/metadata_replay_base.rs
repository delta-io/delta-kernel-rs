use super::MetadataPlanner;
use crate::actions::Add;
use crate::checkpoint::CheckpointShape;
use crate::{Engine, KernelResult, Snapshot, Version};

/// Reconciled table state from which metadata replay starts.
pub(in crate::scan) enum MetadataReplayBase {
    /// Complete live file state from a CRC at least as new as the checkpoint.
    Crc { version: Version, files: Vec<Add> },
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
        if let Some((version, files)) = snapshot
            .base_crc()
            .and_then(|crc| Some((crc.version, crc.all_files()?)))
            .filter(|(version, _)| {
                checkpoint_version.is_none_or(|checkpoint| *version >= checkpoint)
            })
        {
            return Ok(Self::Crc {
                version,
                files: files.to_vec(),
            });
        }

        // Resolve the checkpoint shape once. The planner owns the decision to retain the file-
        // action schema; checkpoint discovery owns how that schema is obtained.
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

    /// Exclusive lower bound for commit replay; `None` replays all available commits.
    pub(super) fn version(&self) -> Option<Version> {
        match self {
            Self::Crc { version, .. } => Some(*version),
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
    #[case::without_checkpoint(4, None, true)]
    #[case::equal_to_checkpoint(5, Some(5), true)]
    #[case::without_all_files(5, Some(5), false)]
    fn crc_selection_and_checkpoint_fallback(
        #[case] version: Version,
        #[case] checkpoint_version: Option<Version>,
        #[case] has_all_files: bool,
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
        let mut crc = snapshot.crc_at_version().expect("fixture CRC").clone();
        let mut engine = DelegatingEngine::new(engine);
        if has_all_files {
            engine = engine.without_plan_executor();
        } else {
            Arc::make_mut(&mut crc).all_files = None;
        }
        let segment = snapshot.log_segment().clone();
        let config = snapshot.table_configuration().clone();
        let crc = Some(crc);
        let snapshot = Arc::new(Snapshot::new_with_crc(segment, config, crc, false, false)?);
        let scan = snapshot.scan_builder().build()?;
        let planner = MetadataPlanner::try_new(&scan)?;
        let base = MetadataReplayBase::try_new(&scan.snapshot, &engine, &planner)?;
        assert_eq!(base.version(), Some(version));
        let is_crc = matches!(base, MetadataReplayBase::Crc { .. });
        assert_eq!(is_crc, has_all_files);
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
        let base_crc = base.base_crc().expect("version 4 CRC");
        let version = base_crc.version;
        let files = base_crc.all_files().expect("CRC allFiles");
        let crc = Some(Arc::new(Crc {
            version,
            all_files: Some(if empty_crc { vec![] } else { files.to_vec() }),
            ..Default::default()
        }));
        let log_root = latest.log_segment().log_root.clone();
        let commits: Vec<_> = (0..=target_version)
            .map(|version| format!("{log_root}{version:020}.json"))
            .collect();
        let paths: Vec<_> = commits.iter().map(String::as_str).collect();
        let segment = log_segment(log_root, &paths, None);
        let config = target.table_configuration().clone();
        let snapshot = Arc::new(Snapshot::new_with_crc(segment, config, crc, false, false)?);
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
            .flat_map(|node| match &node.op {
                Operator::ScanJson(scan) => scan.files.as_slice(),
                _ => &[],
            })
            .map(|file| file.meta.location.to_string())
            .collect();
        assert_eq!(json_paths, commits[(version + 1) as usize..]);

        let row_count = engine
            .plan_executor()
            .expect("plan executor")
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?
            .try_fold(0, |count, batch| -> KernelResult<_> {
                Ok(count + batch?.try_into_record_batch()?.num_rows())
            })?;
        assert_eq!(row_count, expected_rows);
        Ok(())
    }
}
