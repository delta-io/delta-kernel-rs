use std::sync::Arc;

use crate::actions::Add;
use crate::checkpoint::CheckpointInfo;
use crate::metrics::{ReplayBase, ScanPlanReport};
use crate::{Engine, KernelResult, Snapshot, Version};

/// Reconciled table state from which metadata replay starts.
pub(in crate::scan) enum MetadataReplayBase {
    /// Complete live file state from a CRC at least as new as the checkpoint.
    Crc { version: Version, files: Vec<Add> },
    /// Checkpoint replay base, or an empty base when no checkpoint exists.
    Checkpoint {
        version: Option<Version>,
        info: Option<Arc<CheckpointInfo>>,
    },
}

impl MetadataReplayBase {
    /// Select the newest eligible metadata base and resolve checkpoint information only when
    /// needed.
    pub(in crate::scan) fn try_new(
        snapshot: &Snapshot,
        engine: &dyn Engine,
        report: &mut ScanPlanReport,
    ) -> KernelResult<Self> {
        let checkpoint_version = snapshot.log_segment().checkpoint_version;
        // Keep replay eligibility independent of SnapshotCrc's validation.
        if let Some((version, files)) = snapshot
            .base_crc()
            .and_then(|crc| Some((crc.version, crc.all_files()?)))
            .filter(|(version, _)| {
                checkpoint_version.is_none_or(|checkpoint| *version >= checkpoint)
                    && snapshot.log_segment().has_replay_after(*version)
            })
        {
            report.metadata_planning.replay_base = Some(ReplayBase::Crc);
            report.metadata_planning.base_version = Some(version);
            return Ok(Self::Crc {
                version,
                files: files.to_vec(),
            });
        }

        report.metadata_planning.replay_base = Some(if checkpoint_version.is_some() {
            ReplayBase::Checkpoint
        } else {
            ReplayBase::Empty
        });
        report.metadata_planning.base_version = checkpoint_version;
        let info = if let Some(resolved) = snapshot.checkpoint_info_with_metrics(engine) {
            report.checkpoint = Some(resolved.metrics);
            Some(resolved.result?)
        } else {
            None
        };
        Ok(Self::Checkpoint {
            version: checkpoint_version,
            info,
        })
    }

    /// Starting state selected for metadata replay.
    pub(in crate::scan) fn kind(&self) -> ReplayBase {
        match self {
            Self::Crc { .. } => ReplayBase::Crc,
            Self::Checkpoint {
                version: Some(_), ..
            } => ReplayBase::Checkpoint,
            Self::Checkpoint { version: None, .. } => ReplayBase::Empty,
        }
    }

    /// Exclusive lower bound for commit replay; `None` replays all available commits.
    pub(in crate::scan) fn version(&self) -> Option<Version> {
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
    use crate::Result;
    use crate::crc::Crc;
    use crate::engine::DelegatingEngine;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::plans::Operation as PlanOperation;
    use crate::plans::ir::nodes::Operator;
    use crate::scan::scan_plan::tests::log_segment;
    use crate::unit_test_utils::{assert_scan_plan_metrics, load_test_table};

    #[rstest]
    #[case::without_checkpoint(4, None, true)]
    #[case::equal_to_checkpoint(5, Some(5), true)]
    #[case::without_all_files(5, Some(5), false)]
    fn crc_selection_and_checkpoint_fallback(
        #[case] version: Version,
        #[case] checkpoint_version: Option<Version>,
        #[case] has_all_files: bool,
    ) -> Result<()> {
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
        let mut report = ScanPlanReport::new();
        let base = MetadataReplayBase::try_new(&scan.snapshot, &engine, &mut report)?;
        let kind = if has_all_files {
            ReplayBase::Crc
        } else {
            ReplayBase::Checkpoint
        };
        assert_eq!(base.kind(), kind);
        assert_eq!(report.metadata_planning.replay_base, Some(kind));
        assert_eq!(report.metadata_planning.base_version, Some(version));
        assert_eq!(base.version(), Some(version));
        assert_eq!(
            scan.snapshot.cached_checkpoint_info().is_some(),
            !has_all_files
        );
        Ok(())
    }

    #[rstest]
    #[case::crc_before_compaction(3, true)]
    #[case::crc_inside_compaction(4, false)]
    fn compacted_hint_crc_base_requires_a_replayable_tail(
        #[case] crc_version: Version,
        #[case] use_crc: bool,
    ) -> Result<()> {
        let (engine, latest, _tempdir) =
            load_test_table("v1-multi-part-partitioned-struct-stats-only")?;
        let base = Snapshot::builder_for(latest.table_root())
            .at_version(crc_version)
            .build(engine.as_ref())?;
        let crc = base.crc_at_version().expect("fixture CRC").clone();
        assert!(crc.all_files().is_some());
        let log_root = latest.log_segment().log_root.clone();
        let commits: Vec<_> = (0..=5).map(|v| format!("{log_root}{v:020}.json")).collect();
        let paths: Vec<_> = commits.iter().map(String::as_str).collect();
        let compacted = format!("{log_root}{:020}.{:020}.compacted.json", 4, 5);
        let mut segment = log_segment(log_root.clone(), &paths, None);
        segment.listed.ascending_compaction_files =
            vec![crate::unit_test_utils::create_log_path(&compacted)];
        segment
            .listed
            .ascending_commit_files
            .retain(|p| p.version < 4);
        let segment = crate::log_segment::LogSegment::try_new_with_compactions(
            segment.listed,
            log_root,
            Some(5),
            None,
        )?;
        let target = Snapshot::builder_for(latest.table_root())
            .at_version(5)
            .build(engine.as_ref())?;
        let snapshot = Snapshot::new_with_crc(
            segment,
            target.table_configuration().clone(),
            Some(crc),
            false,
            false,
        )?;
        let selected =
            MetadataReplayBase::try_new(&snapshot, engine.as_ref(), &mut ScanPlanReport::new())?;
        assert_eq!(matches!(selected, MetadataReplayBase::Crc { .. }), use_crc);
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
    ) -> Result<()> {
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
        let mut num_plan_nodes = 0;
        let plan = assert_scan_plan_metrics(
            || {
                scan.declarative_metadata_scan_plan(
                    &DelegatingEngine::new(engine.clone()).without_plan_executor(),
                )
            },
            |event| {
                num_plan_nodes = event.num_plan_nodes;
                let report = &event.metadata_planning;
                assert_eq!(report.replay_base, Some(ReplayBase::Crc));
                assert_eq!(report.base_version, Some(version));
                assert!(event.checkpoint.is_none());
                let bounds = (target_version > version).then_some((version + 1, target_version));
                assert_eq!(
                    report.commit_start_version.zip(report.commit_end_version),
                    bounds
                );
            },
        )?;
        assert_eq!(
            num_plan_nodes,
            plan.as_ref().map_or(0, |plan| plan.nodes.len() as u64)
        );
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
