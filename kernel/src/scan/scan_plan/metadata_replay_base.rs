#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use crate::crc::Crc;
    use crate::engine::DelegatingEngine;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::metrics::ReplayBase;
    use crate::plans::Operation as PlanOperation;
    use crate::plans::ir::nodes::Operator;
    use crate::scan::scan_plan::tests::log_segment;
    use crate::unit_test_utils::{assert_scan_plan_metrics, load_test_table};
    use crate::{KernelResult, Result, Snapshot, Version};

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
        if !has_all_files {
            Arc::make_mut(&mut crc).all_files = None;
        }
        let segment = snapshot.log_segment().clone();
        let config = snapshot.table_configuration().clone();
        let crc = Some(crc);
        let snapshot = Arc::new(Snapshot::new_with_crc(segment, config, crc, false, false)?);
        let scan = snapshot.scan_builder().build()?;
        let kind = if has_all_files {
            ReplayBase::Crc
        } else {
            ReplayBase::Checkpoint
        };
        let plan = assert_scan_plan_metrics(
            || scan.declarative_metadata_scan_plan(engine.as_ref()),
            |event| {
                assert_eq!(event.metadata_planning.replay_base, Some(kind));
                assert_eq!(event.metadata_planning.base_version, Some(version));
                assert_eq!(
                    event.checkpoint.as_ref().map(|metrics| metrics.version),
                    checkpoint_version
                );
            },
        )?;
        assert!(plan.is_some());
        assert!(scan.snapshot.cached_checkpoint_info().is_some());
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
