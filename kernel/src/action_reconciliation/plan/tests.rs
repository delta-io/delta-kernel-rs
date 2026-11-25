use std::sync::Arc;

use ::test_utils::table_builder::VersionTarget;
use ::test_utils::{assert_batches_sorted_eq, build_snapshot};
use rstest::rstest;
use url::Url;

use super::action_set::ActionSet;
use super::*;
use crate::actions::{DomainMetadata, SetTransaction, get_all_actions_schema};
use crate::arrow::compute::{concat_batches, filter_record_batch, is_not_null};
use crate::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use crate::engine::arrow_conversion::TryIntoArrow;
use crate::engine::sync::SyncEngine;
use crate::expressions::{col, lit};
use crate::log_segment::LogSegment;
use crate::log_segment_files::LogSegmentFiles;
use crate::metrics::ReplayBase;
use crate::plans::ir::nodes::Operator;
use crate::scan::StatsOptions;
use crate::schema::schema_ref;
use crate::table_features::TableFeature;
use crate::unit_test_utils::plans::execute_plan;
use crate::unit_test_utils::{
    Action, LocalMockTable, MockProtocolBuilder, MockTableConfigurationBuilder, create_add_action,
    create_log_path_with_size, create_remove_action,
};
use crate::{KernelError, Result, Snapshot};

#[rstest]
#[case::live_adds(ActionSet::live_adds())]
#[case::tombstones(ActionSet::live_adds_with_tombstones())]
#[case::protocol(ActionSet { protocol: true, ..Default::default() })]
#[case::metadata(ActionSet { metadata: true, ..Default::default() })]
#[case::transactions(ActionSet { transactions: true, ..Default::default() })]
#[case::domains(ActionSet { domain_metadata: true, ..Default::default() })]
#[case::all(ActionSet::ALL)]
#[case::mixed_without_tombstones(ActionSet { include_tombstones: false, ..ActionSet::ALL })]
#[tokio::test(flavor = "multi_thread")]
async fn replay_selects_latest_action_without_retention(
    #[case] actions: ActionSet,
    #[values(None, Some(2), Some(4))] remove_version: Option<u64>,
    #[values(
        None,
        Some(CheckpointSpec::V1),
        Some(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        }))
    )]
    checkpoint: Option<CheckpointSpec>,
) -> Result<()> {
    // Given: checkpoint and commit actions with independent identities and optional tombstones.
    let engine = SyncEngine::new();
    let mut table = LocalMockTable::new();
    let table_root = Url::from_directory_path(table.table_root()).unwrap();
    let mut features = vec![TableFeature::DomainMetadata];
    if matches!(checkpoint, Some(CheckpointSpec::V2(_))) {
        features.push(TableFeature::V2Checkpoint);
    }
    let mut protocol = MockProtocolBuilder::new().with_features(&features).build();
    let mut metadata = MockTableConfigurationBuilder::new()
        .build()
        .metadata()
        .clone();
    table
        .commit([
            Action::Protocol(protocol),
            Action::Metadata(metadata.clone()),
        ])
        .await;
    let path = "data.parquet";
    table.commit([create_add_action(path)]).await;
    // Seed two identities before the checkpoint; update only the first in the tail.
    for (version, app, txn_version, domain, configuration) in [
        (2, "app", 1, "test", "2"),
        (3, "other", 8, "other", "keep"),
        (4, "app", 2, "test", "4"),
    ] {
        let mut actions = vec![
            Action::Txn(SetTransaction::new(app.into(), txn_version, None)),
            Action::DomainMetadata(DomainMetadata::new(domain.into(), configuration.into())),
        ];
        if version != 3 {
            metadata = metadata.with_configuration_entry("test", configuration);
            actions.push(Action::Metadata(metadata.clone()));
        }
        if remove_version == Some(version) {
            actions.push(create_remove_action(path));
        }
        table.commit(actions).await;
        if version == 3 && checkpoint.is_some() {
            build_snapshot!(VersionTarget::Latest, &table_root, &engine)
                .checkpoint(&engine, checkpoint.as_ref())?;
        }
    }
    features.push(TableFeature::ChangeDataFeed);
    protocol = MockProtocolBuilder::new().with_features(features).build();
    let domain_deletion = DomainMetadata::remove("test".into(), "4".into());
    table
        .commit([
            Action::Protocol(protocol),
            Action::DomainMetadata(domain_deletion),
        ])
        .await;
    let expected_start = if checkpoint.is_some() { 4 } else { 0 };
    let mut expected_bytes = 0;
    for version in expected_start..=5 {
        let path = table
            .table_root()
            .join(format!("_delta_log/{version:020}.json"));
        expected_bytes += std::fs::metadata(path).unwrap().len();
    }
    // When: shared replay reads the requested actions using complete checkpoint discovery.
    let snapshot = build_snapshot!(VersionTarget::Latest, &table_root, &engine);
    let checkpoint_info = actions
        .live_adds
        .then(|| snapshot.checkpoint_info(&engine))
        .transpose()?
        .flatten();
    let mut planner = MetadataPlanner::new(
        snapshot.log_segment(),
        actions,
        !snapshot.has_adaptive_metadata(),
    )
    .with_stats(StatsOptions::json_only())
    .with_checkpoint_info(checkpoint_info);
    let state = planner.replay()?;
    let report = planner.report();
    assert!(report.stats.synthesize_json);
    assert!(!report.partition_values.parsed_struct);
    assert_eq!(report.partition_values.timestamp_timezone, None);
    assert!(!report.read_collations);
    assert!(!report.has_filter);
    assert_eq!(report.base_version, checkpoint.is_some().then_some(3));
    assert_eq!(report.commit_start_version, Some(expected_start));
    assert_eq!(report.commit_end_version, Some(5));
    assert_eq!(report.compaction_file_count, 0);
    assert_eq!(report.commit_total_size_bytes, expected_bytes);
    assert_eq!(
        report.replay_base,
        Some(if checkpoint.is_some() {
            ReplayBase::Checkpoint
        } else {
            ReplayBase::Empty
        })
    );
    // Then: each requested payload and version comes from its latest action.
    let names = actions.output_action_names().collect::<Vec<_>>();
    let schema = schema_ref! {
        ..(get_all_actions_schema().project_as_struct(&names)?),
        nullable VERSION: LONG,
    };
    assert_eq!(state.schema(), &schema);
    let file_version =
        (remove_version.unwrap_or(1) as i64).max(if checkpoint.is_some() { 3 } else { 0 });
    let plan = state.build()?;
    let parquet_bytes: u64 = plan
        .nodes
        .iter()
        .filter_map(|node| match &node.op {
            Operator::ScanParquet(scan) => Some(&scan.files),
            _ => None,
        })
        .flatten()
        .map(|file| file.meta.size)
        .sum();
    assert_eq!(report.total_json_bytes, expected_bytes);
    assert_eq!(report.total_parquet_bytes, parquet_bytes);
    let actual = concat_batches(
        &Arc::new(
            schema
                .as_ref()
                .try_into_arrow()
                .map_err(KernelError::from)?,
        ),
        &execute_plan(&engine, plan)?,
    )
    .map_err(KernelError::from)?
    .normalize(".", Some(1))
    .map_err(KernelError::from)?;
    #[rustfmt::skip]
    let expected = [
        ("add", &["add.path", "add.dataChange"][..], format!(r#"+--------------+----------------+---------+
| add.path     | add.dataChange | version |
+--------------+----------------+---------+
| data.parquet | true           | {file_version}       |
+--------------+----------------+---------+"#)),
        ("remove", &["remove.path", "remove.dataChange", "remove.deletionTimestamp"][..], format!(r#"+--------------+-------------------+--------------------------+---------+
| remove.path  | remove.dataChange | remove.deletionTimestamp | version |
+--------------+-------------------+--------------------------+---------+
| data.parquet | true              | 9223372036854775807      | {file_version}       |
+--------------+-------------------+--------------------------+---------+"#)),
        ("metaData", &["metaData.configuration"][..], r#"+------------------------+---------+
| metaData.configuration | version |
+------------------------+---------+
| {test: 4}              | 4       |
+------------------------+---------+"#.to_string()),
        ("protocol", &["protocol.writerFeatures"][..], if matches!(checkpoint, Some(CheckpointSpec::V2(_))) {
            r#"+------------------------------------------------+---------+
| protocol.writerFeatures                        | version |
+------------------------------------------------+---------+
| [domainMetadata, v2Checkpoint, changeDataFeed] | 5       |
+------------------------------------------------+---------+"#
        } else {
            r#"+----------------------------------+---------+
| protocol.writerFeatures          | version |
+----------------------------------+---------+
| [domainMetadata, changeDataFeed] | 5       |
+----------------------------------+---------+"#
        }.to_string()),
        ("txn", &["txn.appId", "txn.version"][..], r#"+-----------+-------------+---------+
| txn.appId | txn.version | version |
+-----------+-------------+---------+
| app       | 2           | 4       |
| other     | 8           | 3       |
+-----------+-------------+---------+"#.to_string()),
        ("domainMetadata", &["domainMetadata.domain", "domainMetadata.configuration", "domainMetadata.removed"][..], r#"+-----------------------+------------------------------+------------------------+---------+
| domainMetadata.domain | domainMetadata.configuration | domainMetadata.removed | version |
+-----------------------+------------------------------+------------------------+---------+
| test                  | 4                            | true                   | 5       |
| other                 | keep                         | false                  | 3       |
+-----------------------+------------------------------+------------------------+---------+"#.to_string()),
    ];
    let mut checked_rows = 0;
    for (name, fields, expected) in expected {
        if !names.contains(&name) {
            continue;
        }
        let columns = fields
            .iter()
            .chain([&VERSION])
            .map(|field| actual.schema().index_of(field))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(KernelError::from)?;
        let batch = filter_record_batch(
            &actual.project(&columns).map_err(KernelError::from)?,
            &is_not_null(actual.column_by_name(fields[0]).unwrap()).map_err(KernelError::from)?,
        )
        .map_err(KernelError::from)?;
        checked_rows += batch.num_rows();
        if (name == "add" && remove_version.is_some())
            || (name == "remove" && remove_version.is_none())
        {
            assert_eq!(batch.num_rows(), 0, "{name}");
        } else {
            let mut expected = expected.lines().collect::<Vec<_>>();
            ::test_utils::sort_lines!(expected);
            assert_batches_sorted_eq!(expected, &[batch]);
        }
    }
    assert_eq!(checked_rows, actual.num_rows());
    Ok(())
}

#[rstest]
#[case::compaction(ReplayBase::Empty, true, false, Some((0, 5)), 13)]
#[case::crc_tail(ReplayBase::Crc, false, false, Some((4, 5)), 10)]
#[case::checkpoint_without_tail(ReplayBase::Checkpoint, false, false, None, 0)]
#[case::partial_failure(ReplayBase::Empty, false, true, Some((0, 5)), 30)]
fn report_retains_selected_inputs(
    #[case] base: ReplayBase,
    #[case] compaction: bool,
    #[case] fail: bool,
    #[case] expected_range: Option<(u64, u64)>,
    #[case] expected_bytes: u64,
) {
    let checkpoint = base == ReplayBase::Checkpoint;
    let file =
        |name: &str, size| create_log_path_with_size(&format!("memory:///_delta_log/{name}"), size);
    let segment = LogSegment {
        end_version: 5,
        checkpoint_version: checkpoint.then_some(5),
        log_root: Url::parse("memory:///_delta_log/").unwrap(),
        last_checkpoint_metadata: None,
        listed: LogSegmentFiles {
            ascending_commit_files: (0..=5)
                .filter(|_| !checkpoint)
                .map(|version| file(&format!("{version:020}.json"), 5))
                .collect(),
            ascending_compaction_files: compaction
                .then(|| {
                    file(
                        "00000000000000000000.00000000000000000005.compacted.json",
                        13,
                    )
                })
                .into_iter()
                .collect(),
            checkpoint_parts: checkpoint
                .then(|| file("00000000000000000005.checkpoint.parquet", 1))
                .into_iter()
                .collect(),
            ..Default::default()
        },
    };
    let actions = if checkpoint {
        ActionSet {
            protocol: true,
            ..Default::default()
        }
    } else {
        ActionSet::live_adds()
    };
    let mut planner = MetadataPlanner::new(&segment, actions, compaction)
        .with_stats_filter(fail.then(|| col!("missing").eq(lit(0))));
    let result = if base == ReplayBase::Crc {
        let _ = planner.crc_adds(3, vec![]).unwrap();
        planner.commit_actions(Some(3))
    } else {
        planner.replay()
    };
    assert_eq!(result.is_err(), fail);
    if let Err(error) = result {
        assert!(error.to_string().contains("missing"));
    }
    let report = planner.report();
    assert_eq!(report.replay_base, Some(base));
    assert_eq!(
        report.base_version,
        match base {
            ReplayBase::Crc => Some(3),
            ReplayBase::Checkpoint => Some(5),
            ReplayBase::Empty => None,
        },
    );
    assert_eq!(
        (report.commit_start_version, report.commit_end_version),
        expected_range.unzip(),
    );
    assert_eq!(report.commit_total_size_bytes, expected_bytes);
    assert_eq!(report.compaction_file_count, u64::from(compaction));
    assert_eq!(report.has_filter, fail);
}
