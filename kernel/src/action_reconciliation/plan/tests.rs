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
use crate::scan::StatsOptions;
use crate::schema::schema_ref;
use crate::table_features::TableFeature;
use crate::unit_test_utils::plans::execute_plan;
use crate::unit_test_utils::{
    Action, LocalMockTable, MockProtocolBuilder, MockTableConfigurationBuilder, create_add_action,
    create_remove_action,
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
    // When: shared replay reads the requested actions using complete checkpoint discovery.
    let snapshot = build_snapshot!(VersionTarget::Latest, &table_root, &engine);
    let checkpoint_info = actions
        .live_adds
        .then(|| snapshot.checkpoint_info(&engine))
        .transpose()?
        .flatten();
    let state = MetadataPlanner::new(snapshot.log_segment(), actions)
        .with_stats(StatsOptions::json_only())
        .with_checkpoint_info(checkpoint_info)
        .replay()?;
    // Then: each requested payload and version comes from its latest action.
    let names = actions.output_action_names().collect::<Vec<_>>();
    let schema = schema_ref! {
        ..(get_all_actions_schema().project_as_struct(&names)?),
        nullable VERSION: LONG,
    };
    assert_eq!(state.schema(), &schema);
    let file_version =
        (remove_version.unwrap_or(1) as i64).max(if checkpoint.is_some() { 3 } else { 0 });
    let actual = concat_batches(
        &Arc::new(
            schema
                .as_ref()
                .try_into_arrow()
                .map_err(KernelError::from)?,
        ),
        &execute_plan(&engine, state.build()?)?,
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
