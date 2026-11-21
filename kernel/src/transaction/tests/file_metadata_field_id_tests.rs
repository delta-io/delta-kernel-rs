//! Tests for assigning the `clusteringProvider` file metadata field ID on table creation and
//! update transactions.

use std::sync::Arc;

use rstest::rstest;
use serde_json::Value;
use url::Url;

use crate::clustering::CLUSTERING_DOMAIN_NAME;
use crate::committer::FileSystemCommitter;
use crate::content_tree::file_metadata::{ADAPTIVE_METADATA_DOMAIN_NAME, FileMetadataFieldIds};
use crate::content_tree::stats::MAX_DATA_FIELD_ID;
use crate::engine::sync::SyncEngine;
use crate::row_tracking::ROW_TRACKING_DOMAIN_NAME;
use crate::schema::{ArrayType, DataType, SchemaRef, StructField, StructType, schema_ref};
use crate::snapshot::{Snapshot, SnapshotRef};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{ColumnMappingMode, TableFeature};
use crate::table_properties::{
    COLUMN_MAPPING_MAX_COLUMN_ID, COLUMN_MAPPING_MODE, ENABLE_ICEBERG_NATIVE_V4,
};
use crate::transaction::create_table::create_table;
use crate::transaction::data_layout::DataLayout;
use crate::transaction::file_metadata_field_id::assign_clustering_provider_field_id;
use crate::transaction::{Transaction, UpdateTableOperation};
use crate::unit_test_utils::adaptive_metadata_fixtures::{
    CLUSTERED_AMT_FEATURES, DomainPlacement, add_files_batch, clustering_provider_field_id,
    commit_files, commit_root_manifest_file, commit_update, domain, setup_clustered_table,
};
use crate::unit_test_utils::{
    MockProtocolBuilder, MockTableConfigurationBuilder, assert_result_error_with_message,
    test_schema_flat_with_column_mapping,
};
use crate::{Engine, FileMeta, KernelError, Result};

const ADAPTIVE_CONFIG_ID_100: &str = r#"{"fileMetadataFieldIds":{"clusteringProvider":100}}"#;

/// A mapping with an unrecognized key that holds an ID, which the new ID must not reuse.
const UNKNOWN_KEYS: &str = r#"{"fileMetadataFieldIds":{"other":5},"version":2}"#;

/// [`CLUSTERED_AMT_FEATURES`] plus column mapping, which the mock table needs to resolve its mode.
const MOCK_CLUSTERED_AMT_FEATURES: [TableFeature; 3] = [
    TableFeature::AdaptiveMetadataPreview,
    TableFeature::ClusteredTable,
    TableFeature::ColumnMapping,
];

fn mock_table_config(
    features: &[TableFeature],
    schema: SchemaRef,
    max_column_id: &str,
) -> TableConfiguration {
    MockTableConfigurationBuilder::new()
        .with_schema(schema)
        .with_column_mapping(ColumnMappingMode::Id)
        .with_properties([(COLUMN_MAPPING_MAX_COLUMN_ID, max_column_id)])
        .with_protocol(
            MockProtocolBuilder::new()
                .with_features(features.iter().cloned())
                .build(),
        )
        .build()
}

fn max_column_id(snapshot: &Snapshot) -> Option<i64> {
    snapshot.table_properties().column_mapping_max_column_id
}

/// Builds a write update on `snapshot` with the default committer.
fn build_write(snapshot: SnapshotRef, engine: &dyn Engine) -> Result<Transaction> {
    snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .build(engine, Box::new(FileSystemCommitter::new()))
}

/// A file that a root manifest commit names as the content root, whose bytes need not exist.
fn root_manifest_file(table_root: &Url, name: &str) -> FileMeta {
    FileMeta {
        location: table_root.join(name).unwrap(),
        last_modified: 0,
        size: 1024,
    }
}

// === Assignment on a table configuration ===

#[rstest]
// Persisted maxColumnId matches the schema's largest ID (2).
#[case::next_after_persisted("2", None, 3)]
#[case::stale_persisted_repaired_from_schema("1", None, 3)]
#[case::unrecognized_key_counts_toward_largest(
    "2",
    Some(r#"{"fileMetadataFieldIds":{"other":8}}"#),
    9
)]
fn assigns_next_id_above_every_id_in_use(
    #[case] max_column_id: &str,
    #[case] current: Option<&str>,
    #[case] expected: i32,
) {
    let features = MOCK_CLUSTERED_AMT_FEATURES;
    let config = mock_table_config(
        &features,
        test_schema_flat_with_column_mapping(),
        max_column_id,
    );
    let assigned = assign_clustering_provider_field_id(&config, || Ok(current.map(str::to_string)))
        .unwrap()
        .unwrap();
    assert_eq!(assigned.id, expected);
}

#[test]
fn lookup_is_skipped_when_table_cannot_need_an_id() {
    let features = [TableFeature::ClusteredTable];
    let config = mock_table_config(&features, test_schema_flat_with_column_mapping(), "2");
    let assigned = assign_clustering_provider_field_id(&config, || {
        panic!("the mapping is not read for a table that cannot need an ID")
    })
    .unwrap();
    assert!(assigned.is_none());
}

#[test]
fn no_id_when_mapping_already_assigns_clustering_provider() {
    let features = MOCK_CLUSTERED_AMT_FEATURES;
    let config = mock_table_config(&features, test_schema_flat_with_column_mapping(), "100");
    let assigned =
        assign_clustering_provider_field_id(&config, || Ok(Some(ADAPTIVE_CONFIG_ID_100.into())))
            .unwrap();
    assert!(assigned.is_none());
}

#[test]
fn id_above_data_field_id_limit_is_rejected_with_the_largest_id_in_use() {
    let max = i64::from(MAX_DATA_FIELD_ID);
    let features = MOCK_CLUSTERED_AMT_FEATURES;
    let config = mock_table_config(
        &features,
        test_schema_flat_with_column_mapping(),
        &max.to_string(),
    );
    let result = assign_clustering_provider_field_id(&config, || Ok(None));
    assert_result_error_with_message(result, &format!("after {max}"));
}

// === Commits ===

#[rstest]
#[case::clustered_adaptive_metadata(&CLUSTERED_AMT_FEATURES, Some(3))]
#[case::clustered_without_adaptive_metadata(&[TableFeature::ClusteredTable], None)]
#[case::adaptive_metadata_without_clustering(&[TableFeature::AdaptiveMetadataPreview], None)]
fn assigns_only_on_clustered_adaptive_metadata_tables(
    #[case] features: &[TableFeature],
    #[case] expected: Option<i32>,
) -> Result<()> {
    let (engine, table_root) = setup_clustered_table(features, 2, None);
    let snapshot = commit_update(&engine, &table_root, |builder| builder)?;
    assert_eq!(clustering_provider_field_id(&engine, &snapshot)?, expected);
    let expected_max = expected.map_or(2, i64::from);
    assert_eq!(max_column_id(&snapshot), Some(expected_max));
    Ok(())
}

#[test]
fn first_update_assigns_clustering_provider_id_and_raises_max_column_id() -> Result<()> {
    let (engine, table_root) = setup_clustered_table(&CLUSTERED_AMT_FEATURES, 2, None);
    let snapshot = commit_update(&engine, &table_root, |builder| builder)?;
    assert_eq!(clustering_provider_field_id(&engine, &snapshot)?, Some(3));
    assert_eq!(max_column_id(&snapshot), Some(3));
    Ok(())
}

#[test]
fn later_update_keeps_the_assigned_id() -> Result<()> {
    let (engine, table_root) = setup_clustered_table(&CLUSTERED_AMT_FEATURES, 2, None);
    commit_update(&engine, &table_root, |builder| builder)?;
    let snapshot = commit_update(&engine, &table_root, |builder| builder)?;
    assert_eq!(clustering_provider_field_id(&engine, &snapshot)?, Some(3));
    assert_eq!(max_column_id(&snapshot), Some(3));
    Ok(())
}

#[test]
fn blind_append_does_not_assign_an_id() -> Result<()> {
    let (engine, table_root) = setup_clustered_table(&CLUSTERED_AMT_FEATURES, 2, None);
    let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_blind_append()
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    txn.add_files(add_files_batch(&["blind.parquet"]));
    let snapshot = txn.commit(&engine)?.unwrap_post_commit_snapshot();
    assert_eq!(clustering_provider_field_id(&engine, &snapshot)?, None);
    assert_eq!(max_column_id(&snapshot), Some(2));
    Ok(())
}

#[rstest]
#[case::domain_in_first_commit(DomainPlacement::FirstCommit)]
#[case::domain_in_later_commit(DomainPlacement::LaterCommit)]
#[case::domain_in_commit_after_checkpoint_action(DomainPlacement::AfterCheckpoint)]
fn existing_domain_keeps_unrecognized_keys_and_fields(
    #[case] placement: DomainPlacement,
) -> Result<()> {
    let existing = domain(ADAPTIVE_METADATA_DOMAIN_NAME, UNKNOWN_KEYS);
    let (engine, table_root) =
        setup_clustered_table(&CLUSTERED_AMT_FEATURES, 2, Some((placement, existing)));
    let snapshot = commit_update(&engine, &table_root, |builder| builder)?;
    assert_eq!(clustering_provider_field_id(&engine, &snapshot)?, Some(6));
    let configuration = snapshot
        .reader()
        .get_domain_metadata_internal(ADAPTIVE_METADATA_DOMAIN_NAME, &engine)?
        .unwrap();
    let configuration: Value = serde_json::from_str(&configuration).map_err(KernelError::from)?;
    assert_eq!(configuration["fileMetadataFieldIds"]["other"], 5);
    assert_eq!(configuration["version"], 2);
    Ok(())
}

#[rstest]
// Stale persisted maxColumnId: the schema's largest ID (2) is the seed.
#[case::stale_max_column_id(1, None, false, 3, None)]
// The mapping's unrecognized key at 8 is above the persisted maxColumnId.
#[case::unrecognized_mapping_key_skipped_over(
    2,
    Some(r#"{"fileMetadataFieldIds":{"other":8}}"#),
    false,
    9,
    None
)]
// add_column takes ID 3 first, so the assigned ID comes after it.
#[case::add_column_and_assignment_in_one_transaction(2, None, true, 4, Some(3))]
fn assigned_id_never_reuses_an_id_in_use(
    #[case] max_column_id: i32,
    #[case] existing: Option<&str>,
    #[case] add_column: bool,
    #[case] expected_id: i32,
    #[case] expected_column_id: Option<i64>,
) -> Result<()> {
    let existing = existing.map(|configuration| {
        (
            DomainPlacement::FirstCommit,
            domain(ADAPTIVE_METADATA_DOMAIN_NAME, configuration),
        )
    });
    let (engine, table_root) =
        setup_clustered_table(&CLUSTERED_AMT_FEATURES, max_column_id, existing);
    let snapshot = commit_update(&engine, &table_root, |builder| {
        if add_column {
            builder.add_column(StructField::nullable("extra", DataType::LONG))
        } else {
            builder
        }
    })?;
    assert_eq!(
        clustering_provider_field_id(&engine, &snapshot)?,
        Some(expected_id)
    );
    let schema = snapshot.schema();
    let column_id = schema
        .field("extra")
        .and_then(StructField::column_mapping_id);
    assert_eq!(column_id, expected_column_id);
    Ok(())
}

#[test]
fn later_add_column_gets_an_id_above_the_assigned_one() -> Result<()> {
    let (engine, table_root) = setup_clustered_table(&CLUSTERED_AMT_FEATURES, 2, None);
    commit_update(&engine, &table_root, |builder| builder)?;
    let snapshot = commit_update(&engine, &table_root, |builder| {
        builder.add_column(StructField::nullable("later", DataType::LONG))
    })?;
    let schema = snapshot.schema();
    let later = schema.field("later").unwrap();
    assert_eq!(later.column_mapping_id(), Some(4));
    assert_eq!(clustering_provider_field_id(&engine, &snapshot)?, Some(3));
    Ok(())
}

#[test]
fn assignment_above_the_data_field_id_limit_fails_the_build() -> Result<()> {
    let (engine, table_root) =
        setup_clustered_table(&CLUSTERED_AMT_FEATURES, MAX_DATA_FIELD_ID, None);
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let built = build_write(snapshot, &engine);
    assert_result_error_with_message(built, &format!("after {MAX_DATA_FIELD_ID}"));
    Ok(())
}

#[test]
fn malformed_existing_domain_fails_the_build() -> Result<()> {
    let existing = domain(ADAPTIVE_METADATA_DOMAIN_NAME, r#"{"other":1}"#);
    let (engine, table_root) = setup_clustered_table(
        &CLUSTERED_AMT_FEATURES,
        2,
        Some((DomainPlacement::FirstCommit, existing)),
    );
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let built = build_write(snapshot, &engine);
    assert_result_error_with_message(built, ADAPTIVE_METADATA_DOMAIN_NAME);
    Ok(())
}

#[test]
fn checksum_written_after_assignment_is_accepted_on_read() -> Result<()> {
    let (engine, table_root) = setup_clustered_table(&CLUSTERED_AMT_FEATURES, 2, None);
    let snapshot = commit_update(&engine, &table_root, |builder| builder)?;
    snapshot.write_checksum(&engine)?;
    // The reader drops a CRC whose fileMetadataFieldIds disagree with the domain.
    let reread = Snapshot::builder_for(table_root).build(&engine)?;
    assert!(reread.crc_at_version().is_some());
    assert_eq!(clustering_provider_field_id(&engine, &reread)?, Some(3));
    Ok(())
}

#[test]
fn row_tracking_domain_and_assignment_share_one_commit() -> Result<()> {
    let (engine, table_root) = setup_clustered_table(&CLUSTERED_AMT_FEATURES, 2, None);
    let version = commit_files(&engine, &table_root, &["a.parquet"], &[]);
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    assert_eq!(snapshot.version(), version);
    assert!(
        snapshot
            .reader()
            .get_domain_metadata_internal(ROW_TRACKING_DOMAIN_NAME, &engine)?
            .is_some()
    );
    assert_eq!(clustering_provider_field_id(&engine, &snapshot)?, Some(3));
    Ok(())
}

#[rstest]
#[case::assigned_in_root_manifest_commit(false)]
#[case::carried_forward_from_earlier_commit(true)]
fn root_manifest_checkpoint_holds_domain_and_max_column_id(
    #[case] assign_in_earlier_commit: bool,
) -> Result<()> {
    let (engine, table_root) = setup_clustered_table(&CLUSTERED_AMT_FEATURES, 2, None);
    if assign_in_earlier_commit {
        commit_update(&engine, &table_root, |builder| builder)?;
    }
    commit_root_manifest_file(
        &engine,
        &table_root,
        root_manifest_file(&table_root, "metadata/root-1.parquet"),
    );
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let checkpoint = snapshot.latest_checkpoint_action(&engine)?.unwrap();
    assert_eq!(
        checkpoint.metadata().configuration()[COLUMN_MAPPING_MAX_COLUMN_ID],
        "3"
    );
    let domain = checkpoint
        .domain_metadata
        .iter()
        .find(|domain| domain.domain() == ADAPTIVE_METADATA_DOMAIN_NAME)
        .unwrap();
    let ids = FileMetadataFieldIds::try_from_configuration(domain.configuration()).unwrap();
    assert_eq!(ids.clustering_provider, Some(3));
    Ok(())
}

#[test]
fn concurrent_assignments_conflict_and_rebuilt_update_keeps_first_id() -> Result<()> {
    let (engine, table_root) = setup_clustered_table(&CLUSTERED_AMT_FEATURES, 2, None);
    let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    let first = build_write(snapshot.clone(), &engine)?;
    let second = build_write(snapshot, &engine)?;
    first.commit(&engine)?.unwrap_committed();
    let conflict = second.commit(&engine)?;
    assert!(!conflict.is_committed());

    let latest = Snapshot::builder_for(table_root).build(&engine)?;
    let rebuilt = build_write(latest, &engine)?;
    assert!(!rebuilt.should_emit_metadata);
    let snapshot = rebuilt.commit(&engine)?.unwrap_post_commit_snapshot();
    assert_eq!(clustering_provider_field_id(&engine, &snapshot)?, Some(3));
    assert_eq!(max_column_id(&snapshot), Some(3));
    Ok(())
}

#[test]
fn user_supplied_adaptive_metadata_domain_is_rejected() -> Result<()> {
    let (engine, table_root) = setup_clustered_table(&CLUSTERED_AMT_FEATURES, 2, None);
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let built = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_domain_metadata(ADAPTIVE_METADATA_DOMAIN_NAME, "{}")
        .build(&engine, Box::new(FileSystemCommitter::new()));
    assert_result_error_with_message(built, "Cannot modify domains that start with 'delta.'");
    Ok(())
}

// === Create table ===

fn flat_schema() -> SchemaRef {
    schema_ref! {
        nullable "id": LONG,
        nullable "name": STRING,
    }
}

/// Creates a table with column mapping in id mode, clustered on `id` when `clustered` and
/// adaptiveMetadata when `adaptive_metadata`. Returns the engine and the first commit's snapshot.
fn create_table_snapshot(
    schema: SchemaRef,
    clustered: bool,
    adaptive_metadata: bool,
) -> Result<(SyncEngine, SnapshotRef)> {
    let mut properties = vec![(COLUMN_MAPPING_MODE, "id")];
    if adaptive_metadata {
        properties.push((ENABLE_ICEBERG_NATIVE_V4, "true"));
    }
    let layout = if clustered {
        DataLayout::clustered(["id"])
    } else {
        DataLayout::None
    };
    let engine = SyncEngine::in_memory();
    let snapshot = create_table("memory:///create-table/", schema, "test", true)
        .with_table_properties(properties)
        .with_data_layout(layout)
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
    Ok((engine, snapshot))
}

fn has_clustering_domain(engine: &dyn Engine, snapshot: &Snapshot) -> Result<bool> {
    Ok(snapshot
        .reader()
        .get_domain_metadata_internal(CLUSTERING_DOMAIN_NAME, engine)?
        .is_some())
}

#[rstest]
#[case::clustered_adaptive_metadata(true, true, Some(3))]
#[case::clustered_without_adaptive_metadata(true, false, None)]
#[case::adaptive_metadata_without_clustering(false, true, None)]
#[case::neither_clustered_nor_adaptive_metadata(false, false, None)]
fn create_assigns_id_in_first_commit_only_for_clustered_adaptive_metadata(
    #[case] clustered: bool,
    #[case] adaptive_metadata: bool,
    #[case] expected_id: Option<i32>,
) -> Result<()> {
    let (engine, snapshot) = create_table_snapshot(flat_schema(), clustered, adaptive_metadata)?;
    assert_eq!(snapshot.version(), 0);
    assert_eq!(
        clustering_provider_field_id(&engine, &snapshot)?,
        expected_id
    );
    assert_eq!(
        max_column_id(&snapshot),
        Some(expected_id.map_or(2, i64::from))
    );
    assert_eq!(has_clustering_domain(&engine, &snapshot)?, clustered);
    let column_ids: Vec<i64> = snapshot
        .schema()
        .fields()
        .filter_map(StructField::column_mapping_id)
        .collect();
    if let Some(id) = expected_id {
        assert!(!column_ids.contains(&i64::from(id)));
    }
    Ok(())
}

#[rstest]
#[case::without_checksum(false)]
#[case::with_checksum(true)]
fn create_then_reload_then_update_keeps_the_assigned_id(
    #[case] write_checksum: bool,
) -> Result<()> {
    let (engine, created) = create_table_snapshot(flat_schema(), true, true)?;
    if write_checksum {
        created.write_checksum(&engine)?;
    }
    let table_root = created.table_root().clone();
    let reloaded = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    assert_eq!(reloaded.crc_at_version().is_some(), write_checksum);
    assert_eq!(clustering_provider_field_id(&engine, &reloaded)?, Some(3));
    assert_eq!(max_column_id(&reloaded), Some(3));

    let updated = commit_update(&engine, &table_root, |builder| builder)?;
    assert_eq!(clustering_provider_field_id(&engine, &updated)?, Some(3));
    assert_eq!(max_column_id(&updated), Some(3));
    Ok(())
}

#[test]
fn create_assigns_id_above_nested_column_ids() -> Result<()> {
    let item = StructType::try_new([StructField::nullable("sku", DataType::LONG)])?;
    let schema = Arc::new(StructType::try_new([
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("items", ArrayType::new(item, true)),
    ])?);
    let (engine, snapshot) = create_table_snapshot(schema, true, true)?;
    // Column mapping gives `id`, `items`, the array element, and `sku` the IDs 1 to 4.
    assert_eq!(clustering_provider_field_id(&engine, &snapshot)?, Some(5));
    assert_eq!(max_column_id(&snapshot), Some(5));
    Ok(())
}
