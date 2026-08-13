//! Integration tests for reading column-mapping tables.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::object_store::path::Path as ObjectStorePath;
use delta_kernel::object_store::{DynObjectStore, ObjectStoreExt as _};
use delta_kernel::schema::{DataType, StructType};
use delta_kernel::table_features::TableFeature;
use delta_kernel::Snapshot;
use rstest::rstest;
use serde_json::json;
use test_utils::column_mapping_fixtures::cm_field;
use test_utils::{add_commit, engine_store_setup, read_scan, record_batch_to_bytes};

/// Logical (display) names, physical names, and column-mapping ids. Physical names differ from the
/// logical ones so a physical-name read is distinguishable from a logical-name read.
const ID_LOGICAL: &str = "id";
const ID_PHYSICAL: &str = "col-9f1c2b7e-0000-4000-8000-000000000000";
const ID_FIELD_ID: i64 = 1;
const PAYLOAD_LOGICAL: &str = "payload_renamed";
const PAYLOAD_PHYSICAL: &str = "col-9f1c2b7e-0000-4000-8000-000000000001";
const PAYLOAD_FIELD_ID: i64 = 2;

/// Builds the `metaData` action for a column-mapping table in the given mode. `cm_field` attaches
/// the per-field `delta.columnMapping.{id,physicalName}` metadata; the physical names differ from
/// the logical ones so the two read paths stay distinguishable.
fn metadata_action(mode: &str) -> serde_json::Value {
    let schema = StructType::new_unchecked([
        cm_field(ID_LOGICAL, ID_FIELD_ID, ID_PHYSICAL, DataType::INTEGER),
        cm_field(
            PAYLOAD_LOGICAL,
            PAYLOAD_FIELD_ID,
            PAYLOAD_PHYSICAL,
            DataType::STRING,
        ),
    ]);

    json!({
        "metaData": {
            "id": "cm-reader-features-test-table",
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(&schema).unwrap(),
            "partitionColumns": [],
            "configuration": {
                "delta.columnMapping.mode": mode,
                "delta.columnMapping.maxColumnId": "2"
            },
            "createdTime": 1_700_000_000_000i64
        }
    })
}

/// `protocol` action for reader version 3 / writer version 7 with the supplied feature lists.
fn protocol_action(reader_features: &[&str], writer_features: &[&str]) -> serde_json::Value {
    json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": reader_features,
            "writerFeatures": writer_features,
        }
    })
}

/// Commits version 0 (protocol + metadata) for a `name`-mode column-mapping table.
async fn setup_table(
    store: &DynObjectStore,
    table_root: &str,
    reader_features: &[&str],
    writer_features: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let commit = format!(
        "{}\n{}\n",
        protocol_action(reader_features, writer_features),
        metadata_action("name")
    );
    add_commit(table_root, store, 0, commit).await?;
    Ok(())
}

fn field_names(snapshot: &Snapshot) -> Vec<String> {
    snapshot
        .schema()
        .fields()
        .map(|f| f.name().to_string())
        .collect()
}

// ---------------------------------------------------------------------------
// Feature-support matrix at reader version 3.
//
// For a ReaderWriter feature, `is_feature_supported` is `reader_supported && writer_supported`.
// ColumnMapping is a legacy ReaderWriter feature (min reader version 2), so at reader version 3 the
// reader half is always satisfied by the version and support collapses to writer-list membership.
// The reader-only row never reaches this function: `Protocol::try_new` rejects it during load.
//
//   reader list        writer list        supported   note
//   [columnMapping]     [columnMapping]    true        conformant, unchanged
//   []                  [columnMapping]    true        orphaned, the fix
//   []                  [appendOnly]       false       absent from both, invariant held
// ---------------------------------------------------------------------------
#[rstest]
#[case::both_lists(&["columnMapping"], &["columnMapping"], true)]
#[case::orphaned_writer_only(&[], &["columnMapping"], true)]
#[case::absent_from_both(&[], &["appendOnly"], false)]
#[tokio::test]
async fn column_mapping_support_matrix(
    #[case] reader_features: &[&str],
    #[case] writer_features: &[&str],
    #[case] expected_supported: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let scenario = format!(
        "cm_support_{}_{}",
        reader_features.len(),
        writer_features.len()
    );
    let (store, engine, table_url) = engine_store_setup(&scenario, None);
    setup_table(
        store.as_ref(),
        table_url.as_str(),
        reader_features,
        writer_features,
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    let table_config = snapshot.table_configuration();
    assert_eq!(
        table_config.is_feature_supported(&TableFeature::ColumnMapping),
        expected_supported,
    );
    // Enablement tracks support here: the mode property is set, so a supported feature is enabled
    // and an unsupported one is not.
    assert_eq!(
        table_config.is_feature_enabled(&TableFeature::ColumnMapping),
        expected_supported,
    );
    // The logical schema always reports display names, whatever the reader-list shape.
    assert_eq!(field_names(&snapshot), vec![ID_LOGICAL, PAYLOAD_LOGICAL]);
    Ok(())
}

/// Once an orphaned column-mapping table loads, a scan must resolve columns the same way
/// delta-spark does: by physical name in `name` mode, by parquet field id in `id` mode. Both modes
/// carry the full mapping metadata; only the data-file column identity differs, which is what this
/// asserts by rebuilding the parquet file per mode. Reading by logical name would miss every column
/// and return nulls, so recovering the values proves the resolution ran.
#[rstest]
#[case::name_mode("name")]
#[case::id_mode("id")]
#[tokio::test]
async fn orphaned_column_mapping_resolves_data_columns(
    #[case] mode: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let scenario = format!("cm_orphan_read_{mode}");
    let (store, engine, table_url) = engine_store_setup(&scenario, None);

    // Orphaned protocol: columnMapping writer-side only.
    let commit = format!(
        "{}\n{}\n",
        protocol_action(&[], &["columnMapping"]),
        metadata_action(mode)
    );
    add_commit(table_url.as_str(), store.as_ref(), 0, commit).await?;

    // The data file's columns are addressed the way each mode expects: `name` mode matches by the
    // physical column name; `id` mode matches by the parquet `PARQUET:field_id` metadata (the
    // physical names are intentionally omitted so a name-based read could not accidentally pass).
    let (id_field, payload_field) = match mode {
        "name" => (
            ArrowField::new(ID_PHYSICAL, ArrowDataType::Int32, true),
            ArrowField::new(PAYLOAD_PHYSICAL, ArrowDataType::Utf8, true),
        ),
        "id" => (
            ArrowField::new("c0", ArrowDataType::Int32, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                ID_FIELD_ID.to_string(),
            )])),
            ArrowField::new("c1", ArrowDataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                PAYLOAD_FIELD_ID.to_string(),
            )])),
        ),
        other => panic!("unexpected mode {other}"),
    };
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![id_field, payload_field])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
        ],
    )?;
    let parquet_bytes = record_batch_to_bytes(&batch);
    let parquet_len = parquet_bytes.len();
    let data_path = "data_file.parquet";
    let data_url = table_url.join(data_path)?;
    store
        .put(
            &ObjectStorePath::from_url_path(data_url.path())?,
            parquet_bytes.into(),
        )
        .await?;

    let add = json!({
        "add": {
            "path": data_path,
            "partitionValues": {},
            "size": parquet_len,
            "modificationTime": 1,
            "dataChange": true,
        }
    });
    add_commit(table_url.as_str(), store.as_ref(), 1, add.to_string()).await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, Arc::new(engine))?;

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    // Scan output is addressed by logical names ...
    assert_eq!(batch.schema().field(0).name(), ID_LOGICAL);
    assert_eq!(batch.schema().field(1).name(), PAYLOAD_LOGICAL);
    // ... and carries the real values for every column, so the mode-specific column resolution
    // fired. Assert `id` too: a broken physical mapping for it would otherwise surface as nulls
    // with the test still green.
    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id column is an int32 array");
    assert_eq!(id.value(0), 1);
    assert_eq!(id.value(1), 2);
    let payload = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("payload column is a string array");
    assert_eq!(payload.value(0), "alpha");
    assert_eq!(payload.value(1), "beta");
    Ok(())
}

/// Commits the given feature lists, loads the snapshot, and returns the load error.
async fn load_error(
    scenario: &str,
    reader_features: &[&str],
    writer_features: &[&str],
) -> delta_kernel::Error {
    let (store, engine, table_url) = engine_store_setup(scenario, None);
    setup_table(
        store.as_ref(),
        table_url.as_str(),
        reader_features,
        writer_features,
    )
    .await
    .unwrap();
    Snapshot::builder_for(table_url)
        .build(&engine)
        .expect_err(&format!("{scenario}: expected snapshot load to fail"))
}

/// Shapes that must still be rejected at load. The orphaned-feature exemption is scoped to legacy
/// ReaderWriter features present writer-side, so it must not admit any of these.
#[rstest]
// A non-legacy ReaderWriter feature (deletionVectors) missing from readerFeatures.
#[case::non_legacy_writer_only(&[], &["columnMapping", "deletionVectors"])]
// A ReaderWriter feature listed only in readerFeatures (the spec forbids this outright).
#[case::reader_only(&["columnMapping"], &["appendOnly"])]
#[tokio::test]
async fn invalid_protocol_shapes_are_rejected(
    #[case] reader_features: &[&str],
    #[case] writer_features: &[&str],
) {
    let scenario = format!(
        "cm_reject_{}_{}",
        reader_features.len(),
        writer_features.len()
    );
    let err = load_error(&scenario, reader_features, writer_features).await;
    assert!(
        matches!(err, delta_kernel::Error::InvalidProtocol(_)),
        "{err}"
    );
}

/// Reader version 3 with `readerFeatures` absent entirely (not merely empty) is rejected before the
/// feature-consistency checks run: the protocol requires the field to be present at reader v3. The
/// orphaned-feature exemption deliberately does not extend to this shape.
#[tokio::test]
async fn reader_v3_without_reader_features_is_rejected() {
    let (store, engine, table_url) = engine_store_setup("cm_absent_reader_list", None);
    // readerFeatures is omitted from the protocol action entirely.
    let protocol = json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "writerFeatures": ["columnMapping"],
        }
    });
    let commit = format!("{}\n{}\n", protocol, metadata_action("name"));
    add_commit(table_url.as_str(), store.as_ref(), 0, commit)
        .await
        .unwrap();

    let err = Snapshot::builder_for(table_url)
        .build(&engine)
        .expect_err("expected snapshot load to fail");
    assert!(
        matches!(err, delta_kernel::Error::InvalidProtocol(_)),
        "{err}"
    );
}
