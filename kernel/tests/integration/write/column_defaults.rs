//! Integration tests for the `allowColumnDefaults` writer feature.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT};
use delta_kernel::arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::{TryFromArrow as _, TryIntoArrow as _};
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use delta_kernel::expressions::{ColumnName, Scalar};
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::schema::{
    schema_ref, ArrayType, ColumnMetadataKey, DataType, MetadataValue, SchemaRef, StructField,
    StructType,
};
use delta_kernel::table_features::{
    get_any_level_column_physical_name, ColumnMappingMode, TableFeature,
};
use delta_kernel::transaction::create_table::create_table as kernel_create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    copy_directory, create_default_engine, create_table, engine_store_setup, get_column,
    insert_data, read_actions_from_commit, read_scan, resolve_field, schema_with_column_defaults,
    test_read, test_table_setup, test_table_setup_mt, write_batch_to_table, LoggingTest,
};
use url::Url;

use crate::common::write_utils::load_existing_single_file_checkpoint_path;

async fn setup_unpartitioned_table(
    name: &str,
    schema: SchemaRef,
    writer_features: Vec<&str>,
) -> Result<(DefaultEngine<TokioBackgroundExecutor>, Url), Box<dyn std::error::Error>> {
    let (store, engine, table_location) = engine_store_setup(name, None);
    let table_url = create_table(
        store,
        table_location,
        schema,
        &[],
        true,
        vec![],
        writer_features,
    )
    .await?;
    Ok((engine, table_url))
}

fn add_column_defaults_feature_commit(
    table_path: &Path,
    version: u64,
    schema: Option<&StructType>,
) -> Result<(), Box<dyn std::error::Error>> {
    let initial_commit =
        fs::read_to_string(table_path.join("_delta_log/00000000000000000000.json"))?;
    let mut actions = initial_commit
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let protocol = {
        let protocol = actions
            .iter_mut()
            .find(|action| action.get("protocol").is_some())
            .expect("initial commit must contain a protocol action");
        let writer_features = protocol["protocol"]["writerFeatures"]
            .as_array_mut()
            .expect("writer v7 protocol must contain writerFeatures");
        writer_features.push(serde_json::json!("allowColumnDefaults"));
        protocol.clone()
    };

    let metadata = {
        let metadata = actions
            .iter_mut()
            .find(|action| action.get("metaData").is_some())
            .expect("initial commit must contain a metadata action");
        if let Some(schema) = schema {
            metadata["metaData"]["schemaString"] =
                serde_json::json!(serde_json::to_string(schema)?);
        }
        metadata.clone()
    };

    let commit = format!(
        "{}\n{}\n",
        serde_json::to_string(&protocol)?,
        serde_json::to_string(&metadata)?,
    );
    fs::write(
        table_path.join(format!("_delta_log/{version:020}.json")),
        commit,
    )?;
    Ok(())
}

fn assert_top_level_default(
    snapshot: &Arc<Snapshot>,
    engine: &dyn Engine,
    column: &str,
    expected: Scalar,
) -> DeltaResult<()> {
    let txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine)?;
    assert_eq!(
        txn.top_level_column_defaults()?[column].to_scalar()?,
        Some(expected)
    );
    Ok(())
}

fn assert_no_column_default_metadata(schema: &StructType, path: &[&str]) {
    let display = path.join(".");
    let field = resolve_field(schema, path)
        .unwrap_or_else(|error| panic!("checkpoint schema must contain '{display}': {error}"));
    assert!(
        field
            .get_config_value(&ColumnMetadataKey::CurrentDefault)
            .is_none(),
        "checkpoint-derived field '{display}' must not carry CURRENT_DEFAULT metadata",
    );
}

fn assert_checkpoint_parsed_columns_have_no_column_defaults(
    checkpoint_schema: &StructType,
    data_column: &str,
    partition_column: &str,
) {
    assert_no_column_default_metadata(
        checkpoint_schema,
        &["add", "partitionValues_parsed", partition_column],
    );
    for stats_field in [MIN_VALUES, MAX_VALUES, NULL_COUNT] {
        assert_no_column_default_metadata(
            checkpoint_schema,
            &["add", "stats_parsed", stats_field, data_column],
        );
    }
}

// TODO(#2630): Allow create table to support column defaults
#[test]
fn test_create_table_rejects_col_defaults() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": LONG };

    let err = kernel_create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.allowColumnDefaults", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .expect_err("kernel create_table must reject allowColumnDefaults")
        .to_string();
    assert!(
        err.contains("allowColumnDefaults"),
        "error must name the unsupported feature; got: {err}",
    );
    Ok(())
}

#[test]
fn test_schema_with_column_defaults_errors_on_unknown_column() {
    let schema = StructType::try_new(vec![StructField::nullable("c", DataType::INTEGER)]).unwrap();

    let err = schema_with_column_defaults(&schema, HashMap::from([("does_not_exist", "1")]))
        .expect_err("unknown column must produce an error")
        .to_string();
    assert!(
        err.contains("does_not_exist"),
        "error should name the unknown column; got: {err}",
    );
}

#[test]
fn test_schema_with_column_defaults_reports_all_unknown_columns() {
    let schema = StructType::try_new(vec![StructField::nullable("c", DataType::INTEGER)]).unwrap();

    let err =
        schema_with_column_defaults(&schema, HashMap::from([("ghost1", "1"), ("ghost2", "2")]))
            .expect_err("all unknown columns must produce an error")
            .to_string();
    assert!(err.contains("ghost1"), "error must name ghost1; got: {err}");
    assert!(err.contains("ghost2"), "error must name ghost2; got: {err}");
}

#[test]
fn test_schema_with_column_defaults_overwrites_existing_default() {
    let base = StructType::try_new(vec![StructField::nullable("c", DataType::INTEGER)]).unwrap();
    let with_first = schema_with_column_defaults(&base, HashMap::from([("c", "1")])).unwrap();
    let with_second =
        schema_with_column_defaults(&with_first, HashMap::from([("c", "99")])).unwrap();

    let field = with_second.field("c").expect("c field must exist");
    assert_eq!(
        field.get_config_value(&ColumnMetadataKey::CurrentDefault),
        Some(&MetadataValue::String("99".to_string())),
        "second call must overwrite the first CURRENT_DEFAULT value",
    );
}

#[tokio::test]
async fn test_blind_append_to_column_defaults_table_is_supported(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
    ])?);

    let (store, engine, table_location) = engine_store_setup("test_table_col_defaults", None);
    // Use the JSON helper instead of the kernel `create_table` builder because the
    // latter does not whitelist this feature in `ALLOWED_DELTA_FEATURES`.
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],                         /* partition_columns */
        true,                        /* use_37_protocol */
        vec![],                      /* reader_features */
        vec!["allowColumnDefaults"], /* writer_features */
    )
    .await?;
    let engine = Arc::new(engine);

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let writer_features = snapshot
        .table_configuration()
        .protocol()
        .writer_features()
        .expect("writer_features must be present on a writer v7 table");
    assert!(
        writer_features.contains(&TableFeature::AllowColumnDefaults),
        "writer_features must include AllowColumnDefaults; got {writer_features:?}",
    );

    // Blind-append a small batch. No column has a `CURRENT_DEFAULT` yet, so this is a plain
    // append; we only assert the feature does not block the write path.
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["a", "b", "c"])),
    ];
    assert!(insert_data(snapshot, &engine, columns.clone())
        .await?
        .is_committed());

    // Round-trip read.
    let data = RecordBatch::try_new(Arc::new(schema.as_ref().try_into_arrow()?), columns)?;
    test_read(&ArrowEngineData::new(data), &table_url, engine)?;

    Ok(())
}

/// Creates a single-column table, materializes its default through the connector-facing API,
/// and asserts the metadata and written value round-trip.
async fn assert_materialized_column_default_round_trips(
    data_type: DataType,
    default_sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let extra_features: &[&str] = if data_type == DataType::TIMESTAMP_NTZ {
        &["timestampNtz"]
    } else {
        &[]
    };
    let base = StructType::try_new(vec![StructField::nullable("c", data_type.clone())])?;
    let schema = schema_with_column_defaults(&base, HashMap::from([("c", default_sql)]))?;

    let writer_features = [&["allowColumnDefaults"], extra_features].concat();
    let (store, engine, table_location) = engine_store_setup("test_create_with_default", None);
    let table_url = create_table(
        store,
        table_location,
        schema,
        &[],                     /* partition_columns */
        true,                    /* use_37_protocol */
        extra_features.to_vec(), /* reader_features */
        writer_features,
    )
    .await?;

    let engine = Arc::new(engine);
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let loaded_schema = snapshot.schema();
    let field = loaded_schema
        .field("c")
        .expect("c field must exist in loaded schema");

    assert_eq!(field.data_type(), &data_type);
    let column_default = field
        .column_default()
        .expect("column_default must not error for a valid primitive default")
        .expect("CURRENT_DEFAULT metadata must be present in the loaded schema");
    assert_eq!(
        column_default.raw_sql(),
        default_sql,
        "CURRENT_DEFAULT raw SQL must round-trip verbatim",
    );
    assert_eq!(column_default.data_type(), &data_type);

    let scalar = {
        let txn = snapshot
            .clone()
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
        let defaults = txn.top_level_column_defaults()?;
        defaults["c"]
            .to_scalar()?
            .expect("a primitive literal default must materialize")
    };
    let values = scalar.to_array(1)?;
    insert_data(snapshot, &engine, vec![values.clone()])
        .await?
        .unwrap_committed();

    let expected = RecordBatch::try_new(
        Arc::new(loaded_schema.as_ref().try_into_arrow()?),
        vec![values],
    )?;
    test_read(&ArrowEngineData::new(expected), &table_url, engine)?;

    Ok(())
}

#[rstest]
#[case::integer(DataType::INTEGER, "42")]
#[case::long(DataType::LONG, "9876543210")]
#[case::short(DataType::SHORT, "7")]
#[case::byte(DataType::BYTE, "1")]
#[case::float(DataType::FLOAT, "1.5")]
#[case::double(DataType::DOUBLE, "3.14")]
#[case::string(DataType::STRING, "'hello'")]
#[case::boolean(DataType::BOOLEAN, "TRUE")]
#[case::binary(DataType::BINARY, "X'deadbeef'")]
#[case::date(DataType::DATE, "DATE '2024-01-01'")]
#[case::timestamp(DataType::TIMESTAMP, "TIMESTAMP '2024-01-01T12:00:00Z'")]
#[case::timestamp_ntz(DataType::TIMESTAMP_NTZ, "TIMESTAMP_NTZ '2024-01-01 12:00:00'")]
#[case::decimal(DataType::decimal(10, 2).unwrap(), "1.23")]
#[tokio::test]
async fn test_materialized_primitive_column_default_round_trips(
    #[case] data_type: DataType,
    #[case] default_sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_materialized_column_default_round_trips(data_type, default_sql).await
}

#[rstest]
#[case::all_data_columns(&[])]
#[case::default_on_partition_column(&["b"])]
#[tokio::test]
async fn test_transaction_top_level_column_defaults_excludes_nested_defaults(
    #[case] partition_columns: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let nested_default = StructField::nullable("inner", DataType::INTEGER).add_metadata([(
        ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
        MetadataValue::String("7".to_string()),
    )]);

    // `a`: no default, `b`: kernel-parsable default, `c`: non-kernel-parsable default,
    // `s.inner`: nested default that the top-level API must not return.
    let base = StructType::try_new(vec![
        StructField::nullable("a", DataType::INTEGER),
        StructField::nullable("b", DataType::INTEGER),
        StructField::nullable("c", DataType::TIMESTAMP),
        StructField::nullable("s", DataType::try_struct_type([nested_default])?),
    ])?;
    let schema = schema_with_column_defaults(
        &base,
        HashMap::from([("b", "1337"), ("c", "current_timestamp()")]),
    )?;

    let (store, engine, table_location) = engine_store_setup("test_txn_column_defaults", None);
    let table_url = create_table(
        store,
        table_location,
        schema,
        partition_columns,
        true,                        /* use_37_protocol */
        vec![],                      /* reader_features */
        vec!["allowColumnDefaults"], /* writer_features */
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;

    let defaults = txn.top_level_column_defaults()?;
    assert_eq!(defaults.len(), 2, "only b and c declare a default");
    assert!(!defaults.contains_key("a"), "a has no default");
    assert!(
        !defaults.contains_key("s") && !defaults.contains_key("s.inner"),
        "nested defaults must not be returned by the top-level API"
    );

    let b = &defaults["b"];
    assert_eq!(b.raw_sql(), "1337");
    assert_eq!(b.to_scalar()?, Some(Scalar::Integer(1337)));

    let c = &defaults["c"];
    assert_eq!(c.raw_sql(), "current_timestamp()");
    assert_eq!(
        c.to_scalar()?,
        None,
        "a non-kernel-parsable default is not parsed by the kernel",
    );

    Ok(())
}

/// On an `icebergCompatV3` table, a default kernel cannot verify as a literal produces a
/// warning rather than an error, so the snapshot loads and a DML transaction constructs.
#[rstest]
#[case::non_literal(
    "non_literal",
    DataType::TIMESTAMP,
    "current_timestamp()",
    "could not verify"
)]
#[case::unparsable_non_primitive(
    "non_primitive",
    DataType::from(ArrayType::new(DataType::INTEGER, true)),
    "ARRAY(1)",
    "could not verify"
)]
#[tokio::test]
async fn test_load_and_write_tolerate_v3_unverifiable_default(
    #[case] label: &str,
    #[case] field_type: DataType,
    #[case] default_sql: &str,
    #[case] warning_text: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let base = StructType::try_new(vec![StructField::nullable("c", field_type)])?;
    let schema = schema_with_column_defaults(&base, HashMap::from([("c", default_sql)]))?;

    let (engine, table_url) = setup_unpartitioned_table(
        &format!("test_v3_tolerates_{label}"),
        schema,
        vec!["allowColumnDefaults", "icebergCompatV3"],
    )
    .await?;

    // Read: the snapshot loads despite the unverifiable default.
    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;

    let logging = LoggingTest::new();
    snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    assert!(
        logging.logs().contains(warning_text),
        "logs: {}",
        logging.logs()
    );

    Ok(())
}

/// A non-string `CURRENT_DEFAULT` value is corrupt and rejected at snapshot load. Built by
/// hand because `schema_with_column_defaults` only writes string values.
#[tokio::test]
async fn test_load_rejects_non_string_column_default() -> Result<(), Box<dyn std::error::Error>> {
    let field = StructField::nullable("c", DataType::INTEGER).add_metadata([(
        ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
        MetadataValue::Number(7),
    )]);
    let schema = Arc::new(StructType::try_new(vec![field])?);

    let (engine, table_url) = setup_unpartitioned_table(
        "test_load_rejects_non_string_default",
        schema,
        vec!["allowColumnDefaults"],
    )
    .await?;

    let err = Snapshot::builder_for(table_url)
        .build(&engine)
        .expect_err("a non-string CURRENT_DEFAULT must be rejected at load")
        .to_string();
    assert!(err.contains("non-string"), "got: {err}");

    Ok(())
}

/// Orphaned column-default metadata (a `CURRENT_DEFAULT` without the `allowColumnDefaults`
/// feature) is tolerated: the snapshot loads and a write context builds without error.
#[tokio::test]
async fn test_load_and_write_allow_orphan_default() -> Result<(), Box<dyn std::error::Error>> {
    let base = StructType::try_new(vec![StructField::nullable("c", DataType::INTEGER)])?;
    let schema = schema_with_column_defaults(&base, HashMap::from([("c", "42")]))?;

    let (engine, table_url) =
        setup_unpartitioned_table("test_load_orphan_default", schema, vec![]).await?;

    // Read: snapshot loads despite the orphaned metadata.
    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;

    // Write: a write context builds without error.
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    assert!(
        txn.top_level_column_defaults()?.is_empty(),
        "orphaned defaults must not be surfaced without allowColumnDefaults",
    );
    txn.unpartitioned_write_context()?;

    Ok(())
}

#[rstest]
#[case::null_default("null", "NULL", None)]
#[case::non_null_default("non_null", "'value'", Some("must be NULL"))]
#[tokio::test]
async fn test_variant_column_default_validation_at_snapshot_load(
    #[case] label: &str,
    #[case] default_sql: &str,
    #[case] expected_error: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let variant_type = DataType::unshredded_variant();
    let base = StructType::try_new(vec![StructField::nullable("v", variant_type)])?;
    let schema = schema_with_column_defaults(&base, HashMap::from([("v", default_sql)]))?;

    let (store, engine, table_location) =
        engine_store_setup(&format!("test_variant_default_{label}"), None);
    let table_url = create_table(
        store,
        table_location,
        schema,
        &[],
        true,
        vec!["variantType"],
        vec!["variantType", "allowColumnDefaults"],
    )
    .await?;

    match expected_error {
        Some(expected_error) => {
            let error = Snapshot::builder_for(table_url)
                .build(&engine)
                .expect_err("a non-NULL Variant default must be rejected at snapshot load")
                .to_string();
            assert!(error.contains(expected_error), "got: {error}");
        }
        None => {
            let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
            let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
            let defaults = txn.top_level_column_defaults()?;
            let column_default = &defaults["v"];
            assert_eq!(column_default.raw_sql(), default_sql);
            assert!(
                column_default
                    .to_scalar()?
                    .is_some_and(|value| value.is_null()),
                "a Variant NULL default must surface as a null scalar",
            );
        }
    }

    Ok(())
}

/// Defaults kernel cannot materialize surface via raw SQL so the connector can evaluate them.
#[rstest]
#[case::type_mismatch("type_mismatch", DataType::INTEGER, "'not an int'")]
#[case::non_primitive(
    "non_primitive",
    DataType::from(ArrayType::new(DataType::INTEGER, true)),
    "ARRAY(1)"
)]
#[tokio::test]
async fn test_load_tolerates_unmaterializable_default(
    #[case] label: &str,
    #[case] data_type: DataType,
    #[case] default_sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let base = StructType::try_new(vec![StructField::nullable("c", data_type)])?;
    let schema = schema_with_column_defaults(&base, HashMap::from([("c", default_sql)]))?;

    let (engine, table_url) = setup_unpartitioned_table(
        &format!("test_load_{label}_default"),
        schema,
        vec!["allowColumnDefaults"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    let defaults = txn.top_level_column_defaults()?;

    let c = &defaults["c"];
    assert_eq!(c.raw_sql(), default_sql);
    assert_eq!(
        c.to_scalar()?,
        None,
        "kernel must leave an unmaterializable default to the connector"
    );

    Ok(())
}

#[test]
fn test_column_default_composes_with_deletion_vectors() -> Result<(), Box<dyn std::error::Error>> {
    let source_path = fs::canonicalize("./tests/data/table-with-dv-small/")?;
    let temp_dir = tempfile::tempdir()?;
    let table_path = temp_dir.path().join("table-with-dv-and-column-default");
    copy_directory(&source_path, &table_path)?;

    let base = StructType::try_new(vec![StructField::nullable("value", DataType::INTEGER)])?;
    let schema = schema_with_column_defaults(&base, HashMap::from([("value", "42")]))?;
    add_column_defaults_feature_commit(&table_path, 2, Some(schema.as_ref()))?;

    let table_url = Url::from_directory_path(&table_path).expect("table path must be a URL");
    let engine = create_default_engine(&table_url)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_top_level_default(&snapshot, engine.as_ref(), "value", Scalar::Integer(42))?;

    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;
    let mut values = Vec::new();
    for batch in batches {
        let value_array = get_column!(batch, "value", Int32Array);
        values.extend((0..value_array.len()).map(|index| value_array.value(index)));
    }
    values.sort_unstable();
    assert_eq!(values, (1..9).collect::<Vec<_>>());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_defaulted_clustering_column_round_trips_with_stats(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let base = StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("c", DataType::INTEGER),
    ])?;
    let schema = schema_with_column_defaults(&base, HashMap::from([("c", "42")]))?;

    kernel_create_table(&table_path, schema.clone(), "Test/1.0")
        .with_data_layout(DataLayout::clustered(["c"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();
    add_column_defaults_feature_commit(Path::new(&table_path), 1, None)?;

    let table_url = Url::from_directory_path(&table_path).expect("table path must be a URL");
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    assert_top_level_default(&snapshot, engine.as_ref(), "c", Scalar::Integer(42))?;

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(Int32Array::from(vec![42, 42, 42])),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema.as_ref().try_into_arrow()?), columns.clone())?;
    write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;

    let add_actions = read_actions_from_commit(&table_url, 2, "add")?;
    let stats: serde_json::Value = serde_json::from_str(
        add_actions[0]["stats"]
            .as_str()
            .expect("clustered write must include stats"),
    )?;
    assert_eq!(stats["minValues"]["c"], 42);
    assert_eq!(stats["maxValues"]["c"], 42);

    let expected = RecordBatch::try_new(Arc::new(schema.as_ref().try_into_arrow()?), columns)?;
    test_read(&ArrowEngineData::new(expected), &table_url, engine)?;

    Ok(())
}

#[rstest]
#[case::cm_none("none")]
#[case::cm_name("name")]
#[case::cm_id("id")]
#[tokio::test(flavor = "multi_thread")]
async fn test_column_default_round_trips_with_column_mapping_and_checkpoint(
    #[case] column_mapping_mode: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let base = StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("c", DataType::INTEGER),
        StructField::nullable("p", DataType::INTEGER),
    ])?;
    let schema = schema_with_column_defaults(&base, HashMap::from([("c", "42"), ("p", "7")]))?;
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let mut table_properties = vec![("delta.checkpoint.writeStatsAsStruct", "true")];
    if column_mapping_mode != "none" {
        table_properties.push(("delta.columnMapping.mode", column_mapping_mode));
    }
    kernel_create_table(&table_path, schema.clone(), "Test/1.0")
        .with_data_layout(DataLayout::partitioned(["p"]))
        .with_table_properties(table_properties)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();
    add_column_defaults_feature_commit(Path::new(&table_path), 1, None)?;

    let table_url = Url::from_directory_path(&table_path).expect("table path must be a URL");
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    assert_top_level_default(&snapshot, engine.as_ref(), "c", Scalar::Integer(42))?;
    assert_top_level_default(&snapshot, engine.as_ref(), "p", Scalar::Integer(7))?;
    let column_mapping_mode = column_mapping_mode.parse::<ColumnMappingMode>()?;
    let physical_name = |logical: &str| -> String {
        get_any_level_column_physical_name(
            snapshot.schema().as_ref(),
            &ColumnName::new([logical]),
            column_mapping_mode,
        )
        .expect("top-level column must have a physical name")
        .into_inner()
        .into_iter()
        .next()
        .expect("top-level physical column path must not be empty")
    };
    let data_column = physical_name("c");
    let partition_column = physical_name("p");

    let data_schema = StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("c", DataType::INTEGER),
    ])?;
    let data_columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(Int32Array::from(vec![42, 42, 42])),
    ];
    let batch = RecordBatch::try_new(
        Arc::new((&data_schema).try_into_arrow()?),
        data_columns.clone(),
    )?;
    let post_commit_snapshot = write_batch_to_table(
        &snapshot,
        engine.as_ref(),
        batch,
        HashMap::from([("p".to_string(), Scalar::Integer(7))]),
    )
    .await?;

    // Check the engine-facing schema because the Parquet writer does not preserve arbitrary
    // Arrow field metadata.
    let checkpoint_writer = post_commit_snapshot
        .clone()
        .create_checkpoint_writer(engine.as_ref())?;
    let mut checkpoint_data = checkpoint_writer.checkpoint_data(engine.as_ref())?;
    let checkpoint_batch = checkpoint_data
        .next()
        .expect("checkpoint data must contain at least one batch")?
        .apply_selection_vector()?
        .try_into_record_batch()?;
    for batch in checkpoint_data {
        batch?;
    }
    let checkpoint_data_schema = StructType::try_from_arrow(checkpoint_batch.schema())?;
    assert_checkpoint_parsed_columns_have_no_column_defaults(
        &checkpoint_data_schema,
        &data_column,
        &partition_column,
    );

    let checkpoint_version = post_commit_snapshot.version();
    post_commit_snapshot.checkpoint(engine.as_ref(), None)?;
    let checkpoint_path =
        load_existing_single_file_checkpoint_path(&table_path, checkpoint_version);
    let checkpoint_reader =
        ParquetRecordBatchReaderBuilder::try_new(fs::File::open(checkpoint_path)?)?;
    let checkpoint_file_schema = StructType::try_from_arrow(checkpoint_reader.schema().clone())?;
    assert_checkpoint_parsed_columns_have_no_column_defaults(
        &checkpoint_file_schema,
        &data_column,
        &partition_column,
    );

    let reloaded_snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let reloaded_table_schema = reloaded_snapshot.schema();
    let default = reloaded_table_schema
        .field("c")
        .expect("c field must exist after checkpoint reload")
        .column_default()?
        .expect("c default must survive checkpoint reload");
    assert_eq!(default.to_scalar()?, Some(Scalar::Integer(42)));

    let expected = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![
            data_columns[0].clone(),
            data_columns[1].clone(),
            Arc::new(Int32Array::from(vec![7, 7, 7])),
        ],
    )?;
    test_read(&ArrowEngineData::new(expected), &table_url, engine)?;

    Ok(())
}

#[tokio::test]
async fn test_partition_column_default_round_trips_on_read(
) -> Result<(), Box<dyn std::error::Error>> {
    let base = StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("p", DataType::INTEGER),
    ])?;
    let schema = schema_with_column_defaults(&base, HashMap::from([("p", "42")]))?;

    let (store, engine, table_location) =
        engine_store_setup("test_partition_column_default_round_trip", None);
    let table_url = create_table(
        store,
        table_location,
        schema.clone(),
        &["p"],
        true,
        vec![],
        vec!["allowColumnDefaults"],
    )
    .await?;
    let engine = Arc::new(engine);
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;

    let data_schema = StructType::try_new(vec![StructField::nullable("id", DataType::LONG)])?;
    let data = RecordBatch::try_new(
        Arc::new((&data_schema).try_into_arrow()?),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?;
    write_batch_to_table(
        &snapshot,
        engine.as_ref(),
        data,
        HashMap::from([("p".to_string(), Scalar::Integer(42))]),
    )
    .await?;

    let expected = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 42, 42])),
        ],
    )?;
    test_read(&ArrowEngineData::new(expected), &table_url, engine)?;

    Ok(())
}

/// End-to-end: a literal column default composes with `icebergCompatV3`. The default survives
/// the column-mapping transform (so it is still discoverable by its logical name), and a write
/// that materializes the default round-trips on read.
#[tokio::test(flavor = "multi_thread")]
async fn test_column_default_with_iceberg_compat_v3_e2e() -> Result<(), Box<dyn std::error::Error>>
{
    let base = StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("c", DataType::INTEGER),
    ])?;
    let schema = schema_with_column_defaults(&base, HashMap::from([("c", "42")]))?;

    let (store, engine, table_location) = engine_store_setup("test_v3_col_default_e2e", None);
    // The helper auto-enables V3's dependencies (columnMapping, rowTracking, domainMetadata)
    // and assigns the per-field column-mapping metadata a valid V3 table requires.
    let table_url = create_table(
        store,
        table_location,
        schema.clone(),
        &[],    /* partition_columns */
        true,   /* use_37_protocol */
        vec![], /* reader_features */
        vec!["allowColumnDefaults", "icebergCompatV3"],
    )
    .await?;
    let engine = Arc::new(engine);

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let writer_features = snapshot
        .table_configuration()
        .protocol()
        .writer_features()
        .expect("writer_features must be present on a writer v7 table");
    for feature in [
        TableFeature::IcebergCompatV3,
        TableFeature::RowTracking,
        TableFeature::ColumnMapping,
        TableFeature::AllowColumnDefaults,
    ] {
        assert!(
            writer_features.contains(&feature),
            "writer_features must include {feature:?}; got {writer_features:?}",
        );
    }

    // The default is still keyed by the logical name `c` and parses to its literal.
    let txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let defaults = txn.top_level_column_defaults()?;
    assert_eq!(defaults["c"].to_scalar()?, Some(Scalar::Integer(42)));
    drop(defaults);
    drop(txn);

    // The connector materializes the default (42) into the batch, then writes.
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(Int32Array::from(vec![42, 42, 42])),
    ];
    assert!(insert_data(snapshot, &engine, columns.clone())
        .await?
        .is_committed());

    let data = RecordBatch::try_new(Arc::new(schema.as_ref().try_into_arrow()?), columns)?;
    test_read(&ArrowEngineData::new(data), &table_url, engine)?;

    Ok(())
}
