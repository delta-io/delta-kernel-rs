//! Integration tests for the `allowColumnDefaults` writer feature.

use std::collections::HashMap;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{
    schema_ref, ColumnMetadataKey, DataType, MetadataValue, StructField, StructType,
};
use delta_kernel::transaction::create_table::create_table as kernel_create_table;
use delta_kernel::DeltaResult;
use test_utils::{schema_with_column_defaults, test_table_setup};

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

#[cfg(not(feature = "column-defaults-in-dev"))]
mod feature_disabled {
    use std::collections::HashMap;
    use std::sync::Arc;

    use delta_kernel::committer::FileSystemCommitter;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::Snapshot;
    use test_utils::{create_table, engine_store_setup, schema_with_column_defaults};

    #[tokio::test]
    async fn test_col_defaults_blocked_when_cargo_feature_off(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(StructType::try_new(vec![
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ])?);

        let (store, engine, table_location) = engine_store_setup("test_col_defaults_off", None);
        let table_url = create_table(
            store.clone(),
            table_location,
            schema,
            &[],                         /* partition_columns */
            true,                        /* use_37_protocol */
            vec![],                      /* reader_features */
            vec!["allowColumnDefaults"], /* writer_features */
        )
        .await?;

        let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
        let err = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)
            .expect_err("write must be blocked when allowColumnDefaults is unsupported");
        let msg = err.to_string();
        assert!(
            msg.contains("allowColumnDefaults"),
            "error must name the unsupported feature; got: {msg}",
        );

        Ok(())
    }

    /// With the cargo feature off, `V3_CHECKS` omits `check_column_defaults`, so a V3 table whose
    /// default would be rejected with the feature on (here a non-literal primitive default) still
    /// loads. `allowColumnDefaults` is a writer feature, so it does not block snapshot load.
    #[tokio::test]
    async fn test_v3_column_default_check_absent_when_cargo_feature_off(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let base = StructType::try_new(vec![StructField::nullable("c", DataType::TIMESTAMP)])?;
        let schema =
            schema_with_column_defaults(&base, HashMap::from([("c", "current_timestamp()")]))?;

        let (store, engine, table_location) = engine_store_setup("test_v3_default_off", None);
        let table_url = create_table(
            store,
            table_location,
            schema,
            &[],    /* partition_columns */
            true,   /* use_37_protocol */
            vec![], /* reader_features */
            vec!["allowColumnDefaults", "icebergCompatV3"],
        )
        .await?;

        Snapshot::builder_for(table_url)
            .build(&engine)
            .expect("V3 table must load when the column-defaults check is not compiled in");

        Ok(())
    }
}

#[cfg(feature = "column-defaults-in-dev")]
mod feature_enabled {
    use std::collections::HashMap;
    use std::sync::Arc;

    use delta_kernel::arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::committer::FileSystemCommitter;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::Scalar;
    use delta_kernel::schema::{
        ArrayType, ColumnMetadataKey, DataType, MetadataValue, StructField, StructType,
    };
    use delta_kernel::table_features::TableFeature;
    use delta_kernel::Snapshot;
    use rstest::rstest;
    use test_utils::{
        create_table, engine_store_setup, insert_data, schema_with_column_defaults, test_read,
    };

    #[tokio::test]
    async fn test_blind_append_to_col_defaults_table_supported_when_cargo_feature_on(
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

    /// Creates a single-column table whose `c` column has `default_sql` as its `CURRENT_DEFAULT`,
    /// then asserts the type and default round-trip through a freshly loaded snapshot.
    ///
    /// `extra_features` are reader+writer features the column type requires beyond
    /// `allowColumnDefaults` (e.g. `timestampNtz` for a TIMESTAMP_NTZ column); they are added to
    /// both the reader and writer feature lists.
    async fn assert_column_default_persists(
        data_type: DataType,
        default_sql: &str,
        extra_features: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
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

        let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
        let schema = snapshot.schema();
        let field = schema
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
    #[case::decimal(DataType::decimal(10, 2).unwrap(), "1.23")]
    #[tokio::test]
    async fn test_create_table_with_column_default_persists_metadata(
        #[case] data_type: DataType,
        #[case] default_sql: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_column_default_persists(data_type, default_sql, &[]).await
    }

    // TIMESTAMP_NTZ is split out because it needs the orthogonal `timestampNtz` reader+writer
    // feature, unlike every other primitive literal above.
    //
    // TODO(#2630): Merge into the parameterized test once create supports column defaults
    #[tokio::test]
    async fn test_create_table_with_timestamp_ntz_column_default_persists_metadata(
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_column_default_persists(
            DataType::TIMESTAMP_NTZ,
            "TIMESTAMP_NTZ '2024-01-01 12:00:00'",
            &["timestampNtz"],
        )
        .await
    }

    #[rstest]
    #[case::all_data_columns(&[])]
    #[case::default_on_partition_column(&["b"])]
    #[tokio::test]
    async fn test_transaction_column_defaults_exposes_all_top_level_defaults(
        #[case] partition_columns: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        // `a`: no default, `b`: kernel-parsable default, `c`: non-kernel-parsable default.
        let base = StructType::try_new(vec![
            StructField::nullable("a", DataType::INTEGER),
            StructField::nullable("b", DataType::INTEGER),
            StructField::nullable("c", DataType::TIMESTAMP),
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

        let defaults = txn.column_defaults()?;
        assert_eq!(defaults.len(), 2, "only b and c declare a default");
        assert!(!defaults.contains_key("a"), "a has no default");

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

    /// Snapshot load rejects corrupt column-default metadata: a non-NULL default on a
    /// non-primitive column (unsupported by kernel) and a non-literal default on an
    /// `icebergCompatV3` table. The V3 accept/reject matrix itself lives in the
    /// `check_v3_column_default` unit test in `iceberg_compat::v3`; this only asserts the checks
    /// are wired into snapshot load. Orphaned metadata is NOT rejected -- see
    /// `test_load_and_write_allow_orphan_default`.
    #[rstest]
    #[case::non_primitive_default(
        "non_primitive",
        DataType::from(ArrayType::new(DataType::INTEGER, true)),
        "ARRAY(1)",
        vec!["allowColumnDefaults"],
        "not supported"
    )]
    #[case::v3_non_literal_default(
        "v3_non_literal",
        DataType::TIMESTAMP,
        "current_timestamp()",
        vec!["allowColumnDefaults", "icebergCompatV3"],
        "literal"
    )]
    #[tokio::test]
    async fn test_load_rejects_invalid_column_default(
        #[case] label: &str,
        #[case] field_type: DataType,
        #[case] default_sql: &str,
        #[case] writer_features: Vec<&str>,
        #[case] error_needle: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let base = StructType::try_new(vec![StructField::nullable("c", field_type)])?;
        let schema = schema_with_column_defaults(&base, HashMap::from([("c", default_sql)]))?;

        let (store, engine, table_location) =
            engine_store_setup(&format!("test_load_rejects_{label}"), None);
        let table_url = create_table(
            store,
            table_location,
            schema,
            &[],    /* partition_columns */
            true,   /* use_37_protocol */
            vec![], /* reader_features */
            writer_features,
        )
        .await?;

        let err = Snapshot::builder_for(table_url)
            .build(&engine)
            .expect_err("invalid column default must be rejected at load")
            .to_string();
        assert!(err.contains(error_needle), "got: {err}");

        Ok(())
    }

    /// A non-string `CURRENT_DEFAULT` value is corrupt and rejected at snapshot load. Built by
    /// hand because `schema_with_column_defaults` only writes string values.
    #[tokio::test]
    async fn test_load_rejects_non_string_column_default() -> Result<(), Box<dyn std::error::Error>>
    {
        let field = StructField::nullable("c", DataType::INTEGER).add_metadata([(
            ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
            MetadataValue::Number(7),
        )]);
        let schema = Arc::new(StructType::try_new(vec![field])?);

        let (store, engine, table_location) =
            engine_store_setup("test_load_rejects_non_string_default", None);
        let table_url = create_table(
            store,
            table_location,
            schema,
            &[],                         /* partition_columns */
            true,                        /* use_37_protocol */
            vec![],                      /* reader_features */
            vec!["allowColumnDefaults"], /* writer_features */
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

        let (store, engine, table_location) = engine_store_setup("test_load_orphan_default", None);
        let table_url = create_table(
            store,
            table_location,
            schema,
            &[],    /* partition_columns */
            true,   /* use_37_protocol */
            vec![], /* reader_features */
            vec![], /* writer_features: no allowColumnDefaults */
        )
        .await?;

        // Read: snapshot loads despite the orphaned metadata.
        let snapshot = Snapshot::builder_for(table_url).build(&engine)?;

        // Write: a write context builds without error.
        let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
        txn.unpartitioned_write_context()?;

        Ok(())
    }

    /// End-to-end: a literal column default composes with `icebergCompatV3`. The default survives
    /// the column-mapping transform (so it is still discoverable by its logical name), and a write
    /// that materializes the default round-trips on read.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_column_default_with_iceberg_compat_v3_e2e(
    ) -> Result<(), Box<dyn std::error::Error>> {
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
        let defaults = txn.column_defaults()?;
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
}
