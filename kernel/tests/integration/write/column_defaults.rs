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
    use std::sync::Arc;

    use delta_kernel::committer::FileSystemCommitter;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::Snapshot;
    use test_utils::{create_table, engine_store_setup};

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
}

#[cfg(feature = "column-defaults-in-dev")]
mod feature_enabled {
    use std::collections::HashMap;
    use std::sync::Arc;

    use delta_kernel::arrow::array::{ArrayRef, Int64Array, StringArray};
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::schema::{
        ColumnMetadataKey, DataType, MetadataValue, StructField, StructType,
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
        // TODO(#2630): replace this manual metadata read once StructField::column_default exists.
        assert_eq!(
            field.get_config_value(&ColumnMetadataKey::CurrentDefault),
            Some(&MetadataValue::String(default_sql.to_string())),
            "CURRENT_DEFAULT metadata must round-trip verbatim",
        );

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
}
