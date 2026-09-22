//! Write restrictions for user-defined columns.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::column_name;
use delta_kernel::schema::{
    schema, schema_ref, ArrayType, DataType, MapType, MetadataValue, StructField, UserDefinedType,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use rstest::rstest;
use test_utils::{assert_result_error_with_message, test_table_setup};

fn udt(sql_type: DataType) -> DataType {
    UserDefinedType {
        sql_type: Box::new(sql_type),
        annotation: [("class".to_owned(), Some("example.Value".to_owned()))].into(),
    }
    .into()
}

#[rstest]
#[case::partition(DataLayout::Partitioned { columns: vec![column_name!("value")] }, "non-primitive")]
#[case::clustering(DataLayout::Clustered { columns: vec![column_name!("value")] }, "unsupported type")]
fn create_udt_rejects_data_layout(
    #[case] layout: DataLayout,
    #[case] error: &str,
) -> DeltaResult<()> {
    let (_temp, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": LONG, nullable "value": (udt(DataType::LONG)) };
    assert_result_error_with_message(
        create_table(&table_path, schema, "test")
            .with_data_layout(layout)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new())),
        error,
    );
    Ok(())
}

#[rstest]
#[case::generated("delta.generationExpression", "id", "cannot carry")]
#[case::identity_start("delta.identity.start", "1", "cannot carry")]
#[case::identity_step("delta.identity.step", "1", "cannot carry")]
#[case::identity_high_water_mark("delta.identity.highWaterMark", "1", "cannot carry")]
#[case::identity_explicit("delta.identity.allowExplicitInsert", "true", "cannot carry")]
#[case::identity_extension("delta.identity.extension", "opaque", "cannot carry")]
#[case::null_default("CURRENT_DEFAULT", "NULL", "cannot carry a default")]
#[case::literal_default("CURRENT_DEFAULT", "1", "cannot carry a default")]
#[case::expression_default("CURRENT_DEFAULT", "current_timestamp()", "cannot carry a default")]
fn create_udt_rejects_column_metadata(
    #[case] key: &str,
    #[case] value: &str,
    #[case] error: &str,
    #[values("top", "struct", "array", "map")] placement: &str,
) -> DeltaResult<()> {
    let (_temp, table_path, engine) = test_table_setup()?;
    let field = StructField::nullable("value", udt(DataType::LONG))
        .add_metadata([(key.to_owned(), MetadataValue::String(value.to_owned()))]);
    let nested = || schema! { (field.clone()), };
    let field = match placement {
        "top" => field,
        "struct" => StructField::nullable("outer", nested()),
        "array" => StructField::nullable("outer", ArrayType::new(nested(), true)),
        "map" => StructField::nullable("outer", MapType::new(DataType::STRING, nested(), true)),
        _ => unreachable!(),
    };
    assert_result_error_with_message(
        create_table(&table_path, schema_ref! { (field), }, "test")
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new())),
        error,
    );
    Ok(())
}

#[rstest]
#[case::v1("delta.enableIcebergCompatV1", "Setting delta property")]
#[case::v2("delta.enableIcebergCompatV2", "Setting delta property")]
#[case::v3("delta.enableIcebergCompatV3", "does not support type at column")]
fn create_udt_rejects_iceberg_compat(
    #[case] property: &str,
    #[case] error: &str,
    #[values("top", "struct", "array", "map_key", "map_value")] placement: &str,
) -> DeltaResult<()> {
    let (_temp, table_path, engine) = test_table_setup()?;
    let value = udt(DataType::LONG);
    let value = match placement {
        "top" => value,
        "struct" => schema! { nullable "inner": (value) }.into(),
        "array" => ArrayType::new(value, true).into(),
        "map_key" => MapType::new(value, DataType::STRING, true).into(),
        "map_value" => MapType::new(DataType::STRING, value, true).into(),
        _ => unreachable!(),
    };
    assert_result_error_with_message(
        create_table(
            &table_path,
            schema_ref! { nullable "value": (value) },
            "test",
        )
        .with_table_properties([(property, "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new())),
        error,
    );
    Ok(())
}

#[test]
fn alter_udt_rejects_changes_inside_physical_type() -> DeltaResult<()> {
    let (_temp, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! {
        nullable "value": (udt(schema! { nullable "inner": INTEGER }.into())),
    };
    let snapshot = create_table(&table_path, schema, "test")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_result_error_with_message(
        snapshot
            .alter_table()
            .add_column_at(
                column_name!("value"),
                StructField::nullable("added", DataType::LONG),
            )
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new())),
        "not a struct",
    );
    Ok(())
}
