use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, AsArray as _, Decimal128Array, TimestampMillisecondArray, TimestampNanosecondArray,
};
use delta_kernel::arrow::datatypes::{Decimal128Type, TimeUnit, TimestampMicrosecondType};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::{col, lit, Predicate, Scalar};
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::parquet::file::properties::{EnabledStatistics, WriterProperties};
use delta_kernel::schema::{
    schema_ref, ColumnMetadataKey, DataType, MetadataValue, SchemaRef, StructField,
};
use delta_kernel::transaction::create_table::create_table;
use rstest::rstest;
use serde_json::json;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{
    begin_transaction, create_add_files_metadata, read_scan, record_batch_to_bytes_with_props,
};

#[rstest]
#[case::max(1234, 10_000, true)]
#[case::min(-1234, -10_000, false)]
#[tokio::test]
async fn decimal_scale_widening_preserves_matching_rows(
    #[case] unscaled: i128,
    #[case] threshold: i128,
    #[case] greater_than: bool,
    #[values(5, 10, 20)] precision: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = Decimal128Array::from(vec![unscaled]).with_precision_and_scale(precision, 2)?;
    let batch = RecordBatch::try_from_iter([("value", Arc::new(source) as ArrayRef)])?;
    let target = DataType::decimal(precision + 1, 3)?;
    let field = StructField::nullable("value", target).add_metadata([(
        ColumnMetadataKey::TypeChanges.as_ref(),
        MetadataValue::Other(json!([{
            "fromType": format!("decimal({precision},2)"),
            "toType": format!("decimal({},3)", precision + 1),
        }])),
    )]);
    let threshold = lit(Scalar::decimal(threshold, precision + 1, 3)?);
    let predicate = if greater_than {
        col!("value").gt(threshold)
    } else {
        col!("value").lt(threshold)
    };
    let batches = read_with_footer_stats(
        batch,
        schema_ref! { (field) },
        &[("delta.enableTypeWidening", "true")],
        predicate,
    )
    .await?;
    let values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Decimal128Type>()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![unscaled * 10]);
    Ok(())
}

#[rstest]
#[case::millis_max(TimeUnit::Millisecond, 1_000, 500_000, true)]
#[case::millis_min(TimeUnit::Millisecond, -1_000, -500_000, false)]
#[case::nanos_min(TimeUnit::Nanosecond, 1_000_000_000, 2_000_000, false)]
#[case::nanos_max(TimeUnit::Nanosecond, -1_000_000_000, -2_000_000, true)]
#[tokio::test]
async fn timestamp_unit_conversion_preserves_matching_rows(
    #[case] unit: TimeUnit,
    #[case] stored: i64,
    #[case] threshold: i64,
    #[case] greater_than: bool,
    #[values(false, true)] with_timezone: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let timezone = with_timezone.then_some("UTC");
    let (source, expected): (ArrayRef, i64) = match unit {
        TimeUnit::Millisecond => (
            Arc::new(TimestampMillisecondArray::from(vec![stored]).with_timezone_opt(timezone)),
            stored * 1_000,
        ),
        TimeUnit::Nanosecond => (
            Arc::new(TimestampNanosecondArray::from(vec![stored]).with_timezone_opt(timezone)),
            stored / 1_000,
        ),
        _ => unreachable!("test only covers millisecond and nanosecond files"),
    };
    let batch = RecordBatch::try_from_iter([("value", source)])?;
    let threshold = if with_timezone {
        Scalar::Timestamp(threshold)
    } else {
        Scalar::TimestampNtz(threshold)
    };
    let schema = schema_ref! { (StructField::nullable("value", threshold.data_type())) };
    let predicate = if greater_than {
        col!("value").gt(lit(threshold))
    } else {
        col!("value").lt(lit(threshold))
    };
    let batches = read_with_footer_stats(batch, schema, &[], predicate).await?;
    let values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![expected]);
    Ok(())
}

async fn read_with_footer_stats(
    batch: RecordBatch,
    schema: SchemaRef,
    properties: &[(&str, &str)],
    predicate: Predicate,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(DefaultEngineBuilder::new(store.clone()).build());
    let snapshot = create_table("memory:///", schema, "parquet stats regression")
        .with_table_properties(properties.iter().copied())
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let bytes = record_batch_to_bytes_with_props(
        &batch,
        WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build(),
    );
    let size = bytes.len().try_into()?;
    store.put(&Path::from("data.parquet"), bytes.into()).await?;
    let mut txn = begin_transaction(snapshot, engine.as_ref())?;
    // No Delta min/max: the predicate must reach Parquet footer pruning.
    txn.add_files(create_add_files_metadata(
        txn.add_files_schema(),
        vec![("data.parquet", size, 0, Some(batch.num_rows().try_into()?))],
    )?);
    let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;
    Ok(read_scan(&scan, engine)?)
}
