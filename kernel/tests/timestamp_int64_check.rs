//! Quick check: kernel's Timestamp / TimestampNtz both come out as INT64 in parquet.

use std::sync::Arc;

use delta_kernel::arrow::array::TimestampMicrosecondArray;
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
use delta_kernel::parquet::file::reader::{FileReader, SerializedFileReader};
use delta_kernel::schema::{DataType, StructField, StructType};

#[test]
fn test_timestamp_and_timestamp_ntz_write_as_int64() {
    // Build kernel logical schema with both timestamp variants.
    let kernel_schema = StructType::try_new(vec![
        StructField::nullable("ts_utc", DataType::TIMESTAMP),
        StructField::nullable("ts_ntz", DataType::TIMESTAMP_NTZ),
    ])
    .unwrap();

    // Convert to arrow schema using kernel's TryFromKernel impl. This is the same conversion
    // used on the write path via `physical_schema.try_into_arrow()`.
    let arrow_schema: ArrowSchema = (&kernel_schema).try_into_arrow().unwrap();

    // Sanity-check the arrow types.
    assert_eq!(
        arrow_schema.field(0).data_type(),
        &ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "Timestamp should map to Microsecond UTC"
    );
    assert_eq!(
        arrow_schema.field(1).data_type(),
        &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        "TimestampNtz should map to Microsecond no-tz"
    );

    // Build a tiny RecordBatch and write it to a parquet file in memory.
    let utc_field = ArrowField::new(
        "ts_utc",
        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    );
    let ntz_field = ArrowField::new(
        "ts_ntz",
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    );
    let schema_for_batch = Arc::new(ArrowSchema::new(vec![utc_field, ntz_field]));
    let utc_arr = TimestampMicrosecondArray::from(vec![Some(1)]).with_timezone("UTC");
    let ntz_arr = TimestampMicrosecondArray::from(vec![Some(1)]);
    let batch = RecordBatch::try_new(
        schema_for_batch.clone(),
        vec![Arc::new(utc_arr), Arc::new(ntz_arr)],
    )
    .unwrap();

    let mut buf = Vec::new();
    {
        let mut w = ArrowWriter::try_new(&mut buf, schema_for_batch, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    // Crack the parquet schema and read each leaf column's physical type.
    let reader = SerializedFileReader::new(bytes::Bytes::from(buf)).unwrap();
    let descr = reader.metadata().file_metadata().schema_descr();
    let col0 = descr.column(0);
    let col1 = descr.column(1);
    println!(
        "ts_utc: physical={:?}, logical={:?}",
        col0.physical_type(),
        col0.logical_type()
    );
    println!(
        "ts_ntz: physical={:?}, logical={:?}",
        col1.physical_type(),
        col1.logical_type()
    );
    use delta_kernel::parquet::basic::Type as PhysicalType;
    assert_eq!(col0.physical_type(), PhysicalType::INT64);
    assert_eq!(col1.physical_type(), PhysicalType::INT64);
    // None of these should be INT96 (the legacy Spark/Hive form Iceberg rejects).
    assert_ne!(col0.physical_type(), PhysicalType::INT96);
    assert_ne!(col1.physical_type(), PhysicalType::INT96);
}
