use std::sync::{Arc, Mutex};

use bytes::Bytes;
use roaring::RoaringTreemap;
use rstest::rstest;
use test_utils::{
    nested_collection_reconciliation_batches, record_batch_to_bytes,
    record_batch_to_bytes_with_props,
};

use super::*;
use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use crate::arrow::array::{
    Float32Array, Float64Array, Int32Array, Int32Builder, ListArray, MapBuilder, StringArray,
    StringBuilder, StructArray,
};
use crate::arrow::buffer::NullBuffer;
use crate::arrow::compute::concat_batches;
use crate::arrow::datatypes::Schema;
use crate::engine::CancellableEngine;
use crate::engine::arrow_conversion::TryIntoKernel as _;
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::SyncEngine;
use crate::expressions::{
    ArrayData, BinaryPredicateOp, Expression, MapData, Predicate, Scalar, StructData, col, lit,
};
use crate::metrics::MeteredParquetHandler;
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::parquet::data_type::Int32Type as ParquetInt32Type;
use crate::parquet::file::metadata::RowGroupMetaData;
use crate::parquet::file::properties::WriterProperties;
use crate::parquet::file::reader::{ChunkReader, Length};
use crate::parquet::file::writer::SerializedFileWriter;
use crate::parquet::schema::parser::parse_message_type;
use crate::scan::state::DvInfo;
use crate::schema::{
    ArrayType, ColumnMetadataKey, MapType, MetadataColumnSpec, MetadataValue, schema_ref,
};
use crate::unit_test_utils::TestCancellationToken;
use crate::{Engine, FileMeta, FileRowSelection, ParquetHandler};

fn nullable_field_with_id(name: &str, data_type: KernelDataType, id: i64) -> StructField {
    StructField::nullable(name, data_type).with_metadata([(
        ColumnMetadataKey::ParquetFieldId.as_ref(),
        MetadataValue::Number(id),
    )])
}

fn file_bytes() -> Bytes {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "x",
            Arc::new(Int32Array::from(vec![
                Some(0),
                None,
                Some(2),
                Some(3),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(10),
                Some(11),
            ])) as Arc<dyn Array>,
        ),
        (
            "payload",
            Arc::new(StringArray::from_iter_values(
                (0..12).map(|row| format!("row-{row}")),
            )) as Arc<dyn Array>,
        ),
    ])
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .build();
    record_batch_to_bytes_with_props(&batch, properties).into()
}

fn read_options(
    schema: SchemaRef,
    options: ParquetReadOptions,
    batch_size: usize,
) -> KernelResult<Vec<RecordBatch>> {
    read_bytes(file_bytes(), schema, options, batch_size)
}

fn read_bytes(
    bytes: Bytes,
    schema: SchemaRef,
    options: ParquetReadOptions,
    batch_size: usize,
) -> KernelResult<Vec<RecordBatch>> {
    let metadata = ArrowReaderMetadata::load(&bytes, reader_options())?;
    let plan = ParquetReadPlan::new(metadata, schema, options, "memory:///data.parquet".into())?;
    let (builder, finish) = plan.configure(|metadata| {
        ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, metadata)
            .with_batch_size(batch_size)
    });
    builder
        .build()?
        .map(|batch| finish(batch?).map(Into::into))
        .collect()
}

#[rstest]
fn predicate_projection_keeps_complete_file_values(#[values(false, true)] nested: bool) {
    let file = nullable_field_with_id("file", KernelDataType::file_type(), 7);
    let fields = StructType::try_new([
        file.clone(),
        StructField::nullable("unused", KernelDataType::INTEGER),
    ])
    .unwrap();
    let (schema, predicate, expected) = if nested {
        (
            schema_ref! { nullable "parent": (fields), nullable "unused": INTEGER },
            col!("parent.file.size").gt(lit(0i64)),
            schema_ref! { nullable "parent": (StructType::try_new([file]).unwrap()) },
        )
    } else {
        (
            Arc::new(fields),
            col!("file.size").gt(lit(0i64)),
            Arc::new(StructType::try_new([file]).unwrap()),
        )
    };
    let mut projection = PredicateProjection {
        references: predicate.references(),
        path: Vec::new(),
    };
    let actual = projection.transform_struct(&schema).unwrap();
    assert_eq!(actual.as_ref(), expected.as_ref());
}

#[rstest]
#[case::empty(0, vec![], Some(vec![]))]
#[case::empty_groups(0, vec![0, 0], Some(vec![0..0, 0..0]))]
#[case::groups(5, vec![2, 0, 3], Some(vec![0..2, 2..2, 2..5]))]
#[case::negative_file(-1, vec![], None)]
#[case::negative_group(2, vec![3, -1], None)]
#[case::too_few(5, vec![2, 2], None)]
#[case::too_many(3, vec![2, 2], None)]
#[case::overflow(1, vec![i64::MAX, i64::MAX, i64::MAX], None)]
fn exact_read_validates_row_group_counts(
    #[case] file_rows: i64,
    #[case] group_rows: Vec<i64>,
    #[case] expected: Option<Vec<Range<usize>>>,
) {
    let result = validated_row_group_ranges(file_rows, group_rows);
    if let Some(expected) = expected {
        let (total, ranges) = result.unwrap();
        assert_eq!(total, file_rows as usize);
        assert_eq!(ranges, expected);
    } else {
        assert!(result.is_err());
    }
}

#[rstest]
fn row_selection_intersection_keeps_compact_storage(
    #[values(false, true)] scattered_input: bool,
    #[values(false, true)] scattered_pruning: bool,
) {
    let total_rows = 2048;
    let input_ranges = if scattered_input {
        (0..total_rows)
            .step_by(2)
            .map(|row| row..row + 1)
            .collect::<Vec<_>>()
    } else {
        (101..117).chain(300..309).map(|row| row..row + 1).collect()
    };
    let pruning_ranges = if scattered_pruning {
        (0..total_rows)
            .step_by(3)
            .map(|row| row..row + 1)
            .collect::<Vec<_>>()
    } else {
        (105..305).map(|row| row..row + 1).collect()
    };
    let rows = row_selection_from_ranges(input_ranges.into_iter(), total_rows);
    assert_eq!(rows.as_mask().is_some(), scattered_input);
    let result = intersect_row_selection(rows, pruning_ranges.into_iter(), total_rows);
    assert_eq!(
        result.as_mask().is_some(),
        scattered_input || scattered_pruning
    );
    let expected: Vec<bool> = (0..total_rows)
        .map(|row| {
            let selected = if scattered_input {
                row % 2 == 0
            } else {
                (101..117).contains(&row) || (300..309).contains(&row)
            };
            let retained = if scattered_pruning {
                row % 3 == 0
            } else {
                (105..305).contains(&row)
            };
            selected && retained
        })
        .collect();
    assert_eq!(
        result,
        RowSelection::from_boolean_buffer(BooleanBuffer::from(expected))
    );
}

#[rstest]
fn exact_read_scattered_selection_stays_compact_after_pruning(
    #[values("mask", "exclusions")] representation: &str,
    #[values(None, Some(0), Some(256), Some(512))] lower_bound: Option<i32>,
    #[values(false, true)] empty_output: bool,
) {
    let batch = RecordBatch::try_from_iter([(
        "x",
        Arc::new(Int32Array::from_iter_values(0..512)) as Arc<dyn Array>,
    )])
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(128))
        .build();
    let bytes: Bytes = record_batch_to_bytes_with_props(&batch, properties).into();
    let schema = Arc::new(
        schema_ref! { not_null "x": INTEGER }
            .add_metadata_column("position", MetadataColumnSpec::RowIndex)
            .unwrap(),
    );
    let output = schema
        .project(if empty_output {
            &[] as &[&str]
        } else {
            &["position"]
        })
        .unwrap();
    let selection = if representation == "mask" {
        FileRowSelection::from_keep_mask(vec![0x55; 64], 512).unwrap()
    } else {
        FileRowSelection::from_exclusions(RoaringTreemap::from_iter((1..512).step_by(2)))
    };
    let mut options = ParquetReadOptions::default()
        .with_output_schema(output)
        .with_row_selection(selection)
        .with_result_offset(2)
        .with_result_limit(4);
    if let Some(lower_bound) = lower_bound {
        options = options.with_row_filter(Arc::new(col!("x").ge(lit(lower_bound))));
    }
    let metadata = ArrowReaderMetadata::load(&bytes, reader_options()).unwrap();
    let plan =
        ParquetReadPlan::new(metadata, schema, options, "memory:///data.parquet".into()).unwrap();
    assert_eq!(
        plan.selection.as_ref().unwrap().as_mask().is_some(),
        lower_bound != Some(512)
    );
    let (builder, finish) = plan.configure(|metadata| {
        ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, metadata).with_batch_size(3)
    });
    let batches: Vec<RecordBatch> = builder
        .build()
        .unwrap()
        .map(|batch| finish(batch.unwrap()).unwrap().into())
        .collect();
    let expected: Vec<i64> = (0..512)
        .filter(|row| row % 2 == 0 && lower_bound.is_none_or(|bound| *row >= bound))
        .skip(2)
        .take(4)
        .map(i64::from)
        .collect();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        expected.len()
    );
    if empty_output {
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    } else {
        let positions: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(positions, expected);
    }
}

#[rstest]
fn exact_read_selection_filter_window_preserves_row_indexes(
    #[values(1, 2, 32)] batch_size: usize,
    #[values("ranges", "mask", "exclusions")] representation: &str,
    #[values(false, true)] empty_output: bool,
) {
    let schema = Arc::new(
        schema_ref! { nullable "x": INTEGER, nullable "payload": STRING }
            .add_metadata_column("position", MetadataColumnSpec::RowIndex)
            .unwrap(),
    );
    let selection = match representation {
        "ranges" => FileRowSelection::from_ranges(std::iter::once(2..10)).unwrap(),
        "mask" => FileRowSelection::from_keep_mask(vec![0xfc, 0x03], 12).unwrap(),
        _ => FileRowSelection::from_exclusions(RoaringTreemap::from_iter([0, 1, 10, 11])),
    };
    let output = schema
        .project(if empty_output {
            &[]
        } else {
            &["payload", "position"][..]
        })
        .unwrap();
    let options = ParquetReadOptions::default()
        .with_output_schema(output.clone())
        .with_row_selection(selection)
        .with_row_filter(Arc::new(col!("x").gt(lit(3i32))))
        .with_result_offset(1)
        .with_result_limit(3);
    let batches = read_options(schema, options, batch_size).unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert!(batches.iter().all(|batch| batch.num_rows() <= batch_size));
    if empty_output {
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    } else {
        let result = concat_batches(&batches[0].schema(), &batches).unwrap();
        let positions = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(positions.values(), &[6, 7, 8]);
        let payload = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            payload.iter().collect::<Vec<_>>(),
            vec![Some("row-6"), Some("row-7"), Some("row-8")]
        );
    }
}

#[rstest]
#[case::zero_limit(ParquetReadOptions::default().with_result_limit(0), 0)]
#[case::past_end(ParquetReadOptions::default().with_result_offset(99), 0)]
#[case::empty_selection(ParquetReadOptions::default().with_row_selection(FileRowSelection::from_ranges(vec![]).unwrap()), 0)]
#[case::null_filter(ParquetReadOptions::default().with_row_filter(Arc::new(Predicate::NULL)), 0)]
#[case::false_filter(ParquetReadOptions::default().with_row_filter(Arc::new(Predicate::FALSE)), 0)]
#[case::true_filter(ParquetReadOptions::default().with_row_filter(Arc::new(Predicate::TRUE)), 12)]
#[case::default(ParquetReadOptions::default(), 12)]
fn exact_read_empty_and_unfiltered_results(
    #[case] options: ParquetReadOptions,
    #[case] expected: usize,
) {
    let batches = read_options(schema_ref! { nullable "x": INTEGER }, options, 2).unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        expected
    );
}

#[rstest]
#[case::unknown(ParquetReadOptions::default().with_row_filter(Arc::new(Predicate::Unknown("unsupported".into()))))]
#[case::missing_reference(ParquetReadOptions::default().with_row_filter(Arc::new(col!("absent").eq(lit(1i32)))))]
#[case::invalid_projection(ParquetReadOptions::default().with_output_schema(schema_ref! { nullable "x": LONG }))]
#[case::out_of_bounds(ParquetReadOptions::default().with_row_selection(FileRowSelection::from_ranges(std::iter::once(0..13)).unwrap()))]
#[case::short_mask(ParquetReadOptions::default().with_row_selection(FileRowSelection::from_keep_mask(vec![255], 8).unwrap()))]
#[case::unknown_when(ParquetReadOptions::default().with_row_filter(Arc::new(
    Expression::case_when([(col!("x").gt(lit(3i32)), Expression::unknown("unsupported"))], lit(0i32)).eq(lit(0i32))
)))]
#[case::unknown_coalesce(ParquetReadOptions::default().with_row_filter(Arc::new(
    Expression::coalesce([col!("x"), Expression::unknown("unsupported")]).eq(lit(0i32))
)))]
#[case::incompatible_when(ParquetReadOptions::default().with_row_filter(Arc::new(
    Expression::case_when([(col!("x").gt(lit(3i32)), lit("wrong"))], lit(0i32)).eq(lit(0i32))
)))]
#[case::incompatible_coalesce(ParquetReadOptions::default().with_row_filter(Arc::new(
    Expression::coalesce([col!("x"), lit("wrong")]).eq(lit(0i32))
)))]
fn exact_read_rejects_invalid_options_before_decoding(#[case] options: ParquetReadOptions) {
    let bytes = file_bytes();
    let metadata = ArrowReaderMetadata::load(&bytes, reader_options()).unwrap();
    assert!(
        ParquetReadPlan::new(
            metadata,
            schema_ref! { nullable "x": INTEGER },
            options,
            "file".into()
        )
        .is_err()
    );
}

#[rstest]
#[case::array(Scalar::Array(ArrayData::from(vec![1i32])))]
#[case::structure(Scalar::Struct(StructData::try_new(
    vec![StructField::not_null("a", KernelDataType::INTEGER)],
    vec![Scalar::Integer(1)],
).unwrap()))]
#[case::map(Scalar::Map(MapData::try_new(
    MapType::new(KernelDataType::STRING, KernelDataType::INTEGER, false),
    [(Scalar::String("a".into()), Scalar::Integer(1))],
).unwrap()))]
fn exact_read_rejects_compound_literal_in_before_decoding(
    #[case] needle: Scalar,
    #[values("root", "case", "coalesce")] composition: &str,
    #[values(false, true)] negated: bool,
) {
    let values =
        ArrayData::try_new(ArrayType::new(needle.data_type(), false), [needle.clone()]).unwrap();
    let predicate = Predicate::binary(
        BinaryPredicateOp::In,
        lit(needle),
        lit(Scalar::Array(values)),
    );
    let predicate = if negated {
        Predicate::not(predicate)
    } else {
        predicate
    };
    let predicate = match composition {
        "case" => {
            Expression::case_when([(Predicate::FALSE, Expression::from(predicate))], lit(true))
                .eq(lit(true))
        }
        "coalesce" => Expression::coalesce([lit(true), Expression::from(predicate)]).eq(lit(true)),
        _ => predicate,
    };
    let bytes = file_bytes();
    let metadata = ArrowReaderMetadata::load(&bytes, reader_options()).unwrap();
    let error = ParquetReadPlan::new(
        metadata,
        schema_ref! { nullable "x": INTEGER },
        ParquetReadOptions::default().with_row_filter(Arc::new(predicate)),
        "file".into(),
    )
    .err()
    .unwrap();
    assert!(matches!(
        error.without_backtrace(),
        KernelError::Unsupported(_)
    ));
}

#[rstest]
fn exact_read_accepts_nested_case_and_coalesce(#[values(1, 4, 32)] batch_size: usize) {
    let predicate = Expression::case_when(
        [(col!("x").lt(lit(3i32)), lit(99i32))],
        Expression::coalesce([col!("x"), lit(99i32)]),
    )
    .eq(lit(99i32));
    let options = ParquetReadOptions::default()
        .with_output_schema(schema_ref! { nullable "payload": STRING })
        .with_row_filter(Arc::new(predicate));
    let batches = read_options(
        schema_ref! { nullable "x": INTEGER, nullable "payload": STRING },
        options,
        batch_size,
    )
    .unwrap();
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();
    let payload = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        payload.iter().collect::<Vec<_>>(),
        vec![Some("row-0"), Some("row-1"), Some("row-2"), Some("row-5")]
    );
}

#[rstest]
#[case::root("root", 12, 0)]
#[case::and("and", 3, 0)]
#[case::or("or", 12, 3)]
#[case::coalesce("coalesce", 12, 0)]
fn exact_read_composes_literal_in_filters(
    #[case] composition: &str,
    #[case] when_true: usize,
    #[case] when_false: usize,
    #[values(1, 2, 32)] batch_size: usize,
    #[values(1, 3)] needle: i32,
) {
    let elements = ArrayData::try_new(
        ArrayType::new(KernelDataType::INTEGER, false),
        [Scalar::Integer(1), Scalar::Integer(2)],
    )
    .unwrap();
    let predicate = Predicate::binary(
        BinaryPredicateOp::In,
        lit(needle),
        lit(Scalar::Array(elements)),
    );
    let predicate = match composition {
        "and" => Predicate::and(predicate, col!("x").gt(lit(8i32))),
        "or" => Predicate::or(predicate, col!("x").gt(lit(8i32))),
        "coalesce" => Expression::coalesce([Expression::from(predicate), lit(false)]).eq(lit(true)),
        _ => predicate,
    };
    let batches = read_options(
        schema_ref! { nullable "x": INTEGER },
        ParquetReadOptions::default().with_row_filter(Arc::new(predicate)),
        batch_size,
    )
    .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        if needle == 1 { when_true } else { when_false }
    );
}

#[test]
fn exact_read_reconciles_widening_and_missing_predicate_columns() {
    let schema = schema_ref! { nullable "x": LONG, nullable "missing": INTEGER };
    let predicate = Predicate::and(col!("x").gt(lit(8i64)), col!("missing").is_null());
    let batches = read_options(
        schema,
        ParquetReadOptions::default().with_row_filter(Arc::new(predicate)),
        2,
    )
    .unwrap();
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[9, 10, 11]
    );
    assert_eq!(result.column(1).null_count(), 3);
}

#[rstest]
fn exact_read_reconciles_nested_collections(
    #[values("default", "identical", "reordered")] output_projection: &str,
    #[values(false, true)] sync_handler: bool,
) {
    let (stored, widened, expected) = nested_collection_reconciliation_batches();
    let output_schema: SchemaRef = Arc::new(expected.schema().try_into_kernel().unwrap());
    let read_schema = if output_projection == "reordered" {
        Arc::new(widened.schema().try_into_kernel().unwrap())
    } else {
        output_schema.clone()
    };
    let options = if output_projection == "default" {
        ParquetReadOptions::default()
    } else {
        ParquetReadOptions::default().with_output_schema(output_schema)
    };
    let bytes = record_batch_to_bytes(&stored);
    let batches = if sync_handler {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.parquet");
        std::fs::write(&path, &bytes).unwrap();
        let file = FileMeta::new(
            url::Url::from_file_path(path).unwrap(),
            0,
            bytes.len() as u64,
        );
        SyncEngine::new()
            .parquet_handler()
            .read_parquet_file_with_options(&file, read_schema, options)
            .unwrap()
            .map(|batch| batch.try_into_record_batch())
            .collect::<KernelResult<Vec<_>>>()
            .unwrap()
    } else {
        read_bytes(bytes.into(), read_schema, options, 1).unwrap()
    };
    let actual = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn exact_read_filters_generated_row_indexes() {
    let schema = Arc::new(
        schema_ref! { nullable "payload": STRING }
            .add_metadata_column("position", MetadataColumnSpec::RowIndex)
            .unwrap(),
    );
    let options =
        ParquetReadOptions::default().with_row_filter(Arc::new(col!("position").gt(lit(9i64))));
    let batches = read_options(schema, options, 1).unwrap();
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(
        result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[10, 11]
    );
}

struct TrackingReader {
    bytes: Bytes,
    reads: Arc<Mutex<Vec<u64>>>,
}

impl Length for TrackingReader {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

impl ChunkReader for TrackingReader {
    type T = <Bytes as ChunkReader>::T;

    fn get_read(&self, start: u64) -> crate::parquet::errors::Result<Self::T> {
        self.reads.lock().unwrap().push(start);
        self.bytes.get_read(start)
    }

    fn get_bytes(&self, start: u64, length: usize) -> crate::parquet::errors::Result<Bytes> {
        self.reads.lock().unwrap().push(start);
        self.bytes.get_bytes(start, length)
    }
}

#[rstest]
fn exact_read_legacy_empty_lists_are_not_null(#[values(false, true)] empty_output: bool) {
    let parquet_schema = Arc::new(
        parse_message_type("message schema { repeated int32 x; required int32 y; }").unwrap(),
    );
    let mut bytes = Vec::new();
    let mut writer =
        SerializedFileWriter::new(&mut bytes, parquet_schema, Default::default()).unwrap();
    let mut group = writer.next_row_group().unwrap();
    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<ParquetInt32Type>()
        .write_batch(&[], Some(&[0, 0]), Some(&[0, 0]))
        .unwrap();
    column.close().unwrap();
    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<ParquetInt32Type>()
        .write_batch(&[1, 2], None, None)
        .unwrap();
    column.close().unwrap();
    group.close().unwrap();
    writer.close().unwrap();
    let bytes = Bytes::from(bytes);
    let metadata = ArrowReaderMetadata::load(&bytes, reader_options()).unwrap();
    assert!(!RowGroupFilter::apply(
        metadata.metadata().row_group(0),
        &col!("y").gt(lit(5i32))
    ));
    let schema: StructType = metadata.schema().as_ref().try_into_kernel().unwrap();
    let schema = Arc::new(
        schema
            .add_metadata_column("position", MetadataColumnSpec::RowIndex)
            .unwrap(),
    );
    let output = schema
        .project(if empty_output {
            &[]
        } else {
            &["position", "x"][..]
        })
        .unwrap();
    let batches = read_bytes(
        bytes,
        schema,
        ParquetReadOptions::default()
            .with_output_schema(output)
            .with_row_filter(Arc::new(Predicate::not(col!("x").is_null()))),
        1,
    )
    .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    if empty_output {
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    } else {
        let actual = concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(
            actual
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 1]
        );
        let lists = actual
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(lists.null_count(), 0);
        assert_eq!(lists.value_length(0), 0);
        assert_eq!(lists.value_length(1), 0);
    }
}

#[rstest]
#[case::signed_zero(false)]
#[case::nan(true)]
fn exact_read_float_literal_predicates_are_not_pruned(
    #[case] nan: bool,
    #[values(false, true)] double: bool,
    #[values(false, true)] with_column: bool,
) {
    let (left, right) = if nan { (f64::NAN, 10.0) } else { (-0.0, 0.0) };
    let (left, right) = if double {
        (lit(left), lit(right))
    } else {
        (lit(left as f32), lit(right as f32))
    };
    let predicate = if nan {
        left.gt(right)
    } else {
        Predicate::not(left.eq(right))
    };
    let predicate = if with_column {
        Predicate::and(predicate, col!("x").gt(lit(8i32)))
    } else {
        predicate
    };
    let schema = schema_ref! { nullable "x": INTEGER };
    let options = ParquetReadOptions::default()
        .with_output_schema(schema.project(&[] as &[&str]).unwrap())
        .with_row_filter(Arc::new(predicate));
    let batches = read_options(schema, options, 2).unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        if with_column { 3 } else { 12 }
    );
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
}

#[rstest]
fn exact_read_float_predicates_preserve_nan_matches(
    #[values(false, true)] double: bool,
    #[values(false, true)] nested: bool,
    #[values(false, true)] reordered: bool,
    #[values(false, true)] negated_equality: bool,
) {
    let last = if negated_equality { 1.0 } else { 2.0 };
    let (values, data_type, literal): (Arc<dyn Array>, _, _) = if double {
        (
            Arc::new(Float64Array::from(vec![1.0, f64::NAN, last])),
            KernelDataType::DOUBLE,
            lit(if negated_equality { 1.0f64 } else { 10.0 }),
        )
    } else {
        (
            Arc::new(Float32Array::from(vec![1.0, f32::NAN, last as f32])),
            KernelDataType::FLOAT,
            lit(if negated_equality { 1.0f32 } else { 10.0 }),
        )
    };
    let values = StructArray::from(vec![
        (
            Arc::new(Field::new("x", values.data_type().clone(), true)),
            values,
        ),
        (
            Arc::new(Field::new("b", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![1, 1, 1])) as Arc<dyn Array>,
        ),
    ]);
    let fields = schema_ref! { nullable "x": (data_type), nullable "b": INTEGER };
    let fields = fields
        .project_as_struct(if reordered { &["b", "x"] } else { &["x", "b"] })
        .unwrap();
    let (batch, schema, x, b) = if nested {
        (
            RecordBatch::try_from_iter([("s", Arc::new(values) as Arc<dyn Array>)]).unwrap(),
            schema_ref! { nullable "s": (fields) },
            col!("s.x"),
            col!("s.b"),
        )
    } else {
        (
            RecordBatch::from(values),
            Arc::new(fields),
            col!("x"),
            col!("b"),
        )
    };
    let predicate = if negated_equality {
        Predicate::not(x.eq(literal))
    } else {
        x.gt(literal)
    };
    let schema = Arc::new(
        schema
            .add_metadata_column("position", MetadataColumnSpec::RowIndex)
            .unwrap(),
    );
    let options = ParquetReadOptions::default()
        .with_output_schema(schema.project(&["position"]).unwrap())
        .with_row_filter(Arc::new(Predicate::and(predicate, b.gt(lit(0i32)))));
    let batches = read_bytes(record_batch_to_bytes(&batch).into(), schema, options, 1).unwrap();
    let actual = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(actual.num_rows(), 1);
    assert_eq!(
        actual
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1]
    );
}

#[rstest]
fn exact_read_footer_pruning_ignores_column_order(
    #[values(false, true)] nested: bool,
    #[values(false, true)] reordered: bool,
) {
    let values = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("b", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![5, 6, 7])) as Arc<dyn Array>,
        ),
    ]);
    let batch = if nested {
        RecordBatch::try_from_iter([("s", Arc::new(values) as Arc<dyn Array>)]).unwrap()
    } else {
        RecordBatch::from(values)
    };
    let fields = schema_ref! { nullable "a": INTEGER, nullable "b": INTEGER };
    let fields = fields
        .project_as_struct(if reordered { &["b", "a"] } else { &["a", "b"] })
        .unwrap();
    let (schema, predicate) = if nested {
        (
            schema_ref! { nullable "s": (fields) },
            Predicate::and(col!("s.a").gt(lit(100i32)), col!("s.b").gt(lit(0i32))),
        )
    } else {
        (
            Arc::new(fields),
            Predicate::and(col!("a").gt(lit(100i32)), col!("b").gt(lit(0i32))),
        )
    };
    let bytes = Bytes::from(record_batch_to_bytes(&batch));
    let metadata = ArrowReaderMetadata::load(&bytes, reader_options()).unwrap();
    let reads = Arc::new(Mutex::new(Vec::new()));
    let reader = TrackingReader {
        bytes,
        reads: reads.clone(),
    };
    let plan = ParquetReadPlan::new(
        metadata,
        schema,
        ParquetReadOptions::default().with_row_filter(Arc::new(predicate)),
        "file".into(),
    )
    .unwrap();
    let (builder, _) = plan
        .configure(|metadata| ParquetRecordBatchReaderBuilder::new_with_metadata(reader, metadata));
    assert_eq!(builder.build().unwrap().count(), 0);
    assert!(
        reads.lock().unwrap().is_empty(),
        "footer pruning must avoid reading column data"
    );
}

#[test]
fn exact_read_nested_filter_defers_output_only_sibling() {
    let nested = StructArray::from(vec![
        (
            Arc::new(Field::new("key", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("payload", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as Arc<dyn Array>,
        ),
    ]);
    let batch = RecordBatch::try_from_iter([("s", Arc::new(nested) as Arc<dyn Array>)]).unwrap();
    let bytes = record_batch_to_bytes(&batch);
    let bytes = Bytes::from(bytes);
    let metadata = ArrowReaderMetadata::load(&bytes, reader_options()).unwrap();
    let (payload_start, payload_length) = metadata.metadata().row_group(0).column(1).byte_range();
    let reads = Arc::new(Mutex::new(Vec::new()));
    let reader = TrackingReader {
        bytes,
        reads: reads.clone(),
    };
    // Widening prevents footer pruning, so the exact filter must reject these rows.
    let schema = schema_ref! { nullable "s": { nullable "key": LONG, nullable "payload": STRING } };
    let options = ParquetReadOptions::default()
        .with_output_schema(schema_ref! { nullable "s": { nullable "payload": STRING } })
        .with_row_filter(Arc::new(col!("s.key").gt(lit(10i64))));
    let plan = ParquetReadPlan::new(metadata, schema, options, "file".into()).unwrap();
    let (builder, _) = plan
        .configure(|metadata| ParquetRecordBatchReaderBuilder::new_with_metadata(reader, metadata));
    assert_eq!(builder.build().unwrap().count(), 0);
    let reads = reads.lock().unwrap();
    assert!(!reads.is_empty());
    assert!(
        reads
            .iter()
            .all(|start| !(payload_start..payload_start + payload_length).contains(start)),
        "output-only column read before rejecting every row: {reads:?}"
    );
}

#[rstest]
fn exact_read_nested_field_ids_null_parent_and_widening(#[values(1, 3)] batch_size: usize) {
    let stored_fields = StructType::try_new([
        nullable_field_with_id("old_key", KernelDataType::INTEGER, 2),
        nullable_field_with_id("old_payload", KernelDataType::STRING, 3),
    ])
    .unwrap();
    let stored = StructType::try_new([nullable_field_with_id(
        "old_parent",
        stored_fields.into(),
        1,
    )])
    .unwrap();
    let stored: Schema = (&stored).try_into_arrow().unwrap();
    let DataType::Struct(children) = stored.field(0).data_type() else {
        panic!("expected struct")
    };
    let values = StructArray::new(
        children.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
        ],
        Some(NullBuffer::from(vec![true, false, true, true, true, true])),
    );
    let batch = RecordBatch::try_new(Arc::new(stored), vec![Arc::new(values)]).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let bytes = record_batch_to_bytes_with_props(&batch, props);
    let requested = StructType::try_new([
        nullable_field_with_id("payload", KernelDataType::STRING, 3),
        nullable_field_with_id("key", KernelDataType::LONG, 2),
    ])
    .unwrap();
    let schema = Arc::new(
        StructType::try_new([nullable_field_with_id("parent", requested.into(), 1)]).unwrap(),
    );
    let predicate = Predicate::or(
        col!("parent.key").gt(lit(1i64)),
        col!("parent.key").is_null(),
    );
    let options = ParquetReadOptions::default()
        .with_row_filter(Arc::new(predicate))
        .with_row_selection(FileRowSelection::from_exclusions(
            RoaringTreemap::from_iter([0, 4]),
        ))
        .with_result_limit(3);
    let batches = read_bytes(bytes.into(), schema.clone(), options, batch_size).unwrap();
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();
    let expected_schema: Schema = schema.as_ref().try_into_arrow().unwrap();
    let DataType::Struct(children) = expected_schema.field(0).data_type() else {
        panic!("expected struct")
    };
    let expected = StructArray::new(
        children.clone(),
        vec![
            Arc::new(StringArray::from(vec![None, Some("c"), Some("d")])),
            Arc::new(Int64Array::from(vec![None, Some(2), Some(3)])),
        ],
        Some(NullBuffer::from(vec![false, true, true])),
    );
    let expected =
        RecordBatch::try_new(Arc::new(expected_schema), vec![Arc::new(expected)]).unwrap();
    assert_eq!(result, expected);
}

#[rstest]
fn exact_read_field_id_renames_do_not_use_other_columns_footer_stats(
    #[values(false, true)] both_references: bool,
) {
    let stored = StructType::try_new([
        nullable_field_with_id("a", KernelDataType::INTEGER, 1),
        nullable_field_with_id("b", KernelDataType::INTEGER, 2),
    ])
    .unwrap();
    let stored: Schema = (&stored).try_into_arrow().unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(stored),
        vec![
            Arc::new(Int32Array::from(vec![5, 6])),
            Arc::new(Int32Array::from(vec![0, 0])),
        ],
    )
    .unwrap();
    let bytes = record_batch_to_bytes(&batch);
    let schema = Arc::new(
        StructType::try_new([
            nullable_field_with_id("b", KernelDataType::INTEGER, 1),
            nullable_field_with_id("a", KernelDataType::INTEGER, 2),
        ])
        .unwrap(),
    );
    let predicate = if both_references {
        Predicate::and(col!("b").gt(lit(4i32)), col!("a").eq(lit(0i32)))
    } else {
        col!("b").gt(lit(4i32))
    };
    let batches = read_bytes(
        bytes.into(),
        schema,
        ParquetReadOptions::default().with_row_filter(Arc::new(predicate)),
        32,
    )
    .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[5, 6]
    );
}

#[rstest]
fn exact_read_field_id_name_fallback_reconciles(
    #[values(false, true)] nested: bool,
    #[values(false, true)] widen: bool,
    #[values("output", "filter_and_output", "predicate_only")] usage: &str,
) {
    let make_schema = |id, data_type| {
        let fields = StructType::try_new([nullable_field_with_id("x", data_type, id)]).unwrap();
        Arc::new(if nested {
            StructType::try_new([nullable_field_with_id("s", fields.into(), id + 1)]).unwrap()
        } else {
            fields
        })
    };
    let make_batch = |schema: &SchemaRef, values: Arc<dyn Array>| {
        let schema: Schema = schema.as_ref().try_into_arrow().unwrap();
        let values = if nested {
            let DataType::Struct(children) = schema.field(0).data_type() else {
                panic!("expected struct")
            };
            Arc::new(StructArray::new(children.clone(), vec![values], None)) as Arc<dyn Array>
        } else {
            values
        };
        RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap()
    };
    let stored = make_schema(42, KernelDataType::INTEGER);
    let bytes = record_batch_to_bytes(&make_batch(
        &stored,
        Arc::new(Int32Array::from(vec![1, 2, 3])),
    ));
    let schema = make_schema(
        3,
        if widen {
            KernelDataType::LONG
        } else {
            KernelDataType::INTEGER
        },
    );
    let mut options = ParquetReadOptions::default();
    let filtered = usage != "output";
    if filtered {
        let column = if nested { col!("s.x") } else { col!("x") };
        let threshold = if widen { lit(1i64) } else { lit(1i32) };
        options = options.with_row_filter(Arc::new(column.gt(threshold)));
    }
    if usage == "predicate_only" {
        options = options.with_output_schema(schema.project(&[] as &[&str]).unwrap());
    }
    let batches = read_bytes(bytes.into(), schema.clone(), options, 1).unwrap();
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(result.num_rows(), if filtered { 2 } else { 3 });
    if usage == "predicate_only" {
        assert_eq!(result.num_columns(), 0);
    } else {
        let values = if filtered { vec![2, 3] } else { vec![1, 2, 3] };
        let values: Arc<dyn Array> = if widen {
            Arc::new(Int64Array::from(
                values.into_iter().map(i64::from).collect::<Vec<_>>(),
            ))
        } else {
            Arc::new(Int32Array::from(values))
        };
        assert_eq!(result, make_batch(&schema, values));
    }
}

#[rstest]
#[case::project_missing(None, vec![None, None])]
#[case::filter_missing(Some(true), vec![Some(5), Some(6)])]
#[case::reject_missing(Some(false), vec![])]
fn exact_read_projection_preserves_full_schema_field_id_bindings(
    #[values(false, true)] nested: bool,
    #[case] keep_null: Option<bool>,
    #[case] expected: Vec<Option<i32>>,
) {
    let wrap = |schema: StructType| {
        if nested {
            StructType::try_new([StructField::nullable("s", schema)]).unwrap()
        } else {
            schema
        }
    };
    let stored = wrap(
        StructType::try_new([nullable_field_with_id("a", KernelDataType::INTEGER, 2)]).unwrap(),
    );
    let stored: Schema = (&stored).try_into_arrow().unwrap();
    let values: Arc<dyn Array> = Arc::new(Int32Array::from(vec![5, 6]));
    let values = if nested {
        let DataType::Struct(children) = stored.field(0).data_type() else {
            panic!("expected struct")
        };
        Arc::new(StructArray::new(children.clone(), vec![values], None)) as Arc<dyn Array>
    } else {
        values
    };
    let batch = RecordBatch::try_new(Arc::new(stored), vec![values]).unwrap();
    let bytes = record_batch_to_bytes(&batch);

    let schema = Arc::new(wrap(
        StructType::try_new([
            nullable_field_with_id("a", KernelDataType::INTEGER, 1),
            nullable_field_with_id("b", KernelDataType::INTEGER, 2),
        ])
        .unwrap(),
    ));
    let output = if keep_null.is_some() {
        nullable_field_with_id("b", KernelDataType::INTEGER, 2)
    } else {
        nullable_field_with_id("a", KernelDataType::INTEGER, 1)
    };
    let output = Arc::new(wrap(StructType::try_new([output]).unwrap()));
    let mut options = ParquetReadOptions::default().with_output_schema(output);
    if let Some(keep_null) = keep_null {
        let column = if nested { col!("s.a") } else { col!("a") };
        let predicate = if keep_null {
            column.is_null()
        } else {
            column.is_not_null()
        };
        options = options.with_row_filter(Arc::new(predicate));
    }
    let batches = read_bytes(bytes.into(), schema, options, 1).unwrap();
    let actual: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let values = if nested {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .column(0)
            } else {
                batch.column(0)
            };
            values.as_any().downcast_ref::<Int32Array>().unwrap().iter()
        })
        .collect();
    assert_eq!(actual, expected);
}

#[rstest]
#[case::projection(None)]
#[case::null_filter(Some(true))]
#[case::nonnull_filter(Some(false))]
fn exact_read_missing_children_preserve_parent_validity(
    #[values(false, true)] nullable: bool,
    #[values(false, true)] read_stored_child: bool,
    #[values(false, true)] map_carrier: bool,
    #[case] keep_null: Option<bool>,
) {
    let (stored_values, stored_type): (Arc<dyn Array>, KernelDataType) = if map_carrier {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        for value in [5, 6, 7] {
            builder.keys().append_value("key");
            builder.values().append_value(value);
            builder.append(true).unwrap();
        }
        (
            Arc::new(builder.finish()),
            MapType::new(KernelDataType::STRING, KernelDataType::INTEGER, true).into(),
        )
    } else {
        (
            Arc::new(Int32Array::from(vec![5, 6, 7])),
            KernelDataType::INTEGER,
        )
    };
    let stored_child = Arc::new(Field::new("b", stored_values.data_type().clone(), true));
    let values = StructArray::new(
        vec![stored_child].into(),
        vec![stored_values],
        nullable.then(|| NullBuffer::from(vec![true, false, true])),
    );
    let stored = Schema::new(vec![Field::new("s", values.data_type().clone(), nullable)]);
    let batch = RecordBatch::try_new(Arc::new(stored), vec![Arc::new(values)]).unwrap();
    let bytes = record_batch_to_bytes(&batch);

    let missing_child = StructField::nullable("a", KernelDataType::INTEGER);
    let make_schema = |children| {
        Arc::new(
            StructType::try_new([StructField::new("s", children, nullable)])
                .unwrap()
                .add_metadata_column("position", MetadataColumnSpec::RowIndex)
                .unwrap(),
        )
    };
    let output = make_schema(StructType::try_new([missing_child.clone()]).unwrap());
    let mut children = vec![missing_child];
    if read_stored_child {
        children.push(StructField::nullable("b", stored_type));
    }
    let schema = make_schema(StructType::try_new(children).unwrap());
    let mut options = ParquetReadOptions::default().with_output_schema(output);
    if let Some(keep_null) = keep_null {
        let predicate = if keep_null {
            col!("s").is_null()
        } else {
            col!("s").is_not_null()
        };
        options = options.with_row_filter(Arc::new(predicate));
    }
    let batches = read_bytes(bytes.into(), schema, options, 1).unwrap();
    let mut actual_positions = Vec::new();
    for batch in &batches {
        let parent = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(parent.column(0).null_count(), batch.num_rows());
        for row in 0..batch.num_rows() {
            let position = positions.value(row);
            actual_positions.push(position);
            assert_eq!(parent.is_null(row), nullable && position == 1);
        }
    }
    let expected_positions: Vec<i64> = (0..3)
        .filter(|position| {
            keep_null.is_none_or(|keep_null| keep_null == (nullable && *position == 1))
        })
        .collect();
    assert_eq!(actual_positions, expected_positions);
}

#[test]
fn exact_read_row_indexes_use_footer_order_with_missing_ordinal() {
    let bytes = file_bytes();
    let metadata = ArrowReaderMetadata::load(&bytes, reader_options()).unwrap();
    let mut row_groups = metadata.metadata().row_groups().to_vec();
    let group = &row_groups[1];
    row_groups[1] = RowGroupMetaData::builder(group.schema_descr_ptr())
        .set_num_rows(group.num_rows())
        .set_total_byte_size(group.total_byte_size())
        .set_column_metadata(group.columns().to_vec())
        .build()
        .unwrap();
    let metadata = metadata
        .metadata()
        .as_ref()
        .clone()
        .into_builder()
        .set_row_groups(row_groups)
        .build();
    let metadata = ArrowReaderMetadata::try_new(Arc::new(metadata), reader_options()).unwrap();
    let schema = Arc::new(
        schema_ref! {}
            .add_metadata_column("position", MetadataColumnSpec::RowIndex)
            .unwrap(),
    );
    let options = ParquetReadOptions::default()
        .with_row_filter(Arc::new(col!("position").gt(lit(3i64))))
        .with_row_selection(FileRowSelection::from_exclusions(
            RoaringTreemap::from_iter([5, 8]),
        ))
        .with_result_offset(1)
        .with_result_limit(3);
    let plan = ParquetReadPlan::new(metadata, schema, options, "file".into()).unwrap();
    let (builder, finish) = plan.configure(|metadata| {
        ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, metadata).with_batch_size(2)
    });
    let batches = builder
        .build()
        .unwrap()
        .map(|batch| finish(batch.unwrap()).map(Into::into))
        .collect::<KernelResult<Vec<RecordBatch>>>()
        .unwrap();
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[6, 7, 9]
    );
}

#[test]
fn exact_read_dv_selection_preserves_live_tail() {
    let descriptor = DeletionVectorDescriptor {
        storage_type: DeletionVectorStorageType::Inline,
        path_or_inline_dv: "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L".into(),
        offset: None,
        size_in_bytes: 44,
        cardinality: 6,
    };
    let engine = SyncEngine::new();
    let root = url::Url::parse("memory:///table/").unwrap();
    let selection = DvInfo::from(descriptor)
        .get_file_row_selection(&engine, &root)
        .unwrap()
        .unwrap();
    assert_eq!(
        selection.ranges(40).unwrap().collect::<Vec<_>>(),
        vec![0..3, 5..7, 8..11, 12..18, 19..29, 30..40]
    );
    assert!(
        DvInfo::default()
            .get_file_row_selection(&engine, &root)
            .unwrap()
            .is_none()
    );
}

#[test]
fn exact_read_sync_wrappers_forward_options_and_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data.parquet");
    let bytes = file_bytes();
    std::fs::write(&path, &bytes).unwrap();
    let file = FileMeta::new(
        url::Url::from_file_path(path).unwrap(),
        0,
        bytes.len() as u64,
    );
    let token = Arc::new(TestCancellationToken::default());
    let engine = CancellableEngine::new(&SyncEngine::new(), token.clone());
    let handler = MeteredParquetHandler::new(engine.parquet_handler());
    let schema = schema_ref! { nullable "x": INTEGER };
    let options = ParquetReadOptions::default().with_row_filter(Arc::new(col!("x").gt(lit(8i32))));
    let mut batches = handler
        .read_parquet_file_with_options(&file, schema.clone(), options.clone())
        .unwrap();
    assert_eq!(batches.next().unwrap().unwrap().len(), 3);
    token.cancel();
    assert!(
        matches!(batches.next(), Some(Err(error)) if matches!(error.without_backtrace(), KernelError::Cancelled))
    );
    assert!(
        handler
            .read_parquet_file_with_options(&file, schema.clone(), options.clone())
            .is_err()
    );
    let overridden = options.with_cancellation_token(Arc::new(TestCancellationToken::default()));
    assert_eq!(
        handler
            .read_parquet_file_with_options(&file, schema, overridden)
            .unwrap()
            .map(|batch| batch.unwrap().len())
            .sum::<usize>(),
        3
    );
}
