use std::sync::{Arc, Mutex};

use bytes::Bytes;
use roaring::RoaringTreemap;
use rstest::rstest;
use test_utils::{record_batch_to_bytes, record_batch_to_bytes_with_props};

use super::*;
use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use crate::arrow::array::{Int32Array, StringArray, StructArray};
use crate::arrow::compute::concat_batches;
use crate::engine::CancellableEngine;
use crate::engine::sync::SyncEngine;
use crate::expressions::{Predicate, col, lit};
use crate::metrics::MeteredParquetHandler;
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::parquet::file::metadata::{FileMetaData, ParquetMetaData, RowGroupMetaData};
use crate::parquet::file::properties::WriterProperties;
use crate::parquet::file::reader::{ChunkReader, Length};
use crate::scan::state::DvInfo;
use crate::schema::{MetadataColumnSpec, schema_ref};
use crate::unit_test_utils::TestCancellationToken;
use crate::{Engine, FileMeta, FileRowSelection, ParquetHandler};

#[rstest]
#[case::empty(0, vec![], true)]
#[case::groups(5, vec![2, 0, 3], true)]
#[case::negative_file(-1, vec![], false)]
#[case::negative_group(2, vec![3, -1], false)]
#[case::too_few(5, vec![2, 2], false)]
#[case::too_many(3, vec![2, 2], false)]
#[case::overflow(1, vec![i64::MAX, i64::MAX, i64::MAX], false)]
fn exact_read_validates_row_group_counts(
    #[case] file_rows: i64,
    #[case] group_rows: Vec<i64>,
    #[case] valid: bool,
) {
    let result = validated_row_count(file_rows, group_rows);
    assert_eq!(result.is_ok(), valid);
    if valid {
        assert_eq!(result.unwrap(), file_rows as usize);
    }
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
    let file = StructField::nullable("file", KernelDataType::file_type());
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
fn exact_read_filters_generated_row_indexes(
    #[values("default", "payload", "empty")] projection: &str,
) {
    let schema = Arc::new(
        schema_ref! { nullable "payload": STRING }
            .add_metadata_column("position", MetadataColumnSpec::RowIndex)
            .unwrap(),
    );
    let mut options =
        ParquetReadOptions::default().with_row_filter(Arc::new(col!("position").gt(lit(9i64))));
    let output = match projection {
        "payload" => schema.project(&["payload"]).unwrap(),
        "empty" => schema_ref! {},
        _ => schema.clone(),
    };
    if projection != "default" {
        options = options.with_output_schema(output.clone());
    }
    let batches = read_options(schema, options, 1).unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let expected_schema: crate::arrow::datatypes::Schema =
        output.as_ref().try_into_arrow().unwrap();
    for batch in &batches {
        assert_eq!(batch.schema().as_ref(), &expected_schema);
    }
    if projection == "empty" {
        return;
    }
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();
    let payload = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        payload.iter().collect::<Vec<_>>(),
        vec![Some("row-10"), Some("row-11")]
    );
    if projection == "default" {
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
#[case::all_rejected(10, vec![])]
#[case::survivors(1, vec!["b", "c"])]
fn exact_read_nested_filter_materializes_output_only_for_survivors(
    #[case] lower_bound: i64,
    #[case] expected: Vec<&str>,
) {
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
    let schema = schema_ref! { nullable "s": { nullable "key": LONG, nullable "payload": STRING } };
    let output = schema_ref! { nullable "s": { nullable "payload": STRING } };
    let options = ParquetReadOptions::default()
        .with_output_schema(output.clone())
        .with_row_filter(Arc::new(col!("s.key").gt(lit(lower_bound))));
    let plan = ParquetReadPlan::new(metadata, schema, options, "file".into()).unwrap();
    let (builder, finish) = plan
        .configure(|metadata| ParquetRecordBatchReaderBuilder::new_with_metadata(reader, metadata));
    let batches: Vec<RecordBatch> = builder
        .build()
        .unwrap()
        .map(|batch| finish(batch.unwrap()).unwrap().into())
        .collect();
    let expected_schema: crate::arrow::datatypes::Schema =
        output.as_ref().try_into_arrow().unwrap();
    let mut payloads = Vec::new();
    for batch in &batches {
        assert_eq!(batch.schema().as_ref(), &expected_schema);
        let nested = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let payload = nested
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        payloads.extend(payload.iter().map(Option::unwrap));
    }
    assert_eq!(payloads, expected);
    let reads = reads.lock().unwrap();
    assert!(!reads.is_empty());
    if expected.is_empty() {
        assert!(
            reads
                .iter()
                .all(|start| !(payload_start..payload_start + payload_length).contains(start)),
            "output-only column read before rejecting every row: {reads:?}"
        );
    }
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

#[rstest]
fn exact_read_row_group_limit_depends_on_arrow_version_and_row_indexes(
    #[values(32_768, 32_769)] row_group_count: usize,
    #[values(false, true)] project_row_index: bool,
    #[values(false, true)] filter_row_index: bool,
) {
    let bytes = file_bytes();
    let metadata = ArrowReaderMetadata::load(&bytes, reader_options()).unwrap();
    let group = metadata.metadata().row_group(0);
    // Reuse real column chunks to keep the boundary fixture small. Omit the optional ordinals.
    let group = RowGroupMetaData::builder(group.schema_descr_ptr())
        .set_num_rows(group.num_rows())
        .set_total_byte_size(group.total_byte_size())
        .set_column_metadata(group.columns().to_vec())
        .build()
        .unwrap();
    let total_rows = row_group_count as i64 * group.num_rows();
    let file_metadata =
        FileMetaData::new(1, total_rows, None, None, group.schema_descr_ptr(), None);
    let metadata = ArrowReaderMetadata::try_new(
        Arc::new(ParquetMetaData::new(
            file_metadata,
            vec![group; row_group_count],
        )),
        reader_options(),
    )
    .unwrap();
    let schema = Arc::new(
        schema_ref! { nullable "x": INTEGER }
            .add_metadata_column("position", MetadataColumnSpec::RowIndex)
            .unwrap(),
    );
    let output = schema
        .project(if project_row_index {
            &["x", "position"][..]
        } else {
            &["x"][..]
        })
        .unwrap();
    let mut options = ParquetReadOptions::default()
        .with_output_schema(output)
        .with_row_selection(
            FileRowSelection::from_ranges(std::iter::once(
                (total_rows - 4) as u64..total_rows as u64,
            ))
            .unwrap(),
        )
        .with_result_offset(1)
        .with_result_limit(1);
    if filter_row_index {
        options = options.with_row_filter(Arc::new(col!("position").ge(lit(total_rows - 3))));
    }
    let plan = ParquetReadPlan::new(metadata, schema, options, "file".into());
    if cfg!(all(feature = "arrow-59", not(feature = "arrow-60")))
        && row_group_count > 32_768
        && (project_row_index || filter_row_index)
    {
        let Err(error) = plan else {
            panic!("Arrow 59 must reject unsupported row indexes before decoding");
        };
        let KernelError::Unsupported(message) = error.without_backtrace() else {
            panic!("Expected Unsupported, got {error}");
        };
        assert!(message.contains("Arrow 59"));
        assert!(message.contains("32,768 row groups"));
        return;
    }
    let (builder, finish) = plan.unwrap().configure(|metadata| {
        ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, metadata).with_batch_size(2)
    });
    let batches = builder
        .build()
        .unwrap()
        .map(|batch| finish(batch.unwrap()).map(Into::into))
        .collect::<KernelResult<Vec<RecordBatch>>>()
        .unwrap();
    let result = concat_batches(&batches[0].schema(), &batches).unwrap();
    let values = result
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![if filter_row_index { None } else { Some(0) }]
    );
    if project_row_index {
        let positions = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            positions.values(),
            &[total_rows - if filter_row_index { 2 } else { 3 }]
        );
    }
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

#[rstest]
fn rejects_empty_nested_structs_before_decoding(
    #[values(false, true)] nullable: bool,
    #[values("output", "default", "predicate")] use_site: &str,
) {
    let nested = |schema: StructType| {
        Arc::new(StructType::try_new([StructField::new("s", schema, nullable)]).unwrap())
    };
    let complete = nested(
        StructType::try_new([StructField::nullable("key", KernelDataType::INTEGER)]).unwrap(),
    );
    let empty = nested(StructType::try_new([]).unwrap());
    let (read_schema, options) = match use_site {
        "output" => (
            complete,
            ParquetReadOptions::default().with_output_schema(empty),
        ),
        "predicate" => (
            empty,
            ParquetReadOptions::default()
                .with_output_schema(schema_ref! {})
                .with_row_filter(Arc::new(col!("s").is_null())),
        ),
        _ => (empty, ParquetReadOptions::default()),
    };
    options.validate(&read_schema).unwrap();
    assert!(matches!(read_options(read_schema, options, 2),
        Err(error) if matches!(error.without_backtrace(), KernelError::Unsupported(_))));
}
