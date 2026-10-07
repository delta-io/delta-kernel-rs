use ::test_utils::add_commit;

use super::*;
use crate::actions::{get_commit_schema, TIGHT_BOUNDS};
use crate::arrow::array::{ArrayRef, AsArray, BinaryArray};
use crate::engine::arrow_expression::evaluate_expression::evaluate_expression;
use crate::engine::arrow_expression::opaque::{ArrowOpaqueExpression, ArrowOpaqueExpressionOp};
use crate::expressions::{ParseJsonExpression, ScalarExpressionEvaluator};
use crate::object_store::ObjectStoreExt;
use crate::parquet::arrow::ArrowSchemaConverter;
use crate::parquet::basic::{LogicalType, Type as PhysicalType};
use crate::parquet::schema::types::{SchemaDescriptor, Type as ParquetType};
use crate::schema::{PrimitiveType, SchemaStructPatchBuilder};
use crate::unit_test_utils::{geometry_type, load_test_table, string_array_to_engine_data};
use crate::{EvaluationHandler, ExpressionEvaluator, PredicateEvaluator};

#[rstest]
#[case::all_struct(StatsOptions::all_struct())]
#[case::struct_columns(StatsOptions::struct_columns(vec![column_name!("id")]))]
#[case::extra_indexed(StatsOptions::all_struct_with_extra_indexed(vec![column_name!("g")]))]
fn scan_builder_accepts_geometry_stats_with_typed_output(
    #[case] stats: StatsOptions,
    #[values(false, true)] variant: bool,
) {
    let (_, snapshot, _tempdir) = load_test_table("parsed-stats").unwrap();
    snapshot
        .scan_builder()
        .with_stats(
            stats
                .with_variant_min_max_stats(variant)
                .with_geometry_min_max_stats(true),
        )
        .build()
        .unwrap();
}

#[rstest]
#[case::json_only(StatsOptions::json_only(), "requires struct stats output")]
#[case::none(StatsOptions::none(), "requires struct stats output")]
#[case::all(StatsOptions::all(), "cannot be combined with JSON stats synthesis")]
fn scan_builder_rejects_geometry_stats_without_typed_output_or_with_json_synthesis(
    #[case] stats: StatsOptions,
    #[case] message: &str,
) {
    let (_, snapshot, _tempdir) = load_test_table("parsed-stats").unwrap();
    assert_result_error_with_message(
        snapshot
            .clone()
            .scan_builder()
            .with_stats(stats.clone().with_geometry_min_max_stats(true))
            .build(),
        message,
    );
    snapshot
        .scan_builder()
        .with_stats(
            stats
                .with_geometry_min_max_stats(true)
                .with_geometry_min_max_stats(false),
        )
        .build()
        .unwrap();
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum CheckpointStats {
    ParsedWkb,
    MissingMin,
    IncompatibleString,
    NullBounds,
    MissingStats,
}

#[rstest]
#[case::parsed_only(CheckpointStats::ParsedWkb)]
#[case::missing_bound_is_null(CheckpointStats::MissingMin)]
#[case::incompatible_type_uses_json(CheckpointStats::IncompatibleString)]
#[case::null_bounds(CheckpointStats::NullBounds)]
#[case::missing_stats(CheckpointStats::MissingStats)]
#[tokio::test]
async fn geometry_bounds_reach_typed_scan_output_from_commits_and_checkpoints(
    #[case] checkpoint_stats: CheckpointStats,
    #[values(false, true)] scalar_predicate: bool,
) {
    let (engine, _, table_root) = setup_geometry_stats_table(checkpoint_stats).await;
    let snapshot = Snapshot::builder_for(&table_root).build(&engine).unwrap();
    assert_eq!(snapshot.log_segment().checkpoint_version, Some(1));
    let builder = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all_struct().with_geometry_min_max_stats(true))
        .with_predicate(scalar_predicate.then(|| Arc::new(Pred::gt(col!("id"), lit(3_i64)))))
        .without_row_transforms();
    let preview = builder.stats_output_schemas().unwrap().unwrap();
    assert_eq!(preview.logical, preview.physical);
    for bound in [MIN_VALUES, MAX_VALUES] {
        assert_eq!(
            stats_struct_field(&preview.logical, bound)
                .field("g")
                .unwrap()
                .data_type(),
            &geometry_type("EPSG:4326"),
        );
    }
    let scan = builder.build().unwrap();
    assert_eq!(
        scan.state_info.physical_stats_read_schema(),
        Some(&preview.physical)
    );
    let has_bounds = !matches!(
        checkpoint_stats,
        CheckpointStats::NullBounds | CheckpointStats::MissingStats
    );
    let mut expected = vec![
        (
            "b.parquet".to_owned(),
            MAX_VALUES,
            has_bounds.then(|| point_wkb(6.0, 16.0)),
        ),
        (
            "b.parquet".to_owned(),
            MIN_VALUES,
            has_bounds.then(|| point_wkb(4.0, 14.0)),
        ),
    ];
    if !scalar_predicate || checkpoint_stats == CheckpointStats::MissingStats {
        expected.extend([
            (
                "a.parquet".to_owned(),
                MAX_VALUES,
                has_bounds.then(|| point_wkb(3.0, 13.0)),
            ),
            (
                "a.parquet".to_owned(),
                MIN_VALUES,
                (has_bounds && checkpoint_stats != CheckpointStats::MissingMin)
                    .then(|| point_wkb(1.0, 11.0)),
            ),
        ]);
    }
    expected.sort();
    assert_eq!(collect_geometry_bounds(&scan, &engine), expected);
}

#[rstest]
#[case::default_off(false)]
#[case::unsupported_handler(true)]
#[tokio::test]
async fn geometry_stats_require_supporting_handlers_only_when_requested(#[case] enabled: bool) {
    let (_, sync, table_root) = setup_geometry_stats_table(CheckpointStats::ParsedWkb).await;
    let snapshot = Snapshot::builder_for(&table_root)
        .build(sync.as_ref())
        .unwrap();
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all_struct().with_geometry_min_max_stats(enabled))
        .with_predicate(Arc::new(Pred::gt(col!("id"), lit(3_i64))))
        .without_row_transforms()
        .build()
        .unwrap();
    let result = scan
        .scan_metadata(sync.as_ref())
        .and_then(|iter| iter.collect::<Result<Vec<_>>>());
    if enabled {
        assert_result_error_with_message(result, "Geo types are not yet supported");
    } else {
        let mut paths = vec![];
        for metadata in result.unwrap() {
            let (data, selected) = metadata.scan_files.into_parts();
            let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data).unwrap().into();
            let batch = filter_record_batch(&batch, &BooleanArray::from(selected)).unwrap();
            let stats = get_column!(batch, STATS_PARSED, StructArray);
            for bound in [MIN_VALUES, MAX_VALUES] {
                assert!(get_column!(stats, bound, StructArray)
                    .column_by_name("g")
                    .is_none());
            }
            paths.extend(
                get_column!(batch, "path", StringArray)
                    .iter()
                    .flatten()
                    .map(str::to_owned),
            );
        }
        assert_eq!(paths, ["b.parquet"]);
    }
}

// Kernel writers reject geospatial tables, so build protocol-shaped read fixtures directly.
async fn setup_geometry_stats_table(
    checkpoint_stats: CheckpointStats,
) -> (DelegatingEngine, Arc<SyncEngine>, String) {
    let table_root = String::from("memory:///");
    let store = Arc::new(InMemory::new());
    let sync = Arc::new(SyncEngine::new_with_store(store.clone()));
    let handler = Arc::new(GeometryStatsHandler {
        evaluation: sync.evaluation_handler(),
        parquet: sync.parquet_handler(),
    });
    let engine = DelegatingEngine::new(sync.clone())
        .with_evaluation_handler(handler.clone())
        .with_parquet_handler(handler);
    let table_schema = schema! {
        nullable "id": LONG,
        nullable "g": (geometry_type("EPSG:4326")),
    };
    let protocol = serde_json::json!({"protocol": {
        "minReaderVersion": 3, "minWriterVersion": 7,
        "readerFeatures": ["geospatial"], "writerFeatures": ["geospatial"],
    }});
    let metadata = serde_json::json!({"metaData": {
        "id": "geometry-stats", "format": {"provider": "parquet", "options": {}},
        "schemaString": serde_json::to_string(&table_schema).unwrap(),
        "partitionColumns": [], "configuration": {}, "createdTime": 1,
    }});
    let stats = |min: u8, max: u8, wkb: bool| {
        let corner = |n: u8| {
            if wkb {
                hex_bytes(&point_wkb(n.into(), (n + 10).into()))
            } else {
                format!("POINT ({n} {})", n + 10)
            }
        };
        let mut stats = serde_json::json!({
            "numRecords": 3, "nullCount": {"id": 0, "g": 0},
            "minValues": {"id": min, "g": corner(min)},
            "maxValues": {"id": max, "g": corner(max)}, "tightBounds": true,
        });
        if checkpoint_stats == CheckpointStats::NullBounds {
            stats[MIN_VALUES]["g"] = serde_json::Value::Null;
            stats[MAX_VALUES]["g"] = serde_json::Value::Null;
            stats[NULL_COUNT]["g"] = 3.into();
        }
        stats
    };
    let add = |path: &str, min, max| {
        let mut add = serde_json::json!({"add": {
            "path": path, "partitionValues": {}, "size": 1, "modificationTime": 1,
            "dataChange": true, "stats": stats(min, max, false).to_string(),
        }});
        if checkpoint_stats == CheckpointStats::MissingStats {
            add["add"].as_object_mut().unwrap().remove("stats");
        }
        add
    };
    add_commit(
        &table_root,
        store.as_ref(),
        0,
        format!("{protocol}\n{metadata}"),
    )
    .await
    .unwrap();
    add_commit(
        &table_root,
        store.as_ref(),
        1,
        add("a.parquet", 1, 3).to_string(),
    )
    .await
    .unwrap();

    let physical_type = if checkpoint_stats == CheckpointStats::IncompatibleString {
        DataType::STRING
    } else {
        DataType::BINARY
    };
    let bounds = schema! { nullable "id": LONG, nullable "g": (physical_type) };
    let min_bounds = if checkpoint_stats == CheckpointStats::MissingMin {
        schema! { nullable "id": LONG }
    } else {
        bounds.clone()
    };
    let checkpoint_schema = Arc::new(
        SchemaStructPatchBuilder::new()
            .append_at(
                ["add"],
                StructField::nullable(
                    STATS_PARSED,
                    schema! {
                        nullable NUM_RECORDS: LONG,
                        nullable NULL_COUNT: { nullable "id": LONG, nullable "g": LONG },
                        nullable MIN_VALUES: (min_bounds), nullable MAX_VALUES: (bounds),
                        nullable TIGHT_BOUNDS: BOOLEAN,
                    },
                ),
            )
            .build(get_commit_schema().as_ref())
            .unwrap(),
    );
    let mut checkpoint_add = add("a.parquet", 1, 3);
    if checkpoint_stats != CheckpointStats::MissingStats {
        checkpoint_add["add"][STATS_PARSED] = stats(1, 3, true);
    }
    if checkpoint_stats == CheckpointStats::ParsedWkb {
        // No JSON fallback can hide a failure to read typed checkpoint bounds.
        checkpoint_add["add"]
            .as_object_mut()
            .unwrap()
            .remove("stats");
    }
    // MissingMin deliberately keeps complete JSON stats: the missing typed bound must stay null.
    let checkpoint = sync
        .json_handler()
        .parse_json(
            string_array_to_engine_data(StringArray::from_iter_values(
                [protocol, metadata, checkpoint_add]
                    .iter()
                    .map(ToString::to_string),
            )),
            checkpoint_schema,
        )
        .unwrap();
    let batch: RecordBatch = ArrowEngineData::try_from_engine_data(checkpoint)
        .unwrap()
        .into();
    let parquet_schema = ArrowSchemaConverter::new()
        .convert(&batch.schema())
        .unwrap();
    let parquet_schema =
        SchemaDescriptor::new(Arc::new(annotate_geometry(parquet_schema.root_schema())));
    let options = crate::engine::writer_options().with_parquet_schema(parquet_schema);
    let mut writer =
        ArrowWriter::try_new_with_options(Vec::new(), batch.schema(), options).unwrap();
    writer.write(&batch).unwrap();
    store
        .put(
            &"_delta_log/00000000000000000001.checkpoint.parquet".into(),
            writer.into_inner().unwrap().into(),
        )
        .await
        .unwrap();
    add_commit(
        &table_root,
        store.as_ref(),
        2,
        add("b.parquet", 4, 6).to_string(),
    )
    .await
    .unwrap();
    (engine, sync, table_root)
}

fn annotate_geometry(data_type: &ParquetType) -> ParquetType {
    match data_type {
        ParquetType::GroupType { basic_info, fields } => ParquetType::GroupType {
            basic_info: basic_info.clone(),
            fields: fields
                .iter()
                .map(|f| Arc::new(annotate_geometry(f)))
                .collect(),
        },
        ParquetType::PrimitiveType {
            physical_type: PhysicalType::BYTE_ARRAY,
            ..
        } if data_type.name() == "g" && data_type.get_basic_info().logical_type_ref().is_none() => {
            ParquetType::primitive_type_builder("g", PhysicalType::BYTE_ARRAY)
                .with_repetition(data_type.get_basic_info().repetition())
                .with_logical_type(Some(LogicalType::geometry(Some("EPSG:4326".into()))))
                .build()
                .unwrap()
        }
        _ => data_type.clone(),
    }
}

fn collect_geometry_bounds(
    scan: &Scan,
    engine: &dyn Engine,
) -> Vec<(String, &'static str, Option<Vec<u8>>)> {
    let mut actual = vec![];
    for metadata in scan.scan_metadata(engine).unwrap() {
        let (data, selected) = metadata.unwrap().scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data).unwrap().into();
        let batch = filter_record_batch(&batch, &BooleanArray::from(selected)).unwrap();
        let paths = get_column!(batch, "path", StringArray);
        let stats = get_column!(batch, STATS_PARSED, StructArray);
        for bound in [MIN_VALUES, MAX_VALUES] {
            let corners = get_column!(get_column!(stats, bound, StructArray), "g", BinaryArray);
            for row in 0..batch.num_rows() {
                actual.push((
                    paths.value(row).to_owned(),
                    bound,
                    corners.is_valid(row).then(|| corners.value(row).to_vec()),
                ));
            }
        }
    }
    actual.sort();
    actual
}

// The test connector uses Binary WKB for Geometry. This is not a Kernel representation contract.
struct GeometryStatsHandler {
    evaluation: Arc<dyn EvaluationHandler>,
    parquet: Arc<dyn ParquetHandler>,
}

impl EvaluationHandler for GeometryStatsHandler {
    fn new_expression_evaluator(
        &self,
        input: SchemaRef,
        expr: ExpressionRef,
        output: DataType,
    ) -> Result<Arc<dyn ExpressionEvaluator>> {
        self.evaluation.new_expression_evaluator(
            Arc::new(GeometryAsBinary.transform_struct(&input).into_owned()),
            Arc::new(GeometryAsBinary.transform_expr(&expr).into_owned()),
            GeometryAsBinary.transform(&output).into_owned(),
        )
    }

    fn new_predicate_evaluator(
        &self,
        input: SchemaRef,
        predicate: PredicateRef,
    ) -> Result<Arc<dyn PredicateEvaluator>> {
        self.evaluation.new_predicate_evaluator(
            Arc::new(GeometryAsBinary.transform_struct(&input).into_owned()),
            predicate,
        )
    }

    fn create_many(
        &self,
        schema: SchemaRef,
        rows: Vec<Vec<Scalar>>,
    ) -> Result<Box<dyn EngineData>> {
        self.evaluation.create_many(schema, rows)
    }
}

impl ParquetHandler for GeometryStatsHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Result<FileDataReadResultIterator> {
        self.parquet.read_parquet_files(
            files,
            Arc::new(GeometryAsBinary.transform_struct(&schema).into_owned()),
            predicate,
        )
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> Result<ParquetFooter> {
        // This reader's Arrow-derived footer reports GEOMETRY's physical Binary type.
        self.parquet.read_parquet_footer(file)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: ResultIteratorStatic<Box<dyn EngineData>>,
    ) -> Result<FileSize> {
        self.parquet.write_parquet_file(location, data)
    }
}

struct GeometryAsBinary;

impl<'a> SchemaTransform<'a> for GeometryAsBinary {
    transform_output_type!(|'a, T| Cow<'a, T>);

    fn transform_primitive(&mut self, primitive: &'a PrimitiveType) -> Cow<'a, PrimitiveType> {
        match primitive {
            PrimitiveType::Geometry(_) => Cow::Owned(PrimitiveType::Binary),
            _ => Cow::Borrowed(primitive),
        }
    }
}

impl<'a> ExpressionTransform<'a> for GeometryAsBinary {
    transform_output_type!(|'a, T| Cow<'a, T>);

    fn transform_expr_parse_json(
        &mut self,
        expr: &'a ParseJsonExpression,
    ) -> Cow<'a, ParseJsonExpression> {
        match self.transform_struct(&expr.output_schema) {
            Cow::Borrowed(_) => Cow::Borrowed(expr),
            Cow::Owned(schema) => Cow::Owned(ParseJsonExpression::new(
                Expr::arrow_opaque(
                    DecodePointStats(expr.output_schema.clone()),
                    [expr.json_expr.as_ref().clone()],
                ),
                Arc::new(schema),
            )),
        }
    }
}

#[derive(Debug, PartialEq)]
struct DecodePointStats(SchemaRef);

impl ArrowOpaqueExpressionOp for DecodePointStats {
    fn name(&self) -> &str {
        "decode_point_stats"
    }

    fn eval_expr(
        &self,
        args: &[Expr],
        batch: &RecordBatch,
        _: Option<&DataType>,
    ) -> Result<ArrayRef> {
        let [json] = args else {
            panic!("expected one JSON argument")
        };
        let json = evaluate_expression(json, batch, Some(&DataType::STRING))?;
        let decoded: StringArray = json
            .as_string::<i32>()
            .iter()
            .map(|json| {
                let mut value: serde_json::Value = serde_json::from_str(json?).unwrap();
                normalize_point_stats(&mut value, &self.0);
                Some(value.to_string())
            })
            .collect();
        Ok(Arc::new(decoded))
    }

    fn eval_expr_scalar(&self, _: &ScalarExpressionEvaluator<'_>, _: &[Expr]) -> Result<Scalar> {
        unimplemented!()
    }
}

// Only accepts the finite, two-dimensional POINT subset used by these fixtures.
fn normalize_point_stats(value: &mut serde_json::Value, schema: &StructType) {
    for field in schema.fields() {
        let Some(value) = value.get_mut(field.name()) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        match field.data_type() {
            DataType::Struct(schema) => normalize_point_stats(value, schema),
            DataType::Primitive(PrimitiveType::Geometry(_)) => {
                let text = value.as_str().unwrap();
                let coordinates = text
                    .strip_prefix("POINT (")
                    .unwrap()
                    .strip_suffix(')')
                    .unwrap();
                let xy: Vec<f64> = coordinates
                    .split_whitespace()
                    .map(|v| v.parse().unwrap())
                    .collect();
                assert_eq!(xy.len(), 2);
                assert!(xy.iter().all(|v| v.is_finite()));
                *value = hex_bytes(&point_wkb(xy[0], xy[1])).into();
            }
            _ => {}
        }
    }
}

fn point_wkb(x: f64, y: f64) -> Vec<u8> {
    let mut bytes = vec![1, 1, 0, 0, 0]; // Little-endian, geometry type POINT.
    bytes.extend_from_slice(&x.to_le_bytes());
    bytes.extend_from_slice(&y.to_le_bytes());
    bytes
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
