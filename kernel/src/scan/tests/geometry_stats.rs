use ::test_utils::add_commit;

use super::*;
use crate::actions::{TIGHT_BOUNDS, get_commit_schema};
use crate::arrow::array::{ArrayRef, AsArray, BinaryArray};
use crate::engine::arrow_expression::evaluate_expression::evaluate_expression;
use crate::engine::arrow_expression::opaque::{ArrowOpaqueExpression, ArrowOpaqueExpressionOp};
use crate::expressions::{ParseJsonExpression, ScalarExpressionEvaluator};
use crate::schema::{PrimitiveType, SchemaStructPatchBuilder};
use crate::unit_test_utils::{geometry_type, string_array_to_engine_data};
use crate::{EvaluationHandler, ExpressionEvaluator, PredicateEvaluator};

#[rstest]
#[case::struct_output(StatsOptions::all_struct(), None)]
#[case::no_struct_output(StatsOptions::none(), Some("requires struct stats output"))]
#[case::json_synthesis(
    StatsOptions::all(),
    Some("cannot be combined with JSON stats synthesis")
)]
fn geometry_stats_options_validation(#[case] stats: StatsOptions, #[case] error: Option<&str>) {
    let result = stats.with_geometry_min_max_stats(true).validate();
    if let Some(error) = error {
        assert_result_error_with_message(result, error);
    } else {
        result.unwrap();
    }
}

#[test]
fn geometry_stats_are_disabled_by_default() {
    assert!(!StatsOptions::default().geometry_min_max);
}

#[tokio::test]
async fn geometry_bounds_reach_typed_scan_output_from_commits_and_checkpoints() {
    let (engine, table_root) = setup_geometry_stats_table().await;
    let snapshot = Snapshot::builder_for(&table_root).build(&engine).unwrap();
    assert_eq!(snapshot.log_segment().checkpoint_version, Some(1));
    let builder = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all_struct().with_geometry_min_max_stats(true))
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
    let expected = |path: &str, bound, x| (path.to_owned(), bound, Some(point_wkb(x, x + 10.0)));
    assert_eq!(
        collect_geometry_bounds(&scan, &engine),
        vec![
            expected("a.parquet", MAX_VALUES, 3.0),
            expected("a.parquet", MIN_VALUES, 1.0),
            expected("b.parquet", MAX_VALUES, 6.0),
            expected("b.parquet", MIN_VALUES, 4.0),
        ],
    );
}

// Kernel writers reject geospatial tables, so build protocol-shaped read fixtures directly.
async fn setup_geometry_stats_table() -> (DelegatingEngine, String) {
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
        serde_json::json!({
            "numRecords": 3, "nullCount": {"id": 0, "g": 0},
            "minValues": {"id": min, "g": corner(min)},
            "maxValues": {"id": max, "g": corner(max)}, "tightBounds": true,
        })
    };
    let add = |path: &str, min, max| {
        serde_json::json!({"add": {
            "path": path, "partitionValues": {}, "size": 1, "modificationTime": 1,
            "dataChange": true, "stats": stats(min, max, false).to_string(),
        }})
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

    let bounds = schema! { nullable "id": LONG, nullable "g": BINARY };
    let checkpoint_schema = Arc::new(
        SchemaStructPatchBuilder::new()
            .append_at(
                ["add"],
                StructField::nullable(
                    STATS_PARSED,
                    schema! {
                        nullable NUM_RECORDS: LONG,
                        nullable NULL_COUNT: { nullable "id": LONG, nullable "g": LONG },
                        nullable MIN_VALUES: (bounds.clone()), nullable MAX_VALUES: (bounds),
                        nullable TIGHT_BOUNDS: BOOLEAN,
                    },
                ),
            )
            .build(get_commit_schema().as_ref())
            .unwrap(),
    );
    let mut checkpoint_add = add("a.parquet", 1, 3);
    checkpoint_add["add"][STATS_PARSED] = stats(1, 3, true);
    // No JSON fallback can hide a failure to read typed checkpoint bounds.
    checkpoint_add["add"]
        .as_object_mut()
        .unwrap()
        .remove("stats");
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
    engine
        .parquet_handler()
        .write_parquet_file(
            Url::parse(&table_root)
                .unwrap()
                .join("_delta_log/00000000000000000001.checkpoint.parquet")
                .unwrap(),
            Box::new(std::iter::once(Ok(checkpoint))),
        )
        .unwrap();
    add_commit(
        &table_root,
        store.as_ref(),
        2,
        add("b.parquet", 4, 6).to_string(),
    )
    .await
    .unwrap();
    (engine, table_root)
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
    ) -> KernelResult<Arc<dyn ExpressionEvaluator>> {
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
    ) -> KernelResult<Arc<dyn PredicateEvaluator>> {
        self.evaluation.new_predicate_evaluator(
            Arc::new(GeometryAsBinary.transform_struct(&input).into_owned()),
            predicate,
        )
    }

    fn create_many(
        &self,
        schema: SchemaRef,
        rows: Vec<Vec<Scalar>>,
    ) -> KernelResult<Box<dyn EngineData>> {
        self.evaluation.create_many(schema, rows)
    }
}

impl ParquetHandler for GeometryStatsHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.parquet.read_parquet_files(
            files,
            Arc::new(GeometryAsBinary.transform_struct(&schema).into_owned()),
            predicate,
        )
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        self.parquet.read_parquet_footer(file)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: Box<dyn KernelResultIterator<Box<dyn EngineData>>>,
    ) -> KernelResult<FileSize> {
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
    ) -> KernelResult<ArrayRef> {
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

    fn eval_expr_scalar(
        &self,
        _: &ScalarExpressionEvaluator<'_>,
        _: &[Expr],
    ) -> KernelResult<Scalar> {
        Err(KernelError::unsupported(
            "The test Geometry JSON decoder does not support scalar evaluation",
        ))
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
