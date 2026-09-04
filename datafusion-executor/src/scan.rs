//! Static-file scan lowering and physical planning.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, FieldRef as ArrowFieldRef, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, JsonSource, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{
    Expr as DFExpr, LogicalPlan as DFLogicalPlan, LogicalPlanBuilder, TableType,
};
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::{
    ScanFile as KernelScanFile, ScanJson as KernelScanJson, ScanParquet as KernelScanParquet,
};
use delta_kernel::schema::StructType as KernelStructType;
use itertools::Itertools;

use crate::parquet_expr_adapter::KernelParquetExprAdapterFactory;
use crate::scalar::to_df_scalar;

/// File format whose scan-schema constraints are being validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ScanFormat {
    Parquet,
    Json,
}

struct StaticFileProvider {
    file_source: Arc<dyn FileSource>,
    expr_adapter: Option<Arc<dyn PhysicalExprAdapterFactory>>,
    store_url: Option<ObjectStoreUrl>,
    files: Vec<PartitionedFile>,
}

impl std::fmt::Debug for StaticFileProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticFileProvider")
            .field("file_type", &self.file_source.file_type())
            .field("expr_adapter", &self.expr_adapter)
            .field("store_url", &self.store_url)
            .field("files", &self.files)
            .finish()
    }
}

/// Lowers a kernel [`ScanParquet`](KernelScanParquet) into a DataFusion
/// [`LogicalPlan`](DFLogicalPlan) that reads exactly the files named by the scan node.
///
/// The physical scan uses [`KernelParquetExprAdapterFactory`] to reconcile each checkpoint's
/// physical schema with the schema requested by kernel.
///
/// # Errors
/// Returns an error if the schema or file locations cannot be represented by DataFusion.
pub(crate) fn lower_parquet_scan(
    scan: &KernelScanParquet,
) -> Result<DFLogicalPlan, DataFusionError> {
    lower_scan(
        ScanFormat::Parquet,
        &scan.files,
        &scan.file_constant_columns,
        scan.schema.as_ref(),
    )
}

/// Lowers a kernel [`ScanJson`](KernelScanJson) into a DataFusion [`LogicalPlan`](DFLogicalPlan)
/// that reads exactly the files named by the scan node.
///
/// # Errors
/// Returns an error if the schema or file locations cannot be represented by DataFusion.
pub(crate) fn lower_json_scan(scan: &KernelScanJson) -> Result<DFLogicalPlan, DataFusionError> {
    lower_scan(
        ScanFormat::Json,
        &scan.files,
        &scan.file_constant_columns,
        scan.schema.as_ref(),
    )
}

fn lower_scan(
    format: ScanFormat,
    files: &[KernelScanFile],
    file_constant_columns: &[String],
    schema: &KernelStructType,
) -> Result<DFLogicalPlan, DataFusionError> {
    let output_schema: ArrowSchemaRef = Arc::new(schema.try_into_arrow()?);
    validate_scan_schema(format, schema, &output_schema)?;

    let (table_schema, output_to_table_indices) =
        build_scan_table_layout(&output_schema, file_constant_columns);
    let (store_url, files) = scan_files(files)?;
    let file_source: Arc<dyn FileSource> = match format {
        ScanFormat::Parquet => Arc::new(ParquetSource::new(table_schema)),
        ScanFormat::Json => Arc::new(JsonSource::new(table_schema)),
    };
    let expr_adapter = (format == ScanFormat::Parquet)
        .then(|| Arc::new(KernelParquetExprAdapterFactory) as Arc<dyn PhysicalExprAdapterFactory>);
    let provider = StaticFileProvider {
        file_source,
        expr_adapter,
        store_url,
        files,
    };
    let source = Arc::new(DefaultTableSource::new(Arc::new(provider)));
    let table_name = match format {
        ScanFormat::Parquet => "scan_parquet",
        ScanFormat::Json => "scan_json",
    };
    LogicalPlanBuilder::scan(table_name, source, Some(output_to_table_indices))?.build()
}

/// Validates a converted scan schema against the executor's format-specific limitations.
///
/// # Errors
/// Returns an error when the schema contains unsupported metadata columns or when a Parquet schema
/// requires field-ID resolution.
// TODO(#3167): support metadata columns and Parquet field-ID resolution.
pub(crate) fn validate_scan_schema(
    format: ScanFormat,
    schema: &KernelStructType,
    output_schema: &ArrowSchemaRef,
) -> Result<(), DataFusionError> {
    if let Some(field) = schema.metadata_columns().next() {
        return Err(DataFusionError::NotImplemented(format!(
            "scan metadata column `{}` is not supported by the DataFusion executor",
            field.name()
        )));
    }
    if format == ScanFormat::Parquet {
        if let Some(field_name) = parquet_field_id_name(output_schema.fields()) {
            return Err(DataFusionError::NotImplemented(format!(
                "Parquet field-ID resolution for `{}` is not supported by the DataFusion executor",
                field_name
            )));
        }
    }
    Ok(())
}

fn parquet_field_id_name(fields: &[ArrowFieldRef]) -> Option<String> {
    fields.iter().find_map(|field| {
        if field.metadata().contains_key(PARQUET_FIELD_ID_META_KEY) {
            return Some(field.name().clone());
        }

        match field.data_type() {
            ArrowDataType::Struct(fields) => parquet_field_id_name(fields),
            ArrowDataType::List(field)
            | ArrowDataType::LargeList(field)
            | ArrowDataType::FixedSizeList(field, _)
            | ArrowDataType::ListView(field)
            | ArrowDataType::LargeListView(field)
            | ArrowDataType::Map(field, _) => parquet_field_id_name(std::slice::from_ref(field)),
            _ => None,
        }
    })
}

#[async_trait]
impl TableProvider for StaticFileProvider {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(self.file_source.table_schema().table_schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[DFExpr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let Some(store_url) = &self.store_url else {
            let schema = match projection {
                Some(projection) => Arc::new(self.schema().project(projection)?),
                None => self.schema(),
            };
            return Ok(Arc::new(EmptyExec::new(schema)));
        };

        // A single initial group lets DataFusion's physical optimizer choose file and
        // Parquet row-group splits from the caller's target-partition configuration.
        let config = FileScanConfigBuilder::new(store_url.clone(), Arc::clone(&self.file_source))
            .with_file_group(FileGroup::new(self.files.clone()))
            .with_projection_indices(projection.cloned())?
            .with_expr_adapter(self.expr_adapter.clone())
            .with_limit(limit)
            .with_partitioned_by_file_group(false)
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}

/// Builds DataFusion's scan table schema and a mapping from output positions to table indices.
///
/// The table schema places physical file columns before per-file constants. The returned mapping
/// contains one table-schema index for each field in `output_schema`, preserving its field order.
pub(crate) fn build_scan_table_layout(
    output_schema: &ArrowSchemaRef,
    file_constant_columns: &[String],
) -> (TableSchema, Vec<usize>) {
    // DataFusion models per-file constants as partition columns appended after physical file
    // columns. The table-scan projection restores the kernel schema's arbitrary indexing.
    let constant_positions: HashMap<&str, usize> = file_constant_columns
        .iter()
        .enumerate()
        .map(|(index, name)| (name.as_str(), index))
        .collect();
    let mut file_fields = Vec::new();
    let mut constant_fields = Vec::new();
    for field in output_schema.fields() {
        if let Some(&constant_index) = constant_positions.get(field.name().as_str()) {
            constant_fields.push((constant_index, Arc::clone(field)));
        } else {
            file_fields.push(Arc::clone(field));
        }
    }
    constant_fields.sort_by_key(|(index, _)| *index);

    let file_field_count = file_fields.len();
    let mut output_to_table_indices = Vec::with_capacity(output_schema.fields().len());
    let mut file_index = 0;
    for field in output_schema.fields() {
        if let Some(&constant_index) = constant_positions.get(field.name().as_str()) {
            output_to_table_indices.push(file_field_count + constant_index);
        } else {
            output_to_table_indices.push(file_index);
            file_index += 1;
        }
    }
    let constant_fields = constant_fields
        .into_iter()
        .map(|(_, field)| field)
        .collect();

    let file_schema = Arc::new(ArrowSchema::new(file_fields));
    (
        TableSchema::new(file_schema, constant_fields),
        output_to_table_indices,
    )
}

fn scan_files(
    files: &[KernelScanFile],
) -> Result<(Option<ObjectStoreUrl>, Vec<PartitionedFile>), DataFusionError> {
    let Some((first_file, remaining_files)) = files.split_first() else {
        return Ok((None, Vec::new()));
    };

    let (store_url, first_scan_file) = to_df_object_store_and_partitioned_file(first_file)?;
    let mut scan_files = Vec::with_capacity(files.len());
    scan_files.push(first_scan_file);
    for file in remaining_files {
        let (file_store_url, scan_file) = to_df_object_store_and_partitioned_file(file)?;
        if file_store_url != store_url {
            return Err(DataFusionError::Plan(format!(
                "scan files must use one object store, found `{store_url}` and \
                 `{file_store_url}`"
            )));
        }
        scan_files.push(scan_file);
    }
    Ok((Some(store_url), scan_files))
}

fn to_df_object_store_and_partitioned_file(
    file: &KernelScanFile,
) -> Result<(ObjectStoreUrl, PartitionedFile), DataFusionError> {
    let constants = file
        .file_constants
        .iter()
        .map(|scalar| {
            to_df_scalar(scalar).map_err(|error| DataFusionError::External(Box::new(error)))
        })
        .try_collect()?;
    let location = ListingTableUrl::parse(file.meta.location.as_str())?;
    let store_url = location.object_store();
    let mut partitioned_file = PartitionedFile::new("", file.meta.size);
    partitioned_file.object_meta.location = location.prefix().clone();
    partitioned_file.partition_values = constants;
    Ok((store_url, partitioned_file))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Cursor;

    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use datafusion::arrow::json::ReaderBuilder as JsonReaderBuilder;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::logical_expr::col as df_col;
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionContext;
    use delta_kernel::expressions::{
        ArrayData as KernelArrayData, MapData as KernelMapData, Scalar as KernelScalar,
    };
    use delta_kernel::plans::ir::nodes::Operator as KernelOperator;
    use delta_kernel::schema::{
        ArrayType as KernelArrayType, DataType as KernelDataType, MapType as KernelMapType,
        StructField as KernelStructField, StructType as KernelStructType,
    };
    use delta_kernel::FileMeta as KernelFileMeta;
    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;
    use crate::operator::lower_operator;

    // === Shared helpers ===

    #[derive(Debug, Clone, Copy)]
    enum TestFormat {
        Parquet,
        Json,
    }

    impl TestFormat {
        fn extension(self) -> &'static str {
            match self {
                Self::Parquet => "parquet",
                Self::Json => "json",
            }
        }
    }

    fn scan_schema() -> KernelStructType {
        KernelStructType::try_new([
            KernelStructField::not_null("partition", KernelDataType::STRING),
            KernelStructField::not_null("id", KernelDataType::LONG),
            KernelStructField::not_null("score", KernelDataType::LONG),
            KernelStructField::not_null("version", KernelDataType::LONG),
        ])
        .unwrap()
    }

    fn scan_file(
        directory: &TempDir,
        format: TestFormat,
        stem: &str,
        ids: &[i64],
        partition: &str,
        version: i64,
    ) -> KernelScanFile {
        let path = directory
            .path()
            .join(format!("{stem}.{}", format.extension()));
        match format {
            TestFormat::Parquet => write_parquet(&path, ids),
            TestFormat::Json => write_json(&path, ids),
        }
        data_scan_file(&path, partition, version)
    }

    fn data_scan_file(path: &std::path::Path, partition: &str, version: i64) -> KernelScanFile {
        let size = std::fs::metadata(path).unwrap().len();
        let location = format!("file://{}", path.display()).parse().unwrap();
        KernelScanFile {
            meta: KernelFileMeta {
                location,
                last_modified: 0,
                size,
            },
            file_constants: vec![
                KernelScalar::Long(version),
                KernelScalar::String(partition.to_string()),
            ],
        }
    }

    fn write_parquet(path: &std::path::Path, ids: &[i64]) {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("score", ArrowDataType::Int64, false),
        ]));
        let scores: Vec<_> = ids.iter().map(|id| id * 10).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(Int64Array::from(scores)),
            ],
        )
        .unwrap();
        write_parquet_batch(path, &batch);
    }

    fn write_parquet_batch(path: &std::path::Path, batch: &RecordBatch) {
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    fn write_json(path: &std::path::Path, ids: &[i64]) {
        let lines: Vec<String> = ids
            .iter()
            .map(|id| format!(r#"{{"id":{id},"score":{}}}"#, id * 10))
            .collect();
        let contents = format!("{}\n", lines.join("\n"));
        std::fs::write(path, contents).unwrap();
    }

    fn nested_file_fields(contains_null: bool) -> [KernelStructField; 6] {
        [
            KernelStructField::not_null(
                "array",
                KernelArrayType::new(KernelDataType::LONG, contains_null),
            ),
            KernelStructField::not_null(
                "array_of_arrays",
                KernelArrayType::new(
                    KernelArrayType::new(KernelDataType::LONG, contains_null),
                    contains_null,
                ),
            ),
            KernelStructField::not_null(
                "map",
                KernelMapType::new(KernelDataType::STRING, KernelDataType::LONG, contains_null),
            ),
            KernelStructField::not_null(
                "map_of_maps",
                KernelMapType::new(
                    KernelDataType::STRING,
                    KernelMapType::new(KernelDataType::STRING, KernelDataType::LONG, contains_null),
                    contains_null,
                ),
            ),
            KernelStructField::not_null(
                "array_of_maps",
                KernelArrayType::new(
                    KernelMapType::new(KernelDataType::STRING, KernelDataType::LONG, contains_null),
                    contains_null,
                ),
            ),
            KernelStructField::not_null(
                "map_of_arrays",
                KernelMapType::new(
                    KernelDataType::STRING,
                    KernelArrayType::new(
                        KernelArrayType::new(KernelDataType::LONG, contains_null),
                        contains_null,
                    ),
                    contains_null,
                ),
            ),
        ]
    }

    fn nested_scan_schema() -> KernelStructType {
        let [array, array_of_arrays, map, map_of_maps, array_of_maps, map_of_arrays] =
            nested_file_fields(false);
        KernelStructType::try_new([
            KernelStructField::not_null("partition", KernelDataType::STRING),
            array,
            array_of_arrays,
            map,
            map_of_maps,
            array_of_maps,
            map_of_arrays,
            KernelStructField::not_null("version", KernelDataType::LONG),
        ])
        .unwrap()
    }

    fn nested_scan_file(
        directory: &TempDir,
        format: TestFormat,
    ) -> (KernelScanFile, Option<ArrowSchemaRef>) {
        let path = directory
            .path()
            .join(format!("nested.{}", format.extension()));
        let contents = concat!(
            r#"{"array":[1,2],"array_of_arrays":[[1,2],[3]],"map":{"a":10,"b":20},"#,
            r#""map_of_maps":{"outer":{"inner":30}},"#,
            r#""array_of_maps":[{"c":30},{"d":40}],"#,
            r#""map_of_arrays":{"nested":[[5,6],[7]]}}"#,
            "\n",
            r#"{"array":[],"array_of_arrays":[],"map":{},"map_of_maps":{},"#,
            r#""array_of_maps":[],"map_of_arrays":{}}"#,
            "\n",
        );
        let physical_schema = match format {
            TestFormat::Json => {
                std::fs::write(&path, contents).unwrap();
                None
            }
            TestFormat::Parquet => {
                let kernel_schema = KernelStructType::try_new(nested_file_fields(true)).unwrap();
                let schema = Arc::new((&kernel_schema).try_into_arrow().unwrap());
                let batch = JsonReaderBuilder::new(Arc::clone(&schema))
                    .build(Cursor::new(contents))
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap();
                write_parquet_batch(&path, &batch);
                let reader =
                    ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
                Some(reader.schema().clone())
            }
        };
        (data_scan_file(&path, "nested", 7), physical_schema)
    }

    fn array_to_arrays_map_type(contains_null: bool) -> KernelMapType {
        KernelMapType::new(
            KernelArrayType::new(KernelDataType::LONG, contains_null),
            KernelArrayType::new(
                KernelArrayType::new(KernelDataType::LONG, contains_null),
                contains_null,
            ),
            contains_null,
        )
    }

    fn kernel_array(
        array_type: KernelArrayType,
        values: impl IntoIterator<Item = KernelScalar>,
    ) -> KernelScalar {
        KernelScalar::Array(KernelArrayData::try_new(array_type, values).unwrap())
    }

    fn lower_test_scan(
        format: TestFormat,
        files: Vec<KernelScanFile>,
        schema: KernelStructType,
    ) -> Result<DFLogicalPlan, DataFusionError> {
        let file_constant_columns = vec!["version".to_string(), "partition".to_string()];
        let operator = match format {
            TestFormat::Parquet => KernelOperator::ScanParquet(KernelScanParquet {
                files,
                file_constant_columns,
                schema: Arc::new(schema),
            }),
            TestFormat::Json => KernelOperator::ScanJson(KernelScanJson {
                files,
                file_constant_columns,
                schema: Arc::new(schema),
            }),
        };
        lower_operator(&operator, &[])
    }

    // === Tests ===

    // In schema summaries, `?` marks a nullable field, list element, or map value.

    #[rstest]
    #[case::parquet(TestFormat::Parquet)]
    #[case::json(TestFormat::Json)]
    #[tokio::test]
    async fn scans_multiple_files_with_constants_and_projection(#[case] format: TestFormat) {
        let directory = TempDir::new().unwrap();
        let files = vec![
            scan_file(&directory, format, "one", &[1, 2], "a", 10),
            scan_file(&directory, format, "two", &[3], "b", 20),
        ];
        let plan = lower_test_scan(format, files, scan_schema()).unwrap();
        let DFLogicalPlan::TableScan(scan) = &plan else {
            panic!("expected a table scan")
        };
        assert_eq!(scan.projection.as_deref(), Some(&[3, 0, 1, 2][..]));
        let projected = LogicalPlanBuilder::from(plan)
            .project([
                df_col("version"),
                df_col("score"),
                df_col("partition"),
                df_col("id"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let batches = SessionContext::new()
            .execute_logical_plan(projected)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_sorted_eq!(
            [
                "+---------+-------+-----------+----+",
                "| version | score | partition | id |",
                "+---------+-------+-----------+----+",
                "| 10      | 10    | a         | 1  |",
                "| 10      | 20    | a         | 2  |",
                "| 20      | 30    | b         | 3  |",
                "+---------+-------+-----------+----+",
            ],
            &batches
        );
    }

    #[rstest]
    #[case::parquet(TestFormat::Parquet)]
    #[case::json(TestFormat::Json)]
    #[tokio::test]
    async fn fills_nullable_file_columns_missing_from_a_file(#[case] format: TestFormat) {
        let directory = TempDir::new().unwrap();
        let files = vec![scan_file(&directory, format, "one", &[7], "west", 42)];
        let schema = KernelStructType::try_new([
            KernelStructField::not_null("partition", KernelDataType::STRING),
            KernelStructField::not_null("id", KernelDataType::LONG),
            KernelStructField::nullable("missing", KernelDataType::STRING),
            KernelStructField::not_null("version", KernelDataType::LONG),
        ])
        .unwrap();
        let plan = lower_test_scan(format, files, schema).unwrap();

        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_sorted_eq!(
            [
                "+-----------+----+---------+---------+",
                "| partition | id | missing | version |",
                "+-----------+----+---------+---------+",
                "| west      | 7  |         | 42      |",
                "+-----------+----+---------+---------+",
            ],
            &batches
        );
    }

    // Physical Parquet:
    //   array<i64?>, array<array<i64?>?>, map<string, i64?>,
    //   map<string, map<string, i64?>?>, array<map<string, i64?>?>,
    //   map<string, array<array<i64?>?>?>
    // Requested Kernel: the same types with every `?` removed.
    // Expected: values below, with every output field exactly matching its requested Kernel field.
    #[rstest]
    #[case::basic(
        &["array", "array_of_arrays", "map"],
        &[
            "+--------+-----------------+----------------+",
            "| array  | array_of_arrays | map            |",
            "+--------+-----------------+----------------+",
            "| [1, 2] | [[1, 2], [3]]   | {a: 10, b: 20} |",
            "| []     | []              | {}             |",
            "+--------+-----------------+----------------+",
        ]
    )]
    #[case::cursed(
        &["map_of_maps", "array_of_maps", "map_of_arrays"],
        &[
            "+----------------------+--------------------+-------------------------+",
            "| map_of_maps          | array_of_maps      | map_of_arrays           |",
            "+----------------------+--------------------+-------------------------+",
            "| {outer: {inner: 30}} | [{c: 30}, {d: 40}] | {nested: [[5, 6], [7]]} |",
            "| {}                   | []                 | {}                      |",
            "+----------------------+--------------------+-------------------------+",
        ]
    )]
    #[tokio::test]
    async fn reads_nested_array_and_map_columns(
        #[values(TestFormat::Parquet, TestFormat::Json)] format: TestFormat,
        #[case] columns: &[&str],
        #[case] expected: &[&str],
    ) {
        let directory = TempDir::new().unwrap();
        let (file, physical_schema) = nested_scan_file(&directory, format);
        let requested_schema = nested_scan_schema();
        let requested_arrow_schema: ArrowSchema = (&requested_schema).try_into_arrow().unwrap();
        if let Some(physical_schema) = physical_schema {
            for name in columns {
                assert_ne!(
                    physical_schema.field_with_name(name).unwrap(),
                    requested_arrow_schema.field_with_name(name).unwrap()
                );
            }
        }

        let scan = lower_test_scan(format, vec![file], requested_schema).unwrap();
        let projected = LogicalPlanBuilder::from(scan)
            .project(columns.iter().map(|name| df_col(*name)))
            .unwrap()
            .build()
            .unwrap();

        let batches = SessionContext::new()
            .execute_logical_plan(projected)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        for batch in &batches {
            for name in columns {
                assert_eq!(
                    batch.schema().field_with_name(name).unwrap(),
                    requested_arrow_schema.field_with_name(name).unwrap()
                );
            }
        }
        assert_batches_sorted_eq!(expected, &batches);
    }

    // Physical Parquet: map<array<i64?>, array<array<i64?>?>?>
    // Requested Kernel: map<array<i64>, array<array<i64>>>
    // Expected: `{[1, 2]: [[3, 4], [5]]}` with the complete requested Kernel field.
    #[tokio::test]
    async fn parquet_reads_map_from_array_keys_to_nested_array_values() {
        let physical_array_type = KernelArrayType::new(KernelDataType::LONG, true);
        let key = kernel_array(
            physical_array_type.clone(),
            [KernelScalar::Long(1), KernelScalar::Long(2)],
        );
        let value = kernel_array(
            KernelArrayType::new(physical_array_type.clone(), true),
            [
                kernel_array(
                    physical_array_type.clone(),
                    [KernelScalar::Long(3), KernelScalar::Long(4)],
                ),
                kernel_array(physical_array_type, [KernelScalar::Long(5)]),
            ],
        );
        let physical_map_type = array_to_arrays_map_type(true);
        let map = KernelScalar::Map(
            KernelMapData::try_new(physical_map_type.clone(), [(key, value)]).unwrap(),
        );

        let physical_schema = KernelStructType::try_new([KernelStructField::not_null(
            "array_to_arrays_map",
            physical_map_type,
        )])
        .unwrap();
        let physical_arrow_schema: ArrowSchema = (&physical_schema).try_into_arrow().unwrap();
        let physical_arrow_schema = Arc::new(physical_arrow_schema);
        let array = to_df_scalar(&map).unwrap().to_array_of_size(1).unwrap();
        let batch = RecordBatch::try_new(Arc::clone(&physical_arrow_schema), vec![array]).unwrap();
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("array-map.parquet");
        write_parquet_batch(&path, &batch);

        let requested_schema = KernelStructType::try_new([
            KernelStructField::not_null("partition", KernelDataType::STRING),
            KernelStructField::not_null("array_to_arrays_map", array_to_arrays_map_type(false)),
            KernelStructField::not_null("version", KernelDataType::LONG),
        ])
        .unwrap();
        let requested_arrow_schema: ArrowSchema = (&requested_schema).try_into_arrow().unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        assert_ne!(
            reader
                .schema()
                .field_with_name("array_to_arrays_map")
                .unwrap(),
            requested_arrow_schema
                .field_with_name("array_to_arrays_map")
                .unwrap()
        );

        let file = data_scan_file(&path, "nested", 7);
        let scan = lower_test_scan(TestFormat::Parquet, vec![file], requested_schema).unwrap();
        let projected = LogicalPlanBuilder::from(scan)
            .project([df_col("array_to_arrays_map")])
            .unwrap()
            .build()
            .unwrap();
        let batches = SessionContext::new()
            .execute_logical_plan(projected)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .schema()
                .field_with_name("array_to_arrays_map")
                .unwrap(),
            requested_arrow_schema
                .field_with_name("array_to_arrays_map")
                .unwrap()
        );
        assert_batches_sorted_eq!(
            [
                "+-------------------------+",
                "| array_to_arrays_map     |",
                "+-------------------------+",
                "| {[1, 2]: [[3, 4], [5]]} |",
                "+-------------------------+",
            ],
            &batches
        );
    }

    #[rstest]
    #[case::parquet(TestFormat::Parquet)]
    #[case::json(TestFormat::Json)]
    #[tokio::test]
    async fn empty_scan_preserves_declared_schema(#[case] format: TestFormat) {
        let plan = lower_test_scan(format, vec![], scan_schema()).unwrap();
        let dataframe = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap();
        let physical = dataframe.create_physical_plan().await.unwrap();

        let physical_schema = physical.schema();
        let names: Vec<&str> = physical_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, ["partition", "id", "score", "version"]);
        assert!(dataframe.collect().await.unwrap().is_empty());
    }

    #[rstest]
    #[case::parquet(TestFormat::Parquet)]
    #[case::json(TestFormat::Json)]
    fn scan_rejects_input_plan(#[case] format: TestFormat) {
        let operator = match format {
            TestFormat::Parquet => KernelOperator::ScanParquet(KernelScanParquet {
                files: vec![],
                file_constant_columns: vec![],
                schema: Arc::new(KernelStructType::new_unchecked([])),
            }),
            TestFormat::Json => KernelOperator::ScanJson(KernelScanJson {
                files: vec![],
                file_constant_columns: vec![],
                schema: Arc::new(KernelStructType::new_unchecked([])),
            }),
        };
        let input = Arc::new(lower_operator(&operator, &[]).unwrap());
        let error = lower_operator(&operator, &[input]).unwrap_err();
        assert!(error
            .to_string()
            .contains("expects 0 input(s), but received 1"));
    }

    #[test]
    fn scan_rejects_multiple_object_stores() {
        let files = ["s3://first/table/a.json", "s3://second/table/b.json"]
            .into_iter()
            .map(|location| KernelScanFile {
                meta: KernelFileMeta {
                    location: location.parse().unwrap(),
                    last_modified: 0,
                    size: 1,
                },
                file_constants: vec![KernelScalar::Long(1), KernelScalar::String("a".to_string())],
            })
            .collect();

        let error = lower_test_scan(TestFormat::Json, files, scan_schema()).unwrap_err();
        assert!(
            error.to_string().contains(
                "scan files must use one object store, found `s3://first/` and `s3://second/`"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn execution_rejects_unregistered_object_store() {
        let file = KernelScanFile {
            meta: KernelFileMeta {
                location: "unregistered://bucket/file.json".parse().unwrap(),
                last_modified: 0,
                size: 1,
            },
            file_constants: vec![KernelScalar::Long(1), KernelScalar::String("a".to_string())],
        };
        let plan = lower_test_scan(TestFormat::Json, vec![file], scan_schema()).unwrap();
        let error = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("unregistered://bucket"),
            "{error}"
        );
    }
}
