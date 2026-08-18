//! Static-file scan lowering and physical planning.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, ScalarValue as DFScalarValue};
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
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::{
    ScanFile as KernelScanFile, ScanJson as KernelScanJson, ScanParquet as KernelScanParquet,
};
use delta_kernel::schema::StructType as KernelStructType;
use itertools::Itertools;

use crate::parquet_field_id::ParquetFieldIdAdapterFactory;
use crate::scalar::to_df_scalar;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ScanFormat {
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
/// The physical scan uses [`ParquetFieldIdAdapterFactory`] to resolve per-file field identities
/// and reconcile each file's physical schema with the schema requested by kernel.
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
    validate_scan_schema(schema)?;

    let (table_schema, projection) =
        build_scan_table_schema_and_projection(&output_schema, file_constant_columns);
    let (store_url, files) = scan_files(files)?;
    let file_source: Arc<dyn FileSource> = match format {
        ScanFormat::Parquet => Arc::new(ParquetSource::new(table_schema)),
        ScanFormat::Json => Arc::new(JsonSource::new(table_schema)),
    };
    let expr_adapter = (format == ScanFormat::Parquet)
        .then(|| Arc::new(ParquetFieldIdAdapterFactory) as Arc<dyn PhysicalExprAdapterFactory>);
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
    LogicalPlanBuilder::scan(table_name, source, Some(projection))?.build()
}

fn validate_scan_schema(schema: &KernelStructType) -> Result<(), DataFusionError> {
    if let Some(field) = schema.metadata_columns().next() {
        return Err(DataFusionError::NotImplemented(format!(
            "scan metadata column `{}` is not supported by the DataFusion executor",
            field.name()
        )));
    }
    Ok(())
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

/// Builds DataFusion's file-first table schema and a projection back to kernel output order.
fn build_scan_table_schema_and_projection(
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
    let mut projection = Vec::with_capacity(output_schema.fields().len());
    let mut file_index = 0;
    for field in output_schema.fields() {
        if let Some(&constant_index) = constant_positions.get(field.name().as_str()) {
            projection.push(file_field_count + constant_index);
        } else {
            projection.push(file_index);
            file_index += 1;
        }
    }
    let constant_fields = constant_fields
        .into_iter()
        .map(|(_, field)| field)
        .collect();

    let file_schema = Arc::new(ArrowSchema::new(file_fields));
    (TableSchema::new(file_schema, constant_fields), projection)
}

fn scan_files(
    files: &[KernelScanFile],
) -> Result<(Option<ObjectStoreUrl>, Vec<PartitionedFile>), DataFusionError> {
    let mut store_url = None;
    let mut scan_files = Vec::with_capacity(files.len());
    for file in files {
        let constants = file_constants(file)?;
        let location = ListingTableUrl::parse(file.meta.location.as_str())?;
        let file_store_url = location.object_store();
        match store_url.as_ref() {
            Some(scan_store_url) if scan_store_url != &file_store_url => {
                return Err(DataFusionError::Plan(format!(
                    "scan files must use one object store, found `{scan_store_url}` and \
                     `{file_store_url}`"
                )));
            }
            None => store_url = Some(file_store_url),
            _ => {}
        }
        let mut partitioned_file = PartitionedFile::new("", file.meta.size);
        partitioned_file.object_meta.location = location.prefix().clone();
        partitioned_file.partition_values = constants;
        scan_files.push(partitioned_file);
    }
    Ok((store_url, scan_files))
}

/// Converts a file's kernel constants into DataFusion partition values.
///
/// Kernel owns scan-node validation, so this lowering assumes the file-constant invariants on
/// [`ScanFile`](KernelScanFile). Conversion remains fallible for scalar types that the DataFusion
/// executor does not support.
fn file_constants(file: &KernelScanFile) -> Result<Vec<DFScalarValue>, DataFusionError> {
    file.file_constants
        .iter()
        .map(|scalar| {
            to_df_scalar(scalar).map_err(|error| DataFusionError::External(Box::new(error)))
        })
        .try_collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::Cursor;

    use datafusion::arrow::array::{
        Array, ArrayRef, Int64Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
    };
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use datafusion::arrow::json::ReaderBuilder as JsonReaderBuilder;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::functions::core::expr_fn::get_field;
    use datafusion::logical_expr::{col as df_col, lit as df_lit};
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionContext;
    use delta_kernel::expressions::{
        ArrayData as KernelArrayData, MapData as KernelMapData, Scalar as KernelScalar,
    };
    use delta_kernel::plans::ir::nodes::Operator as KernelOperator;
    use delta_kernel::schema::{
        ArrayType as KernelArrayType, ColumnMetadataKey, DataType as KernelDataType,
        MapType as KernelMapType, MetadataColumnSpec, StructField as KernelStructField,
        StructType as KernelStructType,
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
        write_parquet_batch(path, batch);
    }

    fn write_parquet_batch(path: &std::path::Path, batch: RecordBatch) {
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn parquet_file(path: &std::path::Path) -> KernelScanFile {
        KernelScanFile {
            meta: KernelFileMeta {
                location: format!("file://{}", path.display()).parse().unwrap(),
                last_modified: 0,
                size: std::fs::metadata(path).unwrap().len(),
            },
            file_constants: vec![],
        }
    }

    fn arrow_field_id(id: i64) -> HashMap<String, String> {
        HashMap::from([("PARQUET:field_id".to_string(), id.to_string())])
    }

    fn kernel_field_id(id: i64) -> [(String, i64); 1] {
        [(ColumnMetadataKey::ParquetFieldId.as_ref().to_string(), id)]
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
                write_parquet_batch(&path, batch);
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
        write_parquet_batch(&path, batch);

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
    fn scan_rejects_metadata_columns() {
        let schema = KernelStructType::try_new([KernelStructField::create_metadata_column(
            "row_index",
            MetadataColumnSpec::RowIndex,
        )])
        .unwrap();
        let error = lower_test_scan(TestFormat::Json, vec![], schema).unwrap_err();
        assert!(error.to_string().contains("metadata column `row_index`"));
    }

    #[tokio::test]
    async fn parquet_scan_resolves_renamed_top_level_field_by_id_and_casts() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("renamed.parquet");
        let physical_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "physical",
            ArrowDataType::Int64,
            false,
        )
        .with_metadata(arrow_field_id(1))]));
        write_parquet_batch(
            &path,
            RecordBatch::try_new(
                physical_schema,
                vec![Arc::new(Int64Array::from(vec![7, 9]))],
            )
            .unwrap(),
        );
        let schema = KernelStructType::try_new([KernelStructField::not_null(
            "logical",
            KernelDataType::DOUBLE,
        )
        .add_metadata(kernel_field_id(1))])
        .unwrap();
        let plan = lower_parquet_scan(&KernelScanParquet {
            files: vec![parquet_file(&path)],
            file_constant_columns: vec![],
            schema: Arc::new(schema),
        })
        .unwrap();

        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_sorted_eq!(
            [
                "+---------+",
                "| logical |",
                "+---------+",
                "| 7.0     |",
                "| 9.0     |",
                "+---------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn parquet_scan_resolves_nested_renames_for_projection_and_predicate() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("nested.parquet");
        let physical_child = Arc::new(
            ArrowField::new("child_physical", ArrowDataType::Int64, false)
                .with_metadata(arrow_field_id(2)),
        );
        let physical_struct = StructArray::new(
            vec![Arc::clone(&physical_child)].into(),
            vec![Arc::new(Int64Array::from(vec![3, 7])) as ArrayRef],
            None,
        );
        let physical_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "parent_physical",
            ArrowDataType::Struct(vec![physical_child].into()),
            false,
        )
        .with_metadata(arrow_field_id(1))]));
        write_parquet_batch(
            &path,
            RecordBatch::try_new(physical_schema, vec![Arc::new(physical_struct)]).unwrap(),
        );

        let child = KernelStructField::not_null("child", KernelDataType::LONG)
            .add_metadata(kernel_field_id(2));
        let parent_type = KernelStructType::try_new([child]).unwrap();
        let schema =
            KernelStructType::try_new([
                KernelStructField::not_null("parent", parent_type).add_metadata(kernel_field_id(1))
            ])
            .unwrap();
        let scan = lower_parquet_scan(&KernelScanParquet {
            files: vec![parquet_file(&path)],
            file_constant_columns: vec![],
            schema: Arc::new(schema),
        })
        .unwrap();
        let plan = LogicalPlanBuilder::from(scan)
            .filter(get_field(df_col("parent"), "child").gt(df_lit(5_i64)))
            .unwrap()
            .build()
            .unwrap();
        let context = SessionContext::new();
        let dataframe = context.execute_logical_plan(plan).await.unwrap();
        let batches = dataframe.collect().await.unwrap();
        assert_batches_sorted_eq!(
            [
                "+------------+",
                "| parent     |",
                "+------------+",
                "| {child: 7} |",
                "+------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn parquet_scan_resolves_different_physical_schemas_and_output_order() {
        let directory = TempDir::new().unwrap();
        let first_path = directory.path().join("first.parquet");
        let first_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("left_first", ArrowDataType::Int64, false)
                .with_metadata(arrow_field_id(1)),
            ArrowField::new("right_first", ArrowDataType::Int64, false)
                .with_metadata(arrow_field_id(2)),
        ]));
        write_parquet_batch(
            &first_path,
            RecordBatch::try_new(
                first_schema,
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![10])),
                ],
            )
            .unwrap(),
        );

        let second_path = directory.path().join("second.parquet");
        let second_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("right_second", ArrowDataType::Int64, false)
                .with_metadata(arrow_field_id(2)),
            ArrowField::new("left_second", ArrowDataType::Int64, false)
                .with_metadata(arrow_field_id(1)),
        ]));
        write_parquet_batch(
            &second_path,
            RecordBatch::try_new(
                second_schema,
                vec![
                    Arc::new(Int64Array::from(vec![20])),
                    Arc::new(Int64Array::from(vec![2])),
                ],
            )
            .unwrap(),
        );

        let schema = KernelStructType::try_new([
            KernelStructField::not_null("right", KernelDataType::LONG)
                .add_metadata(kernel_field_id(2)),
            KernelStructField::not_null("left", KernelDataType::LONG)
                .add_metadata(kernel_field_id(1)),
        ])
        .unwrap();
        let plan = lower_parquet_scan(&KernelScanParquet {
            files: vec![parquet_file(&first_path), parquet_file(&second_path)],
            file_constant_columns: vec![],
            schema: Arc::new(schema),
        })
        .unwrap();

        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_sorted_eq!(
            [
                "+-------+------+",
                "| right | left |",
                "+-------+------+",
                "| 10    | 1    |",
                "| 20    | 2    |",
                "+-------+------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn parquet_scan_adapts_list_of_struct_by_field_id() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("list_struct.parquet");
        let physical_child = Arc::new(
            ArrowField::new("child_physical", ArrowDataType::Int64, false)
                .with_metadata(arrow_field_id(2)),
        );
        let values = StructArray::new(
            vec![Arc::clone(&physical_child)].into(),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
            None,
        );
        let element = Arc::new(ArrowField::new(
            "element",
            ArrowDataType::Struct(vec![physical_child].into()),
            false,
        ));
        let list = ListArray::try_new(
            element,
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
            Arc::new(values),
            None,
        )
        .unwrap();
        let physical_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "items_physical",
            list.data_type().clone(),
            false,
        )
        .with_metadata(arrow_field_id(1))]));
        write_parquet_batch(
            &path,
            RecordBatch::try_new(physical_schema, vec![Arc::new(list)]).unwrap(),
        );

        let element_type = KernelStructType::try_new([
            KernelStructField::not_null("child", KernelDataType::LONG)
                .add_metadata(kernel_field_id(2)),
            KernelStructField::nullable("added", KernelDataType::STRING),
        ])
        .unwrap();
        let schema = KernelStructType::try_new([KernelStructField::not_null(
            "items",
            KernelArrayType::new(element_type, false),
        )
        .add_metadata(kernel_field_id(1))])
        .unwrap();
        let plan = lower_parquet_scan(&KernelScanParquet {
            files: vec![parquet_file(&path)],
            file_constant_columns: vec![],
            schema: Arc::new(schema),
        })
        .unwrap();

        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_sorted_eq!(
            [
                "+--------------------------------------------+",
                "| items                                      |",
                "+--------------------------------------------+",
                "| [{child: 1, added: }, {child: 2, added: }] |",
                "+--------------------------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn parquet_scan_adapts_map_of_struct_by_field_id() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("map_struct.parquet");
        let physical_child = Arc::new(
            ArrowField::new("child_physical", ArrowDataType::Int64, false)
                .with_metadata(arrow_field_id(2)),
        );
        let values = StructArray::new(
            vec![Arc::clone(&physical_child)].into(),
            vec![Arc::new(Int64Array::from(vec![7])) as ArrayRef],
            None,
        );
        let key_field = Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false));
        let value_field = Arc::new(ArrowField::new(
            "value",
            ArrowDataType::Struct(vec![physical_child].into()),
            true,
        ));
        let entries_field = Arc::new(ArrowField::new(
            "entries",
            ArrowDataType::Struct(vec![Arc::clone(&key_field), Arc::clone(&value_field)].into()),
            false,
        ));
        let entries = StructArray::new(
            vec![key_field, value_field].into(),
            vec![Arc::new(StringArray::from(vec!["k"])), Arc::new(values)],
            None,
        );
        let map = MapArray::try_new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
            entries,
            None,
            false,
        )
        .unwrap();
        let physical_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "items_physical",
            map.data_type().clone(),
            false,
        )
        .with_metadata(arrow_field_id(1))]));
        write_parquet_batch(
            &path,
            RecordBatch::try_new(physical_schema, vec![Arc::new(map)]).unwrap(),
        );

        let value_type = KernelStructType::try_new([
            KernelStructField::not_null("child", KernelDataType::LONG)
                .add_metadata(kernel_field_id(2)),
            KernelStructField::nullable("added", KernelDataType::STRING),
        ])
        .unwrap();
        let schema = KernelStructType::try_new([KernelStructField::not_null(
            "items",
            KernelMapType::new(KernelDataType::STRING, value_type, true),
        )
        .add_metadata(kernel_field_id(1))])
        .unwrap();
        let plan = lower_parquet_scan(&KernelScanParquet {
            files: vec![parquet_file(&path)],
            file_constant_columns: vec![],
            schema: Arc::new(schema),
        })
        .unwrap();

        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_sorted_eq!(
            [
                "+--------------------------+",
                "| items                    |",
                "+--------------------------+",
                "| {k: {child: 7, added: }} |",
                "+--------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn parquet_scan_rejects_missing_non_nullable_field() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("missing.parquet");
        write_parquet(&path, &[1]);
        let schema = KernelStructType::try_new([
            KernelStructField::not_null("id", KernelDataType::LONG),
            KernelStructField::not_null("missing", KernelDataType::STRING),
        ])
        .unwrap();
        let plan = lower_parquet_scan(&KernelScanParquet {
            files: vec![parquet_file(&path)],
            file_constant_columns: vec![],
            schema: Arc::new(schema),
        })
        .unwrap();

        let error = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Non-nullable column 'missing' is missing"),
            "{error}"
        );
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
