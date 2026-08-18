//! Static-file scan lowering and physical planning.

mod source;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, ScalarValue as DFScalarValue};
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, FileSource};
use datafusion::datasource::provider_as_source;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{
    Expr as DFExpr, LogicalPlan as DFLogicalPlan, LogicalPlanBuilder, TableType,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::{
    ScanFile as KernelScanFile, ScanJson as KernelScanJson, ScanParquet as KernelScanParquet,
};
use delta_kernel::schema::StructType as KernelStructType;

use self::source::{KernelFileSource, ScanFileLocation};
use crate::scalar::to_df_scalar;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanFormat {
    Parquet,
    Json,
}

struct StaticFileProvider {
    file_source: Arc<dyn FileSource>,
    file_groups: Vec<(ObjectStoreUrl, Vec<PartitionedFile>)>,
}

impl std::fmt::Debug for StaticFileProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticFileProvider")
            .field("file_type", &self.file_source.file_type())
            .field("file_groups", &self.file_groups)
            .finish()
    }
}

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
    let read_fields = schema
        .fields()
        .filter(|field| !file_constant_columns.contains(field.name()))
        .cloned();
    let read_schema = Arc::new(
        KernelStructType::try_new(read_fields)
            .map_err(|error| DataFusionError::External(Box::new(error)))?,
    );

    let (table_schema, projection) = datafusion_scan_layout(&output_schema, file_constant_columns);
    let file_groups = scan_files(files)?;
    let file_source: Arc<dyn FileSource> =
        Arc::new(KernelFileSource::new(format, table_schema, read_schema));
    let provider = StaticFileProvider {
        file_source,
        file_groups,
    };
    let source = provider_as_source(Arc::new(provider));
    let table_name = match format {
        ScanFormat::Parquet => "scan_parquet",
        ScanFormat::Json => "scan_json",
    };
    LogicalPlanBuilder::scan(table_name, source, Some(projection))?.build()
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
        if self.file_groups.is_empty() {
            let schema = match projection {
                Some(projection) => Arc::new(self.schema().project(projection)?),
                None => self.schema(),
            };
            return Ok(Arc::new(EmptyExec::new(schema)));
        }

        let plans = self
            .file_groups
            .iter()
            .map(|(store_url, files)| {
                // One initial group per object store lets DataFusion distribute whole files using
                // the caller's target-partition configuration.
                let config =
                    FileScanConfigBuilder::new(store_url.clone(), Arc::clone(&self.file_source))
                        .with_file_group(FileGroup::new(files.clone()))
                        .with_projection_indices(projection.cloned())?
                        .with_limit(limit)
                        .with_partitioned_by_file_group(false)
                        .build();
                Ok(DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>)
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        UnionExec::try_new(plans)
    }
}

/// Builds DataFusion's file-first table schema and a projection back to kernel output order.
fn datafusion_scan_layout(
    output_schema: &ArrowSchemaRef,
    file_constant_columns: &[String],
) -> (TableSchema, Vec<usize>) {
    enum FieldPosition {
        File(usize),
        Constant(usize),
    }

    // DataFusion models per-file constants as partition columns appended after physical file
    // columns. The table-scan projection restores the kernel schema's arbitrary interleaving.
    let constant_positions: HashMap<&str, usize> = file_constant_columns
        .iter()
        .enumerate()
        .map(|(index, name)| (name.as_str(), index))
        .collect();
    let mut file_fields = Vec::new();
    let mut constant_fields = Vec::new();
    let mut field_positions = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        if let Some(&constant_index) = constant_positions.get(field.name().as_str()) {
            constant_fields.push((constant_index, Arc::clone(field)));
            field_positions.push(FieldPosition::Constant(constant_index));
        } else {
            field_positions.push(FieldPosition::File(file_fields.len()));
            file_fields.push(Arc::clone(field));
        }
    }
    constant_fields.sort_by_key(|(index, _)| *index);

    let file_field_count = file_fields.len();
    let projection = field_positions
        .into_iter()
        .map(|position| match position {
            FieldPosition::File(index) => index,
            FieldPosition::Constant(index) => file_field_count + index,
        })
        .collect();
    let constant_fields = constant_fields
        .into_iter()
        .map(|(_, field)| field)
        .collect();

    let file_schema = Arc::new(ArrowSchema::new(file_fields));
    (TableSchema::new(file_schema, constant_fields), projection)
}

fn scan_files(
    files: &[KernelScanFile],
) -> Result<Vec<(ObjectStoreUrl, Vec<PartitionedFile>)>, DataFusionError> {
    let mut file_groups: Vec<(ObjectStoreUrl, Vec<PartitionedFile>)> = Vec::new();
    for file in files {
        let constants = file_constants(file)?;
        let location = ListingTableUrl::parse(file.meta.location.as_str())?;
        let file_store_url = location.object_store();
        let mut partitioned_file = PartitionedFile::new("", file.meta.size);
        partitioned_file.object_meta.location = location.prefix().clone();
        partitioned_file.partition_values = constants;
        partitioned_file =
            partitioned_file.with_extension(ScanFileLocation(file.meta.location.to_string()));
        if let Some((_, group)) = file_groups
            .iter_mut()
            .find(|(store_url, _)| store_url == &file_store_url)
        {
            group.push(partitioned_file);
        } else {
            file_groups.push((file_store_url, vec![partitioned_file]));
        }
    }
    Ok(file_groups)
}

/// Converts a file's kernel constants into DataFusion partition values.
///
/// The scan IR documents value count and type compatibility as producer invariants, but those
/// constraints are not encoded in `ScanFile` itself. This also assumes a NULL constant only
/// targets a nullable field, which the IR contract does not state separately. Callers that
/// construct scan payload structs directly must uphold these constraints. Conversion remains
/// fallible for scalar types that the DataFusion executor does not support.
fn file_constants(file: &KernelScanFile) -> Result<Vec<DFScalarValue>, DataFusionError> {
    file.file_constants
        .iter()
        .map(|scalar| {
            to_df_scalar(scalar).map_err(|error| DataFusionError::External(Box::new(error)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use datafusion::arrow::array::{
        Array as _, ArrayRef as ArrowArrayRef, Int64Array, ListArray, MapArray, RecordBatch,
        StringArray, StructArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
        Schema as ArrowSchema,
    };
    use datafusion::assert_batches_sorted_eq;
    use datafusion::logical_expr::col as df_col;
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::path::Path as ObjectPath;
    use datafusion::object_store::{ObjectStoreExt as _, PutPayload};
    use datafusion::parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use datafusion::prelude::SessionContext;
    use delta_kernel::expressions::Scalar as KernelScalar;
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

    #[derive(Clone, Copy)]
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
        let size = std::fs::metadata(&path).unwrap().len();
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
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
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

    fn parquet_batch_file(directory: &TempDir, stem: &str, batch: &RecordBatch) -> KernelScanFile {
        let path = directory.path().join(format!("{stem}.parquet"));
        write_parquet_batch(&path, batch);
        KernelScanFile {
            meta: KernelFileMeta {
                location: format!("file://{}", path.display()).parse().unwrap(),
                last_modified: 0,
                size: std::fs::metadata(path).unwrap().len(),
            },
            file_constants: vec![],
        }
    }

    fn arrow_field_with_id(
        name: &str,
        data_type: ArrowDataType,
        nullable: bool,
        field_id: i64,
    ) -> ArrowField {
        ArrowField::new(name, data_type, nullable)
            .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string())].into())
    }

    fn kernel_field_with_id(
        name: &str,
        data_type: impl Into<KernelDataType>,
        nullable: bool,
        field_id: i64,
    ) -> KernelStructField {
        KernelStructField::new(name, data_type, nullable)
            .add_metadata([(ColumnMetadataKey::ParquetFieldId.as_ref(), field_id)])
    }

    fn write_json(path: &std::path::Path, ids: &[i64]) {
        let lines: Vec<String> = ids.iter().map(|id| format!(r#"{{"id":{id}}}"#)).collect();
        let contents = format!("{}\n", lines.join("\n"));
        std::fs::write(path, contents).unwrap();
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

    fn lower_test_parquet_scan(
        files: Vec<KernelScanFile>,
        schema: KernelStructType,
    ) -> Result<DFLogicalPlan, DataFusionError> {
        lower_operator(
            &KernelOperator::ScanParquet(KernelScanParquet {
                files,
                file_constant_columns: vec![],
                schema: Arc::new(schema),
            }),
            &[],
        )
    }

    async fn execute_test_parquet_scan(
        files: Vec<KernelScanFile>,
        schema: KernelStructType,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let plan = lower_test_parquet_scan(files, schema)?;
        let dataframe = SessionContext::new().execute_logical_plan(plan).await?;
        dataframe.collect().await
    }

    #[rstest]
    #[case::parquet(TestFormat::Parquet)]
    #[case::json(TestFormat::Json)]
    #[tokio::test]
    async fn scans_multiple_files_with_constants_in_declared_output_order(
        #[case] format: TestFormat,
    ) {
        let directory = TempDir::new().unwrap();
        let files = vec![
            scan_file(&directory, format, "one", &[1, 2], "a", 10),
            scan_file(&directory, format, "two", &[3], "b", 20),
        ];
        let plan = lower_test_scan(format, files, scan_schema()).unwrap();
        let DFLogicalPlan::TableScan(scan) = &plan else {
            panic!("expected a table scan")
        };
        assert_eq!(scan.projection.as_deref(), Some(&[2, 0, 1][..]));

        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_sorted_eq!(
            [
                "+-----------+----+---------+",
                "| partition | id | version |",
                "+-----------+----+---------+",
                "| a         | 1  | 10      |",
                "| a         | 2  | 10      |",
                "| b         | 3  | 20      |",
                "+-----------+----+---------+",
            ],
            &batches
        );
    }

    #[rstest]
    #[case::parquet(TestFormat::Parquet)]
    #[case::json(TestFormat::Json)]
    fn assigns_distinct_scan_indices_to_multiple_file_fields(#[case] format: TestFormat) {
        let schema = KernelStructType::try_new([
            KernelStructField::not_null("partition", KernelDataType::STRING),
            KernelStructField::not_null("first", KernelDataType::LONG),
            KernelStructField::not_null("second", KernelDataType::LONG),
            KernelStructField::not_null("version", KernelDataType::LONG),
        ])
        .unwrap();
        let plan = lower_test_scan(format, vec![], schema).unwrap();
        let DFLogicalPlan::TableScan(scan) = plan else {
            panic!("expected a table scan")
        };

        assert_eq!(scan.projection.as_deref(), Some(&[3, 0, 1, 2][..]));
    }

    #[rstest]
    #[case::parquet(TestFormat::Parquet)]
    #[case::json(TestFormat::Json)]
    #[tokio::test]
    async fn translates_projection_from_output_to_native_scan_order(#[case] format: TestFormat) {
        let directory = TempDir::new().unwrap();
        let files = vec![scan_file(&directory, format, "one", &[7], "west", 42)];
        let scan = lower_test_scan(format, files, scan_schema()).unwrap();
        let projected = LogicalPlanBuilder::from(scan)
            .project([df_col("version"), df_col("id")])
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
                "+---------+----+",
                "| version | id |",
                "+---------+----+",
                "| 42      | 7  |",
                "+---------+----+",
            ],
            &batches
        );
    }

    #[rstest]
    #[case::parquet(TestFormat::Parquet)]
    #[case::json(TestFormat::Json)]
    #[tokio::test]
    async fn projects_only_file_constants(#[case] format: TestFormat) {
        let directory = TempDir::new().unwrap();
        let files = vec![scan_file(&directory, format, "one", &[7, 8], "west", 42)];
        let scan = lower_test_scan(format, files, scan_schema()).unwrap();
        let projected = LogicalPlanBuilder::from(scan)
            .project([df_col("version")])
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
                "+---------+",
                "| version |",
                "+---------+",
                "| 42      |",
                "| 42      |",
                "+---------+",
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
        assert_eq!(names, ["partition", "id", "version"]);
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

    #[tokio::test]
    async fn parquet_scan_populates_row_index_metadata_column() {
        let directory = TempDir::new().unwrap();
        let files = vec![scan_file(
            &directory,
            TestFormat::Parquet,
            "rows",
            &[7, 8],
            "west",
            42,
        )];
        let schema = KernelStructType::try_new([
            KernelStructField::not_null("partition", KernelDataType::STRING),
            KernelStructField::not_null("id", KernelDataType::LONG),
            KernelStructField::create_metadata_column("row_index", MetadataColumnSpec::RowIndex),
            KernelStructField::not_null("version", KernelDataType::LONG),
        ])
        .unwrap();

        let scan = lower_test_scan(TestFormat::Parquet, files, schema).unwrap();
        let plan = LogicalPlanBuilder::from(scan)
            .project([df_col("row_index")])
            .unwrap()
            .build()
            .unwrap();
        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let row_index = batches[0]
            .column_by_name("row_index")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(row_index.values(), &[0, 1]);
    }

    #[rstest]
    #[case::parquet(TestFormat::Parquet)]
    #[case::json(TestFormat::Json)]
    #[tokio::test]
    async fn scan_populates_file_path_metadata_column(#[case] format: TestFormat) {
        let directory = TempDir::new().unwrap();
        let files = vec![scan_file(&directory, format, "rows", &[7, 8], "west", 42)];
        let expected = files[0].meta.location.to_string();
        let schema = KernelStructType::try_new([
            KernelStructField::not_null("partition", KernelDataType::STRING),
            KernelStructField::not_null("id", KernelDataType::LONG),
            KernelStructField::create_metadata_column("_file", MetadataColumnSpec::FilePath),
            KernelStructField::not_null("version", KernelDataType::LONG),
        ])
        .unwrap();

        let scan = lower_test_scan(format, files, schema).unwrap();
        let plan = LogicalPlanBuilder::from(scan)
            .project([df_col("_file")])
            .unwrap()
            .build()
            .unwrap();
        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let file_path = batches[0]
            .column_by_name("_file")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(file_path.iter().all(|value| value == Some(&expected)));
    }

    #[tokio::test]
    async fn parquet_scan_matches_fields_by_id_before_name() {
        let directory = TempDir::new().unwrap();
        let first_schema = Arc::new(ArrowSchema::new(vec![
            arrow_field_with_id("right_name", ArrowDataType::Int64, false, 1),
            arrow_field_with_id("left_name", ArrowDataType::Int64, false, 99),
            arrow_field_with_id("physical_right", ArrowDataType::Int64, false, 2),
        ]));
        let first_batch = RecordBatch::try_new(
            first_schema,
            vec![
                Arc::new(Int64Array::from(vec![10])),
                Arc::new(Int64Array::from(vec![999])),
                Arc::new(Int64Array::from(vec![20])),
            ],
        )
        .unwrap();
        let second_schema = Arc::new(ArrowSchema::new(vec![
            arrow_field_with_id("legacy_left", ArrowDataType::Int64, false, 1),
            arrow_field_with_id("right_name", ArrowDataType::Int64, false, 99),
            arrow_field_with_id("legacy_right", ArrowDataType::Int64, false, 2),
        ]));
        let second_batch = RecordBatch::try_new(
            second_schema,
            vec![
                Arc::new(Int64Array::from(vec![11])),
                Arc::new(Int64Array::from(vec![999])),
                Arc::new(Int64Array::from(vec![21])),
            ],
        )
        .unwrap();
        let files = vec![
            parquet_batch_file(&directory, "first", &first_batch),
            parquet_batch_file(&directory, "second", &second_batch),
        ];
        let requested_schema = KernelStructType::try_new([
            kernel_field_with_id("left_name", KernelDataType::LONG, false, 1),
            kernel_field_with_id("right_name", KernelDataType::LONG, false, 2),
        ])
        .unwrap();

        let batches = execute_test_parquet_scan(files, requested_schema)
            .await
            .unwrap();
        assert_batches_sorted_eq!(
            [
                "+-----------+------------+",
                "| left_name | right_name |",
                "+-----------+------------+",
                "| 10        | 20         |",
                "| 11        | 21         |",
                "+-----------+------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn parquet_scan_falls_back_to_name_without_matching_id() {
        let directory = TempDir::new().unwrap();
        let file_schema = Arc::new(ArrowSchema::new(vec![arrow_field_with_id(
            "stable_name",
            ArrowDataType::Int64,
            false,
            8,
        )]));
        let batch =
            RecordBatch::try_new(file_schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap();
        let file = parquet_batch_file(&directory, "fallback", &batch);
        let requested_schema = KernelStructType::try_new([kernel_field_with_id(
            "stable_name",
            KernelDataType::LONG,
            false,
            7,
        )])
        .unwrap();

        let batches = execute_test_parquet_scan(vec![file], requested_schema)
            .await
            .unwrap();
        assert_batches_sorted_eq!(
            [
                "+-------------+",
                "| stable_name |",
                "+-------------+",
                "| 42          |",
                "+-------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn parquet_scan_rejects_missing_non_nullable_field_id() {
        let directory = TempDir::new().unwrap();
        let file_schema = Arc::new(ArrowSchema::new(vec![arrow_field_with_id(
            "other",
            ArrowDataType::Int64,
            false,
            8,
        )]));
        let batch =
            RecordBatch::try_new(file_schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap();
        let file = parquet_batch_file(&directory, "missing", &batch);
        let requested_schema = KernelStructType::try_new([kernel_field_with_id(
            "required",
            KernelDataType::LONG,
            false,
            7,
        )])
        .unwrap();

        let error = execute_test_parquet_scan(vec![file], requested_schema)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("field is not nullable: required"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn parquet_scan_matches_and_reorders_nested_fields_by_id() {
        let directory = TempDir::new().unwrap();
        let physical_children: ArrowFields = vec![
            arrow_field_with_id("old_second", ArrowDataType::Int64, false, 3),
            arrow_field_with_id("old_first", ArrowDataType::Int64, false, 2),
        ]
        .into();
        let outer = Arc::new(StructArray::new(
            physical_children.clone(),
            vec![
                Arc::new(Int64Array::from(vec![20, 21])),
                Arc::new(Int64Array::from(vec![10, 11])),
            ],
            None,
        )) as ArrowArrayRef;
        let file_schema = Arc::new(ArrowSchema::new(vec![arrow_field_with_id(
            "old_outer",
            ArrowDataType::Struct(physical_children),
            false,
            1,
        )]));
        let batch = RecordBatch::try_new(file_schema, vec![outer]).unwrap();
        let file = parquet_batch_file(&directory, "nested", &batch);

        let requested_children = KernelStructType::try_new([
            kernel_field_with_id("new_first", KernelDataType::LONG, false, 2),
            kernel_field_with_id("new_second", KernelDataType::LONG, false, 3),
            kernel_field_with_id("new_missing", KernelDataType::LONG, true, 4),
        ])
        .unwrap();
        let requested_schema = KernelStructType::try_new([kernel_field_with_id(
            "new_outer",
            requested_children,
            false,
            1,
        )])
        .unwrap();

        let batches = execute_test_parquet_scan(vec![file], requested_schema)
            .await
            .unwrap();
        let outer_column = batches[0].column(0);
        let outer = outer_column.as_any().downcast_ref::<StructArray>().unwrap();
        let field_names: Vec<_> = outer
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(field_names, ["new_first", "new_second", "new_missing"]);
        let first_column = outer.column_by_name("new_first").unwrap();
        let first = first_column.as_any().downcast_ref::<Int64Array>().unwrap();
        let second_column = outer.column_by_name("new_second").unwrap();
        let second = second_column.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(first.values(), &[10, 11]);
        assert_eq!(second.values(), &[20, 21]);
        assert!(outer.column_by_name("new_missing").unwrap().is_null(0));
    }

    #[tokio::test]
    async fn parquet_scan_synthesizes_struct_with_no_matching_children() {
        let directory = TempDir::new().unwrap();
        let physical_children: ArrowFields = vec![arrow_field_with_id(
            "old_child",
            ArrowDataType::Int64,
            false,
            3,
        )]
        .into();
        let outer = Arc::new(StructArray::new(
            physical_children.clone(),
            vec![Arc::new(Int64Array::from(vec![10]))],
            None,
        )) as ArrowArrayRef;
        let file_schema = Arc::new(ArrowSchema::new(vec![arrow_field_with_id(
            "old_outer",
            ArrowDataType::Struct(physical_children),
            true,
            1,
        )]));
        let batch = RecordBatch::try_new(file_schema, vec![outer]).unwrap();
        let file = parquet_batch_file(&directory, "missing_nested", &batch);

        let requested_children = KernelStructType::try_new([kernel_field_with_id(
            "new_child",
            KernelDataType::LONG,
            true,
            2,
        )])
        .unwrap();
        let requested_schema = KernelStructType::try_new([kernel_field_with_id(
            "new_outer",
            requested_children,
            true,
            1,
        )])
        .unwrap();

        let batches = execute_test_parquet_scan(vec![file], requested_schema)
            .await
            .unwrap();
        let outer = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(outer.is_null(0));
        assert!(outer.column(0).is_null(0));
    }

    #[tokio::test]
    async fn parquet_scan_matches_field_ids_inside_lists() {
        let directory = TempDir::new().unwrap();
        let physical_children: ArrowFields = vec![arrow_field_with_id(
            "old_child",
            ArrowDataType::Int64,
            false,
            3,
        )]
        .into();
        let values = Arc::new(StructArray::new(
            physical_children.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 11]))],
            None,
        )) as ArrowArrayRef;
        let physical_element = Arc::new(arrow_field_with_id(
            "item",
            ArrowDataType::Struct(physical_children),
            false,
            2,
        ));
        let list = Arc::new(ListArray::new(
            Arc::clone(&physical_element),
            OffsetBuffer::from_lengths([2]),
            values,
            None,
        )) as ArrowArrayRef;
        let file_schema = Arc::new(ArrowSchema::new(vec![arrow_field_with_id(
            "old_list",
            ArrowDataType::List(physical_element),
            false,
            1,
        )]));
        let batch = RecordBatch::try_new(file_schema, vec![list]).unwrap();
        let file = parquet_batch_file(&directory, "list", &batch);

        let requested_element = KernelStructType::try_new([kernel_field_with_id(
            "new_child",
            KernelDataType::LONG,
            false,
            3,
        )])
        .unwrap();
        let requested_schema = KernelStructType::try_new([kernel_field_with_id(
            "new_list",
            KernelArrayType::new(requested_element, false),
            false,
            1,
        )])
        .unwrap();

        let batches = execute_test_parquet_scan(vec![file], requested_schema)
            .await
            .unwrap();
        let list_column = batches[0].column(0);
        let list = list_column.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.value(0);
        let values = values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(values.fields()[0].name(), "new_child");
        let child_column = values.column(0);
        let child = child_column.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(child.values(), &[10, 11]);
    }

    #[tokio::test]
    async fn json_scan_coerces_primitive_values_to_strings() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("primitive.json");
        std::fs::write(&path, "{\"value\":42}\n{\"value\":true}\n").unwrap();
        let file = KernelScanFile {
            meta: KernelFileMeta {
                location: format!("file://{}", path.display()).parse().unwrap(),
                last_modified: 0,
                size: std::fs::metadata(path).unwrap().len(),
            },
            file_constants: vec![],
        };
        let schema = KernelStructType::try_new([KernelStructField::not_null(
            "value",
            KernelDataType::STRING,
        )])
        .unwrap();
        let plan = lower_operator(
            &KernelOperator::ScanJson(KernelScanJson {
                files: vec![file],
                file_constant_columns: vec![],
                schema: Arc::new(schema),
            }),
            &[],
        )
        .unwrap();

        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            [Some("42"), Some("true")]
        );
    }

    #[tokio::test]
    async fn parquet_scan_matches_field_ids_inside_map_values() {
        let directory = TempDir::new().unwrap();
        let physical_value_fields: ArrowFields = vec![arrow_field_with_id(
            "old_child",
            ArrowDataType::Int64,
            false,
            3,
        )]
        .into();
        let values = Arc::new(StructArray::new(
            physical_value_fields.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 11]))],
            None,
        )) as ArrowArrayRef;
        let physical_entry_fields: ArrowFields = vec![
            ArrowField::new("key", ArrowDataType::Utf8, false),
            ArrowField::new("value", ArrowDataType::Struct(physical_value_fields), true),
        ]
        .into();
        let entries = StructArray::new(
            physical_entry_fields.clone(),
            vec![Arc::new(StringArray::from(vec!["a", "b"])), values],
            None,
        );
        let physical_entries = Arc::new(ArrowField::new(
            "entries",
            ArrowDataType::Struct(physical_entry_fields),
            false,
        ));
        let map = Arc::new(MapArray::new(
            Arc::clone(&physical_entries),
            OffsetBuffer::from_lengths([2]),
            entries,
            None,
            false,
        )) as ArrowArrayRef;
        let file_schema = Arc::new(ArrowSchema::new(vec![arrow_field_with_id(
            "old_map",
            ArrowDataType::Map(physical_entries, false),
            false,
            1,
        )]));
        let batch = RecordBatch::try_new(file_schema, vec![map]).unwrap();
        let file = parquet_batch_file(&directory, "map", &batch);

        let requested_value = KernelStructType::try_new([kernel_field_with_id(
            "new_child",
            KernelDataType::LONG,
            false,
            3,
        )])
        .unwrap();
        let requested_schema = KernelStructType::try_new([kernel_field_with_id(
            "new_map",
            KernelMapType::new(KernelDataType::STRING, requested_value, true),
            false,
            1,
        )])
        .unwrap();

        let batches = execute_test_parquet_scan(vec![file], requested_schema)
            .await
            .unwrap();
        let map_column = batches[0].column(0);
        let map = map_column.as_any().downcast_ref::<MapArray>().unwrap();
        let entries = map.entries();
        let key_column = entries.column(0);
        let keys = key_column.as_any().downcast_ref::<StringArray>().unwrap();
        let keys: Vec<_> = keys.iter().collect();
        assert_eq!(keys, [Some("a"), Some("b")]);
        let value_column = entries.column(1);
        let values = value_column.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(values.fields()[0].name(), "new_child");
        let child_column = values.column(0);
        let child = child_column.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(child.values(), &[10, 11]);
    }

    #[tokio::test]
    async fn scans_files_from_multiple_object_stores() {
        let first_contents = "{\"id\":1}\n";
        let second_contents = "{\"id\":2}\n";
        let first_store = Arc::new(InMemory::new());
        let second_store = Arc::new(InMemory::new());
        first_store
            .put(
                &ObjectPath::from("table/a.json"),
                PutPayload::from(first_contents),
            )
            .await
            .unwrap();
        second_store
            .put(
                &ObjectPath::from("table/b.json"),
                PutPayload::from(second_contents),
            )
            .await
            .unwrap();
        let files = [
            ("s3://first/table/a.json", first_contents.len()),
            ("s3://second/table/b.json", second_contents.len()),
        ]
        .into_iter()
        .map(|(location, size)| KernelScanFile {
            meta: KernelFileMeta {
                location: location.parse().unwrap(),
                last_modified: 0,
                size: size as u64,
            },
            file_constants: vec![],
        })
        .collect();
        let schema =
            KernelStructType::try_new([KernelStructField::not_null("id", KernelDataType::LONG)])
                .unwrap();
        let plan = lower_operator(
            &KernelOperator::ScanJson(KernelScanJson {
                files,
                file_constant_columns: vec![],
                schema: Arc::new(schema),
            }),
            &[],
        )
        .unwrap();
        let context = SessionContext::new();
        context.register_object_store(
            ObjectStoreUrl::parse("s3://first/").unwrap().as_ref(),
            first_store,
        );
        context.register_object_store(
            ObjectStoreUrl::parse("s3://second/").unwrap().as_ref(),
            second_store,
        );

        let batches = context
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_sorted_eq!(
            ["+----+", "| id |", "+----+", "| 1  |", "| 2  |", "+----+",],
            &batches
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
