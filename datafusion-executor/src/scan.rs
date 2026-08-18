//! Static-file scan lowering and physical planning.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, FieldRef as ArrowFieldRef, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, ScalarValue as DFScalarValue};
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, JsonSource, ParquetSource,
};
use datafusion::datasource::provider_as_source;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{
    Expr as DFExpr, LogicalPlan as DFLogicalPlan, LogicalPlanBuilder, TableType,
};
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::{
    ScanFile as KernelScanFile, ScanJson as KernelScanJson, ScanParquet as KernelScanParquet,
};
use delta_kernel::schema::StructType as KernelStructType;

use crate::scalar::to_df_scalar;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanFormat {
    Parquet,
    Json,
}

struct StaticFileProvider {
    file_source: Arc<dyn FileSource>,
    store_url: Option<ObjectStoreUrl>,
    files: Vec<PartitionedFile>,
}

impl std::fmt::Debug for StaticFileProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticFileProvider")
            .field("file_type", &self.file_source.file_type())
            .field("store_url", &self.store_url)
            .field("files", &self.files)
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
    validate_scan_schema(format, schema, &output_schema)?;

    let (table_schema, projection) = datafusion_scan_layout(&output_schema, file_constant_columns);
    let (store_url, files) = scan_files(files)?;
    let file_source: Arc<dyn FileSource> = match format {
        ScanFormat::Parquet => Arc::new(ParquetSource::new(table_schema)),
        ScanFormat::Json => Arc::new(JsonSource::new(table_schema)),
    };
    let provider = StaticFileProvider {
        file_source,
        store_url,
        files,
    };
    let source = provider_as_source(Arc::new(provider));
    let table_name = match format {
        ScanFormat::Parquet => "scan_parquet",
        ScanFormat::Json => "scan_json",
    };
    LogicalPlanBuilder::scan(table_name, source, Some(projection))?.build()
}

fn validate_scan_schema(
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
            .with_limit(limit)
            .with_partitioned_by_file_group(false)
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
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
) -> Result<(Option<ObjectStoreUrl>, Vec<PartitionedFile>), DataFusionError> {
    let mut store_url = None;
    let mut scan_files = Vec::with_capacity(files.len());
    for file in files {
        let constants = file_constants(file)?;
        let location = ListingTableUrl::parse(file.meta.location.as_str())?;
        let file_store_url = location.object_store();
        if let Some(store_url) = &store_url {
            if store_url != &file_store_url {
                return Err(DataFusionError::Plan(format!(
                    "scan files must use one object store, found `{store_url}` and \
                     `{file_store_url}`"
                )));
            }
        } else {
            store_url = Some(file_store_url);
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

    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use datafusion::assert_batches_sorted_eq;
    use datafusion::logical_expr::col as df_col;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionContext;
    use delta_kernel::expressions::Scalar as KernelScalar;
    use delta_kernel::plans::ir::nodes::Operator as KernelOperator;
    use delta_kernel::schema::{
        ColumnMetadataKey, DataType as KernelDataType, MetadataColumnSpec,
        StructField as KernelStructField, StructType as KernelStructType,
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
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
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

    #[test]
    fn parquet_scan_rejects_nested_field_ids() {
        let child = KernelStructField::nullable("child", KernelDataType::LONG)
            .add_metadata([(ColumnMetadataKey::ParquetFieldId.as_ref(), 1_i64)]);
        let nested = KernelStructType::try_new([child]).unwrap();
        let schema =
            KernelStructType::try_new([KernelStructField::nullable("nested", nested)]).unwrap();
        let error = lower_parquet_scan(&KernelScanParquet {
            files: vec![],
            file_constant_columns: vec![],
            schema: Arc::new(schema),
        })
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("field-ID resolution for `child`"));
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
