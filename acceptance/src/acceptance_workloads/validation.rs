//! Result validation for acceptance workload test cases.
//!
//! Compares actual kernel results against expected outcomes from the spec. For read workloads,
//! expected data is loaded from Parquet files in `expected_data/` and compared order-independently.
//! For snapshot workloads, protocol and metadata are compared directly.

use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    new_null_array, Array, ArrayRef, ListArray, MapArray, RecordBatch, StructArray,
    TimestampNanosecondArray,
};
use delta_kernel::arrow::compute::{cast, concat_batches};
use delta_kernel::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, SchemaRef};
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::{DeltaResult, KernelError as Error};
use delta_kernel_workloads::models::{ExpectedError, ReadExpected, SnapshotExpected, TimeTravel};
use itertools::Itertools;
use serde_json::Value;
use tracing::debug;

use super::workload::{ReadResult, SnapshotResult};
use crate::data::assert_data_matches;

fn assert_aligned_data_matches(
    result: Vec<RecordBatch>,
    result_schema: &SchemaRef,
    expected: RecordBatch,
) -> DeltaResult<()> {
    let expected = align_batch_to_schema(expected, result_schema.clone())?;
    assert_data_matches(result, result_schema, expected)
}

fn align_batch_to_schema(batch: RecordBatch, schema: SchemaRef) -> DeltaResult<RecordBatch> {
    let source_schema = batch.schema();
    require_matching_field_order(source_schema.fields(), schema.fields())?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            source_schema
                .index_of(field.name())
                .ok()
                .map(|index| align_array(batch.column(index), field.data_type()))
                .unwrap_or_else(|| missing_void_array(field, batch.num_rows()))
        })
        .try_collect()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn align_array(array: &ArrayRef, data_type: &DataType) -> DeltaResult<ArrayRef> {
    if array.data_type() == data_type {
        return Ok(array.clone());
    }
    if let (Some(source), DataType::Struct(fields)) =
        (array.as_any().downcast_ref::<StructArray>(), data_type)
    {
        require_matching_field_order(source.fields(), fields)?;
        let columns = fields
            .iter()
            .map(|field| {
                source
                    .column_by_name(field.name())
                    .map(|column| align_array(column, field.data_type()))
                    .unwrap_or_else(|| missing_void_array(field, source.len()))
            })
            .try_collect()?;
        return Ok(Arc::new(StructArray::try_new(
            fields.clone(),
            columns,
            source.nulls().cloned(),
        )?));
    }
    if let (Some(source), DataType::List(field)) =
        (array.as_any().downcast_ref::<ListArray>(), data_type)
    {
        let DataType::List(source_field) = source.data_type() else {
            return Err(Error::internal_error("ListArray has a non-list data type"));
        };
        require_same_nullability("list element", source_field, field)?;
        let values = align_array(source.values(), field.data_type())?;
        return Ok(Arc::new(ListArray::try_new(
            field.clone(),
            source.offsets().clone(),
            values,
            source.nulls().cloned(),
        )?));
    }
    if let (Some(source), DataType::Map(field, ordered)) =
        (array.as_any().downcast_ref::<MapArray>(), data_type)
    {
        let DataType::Map(source_field, source_ordered) = source.data_type() else {
            return Err(Error::internal_error("MapArray has a non-map data type"));
        };
        require_map_compatibility(*source_ordered, *ordered, source_field, field)?;
        let entries = align_array(
            &(Arc::new(source.entries().clone()) as ArrayRef),
            field.data_type(),
        )?;
        let entries = entries
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Error::generic("Aligned map entries are not a struct"))?
            .clone();
        return Ok(Arc::new(MapArray::try_new(
            field.clone(),
            source.offsets().clone(),
            entries,
            source.nulls().cloned(),
            *ordered,
        )?));
    }
    if let (
        DataType::Timestamp(delta_kernel::arrow::datatypes::TimeUnit::Nanosecond, None),
        DataType::Timestamp(delta_kernel::arrow::datatypes::TimeUnit::Microsecond, Some(timezone)),
    ) = (array.data_type(), data_type)
    {
        if timezone.as_ref() == "UTC" {
            // The workload generator uses Spark TimestampType, whose precision is microseconds.
            // Arrow infers its expected Parquet output as nanoseconds without a timezone.
            let source = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::internal_error("Timestamp array has an unexpected type"))?;
            if source.iter().flatten().any(|value| value % 1_000 != 0) {
                return Err(Error::generic(
                    "Expected Spark timestamp has sub-microsecond precision",
                ));
            }
            return Ok(cast(array, data_type)?);
        }
    }
    Err(Error::generic(format!(
        "Expected data type {:?} does not match result type {data_type:?}",
        array.data_type()
    )))
}

fn require_matching_field_order(source: &Fields, target: &Fields) -> DeltaResult<()> {
    let source_names = source
        .iter()
        .map(|field| field.name().clone())
        .collect_vec();
    let target_names = target
        .iter()
        .filter(|field| field.data_type() != &DataType::Null || source.find(field.name()).is_some())
        .map(|field| field.name().clone())
        .collect_vec();
    if source_names != target_names {
        return Err(Error::generic(format!(
            "Expected field order {:?} does not match result field order {:?}",
            source_names, target_names
        )));
    }
    for target_field in target {
        if let Some((_, source_field)) = source.find(target_field.name()) {
            if source_field.is_nullable() != target_field.is_nullable() {
                return Err(Error::generic(format!(
                    "Expected nullability for field '{}' does not match the result",
                    target_field.name()
                )));
            }
        }
    }
    Ok(())
}

fn missing_void_array(field: &Field, len: usize) -> DeltaResult<ArrayRef> {
    if field.data_type() == &DataType::Null {
        Ok(new_null_array(field.data_type(), len))
    } else {
        Err(Error::generic(format!(
            "Expected data is missing non-void field '{}'",
            field.name()
        )))
    }
}

fn require_same_nullability(context: &str, source: &Field, target: &Field) -> DeltaResult<()> {
    if source.is_nullable() != target.is_nullable() {
        return Err(Error::generic(format!(
            "Expected {context} nullability does not match the result"
        )));
    }
    Ok(())
}

fn require_map_compatibility(
    source_ordered: bool,
    target_ordered: bool,
    source_field: &Field,
    target_field: &Field,
) -> DeltaResult<()> {
    if source_ordered != target_ordered {
        return Err(Error::generic(
            "Expected map ordering does not match the result",
        ));
    }
    require_same_nullability("map entry", source_field, target_field)
}

fn protocols_equal(
    actual: &delta_kernel::actions::Protocol,
    expected: &delta_kernel::actions::Protocol,
) -> Result<bool, String> {
    fn normalized(protocol: &delta_kernel::actions::Protocol) -> Result<Value, String> {
        let mut value = serde_json::to_value(protocol).map_err(|error| error.to_string())?;
        for name in ["readerFeatures", "writerFeatures"] {
            if let Some(features) = value.get_mut(name).and_then(Value::as_array_mut) {
                features.sort_by_key(Value::to_string);
            }
        }
        Ok(value)
    }

    Ok(normalized(actual)? == normalized(expected)?)
}

fn error_without_backtrace(error: &Error) -> &Error {
    match error {
        Error::Backtraced { source, .. } => error_without_backtrace(source),
        error => error,
    }
}

fn is_missing_metadata_schema_error(error: &Error) -> bool {
    const MESSAGE: &str =
        "whilst decoding field 'metaData': Encountered unmasked nulls in non-nullable StructArray \
         child: Field { \"schemaString\": Utf8 }";

    matches!(
        error,
        Error::MalformedJson(error)
            if error.to_string().contains(MESSAGE)
    ) || matches!(
        error,
        Error::Arrow(delta_kernel::arrow::error::ArrowError::JsonError(message))
            if message == MESSAGE
    )
}

fn expected_error_matches(expected: &ExpectedError, actual: &Error) -> bool {
    let actual = error_without_backtrace(actual);
    match expected.error_code.as_str() {
        "DELTA_STATE_RECOVER_ERROR" => {
            matches!(
                actual,
                Error::MissingMetadata | Error::MissingProtocol | Error::MissingMetadataAndProtocol
            ) || matches!(
                actual,
                Error::InvalidCheckpoint(message)
                    if message == "Had a _last_checkpoint hint but didn't find any checkpoints"
            )
        }
        "DELTA_TABLE_NOT_FOUND" | "DELTA_MISSING_TRANSACTION_LOG" => matches!(
            actual,
            Error::EmptyLog | Error::MissingVersion(_) | Error::FileNotFound(_)
        ),
        "DELTA_LOG_FILE_NOT_FOUND" => {
            matches!(actual, Error::FileNotFound(_))
                || matches!(actual, Error::Generic(message) if message == "Only non-negative snapshot versions are supported")
        }
        "DELTA_TRUNCATED_TRANSACTION_LOG" => {
            matches!(
                actual,
                Error::EmptyLog | Error::MissingVersion(_) | Error::FileNotFound(_)
            )
        }
        "DELTA_VERSIONS_NOT_CONTIGUOUS" | "DELTA_VERSIONS_NOT_CONTIGUOUS.GENERIC" => matches!(
            actual,
            Error::LogTailVersionsNotContiguous { .. } | Error::MissingVersion(_)
        ),
        "ColumnMappingUnsupportedException" => {
            matches!(actual, Error::InvalidColumnMappingMode(_))
        }
        "COLUMN_ALREADY_EXISTS" => {
            matches!(
                actual,
                Error::Schema(message)
                    if message.starts_with("Duplicate field name (case-insensitive):")
            ) || matches!(
                actual,
                Error::MalformedJson(error)
                    if error
                        .to_string()
                        .starts_with("Schema error: Duplicate field name (case-insensitive):")
            )
        }
        "UNRESOLVED_COLUMN" => matches!(
            actual,
            Error::Generic(message)
                if message.starts_with("Cannot determine types for: Identifier(")
        ),
        "FIELD_NOT_FOUND" => matches!(
            actual,
            Error::Generic(message)
                if message.starts_with("Cannot determine types for: CompoundIdentifier(")
        ),
        "DELTA_VERSION_NOT_FOUND" => {
            matches!(actual, Error::MissingVersion(_) | Error::EmptyLog)
        }
        "DELTA_TABLE_RESTORE_VERSION_INVALID" => matches!(
            actual,
            Error::Generic(message)
                if message == "Only non-negative snapshot versions are supported"
        ),
        "DELTA_INVALID_PROTOCOL_VERSION" => {
            matches!(actual, Error::Unsupported(message) if message.starts_with("Unsupported minimum reader version "))
                || matches!(actual, Error::InvalidProtocol(message) if message.contains("min_reader_version"))
                || is_missing_metadata_schema_error(actual)
        }
        "DELTA_UNSUPPORTED_READER_VERSION" => matches!(
            actual,
            Error::InvalidProtocol(message)
                if message == "Writer features must be present when minimum writer version = 7"
        ),
        "DELTA_UNSUPPORTED_FEATURES_FOR_READ" => {
            matches!(actual, Error::Unsupported(message) if message.contains(" is not supported"))
                || is_missing_metadata_schema_error(actual)
        }
        "DELTA_FEATURES_REQUIRE_WRITE_SUPPORT" => is_missing_metadata_schema_error(actual),
        "DELTA_FEATURES_PROTOCOL_METADATA_MISMATCH" => {
            matches!(actual, Error::InvalidProtocol(message) if message.contains("feature"))
                || matches!(actual, Error::Unsupported(message) if message.contains(" requires "))
        }
        "EXPRESSION_DECODING_FAILED" => {
            matches!(actual, Error::InvalidProtocol(message) if message.contains("version") || message.contains("features"))
        }
        "DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION" | "DELTA_TIMESTAMP_GREATER_THAN_COMMIT" => {
            matches!(actual, Error::LogHistory(_))
                || matches!(actual, Error::Generic(message) if message == "Timestamp-based time travel is not yet supported")
        }
        "DELTA_MISSING_COMMIT_INFO" | "DELTA_MISSING_COMMIT_TIMESTAMP" | "INVALID_TIMESTAMP" => {
            matches!(
                actual,
                Error::Generic(message)
                    if message == "Timestamp-based time travel is not yet supported"
            )
        }
        "FAILED_READ_FILE.DBR_FILE_NOT_EXIST" => {
            matches!(actual, Error::FileNotFound(_) | Error::ObjectStore(_))
                || matches!(
                    actual,
                    Error::Arrow(delta_kernel::arrow::error::ArrowError::ExternalError(_))
                )
                || matches!(
                    actual,
                    Error::Parquet(delta_kernel::parquet::errors::ParquetError::External(source))
                        if source.to_string().contains("not found")
                            || source.to_string().contains("No such file or directory")
                )
        }
        "FAILED_READ_FILE.NO_HINT" => {
            matches!(
                actual,
                Error::FileNotFound(_)
                    | Error::ObjectStore(_)
                    | Error::Parquet(_)
                    | Error::DeletionVector(_)
            ) || matches!(actual, Error::InternalError(message) if message.starts_with("Unsupported deletion vector format option:"))
        }
        "IllegalStateException" => matches!(
            actual,
            Error::MissingVersion(_)
                | Error::MalformedJson(_)
                | Error::Schema(_)
                | Error::InvalidCheckpoint(_)
                | Error::InvalidLogSegment(_)
                | Error::Arrow(_)
                | Error::Parquet(_)
        ),
        "SparkException" => matches!(
            actual,
            Error::MalformedJson(_)
                | Error::Schema(_)
                | Error::InvalidCheckpoint(_)
                | Error::InvalidLogSegment(_)
                | Error::Arrow(_)
                | Error::Parquet(_)
        ),
        _ => false,
    }
}

fn validate_expected_error(actual: &Error, expected: &ExpectedError) -> Result<(), String> {
    if expected_error_matches(expected, actual) {
        debug!(
            "Got expected error '{}' with message: {:?}\nKernel error: {}",
            expected.error_code, expected.error_message, actual
        );
        Ok(())
    } else {
        Err(format!(
            "Expected error category '{}', got: {actual}",
            expected.error_code
        ))
    }
}

/// Read expected data from parquet files in expected_dir/expected_data/.
fn read_expected_data(expected_dir: &Path) -> Result<RecordBatch, String> {
    let expected_data_dir = expected_dir.join("expected_data");
    if !expected_data_dir.exists() {
        return Err(format!(
            "Expected data directory not found: {}",
            expected_data_dir.display()
        ));
    }

    let parquet_paths = fs::read_dir(&expected_data_dir)
        .map_err(|e| format!("Failed to read expected_data dir: {e}"))?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let filename = path.file_name()?.to_str()?;

            if filename.starts_with('.') || filename.starts_with('_') {
                return None;
            }

            if path.extension()?.to_str()? == "parquet" {
                Some(path)
            } else {
                None
            }
        })
        .collect_vec();

    let mut batches = vec![];
    let mut inferred_schema = None;

    for path in parquet_paths {
        let file = File::open(&path)
            .map_err(|e| format!("Failed to open parquet file {}: {e}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("Failed to create parquet reader: {e}"))?;

        if inferred_schema.is_none() {
            inferred_schema = Some(builder.schema().clone());
        }

        let reader = builder
            .build()
            .map_err(|e| format!("Failed to build parquet reader: {e}"))?;

        for batch in reader {
            let batch = batch.map_err(|e| format!("Failed to read batch: {e}"))?;
            batches.push(batch);
        }
    }

    let schema = inferred_schema
        .ok_or_else(|| format!("No parquet files found in {}", expected_data_dir.display()))?;
    let all_data =
        concat_batches(&schema, &batches).map_err(|e| format!("Failed to concat batches: {e}"))?;
    Ok(all_data)
}

/// Validate read results against expected outcome.
pub fn validate_read_result(
    result: DeltaResult<ReadResult>,
    expected_dir: &Path,
    expected: &ReadExpected,
) -> Result<(), String> {
    match (result, expected) {
        (Ok(read_result), ReadExpected::Success { expected: exp }) => {
            // TODO: Check file_count and files_skipped against scan metrics once available.
            // Note: These would be informational only, not authoritative, since different
            // data skipping implementations may produce different results.
            let _ = (exp.file_count, exp.files_skipped);

            // Validate data content
            let schema = ArrowSchema::try_from_kernel(read_result.schema.as_ref())
                .map_err(|e| e.to_string())?;
            let schema = std::sync::Arc::new(schema);
            let expected_data = read_expected_data(expected_dir)?;
            assert_aligned_data_matches(read_result.batches, &schema, expected_data)
                .map_err(|e| e.to_string())?;

            // Validate row count against spec's expected row counts
            if read_result.row_count != exp.row_count {
                return Err(format!(
                    "Row count mismatch: expected {}, got {}",
                    exp.row_count, read_result.row_count
                ));
            }

            Ok(())
        }
        (Err(kernel_err), ReadExpected::Error { error }) => {
            validate_expected_error(&kernel_err, error)
        }
        (Ok(_), ReadExpected::Error { error }) => Err(format!(
            "Expected error '{}' but succeeded",
            error.error_code
        )),
        (Err(e), ReadExpected::Success { .. }) => {
            Err(format!("Expected success but got error: {}", e))
        }
    }
}

/// Validate snapshot result against expected outcome.
pub fn validate_snapshot(
    result: DeltaResult<SnapshotResult>,
    time_travel: Option<&TimeTravel>,
    expected: &SnapshotExpected,
) -> Result<(), String> {
    match (result, expected) {
        (Ok(snapshot_result), SnapshotExpected::Success { expected }) => {
            if let Some(TimeTravel::Version { version }) = time_travel {
                let expected_version = u64::try_from(*version)
                    .map_err(|_| "Only non-negative snapshot versions are supported")?;
                if snapshot_result.version != expected_version {
                    return Err(format!(
                        "Snapshot version mismatch: expected {expected_version}, got {}",
                        snapshot_result.version
                    ));
                }
            }
            if !protocols_equal(&snapshot_result.protocol, &expected.protocol)? {
                return Err(format!(
                    "Expected protocol to match:\n{:?}\n{:?}",
                    snapshot_result.protocol, expected.protocol
                ));
            }
            if snapshot_result.metadata != *expected.metadata {
                return Err(format!(
                    "Expected metadata to match:\n{:?}\n{:?}",
                    snapshot_result.metadata, expected.metadata
                ));
            }
            Ok(())
        }
        (Err(kernel_err), SnapshotExpected::Error { error }) => {
            validate_expected_error(&kernel_err, error)
        }
        (Ok(_), SnapshotExpected::Error { error }) => Err(format!(
            "Expected error '{}' but succeeded",
            error.error_code
        )),
        (Err(e), SnapshotExpected::Success { .. }) => {
            Err(format!("Expected success but got error: {}", e))
        }
    }
}

#[cfg(test)]
mod tests {
    use delta_kernel::arrow::array::{ArrayRef, Int32Array, TimestampNanosecondArray};
    use delta_kernel::arrow::datatypes::{Field, Schema, TimeUnit};
    use delta_kernel_workloads::models::{ExpectedError, SnapshotExpected};

    use super::*;

    fn expected_error(code: &str) -> ExpectedError {
        ExpectedError {
            error_code: code.to_string(),
            error_message: None,
        }
    }

    fn snapshot_expected() -> SnapshotExpected {
        serde_json::from_value(serde_json::json!({
            "expected": {
                "protocol": { "minReaderVersion": 1, "minWriterVersion": 2 },
                "metadata": {
                    "id": "id",
                    "format": { "provider": "parquet", "options": {} },
                    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1
                }
            }
        }))
        .unwrap()
    }

    #[test]
    fn expected_error_rejects_unrelated_kernel_error() {
        let expected = expected_error("DELTA_STATE_RECOVER_ERROR");
        assert!(validate_expected_error(&Error::MissingMetadata, &expected).is_ok());
        assert!(validate_expected_error(
            &Error::InvalidCheckpoint(
                "Had a _last_checkpoint hint but didn't find any checkpoints".to_string()
            ),
            &expected
        )
        .is_ok());

        let error = validate_expected_error(&Error::FileNotFound("x".into()), &expected)
            .expect_err("wrong error category must fail");
        assert!(error.contains("Expected error category 'DELTA_STATE_RECOVER_ERROR'"));
    }

    #[test]
    fn protocol_error_categories_do_not_overlap() {
        let invalid_version = Error::unsupported("Unsupported minimum reader version 4");
        assert!(expected_error_matches(
            &expected_error("DELTA_INVALID_PROTOCOL_VERSION"),
            &invalid_version
        ));
        assert!(!expected_error_matches(
            &expected_error("DELTA_UNSUPPORTED_FEATURES_FOR_READ"),
            &invalid_version
        ));

        let unsupported_feature = Error::unsupported("Feature 'future' is not supported");
        assert!(expected_error_matches(
            &expected_error("DELTA_UNSUPPORTED_FEATURES_FOR_READ"),
            &unsupported_feature
        ));
        assert!(!expected_error_matches(
            &expected_error("DELTA_INVALID_PROTOCOL_VERSION"),
            &unsupported_feature
        ));

        let feature_mismatch = Error::invalid_protocol(
            "Reader features must contain only features also listed in writer features",
        );
        assert!(expected_error_matches(
            &expected_error("DELTA_FEATURES_PROTOCOL_METADATA_MISMATCH"),
            &feature_mismatch
        ));
        assert!(!expected_error_matches(
            &expected_error("DELTA_INVALID_PROTOCOL_VERSION"),
            &feature_mismatch
        ));

        let missing_writer_features = Error::invalid_protocol(
            "Writer features must be present when minimum writer version = 7",
        );
        assert!(expected_error_matches(
            &expected_error("DELTA_UNSUPPORTED_READER_VERSION"),
            &missing_writer_features
        ));
    }

    #[test]
    fn timestamp_error_rejects_unrelated_generic_error() {
        let expected = expected_error("DELTA_TIMESTAMP_GREATER_THAN_COMMIT");
        assert!(expected_error_matches(
            &expected,
            &Error::generic("Timestamp-based time travel is not yet supported")
        ));
        assert!(!expected_error_matches(
            &expected,
            &Error::generic("unrelated failure")
        ));
    }

    #[test]
    fn versions_not_contiguous_accepts_missing_version() {
        assert!(expected_error_matches(
            &expected_error("DELTA_VERSIONS_NOT_CONTIGUOUS"),
            &Error::MissingVersion(2)
        ));
    }

    #[test]
    fn column_already_exists_accepts_duplicate_schema_field() {
        let malformed_json = <serde_json::Error as serde::de::Error>::custom(
            "Schema error: Duplicate field name (case-insensitive): 'id'",
        );
        assert!(expected_error_matches(
            &expected_error("COLUMN_ALREADY_EXISTS"),
            &Error::MalformedJson(malformed_json)
        ));
    }

    #[test]
    fn unresolved_column_accepts_unknown_predicate_identifier() {
        assert!(expected_error_matches(
            &expected_error("UNRESOLVED_COLUMN"),
            &Error::generic("Cannot determine types for: Identifier(nonExistentCol) and Value(1)")
        ));
    }

    #[test]
    fn version_errors_match_kernel_version_failures() {
        assert!(expected_error_matches(
            &expected_error("DELTA_VERSION_NOT_FOUND"),
            &Error::MissingVersion(2)
        ));
        assert!(expected_error_matches(
            &expected_error("DELTA_VERSION_NOT_FOUND"),
            &Error::EmptyLog
        ));
        assert!(expected_error_matches(
            &expected_error("DELTA_TABLE_RESTORE_VERSION_INVALID"),
            &Error::generic("Only non-negative snapshot versions are supported")
        ));
    }

    #[test]
    fn unsupported_time_travel_errors_match_harness_limitation() {
        for code in [
            "DELTA_MISSING_COMMIT_INFO",
            "DELTA_MISSING_COMMIT_TIMESTAMP",
            "INVALID_TIMESTAMP",
        ] {
            assert!(expected_error_matches(
                &expected_error(code),
                &Error::generic("Timestamp-based time travel is not yet supported")
            ));
        }
    }

    #[test]
    fn protocol_errors_accept_metadata_decode_failure() {
        let message =
            "whilst decoding field 'metaData': Encountered unmasked nulls in non-nullable \
                       StructArray child: Field { \"schemaString\": Utf8 }";
        for code in [
            "DELTA_INVALID_PROTOCOL_VERSION",
            "DELTA_UNSUPPORTED_FEATURES_FOR_READ",
            "DELTA_FEATURES_REQUIRE_WRITE_SUPPORT",
        ] {
            assert!(expected_error_matches(
                &expected_error(code),
                &Error::Arrow(delta_kernel::arrow::error::ArrowError::JsonError(
                    message.to_string()
                ))
            ));
        }
    }

    #[test]
    fn snapshot_validation_checks_requested_version() {
        let expected = snapshot_expected();
        let SnapshotExpected::Success { expected: state } = &expected else {
            unreachable!()
        };
        let result = SnapshotResult {
            version: 4,
            protocol: state.protocol.as_ref().clone(),
            metadata: state.metadata.as_ref().clone(),
        };
        let time_travel = TimeTravel::Version { version: 3 };

        let error = validate_snapshot(Ok(result), Some(&time_travel), &expected)
            .expect_err("wrong snapshot version must fail");
        assert_eq!(error, "Snapshot version mismatch: expected 3, got 4");
    }

    #[test]
    fn protocol_feature_order_is_semantically_irrelevant() {
        let first = serde_json::from_value(serde_json::json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping", "deletionVectors"],
            "writerFeatures": ["columnMapping", "deletionVectors"]
        }))
        .unwrap();
        let second = serde_json::from_value(serde_json::json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["deletionVectors", "columnMapping"],
            "writerFeatures": ["deletionVectors", "columnMapping"]
        }))
        .unwrap();

        assert!(protocols_equal(&first, &second).unwrap());
    }

    #[test]
    fn timestamp_normalization_accepts_microsecond_precision() {
        let array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![Some(1_234_000)]));
        let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

        let aligned = align_array(&array, &target).unwrap();
        assert_eq!(aligned.data_type(), &target);
    }

    #[test]
    fn timestamp_normalization_rejects_sub_microsecond_precision() {
        let array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![Some(1_234_567)]));
        let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

        let error = align_array(&array, &target).unwrap_err();
        assert!(error.to_string().contains("sub-microsecond precision"));
    }

    #[test]
    fn normalization_rejects_arbitrary_type_coercion() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let error = align_array(&array, &DataType::Int64).unwrap_err();
        assert!(error.to_string().contains("does not match result type"));
    }

    #[test]
    fn ordinary_struct_fields_cannot_be_reordered() {
        let ordinary_source = Fields::from(vec![
            Field::new("b", DataType::Binary, true),
            Field::new("a", DataType::Binary, true),
        ]);
        let ordinary_target = Fields::from(vec![
            Field::new("a", DataType::Binary, true),
            Field::new("b", DataType::Binary, true),
        ]);
        assert!(require_matching_field_order(&ordinary_source, &ordinary_target).is_err());
    }

    #[test]
    fn field_nullability_mismatch_is_rejected() {
        let source = Fields::from(vec![Field::new("a", DataType::Int32, true)]);
        let target = Fields::from(vec![Field::new("a", DataType::Int32, false)]);

        let error = require_matching_field_order(&source, &target).unwrap_err();
        assert!(error.to_string().contains("nullability for field 'a'"));
    }

    #[test]
    fn list_element_nullability_mismatch_is_rejected() {
        let source = Field::new("element", DataType::Int32, true);
        let target = Field::new("element", DataType::Int32, false);

        let error = require_same_nullability("list element", &source, &target).unwrap_err();
        assert!(error.to_string().contains("list element nullability"));
    }

    #[test]
    fn map_ordering_and_entry_nullability_mismatches_are_rejected() {
        let nullable = Field::new("entries", DataType::Int32, true);
        let required = Field::new("entries", DataType::Int32, false);

        let ordering_error =
            require_map_compatibility(false, true, &nullable, &nullable).unwrap_err();
        assert!(ordering_error.to_string().contains("map ordering"));

        let nullability_error =
            require_map_compatibility(false, false, &nullable, &required).unwrap_err();
        assert!(nullability_error
            .to_string()
            .contains("map entry nullability"));
    }

    #[test]
    fn missing_void_field_is_synthesized_but_non_void_is_rejected() {
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let void_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Null, true)]));
        assert_eq!(
            align_batch_to_schema(batch.clone(), void_schema)
                .unwrap()
                .num_columns(),
            1
        );

        let value_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        assert!(align_batch_to_schema(batch, value_schema).is_err());
    }

    #[test]
    fn unexpected_void_field_is_rejected_at_top_level_and_in_struct() {
        let void_field = Arc::new(Field::new("v", DataType::Null, true));
        let void_array = new_null_array(&DataType::Null, 1);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![void_field.as_ref().clone()])),
            vec![void_array.clone()],
        )
        .unwrap();
        assert!(align_batch_to_schema(batch, Arc::new(Schema::empty())).is_err());

        let source_struct = Arc::new(
            StructArray::try_new(Fields::from(vec![void_field]), vec![void_array], None).unwrap(),
        ) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                source_struct.data_type().clone(),
                true,
            )])),
            vec![source_struct],
        )
        .unwrap();
        let target = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(Fields::empty()),
            true,
        )]));
        assert!(align_batch_to_schema(batch, target).is_err());
    }

    #[test]
    fn existing_void_field_cannot_be_reordered_at_top_level_or_in_struct() {
        let void_field = Arc::new(Field::new("v", DataType::Null, true));
        let value_field = Arc::new(Field::new("a", DataType::Int32, true));
        let void_array = new_null_array(&DataType::Null, 1);
        let value_array = Arc::new(Int32Array::from(vec![1])) as ArrayRef;

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                void_field.as_ref().clone(),
                value_field.as_ref().clone(),
            ])),
            vec![void_array.clone(), value_array.clone()],
        )
        .unwrap();
        let target = Arc::new(Schema::new(vec![
            value_field.as_ref().clone(),
            void_field.as_ref().clone(),
        ]));
        assert!(align_batch_to_schema(batch, target).is_err());

        let source_struct = Arc::new(
            StructArray::try_new(
                Fields::from(vec![void_field.clone(), value_field.clone()]),
                vec![void_array, value_array],
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                source_struct.data_type().clone(),
                true,
            )])),
            vec![source_struct],
        )
        .unwrap();
        let target = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![value_field, void_field])),
            true,
        )]));
        assert!(align_batch_to_schema(batch, target).is_err());
    }
}
