//! Shared registry of known `delta.*` table properties for CREATE and UPDATE.
//!
//! Canonical names, create/update access, and value validators live here so both builders share
//! case-insensitive key matching and value checks. Feature signals (`delta.feature.*`) are handled
//! by create-table separately and are rejected on update.

use std::collections::HashMap;

use super::deserialize::{
    parse_bool, parse_column_names, parse_interval_strict, parse_positive_int,
};
use super::{
    APPEND_ONLY, AUTO_COMPACT, CHECKPOINT_INTERVAL, CHECKPOINT_POLICY,
    CHECKPOINT_WRITE_STATS_AS_JSON, CHECKPOINT_WRITE_STATS_AS_STRUCT, COLUMN_MAPPING_MAX_COLUMN_ID,
    COLUMN_MAPPING_MODE, CheckpointPolicy, DATA_SKIPPING_NUM_INDEXED_COLS,
    DATA_SKIPPING_STATS_COLUMNS, DELETED_FILE_RETENTION_DURATION, DELTA_PROPERTY_PREFIX,
    DataSkippingNumIndexedCols, ENABLE_CHANGE_DATA_FEED, ENABLE_DELETION_VECTORS,
    ENABLE_EXPIRED_LOG_CLEANUP, ENABLE_ICEBERG_COMPAT_V1, ENABLE_ICEBERG_COMPAT_V2,
    ENABLE_ICEBERG_COMPAT_V3, ENABLE_ICEBERG_NATIVE_V4, ENABLE_IN_COMMIT_TIMESTAMPS,
    ENABLE_ROW_TRACKING, ENABLE_TYPE_WIDENING, IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
    IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, ISOLATION_LEVEL, LOG_RETENTION_DURATION,
    MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME, MATERIALIZED_ROW_ID_COLUMN_NAME, OPTIMIZE_WRITE,
    PARQUET_COMPRESSION_CODEC, PARQUET_FORMAT_VERSION, ParquetCompressionCodec,
    RANDOM_PREFIX_LENGTH, RANDOMIZE_FILE_PREFIXES, ROW_TRACKING_SUSPENDED,
    SET_TRANSACTION_RETENTION_DURATION, TARGET_FILE_SIZE, TUNE_FILE_SIZES_FOR_REWRITES,
    strip_check_constraint_prefix,
};
use crate::table_features::{ColumnMappingMode, SET_TABLE_FEATURE_SUPPORTED_PREFIX};
use crate::{KernelError, KernelResult};

/// Spark / protocol defaults used when a retention key is absent from the table properties.
pub(crate) const DEFAULT_LOG_RETENTION_DURATION: &str = "interval 30 days";
pub(crate) const DEFAULT_DELETED_FILE_RETENTION_DURATION: &str = "interval 1 week";

/// Where a recognized property may be set.
#[derive(Clone, Copy)]
enum PropertyAccess {
    /// Accepted on CREATE and UPDATE with value validation.
    Editable(fn(&str) -> bool),
    /// Accepted on CREATE with value validation; rejected on UPDATE until protocol prep exists.
    CreateOnly(fn(&str) -> bool),
    /// Known to kernel but not settable through property APIs. Either kernel owns the value (for
    /// example `delta.columnMapping.maxColumnId`, which kernel advances itself when columns are
    /// added) or kernel does not yet support the behavior the key controls (for example
    /// `delta.autoOptimize.autoCompact`).
    Recognized,
}

/// Definition of one recognized `delta.*` table property.
#[derive(Clone, Copy)]
struct TablePropertyDef {
    key: &'static str,
    access: PropertyAccess,
}

const fn editable(key: &'static str, validate: fn(&str) -> bool) -> TablePropertyDef {
    TablePropertyDef {
        key,
        access: PropertyAccess::Editable(validate),
    }
}

const fn create_only(key: &'static str, validate: fn(&str) -> bool) -> TablePropertyDef {
    TablePropertyDef {
        key,
        access: PropertyAccess::CreateOnly(validate),
    }
}

const fn recognized(key: &'static str) -> TablePropertyDef {
    TablePropertyDef {
        key,
        access: PropertyAccess::Recognized,
    }
}

const EDITABLE_CONFIG_PROPERTIES: &[TablePropertyDef] = &[
    editable(CHECKPOINT_WRITE_STATS_AS_JSON, validate_bool),
    editable(CHECKPOINT_WRITE_STATS_AS_STRUCT, validate_bool),
    editable(
        DATA_SKIPPING_NUM_INDEXED_COLS,
        validate_data_skipping_num_indexed_cols,
    ),
    editable(DATA_SKIPPING_STATS_COLUMNS, validate_column_names),
    editable(LOG_RETENTION_DURATION, validate_interval),
    editable(DELETED_FILE_RETENTION_DURATION, validate_interval),
    editable(ENABLE_EXPIRED_LOG_CLEANUP, validate_bool),
    editable(CHECKPOINT_INTERVAL, validate_positive_int),
    editable(PARQUET_COMPRESSION_CODEC, validate_compression_codec),
    editable(SET_TRANSACTION_RETENTION_DURATION, validate_interval),
    editable(PARQUET_FORMAT_VERSION, validate_parquet_format_version),
];

/// Feature-enablement properties accepted on CREATE. UPDATE rejects them until protocol
/// preparation can upgrade the table.
const FEATURE_ENABLEMENT_PROPERTIES: &[TablePropertyDef] = &[
    create_only(APPEND_ONLY, validate_bool),
    create_only(COLUMN_MAPPING_MODE, validate_column_mapping_mode),
    create_only(ENABLE_CHANGE_DATA_FEED, validate_bool),
    create_only(ENABLE_DELETION_VECTORS, validate_bool),
    create_only(ENABLE_TYPE_WIDENING, validate_bool),
    create_only(ENABLE_ICEBERG_COMPAT_V2, validate_bool),
    create_only(ENABLE_ICEBERG_COMPAT_V3, validate_bool),
    #[cfg(feature = "adaptive-metadata-in-dev")]
    create_only(ENABLE_ICEBERG_NATIVE_V4, validate_bool),
    create_only(CHECKPOINT_POLICY, validate_checkpoint_policy),
    create_only(ENABLE_ROW_TRACKING, validate_bool),
    create_only(ENABLE_IN_COMMIT_TIMESTAMPS, validate_bool),
    // Known enablement keys that CREATE does not accept today.
    recognized(ENABLE_ICEBERG_COMPAT_V1),
    #[cfg(not(feature = "adaptive-metadata-in-dev"))]
    recognized(ENABLE_ICEBERG_NATIVE_V4),
];

/// Kernel-managed keys. Writers must not set these directly.
const KERNEL_MANAGED_PROPERTIES: &[TablePropertyDef] = &[
    recognized(COLUMN_MAPPING_MAX_COLUMN_ID),
    recognized(MATERIALIZED_ROW_ID_COLUMN_NAME),
    recognized(MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME),
    recognized(IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION),
    recognized(IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP),
    recognized(ROW_TRACKING_SUSPENDED),
];

const OTHER_RECOGNIZED_PROPERTIES: &[TablePropertyDef] = &[
    recognized(AUTO_COMPACT),
    recognized(OPTIMIZE_WRITE),
    recognized(ISOLATION_LEVEL),
    recognized(RANDOMIZE_FILE_PREFIXES),
    recognized(RANDOM_PREFIX_LENGTH),
    recognized(TARGET_FILE_SIZE),
    recognized(TUNE_FILE_SIZES_FOR_REWRITES),
];

/// All known `delta.*` properties, grouped by ownership.
const KNOWN_DELTA_PROPERTY_GROUPS: &[&[TablePropertyDef]] = &[
    EDITABLE_CONFIG_PROPERTIES,
    FEATURE_ENABLEMENT_PROPERTIES,
    KERNEL_MANAGED_PROPERTIES,
    OTHER_RECOGNIZED_PROPERTIES,
];

pub(crate) fn is_delta_property_key(key: &str) -> bool {
    key.get(..DELTA_PROPERTY_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(DELTA_PROPERTY_PREFIX))
}

/// Returns `true` when `key` is a `delta.feature.*` signal (case-insensitive prefix).
pub(crate) fn is_table_feature_signal_key(key: &str) -> bool {
    key.get(..SET_TABLE_FEATURE_SUPPORTED_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(SET_TABLE_FEATURE_SUPPORTED_PREFIX))
}

/// Strips a case-insensitive `delta.feature.` prefix, returning the feature name.
pub(crate) fn strip_table_feature_signal_prefix(key: &str) -> Option<&str> {
    is_table_feature_signal_key(key).then(|| &key[SET_TABLE_FEATURE_SUPPORTED_PREFIX.len()..])
}

/// Canonicalizes the keys of one batch of CREATE properties (one `with_table_properties` call)
/// without validating values.
///
/// Rejects keys CREATE may not set. Two entries for the same `delta.*` key (including exact
/// duplicates and case variants) are rejected. Duplicate non-`delta.*` keys keep the last value.
/// Callers merge batches in call order so a later batch overrides an earlier one for the same
/// canonical key, then check the merged result with [`validate_property_values_for_create`].
pub(crate) fn canonicalize_property_batch_for_create(
    batch: impl IntoIterator<Item = (String, String)>,
) -> KernelResult<HashMap<String, String>> {
    canonicalize_property_batch(batch, PropertySetOperation::Create)
}

/// UPDATE variant of [`canonicalize_property_batch_for_create`] for one
/// `with_table_properties_added` call.
pub(crate) fn canonicalize_property_batch_for_update(
    batch: impl IntoIterator<Item = (String, String)>,
) -> KernelResult<HashMap<String, String>> {
    canonicalize_property_batch(batch, PropertySetOperation::Update)
}

/// Validates the values of merged, canonicalized CREATE properties.
pub(crate) fn validate_property_values_for_create(
    properties: &HashMap<String, String>,
) -> KernelResult<()> {
    validate_property_values(properties, PropertySetOperation::Create)
}

/// Validates the values of merged, canonicalized UPDATE properties.
pub(crate) fn validate_property_values_for_update(
    properties: &HashMap<String, String>,
) -> KernelResult<()> {
    validate_property_values(properties, PropertySetOperation::Update)
}

/// Rejects `table_properties` where log retention is shorter than deleted-file retention.
///
/// Absent keys use the protocol defaults (`interval 30 days` and `interval 1 week`). Matches
/// Spark's `validateTombstoneAndLogRetentionDurationCompatibility`. Present values must be valid
/// intervals, including values that were already stored on the table. This is the CREATE check;
/// UPDATE goes through [`validate_retention_for_property_adds`].
pub(crate) fn validate_log_and_deleted_file_retention(
    table_properties: &HashMap<String, String>,
) -> KernelResult<()> {
    check_log_and_deleted_file_retention(table_properties, PropertySetOperation::Create)
}

/// UPDATE variant of [`validate_log_and_deleted_file_retention`] that checks the retention pair
/// resulting from applying `validated_adds` on top of `existing`.
///
/// Skips the check unless `validated_adds` sets a retention key, so updates to unrelated
/// properties do not fail on retention values the caller never touched (Spark behaves the same).
pub(crate) fn validate_retention_for_property_adds(
    existing: &HashMap<String, String>,
    validated_adds: &HashMap<String, String>,
) -> KernelResult<()> {
    let sets_log = validated_adds.contains_key(LOG_RETENTION_DURATION);
    let sets_deleted = validated_adds.contains_key(DELETED_FILE_RETENTION_DURATION);
    if !sets_log && !sets_deleted {
        return Ok(());
    }

    let mut resulting = existing.clone();
    resulting.extend(
        validated_adds
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    check_log_and_deleted_file_retention(&resulting, PropertySetOperation::Update)
}

// === Helpers ===

#[derive(Clone, Copy)]
enum PropertySetOperation {
    Create,
    Update,
}

impl PropertySetOperation {
    /// Builds the error kind that matches the operation: generic for CREATE, invalid transaction
    /// state for an update of an existing table.
    fn error(self, message: String) -> KernelError {
        match self {
            Self::Create => KernelError::generic(message),
            Self::Update => KernelError::invalid_transaction_state(message),
        }
    }

    /// Error for a recognized `delta.*` key that this operation may not set.
    fn cannot_set(self, key: &str) -> KernelError {
        self.error(match self {
            Self::Create => {
                format!("Setting delta property '{key}' is not supported during CREATE TABLE")
            }
            Self::Update => format!("Cannot modify the table property '{key}'"),
        })
    }
}

fn find_property_def(key: &str) -> Option<&'static TablePropertyDef> {
    KNOWN_DELTA_PROPERTY_GROUPS
        .iter()
        .flat_map(|group| group.iter())
        .find(|def| key.eq_ignore_ascii_case(def.key))
}

/// Resolves `key` to its canonical spelling, rejecting `delta.*` keys that `operation` may not set.
fn canonicalize_property_key(key: String, operation: PropertySetOperation) -> KernelResult<String> {
    if !is_delta_property_key(&key) {
        return Ok(key);
    }

    if strip_check_constraint_prefix(&key.to_ascii_lowercase()).is_some() {
        return Err(match operation {
            PropertySetOperation::Create => operation.error(format!(
                "Setting CHECK constraint property '{key}' is not supported during CREATE TABLE"
            )),
            PropertySetOperation::Update => operation.cannot_set(&key),
        });
    }

    if is_table_feature_signal_key(&key) {
        return Err(operation.error(match operation {
            PropertySetOperation::Create => {
                format!("Feature signal '{key}' must be handled before property normalization")
            }
            PropertySetOperation::Update => {
                format!("Protocol changes via '{key}' are not supported on existing tables")
            }
        }));
    }

    let Some(def) = find_property_def(&key) else {
        return Err(match operation {
            PropertySetOperation::Create => operation.cannot_set(&key),
            PropertySetOperation::Update => {
                operation.error(format!("Unknown configuration key '{key}'"))
            }
        });
    };

    value_validator(def, operation)
        .map(|_| def.key.to_string())
        .ok_or_else(|| operation.cannot_set(def.key))
}

/// Returns the value validator for `def` when `operation` may set it.
fn value_validator(
    def: &TablePropertyDef,
    operation: PropertySetOperation,
) -> Option<fn(&str) -> bool> {
    match (operation, def.access) {
        (
            PropertySetOperation::Create,
            PropertyAccess::Editable(validate) | PropertyAccess::CreateOnly(validate),
        )
        | (PropertySetOperation::Update, PropertyAccess::Editable(validate)) => Some(validate),
        _ => None,
    }
}

/// Validates the value of an already canonical `key`. Non-`delta.*` keys accept any value.
fn validate_property_value(
    key: &str,
    value: &str,
    operation: PropertySetOperation,
) -> KernelResult<()> {
    if !is_delta_property_key(key) {
        return Ok(());
    }
    let validate = find_property_def(key)
        .and_then(|def| value_validator(def, operation))
        .ok_or_else(|| operation.cannot_set(key))?;
    if validate(value) {
        return Ok(());
    }
    Err(operation.error(match operation {
        PropertySetOperation::Create => format!("Invalid value '{value}' for '{key}'"),
        PropertySetOperation::Update => {
            format!("Invalid value '{value}' for table property '{key}'")
        }
    }))
}

fn validate_property_values(
    properties: &HashMap<String, String>,
    operation: PropertySetOperation,
) -> KernelResult<()> {
    properties
        .iter()
        .try_for_each(|(key, value)| validate_property_value(key, value, operation))
}

#[cfg(test)]
fn validate_and_normalize_property(
    key: impl Into<String>,
    value: impl Into<String>,
    operation: PropertySetOperation,
) -> KernelResult<(String, String)> {
    let key = canonicalize_property_key(key.into(), operation)?;
    let value = value.into();
    validate_property_value(&key, &value, operation)?;
    Ok((key, value))
}

/// Validates and normalizes a single property for CREATE TABLE.
///
/// Non-`delta.*` application properties pass through with their original key casing. Allowed
/// `delta.*` keys (config and feature-enablement) are rewritten to their canonical form and
/// value-validated. Unknown `delta.*` keys, kernel-managed keys, CHECK constraints, and malformed
/// values are rejected. Feature signals must be partitioned out before calling this.
#[cfg(test)]
fn validate_and_normalize_property_for_create(
    key: impl Into<String>,
    value: impl Into<String>,
) -> KernelResult<(String, String)> {
    validate_and_normalize_property(key, value, PropertySetOperation::Create)
}

/// Validates and normalizes a single property for an existing-table update.
///
/// Non-`delta.*` application properties pass through with their original key casing. Recognized
/// editable `delta.*` keys are rewritten to their canonical form and value-validated. Unknown
/// `delta.*` keys, non-editable keys, CHECK constraints, feature signals, and malformed values are
/// rejected.
#[cfg(test)]
fn validate_and_normalize_property_for_update(
    key: impl Into<String>,
    value: impl Into<String>,
) -> KernelResult<(String, String)> {
    validate_and_normalize_property(key, value, PropertySetOperation::Update)
}

fn canonicalize_property_batch(
    batch: impl IntoIterator<Item = (String, String)>,
    operation: PropertySetOperation,
) -> KernelResult<HashMap<String, String>> {
    let mut normalized = HashMap::new();
    for (key, value) in batch {
        let canonical = canonicalize_property_key(key, operation)?;
        if normalized.insert(canonical.clone(), value).is_some()
            && is_delta_property_key(&canonical)
        {
            return Err(operation.error(format!(
                "Conflicting spellings of table property '{canonical}' in the same request"
            )));
        }
    }
    Ok(normalized)
}

/// Checks that log retention is at least deleted-file retention, resolving absent keys to the
/// protocol defaults. Errors are built for `operation`.
fn check_log_and_deleted_file_retention(
    table_properties: &HashMap<String, String>,
    operation: PropertySetOperation,
) -> KernelResult<()> {
    let (log_raw, log) = retention_or_default(
        table_properties,
        LOG_RETENTION_DURATION,
        DEFAULT_LOG_RETENTION_DURATION,
        operation,
    )?;
    let (deleted_raw, deleted) = retention_or_default(
        table_properties,
        DELETED_FILE_RETENTION_DURATION,
        DEFAULT_DELETED_FILE_RETENTION_DURATION,
        operation,
    )?;

    if log < deleted {
        let log_present = table_properties.contains_key(LOG_RETENTION_DURATION);
        let message = if log_present {
            format!(
                "The table property {LOG_RETENTION_DURATION}({log_raw}) needs to be greater than \
                 or equal to {DELETED_FILE_RETENTION_DURATION}({deleted_raw})."
            )
        } else {
            format!(
                "The table property {DELETED_FILE_RETENTION_DURATION}({deleted_raw}) needs to be \
                 less than or equal to {LOG_RETENTION_DURATION}({log_raw})."
            )
        };
        return Err(operation.error(message));
    }
    Ok(())
}

fn retention_or_default(
    table_properties: &HashMap<String, String>,
    key: &str,
    default: &str,
    operation: PropertySetOperation,
) -> KernelResult<(String, std::time::Duration)> {
    let Some(raw) = table_properties.get(key) else {
        let duration = parse_interval_strict(default).ok_or_else(|| {
            KernelError::internal_error(format!("invalid built-in retention default '{default}'"))
        })?;
        return Ok((default.to_string(), duration));
    };
    let duration = parse_interval_strict(raw).ok_or_else(|| {
        operation.error(format!("Invalid value '{raw}' for table property '{key}'"))
    })?;
    Ok((raw.clone(), duration))
}

fn validate_bool(value: &str) -> bool {
    parse_bool(value).is_some()
}

fn validate_positive_int(value: &str) -> bool {
    parse_positive_int(value).is_some()
}

fn validate_interval(value: &str) -> bool {
    parse_interval_strict(value).is_some()
}

fn validate_column_names(value: &str) -> bool {
    parse_column_names(value).is_some()
}

fn validate_data_skipping_num_indexed_cols(value: &str) -> bool {
    DataSkippingNumIndexedCols::try_from(value).is_ok()
}

fn validate_compression_codec(value: &str) -> bool {
    value.parse::<ParquetCompressionCodec>().is_ok()
}

/// Accepts protocol values `"1.0.0"` and `"2.MINOR.PATCH"` with numeric MINOR/PATCH.
fn validate_parquet_format_version(value: &str) -> bool {
    if value == "1.0.0" {
        return true;
    }
    let mut parts = value.split('.');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("2"), Some(minor), Some(patch), None) => {
            !minor.is_empty()
                && !patch.is_empty()
                && minor.chars().all(|c| c.is_ascii_digit())
                && patch.chars().all(|c| c.is_ascii_digit())
        }
        _ => false,
    }
}

fn validate_column_mapping_mode(value: &str) -> bool {
    value.parse::<ColumnMappingMode>().is_ok()
}

fn validate_checkpoint_policy(value: &str) -> bool {
    value.parse::<CheckpointPolicy>().is_ok()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::user_prop("app.owner", "kernel", "app.owner")]
    #[case::canonical("delta.checkpointInterval", "10", "delta.checkpointInterval")]
    #[case::case_insensitive("DELTA.checkpointInterval", "10", "delta.checkpointInterval")]
    #[case::compression_codec(
        "delta.parquet.compression.codec",
        "zstd",
        "delta.parquet.compression.codec"
    )]
    #[case::stats_columns(
        "delta.dataSkippingStatsColumns",
        "a,b.c",
        "delta.dataSkippingStatsColumns"
    )]
    #[case::num_indexed_cols_all(
        "delta.dataSkippingNumIndexedCols",
        "-1",
        "delta.dataSkippingNumIndexedCols"
    )]
    #[case::set_transaction_retention(
        "delta.setTransactionRetentionDuration",
        "interval 1 days",
        "delta.setTransactionRetentionDuration"
    )]
    #[case::parquet_format_version_v1(
        "delta.parquet.format.version",
        "1.0.0",
        "delta.parquet.format.version"
    )]
    #[case::parquet_format_version_v2(
        "DELTA.parquet.format.version",
        "2.12.0",
        "delta.parquet.format.version"
    )]
    fn update_accepts_editable_and_user_properties(
        #[case] key: &str,
        #[case] value: &str,
        #[case] expected_key: &str,
    ) {
        let (canonical, out_value) =
            validate_and_normalize_property_for_update(key, value).unwrap();
        assert_eq!(canonical, expected_key);
        assert_eq!(out_value, value);
    }

    #[rstest]
    #[case::user_prop("app.owner", "kernel", "app.owner")]
    #[case::case_insensitive("DELTA.checkpointInterval", "10", "delta.checkpointInterval")]
    #[case::feature_enablement(
        "DELTA.enableDeletionVectors",
        "true",
        "delta.enableDeletionVectors"
    )]
    #[case::column_mapping("delta.columnMapping.mode", "id", "delta.columnMapping.mode")]
    #[case::checkpoint_policy("delta.checkpointPolicy", "v2", "delta.checkpointPolicy")]
    fn create_accepts_allowed_and_user_properties(
        #[case] key: &str,
        #[case] value: &str,
        #[case] expected_key: &str,
    ) {
        let (canonical, out_value) =
            validate_and_normalize_property_for_create(key, value).unwrap();
        assert_eq!(canonical, expected_key);
        assert_eq!(out_value, value);
    }

    #[rstest]
    #[case::unknown("delta.unknownProperty", "true", "Unknown configuration key")]
    #[case::not_editable("delta.appendOnly", "true", "Cannot modify the table property")]
    #[case::feature_enablement(
        "delta.enableChangeDataFeed",
        "true",
        "Cannot modify the table property"
    )]
    #[case::feature_signal(
        "delta.feature.deletionVectors",
        "supported",
        "Protocol changes via 'delta.feature.deletionVectors' are not supported on existing tables"
    )]
    #[case::feature_signal_case_insensitive(
        "DELTA.feature.rowTracking",
        "supported",
        "Protocol changes via 'DELTA.feature.rowTracking' are not supported on existing tables"
    )]
    #[case::constraint(
        "delta.constraints.positive_id",
        "id > 0",
        "Cannot modify the table property"
    )]
    #[case::invalid_bool(
        "delta.enableExpiredLogCleanup",
        "yes",
        "Invalid value 'yes' for table property"
    )]
    #[case::invalid_interval(
        "delta.checkpointInterval",
        "not-a-number",
        "Invalid value 'not-a-number' for table property"
    )]
    #[case::compound_interval(
        "delta.deletedFileRetentionDuration",
        "interval 7 days 24 hours",
        "Invalid value 'interval 7 days 24 hours' for table property"
    )]
    #[case::trailing_interval_garbage(
        "delta.logRetentionDuration",
        "interval 7 days foo",
        "Invalid value 'interval 7 days foo' for table property"
    )]
    #[case::invalid_set_transaction_retention(
        "delta.setTransactionRetentionDuration",
        "not-an-interval",
        "Invalid value 'not-an-interval' for table property"
    )]
    #[case::invalid_parquet_format_version(
        "delta.parquet.format.version",
        "3.0.0",
        "Invalid value '3.0.0' for table property"
    )]
    #[case::invalid_compression_codec(
        "delta.parquet.compression.codec",
        "brotli",
        "Invalid value 'brotli' for table property"
    )]
    #[case::invalid_num_indexed_cols(
        "delta.dataSkippingNumIndexedCols",
        "many",
        "Invalid value 'many' for table property"
    )]
    fn update_rejects_unsupported_or_invalid(
        #[case] key: &str,
        #[case] value: &str,
        #[case] expected: &str,
    ) {
        let error = validate_and_normalize_property_for_update(key, value).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "error={error}, expected substring={expected}"
        );
    }

    #[rstest]
    #[case::unknown(
        "delta.unknownProperty",
        "true",
        "Setting delta property 'delta.unknownProperty' is not supported during CREATE TABLE"
    )]
    #[case::kernel_managed(
        "delta.columnMapping.maxColumnId",
        "20",
        "Setting delta property 'delta.columnMapping.maxColumnId' is not supported during CREATE TABLE"
    )]
    #[case::invalid_bool("delta.enableDeletionVectors", "yes", "Invalid value 'yes' for")]
    #[case::invalid_interval(
        "delta.checkpointInterval",
        "abc",
        "Invalid value 'abc' for 'delta.checkpointInterval'"
    )]
    #[case::compound_interval(
        "delta.deletedFileRetentionDuration",
        "interval 7 days 24 hours",
        "Invalid value 'interval 7 days 24 hours' for 'delta.deletedFileRetentionDuration'"
    )]
    #[case::constraint(
        "delta.constraints.positive_id",
        "id > 0",
        "Setting CHECK constraint property"
    )]
    fn create_rejects_unsupported_or_invalid(
        #[case] key: &str,
        #[case] value: &str,
        #[case] expected: &str,
    ) {
        let error = validate_and_normalize_property_for_create(key, value).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "error={error}, expected substring={expected}"
        );
    }

    #[test]
    fn create_and_update_agree_on_config_only_keys() {
        let mut editable: Vec<&str> = EDITABLE_CONFIG_PROPERTIES
            .iter()
            .map(|def| def.key)
            .collect();
        editable.sort_unstable();
        assert_eq!(
            editable,
            [
                "delta.checkpoint.writeStatsAsJson",
                "delta.checkpoint.writeStatsAsStruct",
                "delta.checkpointInterval",
                "delta.dataSkippingNumIndexedCols",
                "delta.dataSkippingStatsColumns",
                "delta.deletedFileRetentionDuration",
                "delta.enableExpiredLogCleanup",
                "delta.logRetentionDuration",
                "delta.parquet.compression.codec",
                "delta.parquet.format.version",
                "delta.setTransactionRetentionDuration",
            ]
        );

        for def in EDITABLE_CONFIG_PROPERTIES {
            assert!(matches!(def.access, PropertyAccess::Editable(_)));
            let (canonical, _) =
                validate_and_normalize_property_for_create(def.key, sample_value(def)).unwrap();
            assert_eq!(canonical, def.key);
            let (canonical, _) =
                validate_and_normalize_property_for_update(def.key, sample_value(def)).unwrap();
            assert_eq!(canonical, def.key);
        }
    }

    fn sample_value(def: &TablePropertyDef) -> &'static str {
        match def.key {
            PARQUET_FORMAT_VERSION => "1.0.0",
            PARQUET_COMPRESSION_CODEC => "zstd",
            DATA_SKIPPING_NUM_INDEXED_COLS => "-1",
            DATA_SKIPPING_STATS_COLUMNS => "a",
            LOG_RETENTION_DURATION
            | DELETED_FILE_RETENTION_DURATION
            | SET_TRANSACTION_RETENTION_DURATION => "interval 1 days",
            CHECKPOINT_INTERVAL => "10",
            _ => "true",
        }
    }

    #[test]
    fn update_rejects_every_non_editable_recognized_key() {
        let non_editable = FEATURE_ENABLEMENT_PROPERTIES
            .iter()
            .chain(KERNEL_MANAGED_PROPERTIES.iter())
            .chain(OTHER_RECOGNIZED_PROPERTIES.iter())
            .filter(|def| !matches!(def.access, PropertyAccess::Editable(_)));

        for def in non_editable {
            for key in [def.key.to_string(), def.key.to_ascii_uppercase()] {
                let error = validate_and_normalize_property_for_update(&key, "true").unwrap_err();
                let message = error.to_string();
                assert!(
                    message.contains(&format!("Cannot modify the table property '{}'", def.key)),
                    "key={key}, error={message}"
                );
                assert!(
                    !message.contains("Unknown configuration key"),
                    "recognized key {key} must not be reported as unknown: {message}"
                );
            }
        }
    }

    #[test]
    fn retention_check_accepts_defaults_and_valid_pairs() {
        validate_log_and_deleted_file_retention(&HashMap::new()).unwrap();
        validate_log_and_deleted_file_retention(&HashMap::from([
            (
                LOG_RETENTION_DURATION.to_string(),
                "interval 30 days".to_string(),
            ),
            (
                DELETED_FILE_RETENTION_DURATION.to_string(),
                "interval 7 days".to_string(),
            ),
        ]))
        .unwrap();
    }

    #[test]
    fn retention_check_rejects_log_shorter_than_deleted_files() {
        let error = validate_log_and_deleted_file_retention(&HashMap::from([
            (
                LOG_RETENTION_DURATION.to_string(),
                "interval 1 days".to_string(),
            ),
            (
                DELETED_FILE_RETENTION_DURATION.to_string(),
                "interval 7 days".to_string(),
            ),
        ]))
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("needs to be greater than or equal to"),
            "error={error}"
        );
    }

    #[test]
    fn retention_check_rejects_deleted_longer_than_default_log() {
        let error = validate_log_and_deleted_file_retention(&HashMap::from([(
            DELETED_FILE_RETENTION_DURATION.to_string(),
            "interval 60 days".to_string(),
        )]))
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("needs to be less than or equal to"),
            "error={error}"
        );
    }

    #[test]
    fn update_retention_check_skips_when_no_retention_key_is_set() {
        let existing = HashMap::from([
            (LOG_RETENTION_DURATION.to_string(), "2 weeks".to_string()),
            (
                DELETED_FILE_RETENTION_DURATION.to_string(),
                "INTERVAL 30 DAYS".to_string(),
            ),
        ]);
        let adds = HashMap::from([("app.owner".to_string(), "kernel".to_string())]);
        validate_retention_for_property_adds(&existing, &adds).unwrap();
    }

    #[rstest]
    #[case::spark_weeks_log("2 weeks", DELETED_FILE_RETENTION_DURATION, "interval 20 days")]
    #[case::spark_upper_interval_deleted(
        "INTERVAL 60 DAYS",
        LOG_RETENTION_DURATION,
        "interval 30 days"
    )]
    #[case::bare_days_log("60 days", DELETED_FILE_RETENTION_DURATION, "interval 40 days")]
    fn update_retention_check_rejects_unparseable_existing_untouched_side(
        #[case] existing_value: &str,
        #[case] key_being_set: &str,
        #[case] new_value: &str,
    ) {
        let existing_key = if key_being_set == LOG_RETENTION_DURATION {
            DELETED_FILE_RETENTION_DURATION
        } else {
            LOG_RETENTION_DURATION
        };
        let existing = HashMap::from([(existing_key.to_string(), existing_value.to_string())]);
        let adds = HashMap::from([(key_being_set.to_string(), new_value.to_string())]);
        let error = validate_retention_for_property_adds(&existing, &adds).unwrap_err();
        assert!(
            matches!(
                &error,
                KernelError::InvalidTransactionState(message)
                if message.contains(&format!("Invalid value '{existing_value}' for table property '{existing_key}'"))
            ),
            "error={error}"
        );
    }

    #[rstest]
    #[case::compound_log(LOG_RETENTION_DURATION, "interval 7 days 24 hours")]
    #[case::compound_deleted(DELETED_FILE_RETENTION_DURATION, "interval 7 days 24 hours")]
    #[case::trailing_garbage(LOG_RETENTION_DURATION, "interval 7 days foo")]
    fn update_retention_check_rejects_compound_existing_untouched_side(
        #[case] existing_key: &str,
        #[case] existing_value: &str,
    ) {
        let key_being_set = if existing_key == LOG_RETENTION_DURATION {
            DELETED_FILE_RETENTION_DURATION
        } else {
            LOG_RETENTION_DURATION
        };
        let existing = HashMap::from([(existing_key.to_string(), existing_value.to_string())]);
        let adds = HashMap::from([(key_being_set.to_string(), "interval 7 days".to_string())]);
        let error = validate_retention_for_property_adds(&existing, &adds).unwrap_err();
        assert!(
            matches!(
                &error,
                KernelError::InvalidTransactionState(message)
                if message.contains(&format!("Invalid value '{existing_value}' for table property '{existing_key}'"))
            ),
            "error={error}"
        );
    }

    #[test]
    fn batch_canonicalization_rejects_conflicting_delta_spellings_but_keeps_last_user_value() {
        for operation in [
            canonicalize_property_batch_for_create,
            canonicalize_property_batch_for_update,
        ] {
            let error = operation([
                ("DELTA.checkpointInterval".to_string(), "10".to_string()),
                ("delta.checkpointInterval".to_string(), "20".to_string()),
            ])
            .unwrap_err();
            assert!(
                error.to_string().contains("Conflicting spellings"),
                "error={error}"
            );

            let normalized = operation([
                ("app.owner".to_string(), "a".to_string()),
                ("app.owner".to_string(), "b".to_string()),
            ])
            .unwrap();
            assert_eq!(normalized["app.owner"], "b");
        }
    }

    #[test]
    fn update_retention_check_uses_existing_log_when_only_deleted_is_set() {
        let existing = HashMap::from([(
            LOG_RETENTION_DURATION.to_string(),
            "interval 3 days".to_string(),
        )]);
        let adds = HashMap::from([(
            DELETED_FILE_RETENTION_DURATION.to_string(),
            "interval 7 days".to_string(),
        )]);
        let error = validate_retention_for_property_adds(&existing, &adds).unwrap_err();
        assert!(
            matches!(
                &error,
                KernelError::InvalidTransactionState(message)
                if message.contains("needs to be greater than or equal to")
            ),
            "error={error}"
        );
    }

    #[test]
    fn create_retention_check_uses_generic_error() {
        let error = validate_log_and_deleted_file_retention(&HashMap::from([
            (
                LOG_RETENTION_DURATION.to_string(),
                "interval 1 days".to_string(),
            ),
            (
                DELETED_FILE_RETENTION_DURATION.to_string(),
                "interval 7 days".to_string(),
            ),
        ]))
        .unwrap_err();
        assert!(
            matches!(
                &error,
                KernelError::Generic(message)
                if message.contains("needs to be greater than or equal to")
            ),
            "error={error}"
        );
    }

    #[test]
    fn batch_canonicalization_defers_value_validation_to_merged_properties() {
        let batch = [("delta.checkpointInterval".to_string(), "abc".to_string())];
        let canonical = canonicalize_property_batch_for_create(batch.clone()).unwrap();
        assert_eq!(canonical["delta.checkpointInterval"], "abc");
        validate_property_values_for_create(&canonical).unwrap_err();
        let canonical = canonicalize_property_batch_for_update(batch).unwrap();
        validate_property_values_for_update(&canonical).unwrap_err();
    }
}
