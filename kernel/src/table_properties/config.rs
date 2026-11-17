//! Shared registry of known `delta.*` table properties for CREATE and UPDATE.
//!
//! Canonical names, create/update access, and value validators live here so both builders share
//! case-insensitive key matching and value checks. Feature signals (`delta.feature.*`) are handled
//! by create-table separately and are rejected on update.

use std::collections::HashMap;

use super::deserialize::{parse_bool, parse_column_names, parse_interval, parse_positive_int};
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

/// Spark / protocol defaults used when a retention key is absent from the merged configuration.
pub(crate) const DEFAULT_LOG_RETENTION_DURATION: &str = "interval 30 days";
pub(crate) const DEFAULT_DELETED_FILE_RETENTION_DURATION: &str = "interval 1 week";

/// Where a recognized property may be set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PropertyAccess {
    /// Accepted on CREATE and UPDATE with value validation.
    Editable,
    /// Accepted on CREATE with value validation; rejected on UPDATE until protocol prep exists.
    CreateOnly,
    /// Known but never settable by connectors through property APIs.
    Recognized,
}

/// Definition of one recognized `delta.*` table property.
#[derive(Clone, Copy)]
struct TablePropertyDef {
    key: &'static str,
    access: PropertyAccess,
    /// Returns `true` when `value` is syntactically valid. Unused for
    /// [`PropertyAccess::Recognized`].
    validate: Option<fn(&str) -> bool>,
}

const fn editable(key: &'static str, validate: fn(&str) -> bool) -> TablePropertyDef {
    TablePropertyDef {
        key,
        access: PropertyAccess::Editable,
        validate: Some(validate),
    }
}

const fn create_only(key: &'static str, validate: fn(&str) -> bool) -> TablePropertyDef {
    TablePropertyDef {
        key,
        access: PropertyAccess::CreateOnly,
        validate: Some(validate),
    }
}

const fn recognized(key: &'static str) -> TablePropertyDef {
    TablePropertyDef {
        key,
        access: PropertyAccess::Recognized,
        validate: None,
    }
}

fn validate_bool(value: &str) -> bool {
    parse_bool(value).is_some()
}

fn validate_positive_int(value: &str) -> bool {
    parse_positive_int(value).is_some()
}

fn validate_interval(value: &str) -> bool {
    parse_interval(value).is_some()
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

fn find_property_def(key: &str) -> Option<&'static TablePropertyDef> {
    KNOWN_DELTA_PROPERTY_GROUPS
        .iter()
        .flat_map(|group| group.iter())
        .find(|def| key.eq_ignore_ascii_case(def.key))
}

pub(crate) fn is_delta_property_key(key: &str) -> bool {
    key.get(..DELTA_PROPERTY_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(DELTA_PROPERTY_PREFIX))
}

fn is_table_feature_signal_key(key: &str) -> bool {
    key.get(..SET_TABLE_FEATURE_SUPPORTED_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(SET_TABLE_FEATURE_SUPPORTED_PREFIX))
}

#[derive(Clone, Copy)]
enum PropertySetOperation {
    Create,
    Update,
}

fn validate_and_normalize_property(
    key: impl Into<String>,
    value: impl Into<String>,
    operation: PropertySetOperation,
) -> KernelResult<(String, String)> {
    let key = key.into();
    let value = value.into();

    if !is_delta_property_key(&key) {
        return Ok((key, value));
    }

    if strip_check_constraint_prefix(&key.to_ascii_lowercase()).is_some() {
        return Err(match operation {
            PropertySetOperation::Create => KernelError::generic(format!(
                "Setting CHECK constraint property '{key}' is not supported during CREATE TABLE"
            )),
            PropertySetOperation::Update => KernelError::invalid_transaction_state(format!(
                "Cannot modify the table property '{key}'"
            )),
        });
    }

    if is_table_feature_signal_key(&key) {
        return Err(match operation {
            PropertySetOperation::Create => KernelError::generic(format!(
                "Feature signal '{key}' must be handled before property normalization"
            )),
            PropertySetOperation::Update => KernelError::invalid_transaction_state(format!(
                "Protocol changes via '{key}' are not supported on existing tables"
            )),
        });
    }

    let Some(def) = find_property_def(&key) else {
        return Err(match operation {
            PropertySetOperation::Create => KernelError::generic(format!(
                "Setting delta property '{key}' is not supported during CREATE TABLE"
            )),
            PropertySetOperation::Update => {
                KernelError::invalid_transaction_state(format!("Unknown configuration key '{key}'"))
            }
        });
    };

    let allowed = matches!(
        (operation, def.access),
        (
            PropertySetOperation::Create,
            PropertyAccess::Editable | PropertyAccess::CreateOnly
        ) | (PropertySetOperation::Update, PropertyAccess::Editable)
    );
    if !allowed {
        return Err(match operation {
            PropertySetOperation::Create => KernelError::generic(format!(
                "Setting delta property '{}' is not supported during CREATE TABLE",
                def.key
            )),
            PropertySetOperation::Update => KernelError::invalid_transaction_state(format!(
                "Cannot modify the table property '{}'",
                def.key
            )),
        });
    }

    let Some(validate) = def.validate else {
        // Settable entries always carry a validator; Recognized ones are rejected above.
        return Err(KernelError::internal_error(format!(
            "table property '{}' is marked settable without a validator",
            def.key
        )));
    };

    if !validate(&value) {
        return Err(match operation {
            PropertySetOperation::Create => {
                KernelError::generic(format!("Invalid value '{value}' for '{}'", def.key))
            }
            PropertySetOperation::Update => KernelError::invalid_transaction_state(format!(
                "Invalid value '{value}' for table property '{}'",
                def.key
            )),
        });
    }

    Ok((def.key.to_string(), value))
}

/// Validates and normalizes a single property for CREATE TABLE.
///
/// Non-`delta.*` application properties pass through with their original key casing. Allowed
/// `delta.*` keys (config and feature-enablement) are rewritten to their canonical form and
/// value-validated. Unknown `delta.*` keys, kernel-managed keys, CHECK constraints, and malformed
/// values are rejected. Feature signals must be partitioned out before calling this.
pub(crate) fn validate_and_normalize_property_for_create(
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
pub(crate) fn validate_and_normalize_property_for_update(
    key: impl Into<String>,
    value: impl Into<String>,
) -> KernelResult<(String, String)> {
    validate_and_normalize_property(key, value, PropertySetOperation::Update)
}

/// Rejects a merged configuration where log retention is shorter than deleted-file retention.
///
/// Absent keys use the protocol defaults (`interval 30 days` and `interval 1 week`). Matches
/// Spark's `validateTombstoneAndLogRetentionDurationCompatibility`.
pub(crate) fn validate_log_and_deleted_file_retention(
    configuration: &HashMap<String, String>,
) -> KernelResult<()> {
    let log_raw = configuration
        .get(LOG_RETENTION_DURATION)
        .map(String::as_str)
        .unwrap_or(DEFAULT_LOG_RETENTION_DURATION);
    let deleted_raw = configuration
        .get(DELETED_FILE_RETENTION_DURATION)
        .map(String::as_str)
        .unwrap_or(DEFAULT_DELETED_FILE_RETENTION_DURATION);

    let log = parse_interval(log_raw).ok_or_else(|| {
        KernelError::invalid_transaction_state(format!(
            "Invalid value '{log_raw}' for table property '{LOG_RETENTION_DURATION}'"
        ))
    })?;
    let deleted = parse_interval(deleted_raw).ok_or_else(|| {
        KernelError::invalid_transaction_state(format!(
            "Invalid value '{deleted_raw}' for table property '{DELETED_FILE_RETENTION_DURATION}'"
        ))
    })?;

    if log < deleted {
        let log_present = configuration.contains_key(LOG_RETENTION_DURATION);
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
        return Err(KernelError::invalid_transaction_state(message));
    }
    Ok(())
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
            assert_eq!(def.access, PropertyAccess::Editable);
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
            .filter(|def| def.access != PropertyAccess::Editable);

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
}
