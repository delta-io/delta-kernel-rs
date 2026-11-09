//! Registry of known `delta.*` table properties: canonical names, editability on
//! existing-table updates, and value validators for property-set APIs.

use super::deserialize::{parse_bool, parse_column_names, parse_interval, parse_positive_int};
use super::{
    APPEND_ONLY, AUTO_COMPACT, CHECKPOINT_INTERVAL, CHECKPOINT_POLICY,
    CHECKPOINT_WRITE_STATS_AS_JSON, CHECKPOINT_WRITE_STATS_AS_STRUCT, COLUMN_MAPPING_MAX_COLUMN_ID,
    COLUMN_MAPPING_MODE, DATA_SKIPPING_NUM_INDEXED_COLS, DATA_SKIPPING_STATS_COLUMNS,
    DELETED_FILE_RETENTION_DURATION, DELTA_PROPERTY_PREFIX, DataSkippingNumIndexedCols,
    ENABLE_CHANGE_DATA_FEED, ENABLE_DELETION_VECTORS, ENABLE_EXPIRED_LOG_CLEANUP,
    ENABLE_ICEBERG_COMPAT_V1, ENABLE_ICEBERG_COMPAT_V2, ENABLE_ICEBERG_COMPAT_V3,
    ENABLE_ICEBERG_NATIVE_V4, ENABLE_IN_COMMIT_TIMESTAMPS, ENABLE_ROW_TRACKING,
    ENABLE_TYPE_WIDENING, IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
    IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, ISOLATION_LEVEL, LOG_RETENTION_DURATION,
    MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME, MATERIALIZED_ROW_ID_COLUMN_NAME, OPTIMIZE_WRITE,
    PARQUET_COMPRESSION_CODEC, PARQUET_FORMAT_VERSION, ParquetCompressionCodec,
    RANDOM_PREFIX_LENGTH, RANDOMIZE_FILE_PREFIXES, ROW_TRACKING_SUSPENDED,
    SET_TRANSACTION_RETENTION_DURATION, TARGET_FILE_SIZE, TUNE_FILE_SIZES_FOR_REWRITES,
    strip_check_constraint_prefix,
};
use crate::{KernelError, KernelResult};

/// Definition of one recognized `delta.*` table property.
#[derive(Clone, Copy)]
struct TablePropertyDef {
    key: &'static str,
    /// Returns `true` when `value` is syntactically valid. `None` means the property is
    /// recognized but not editable, so values are never validated.
    validate: Option<fn(&str) -> bool>,
}

const fn editable(key: &'static str, validate: fn(&str) -> bool) -> TablePropertyDef {
    TablePropertyDef {
        key,
        validate: Some(validate),
    }
}

const fn recognized(key: &'static str) -> TablePropertyDef {
    TablePropertyDef {
        key,
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
];

/// TODO: change these to editable once protocol-preparation code is added
const FEATURE_ENABLEMENT_PROPERTIES: &[TablePropertyDef] = &[
    recognized(APPEND_ONLY),
    recognized(COLUMN_MAPPING_MODE),
    recognized(ENABLE_CHANGE_DATA_FEED),
    recognized(ENABLE_DELETION_VECTORS),
    recognized(ENABLE_TYPE_WIDENING),
    recognized(ENABLE_ICEBERG_COMPAT_V1),
    recognized(ENABLE_ICEBERG_COMPAT_V2),
    recognized(ENABLE_ICEBERG_COMPAT_V3),
    recognized(ENABLE_ICEBERG_NATIVE_V4),
    recognized(CHECKPOINT_POLICY),
    recognized(ENABLE_ROW_TRACKING),
    recognized(ENABLE_IN_COMMIT_TIMESTAMPS),
];

/// TODO: ICT version/timestamp remain non-editable here until ICT protocol-preparation is added
const KERNEL_MANAGED_PROPERTIES: &[TablePropertyDef] = &[
    recognized(COLUMN_MAPPING_MAX_COLUMN_ID),
    recognized(MATERIALIZED_ROW_ID_COLUMN_NAME),
    recognized(MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME),
    recognized(IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION),
    recognized(IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP),
    recognized(ROW_TRACKING_SUSPENDED),
];

const OTHER_RECOGNIZED_PROPERTIES: &[TablePropertyDef] = &[
    recognized(SET_TRANSACTION_RETENTION_DURATION),
    recognized(PARQUET_FORMAT_VERSION),
    recognized(AUTO_COMPACT),
    recognized(OPTIMIZE_WRITE),
    recognized(ISOLATION_LEVEL),
    recognized(RANDOMIZE_FILE_PREFIXES),
    recognized(RANDOM_PREFIX_LENGTH),
    recognized(TARGET_FILE_SIZE),
    recognized(TUNE_FILE_SIZES_FOR_REWRITES),
];

/// All known `delta.*` properties, grouped by editability / ownership.
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

/// Validates and normalizes a single property for an existing-table update.
///
/// Non-`delta.*` application properties pass through with their original key casing. Recognized
/// editable `delta.*` keys are rewritten to their canonical form and value-validated. Unknown
/// `delta.*` keys, non-editable keys, CHECK constraints, and malformed values are rejected.
pub(crate) fn validate_and_normalize_property_for_update(
    key: impl Into<String>,
    value: impl Into<String>,
) -> KernelResult<(String, String)> {
    let key = key.into();
    let value = value.into();

    if !is_delta_property_key(&key) {
        return Ok((key, value));
    }

    if strip_check_constraint_prefix(&key.to_ascii_lowercase()).is_some() {
        return Err(KernelError::invalid_transaction_state(format!(
            "Cannot modify the table property '{key}'"
        )));
    }

    let Some(def) = find_property_def(&key) else {
        return Err(KernelError::invalid_transaction_state(format!(
            "Unknown configuration key '{key}'"
        )));
    };

    let Some(validate) = def.validate else {
        return Err(KernelError::invalid_transaction_state(format!(
            "Cannot modify the table property '{}'",
            def.key
        )));
    };

    if !validate(&value) {
        return Err(KernelError::invalid_transaction_state(format!(
            "Invalid value '{value}' for table property '{}'",
            def.key
        )));
    }

    Ok((def.key.to_string(), value))
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
    fn accepts_editable_and_user_properties(
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
    #[case::unknown("delta.unknownProperty", "true", "Unknown configuration key")]
    #[case::not_editable("delta.appendOnly", "true", "Cannot modify the table property")]
    #[case::feature_enablement(
        "delta.enableChangeDataFeed",
        "true",
        "Cannot modify the table property"
    )]
    #[case::set_transaction_retention(
        "delta.setTransactionRetentionDuration",
        "interval 1 days",
        "Cannot modify the table property"
    )]
    #[case::parquet_format_version(
        "delta.parquet.format.version",
        "2.12.0",
        "Cannot modify the table property"
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
    fn rejects_unsupported_or_invalid(
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

    /// Editable UPDATE keys must stay aligned with Java Kernel `TableConfig` entries that are in
    /// `VALID_PROPERTIES`, marked `editable=true`, and do not require protocol preparation.
    #[test]
    fn editable_keys_match_java_config_only_valid_properties() {
        assert!(
            EDITABLE_CONFIG_PROPERTIES
                .iter()
                .all(|def| def.validate.is_some()),
            "EDITABLE_CONFIG_PROPERTIES must only contain editable entries"
        );
        assert!(
            FEATURE_ENABLEMENT_PROPERTIES
                .iter()
                .chain(KERNEL_MANAGED_PROPERTIES.iter())
                .chain(OTHER_RECOGNIZED_PROPERTIES.iter())
                .all(|def| !def.validate.is_some()),
            "non-editable groups must not contain editable entries"
        );

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
            ]
        );
    }

    /// Every recognized non-editable key must resolve through `find_property_def` and reject with
    /// "Cannot modify" (not "Unknown"), including case-insensitive spellings.
    #[test]
    fn rejects_every_non_editable_recognized_key() {
        let non_editable = FEATURE_ENABLEMENT_PROPERTIES
            .iter()
            .chain(KERNEL_MANAGED_PROPERTIES.iter())
            .chain(OTHER_RECOGNIZED_PROPERTIES.iter());

        for def in non_editable {
            assert!(
                !def.validate.is_some(),
                "expected non-editable registry entry for {}",
                def.key
            );

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
}
