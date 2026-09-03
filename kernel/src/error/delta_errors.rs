use url::Url;

use super::{DeltaError, DeltaErrorCondition, DeltaErrorParameter, Error};
use crate::table_features::{
    MAX_VALID_READER_VERSION, MAX_VALID_WRITER_VERSION, MIN_VALID_RW_VERSION,
    TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::Version;

const DELTA_VERSION: &str = env!("CARGO_PKG_VERSION");
const TABLE_FEATURES_DOC_LINK: &str =
    "https://github.com/delta-io/delta/blob/master/PROTOCOL.md#table-features";

/// Creates `DELTA_TABLE_NOT_FOUND` for the requested table root.
pub(crate) fn table_not_found(table_root: impl ToString) -> Error {
    new_delta_error(
        DeltaErrorCondition::DeltaTableNotFound,
        vec![DeltaErrorParameter::new(
            "tableName",
            redacted_location(table_root),
        )],
    )
}

/// Creates `DELTA_VERSION_NOT_FOUND` with the requested and available version bounds.
pub(crate) fn version_not_found(
    user_version: Version,
    earliest: Version,
    latest: Version,
) -> Error {
    new_delta_error(
        DeltaErrorCondition::DeltaVersionNotFound,
        vec![
            DeltaErrorParameter::new("userVersion", user_version),
            DeltaErrorParameter::new("earliest", earliest),
            DeltaErrorParameter::new("latest", latest),
        ],
    )
}

/// Creates `DELTA_VERSIONS_NOT_CONTIGUOUS` for two adjacent observed gap endpoints.
///
/// An absent `version_to_load` is rendered with the catalog's `-1` sentinel.
pub(crate) fn versions_not_contiguous(
    start_version: Version,
    end_version: Version,
    version_to_load: Option<Version>,
) -> Error {
    new_delta_error(
        DeltaErrorCondition::DeltaVersionsNotContiguous,
        vec![
            DeltaErrorParameter::new("versionList", format!("{start_version}, {end_version}")),
            DeltaErrorParameter::new("startVersion", start_version),
            DeltaErrorParameter::new("endVersion", end_version),
            DeltaErrorParameter::new(
                "versionToLoad",
                version_to_load.map_or_else(|| "-1".to_string(), |version| version.to_string()),
            ),
        ],
    )
}

/// Creates `DELTA_LOG_FILE_NOT_FOUND` for a snapshot or checkpoint lookup.
///
/// Absent versions are rendered with the catalog's `LATEST` and `-1` sentinels.
pub(crate) fn log_file_not_found(
    version: Option<Version>,
    checkpoint_version: Option<Version>,
    log_path: &Url,
) -> Error {
    new_delta_error(
        DeltaErrorCondition::DeltaLogFileNotFound,
        vec![
            DeltaErrorParameter::new(
                "version",
                version.map_or_else(|| "LATEST".to_string(), |version| version.to_string()),
            ),
            DeltaErrorParameter::new(
                "checkpointVersion",
                checkpoint_version.map_or_else(|| "-1".to_string(), |version| version.to_string()),
            ),
            DeltaErrorParameter::new("logPath", redacted_url(log_path)),
        ],
    )
}

/// Creates `DELTA_STATE_RECOVER_ERROR` for a state action missing during log replay.
pub(crate) fn state_recover_error(operation: &'static str, version: Version) -> Error {
    new_delta_error(
        DeltaErrorCondition::DeltaStateRecoverError,
        vec![
            DeltaErrorParameter::new("operation", operation),
            DeltaErrorParameter::new("version", version),
        ],
    )
}

/// Creates `DELTA_INVALID_PROTOCOL_VERSION` for protocol requirements kernel cannot satisfy.
pub(crate) fn invalid_protocol_version(
    table_name_or_path: &Url,
    reader_required: i32,
    writer_required: i32,
) -> Error {
    new_delta_error(
        DeltaErrorCondition::DeltaInvalidProtocolVersion,
        vec![
            DeltaErrorParameter::new("tableNameOrPath", redacted_url(table_name_or_path)),
            DeltaErrorParameter::new("readerRequired", reader_required),
            DeltaErrorParameter::new("writerRequired", writer_required),
            DeltaErrorParameter::new("deltaVersion", DELTA_VERSION),
            DeltaErrorParameter::new(
                "supportedReaders",
                version_range(MIN_VALID_RW_VERSION, MAX_VALID_READER_VERSION),
            ),
            DeltaErrorParameter::new(
                "supportedWriters",
                version_range(MIN_VALID_RW_VERSION, MAX_VALID_WRITER_VERSION),
            ),
        ],
    )
}

/// Creates `DELTA_UNSUPPORTED_FEATURES_FOR_READ` for the sorted unsupported feature set.
pub(crate) fn unsupported_features_for_read(
    table_name_or_path: &Url,
    features: impl IntoIterator<Item = impl ToString>,
) -> Error {
    unsupported_features(
        DeltaErrorCondition::DeltaUnsupportedFeaturesForRead,
        table_name_or_path,
        features,
    )
}

/// Creates `DELTA_UNSUPPORTED_FEATURES_FOR_WRITE` for the sorted unsupported feature set.
pub(crate) fn unsupported_features_for_write(
    table_name_or_path: &Url,
    features: impl IntoIterator<Item = impl ToString>,
) -> Error {
    unsupported_features(
        DeltaErrorCondition::DeltaUnsupportedFeaturesForWrite,
        table_name_or_path,
        features,
    )
}

/// Creates `DELTA_FEATURES_PROTOCOL_METADATA_MISMATCH` for schema features absent from protocol.
pub(crate) fn features_protocol_metadata_mismatch(
    features: impl IntoIterator<Item = impl ToString>,
) -> Error {
    new_delta_error(
        DeltaErrorCondition::DeltaFeaturesProtocolMetadataMismatch,
        vec![DeltaErrorParameter::new("features", feature_list(features))],
    )
}

/// Creates `DELTA_READ_FEATURE_PROTOCOL_REQUIRES_WRITE`.
pub(crate) fn read_feature_protocol_requires_write() -> Error {
    new_delta_error(
        DeltaErrorCondition::DeltaReadFeatureProtocolRequiresWrite,
        vec![
            DeltaErrorParameter::new("writerVersion", TABLE_FEATURES_MIN_WRITER_VERSION),
            DeltaErrorParameter::new("docLink", TABLE_FEATURES_DOC_LINK),
        ],
    )
}

/// Creates `DELTA_ADAPTIVE_METADATA_REQUIRES_COLUMN_MAPPING_ID_MODE` for the supplied mode.
pub(crate) fn adaptive_metadata_requires_column_mapping_id_mode(mode: impl ToString) -> Error {
    new_delta_error(
        DeltaErrorCondition::DeltaAdaptiveMetadataRequiresColumnMappingIdMode,
        vec![
            DeltaErrorParameter::new("feature", "adaptiveMetadata-preview"),
            DeltaErrorParameter::new("prop", "delta.columnMapping.mode"),
            DeltaErrorParameter::new("mode", mode),
        ],
    )
}

/// Creates `DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED` for the table path.
pub(crate) fn path_based_access_to_catalog_managed_table_blocked(path: &Url) -> Error {
    new_delta_error(
        DeltaErrorCondition::DeltaPathBasedAccessToCatalogManagedTableBlocked,
        vec![DeltaErrorParameter::new("path", redacted_url(path))],
    )
}

fn unsupported_features(
    condition: DeltaErrorCondition,
    table_name_or_path: &Url,
    features: impl IntoIterator<Item = impl ToString>,
) -> Error {
    new_delta_error(
        condition,
        vec![
            DeltaErrorParameter::new("tableNameOrPath", redacted_url(table_name_or_path)),
            DeltaErrorParameter::new("deltaVersion", DELTA_VERSION),
            DeltaErrorParameter::new("unsupported", feature_list(features)),
        ],
    )
}

fn feature_list(features: impl IntoIterator<Item = impl ToString>) -> String {
    let mut features = features
        .into_iter()
        .map(|feature| feature.to_string())
        .collect::<Vec<_>>();
    features.sort_unstable();
    features.dedup();
    features.join(", ")
}

fn version_range(minimum: i32, maximum: i32) -> String {
    (minimum..=maximum)
        .map(|version| version.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn redacted_location(location: impl ToString) -> String {
    let location = location.to_string();
    match Url::parse(&location) {
        Ok(url) if url.scheme().len() > 1 => redacted_url(&url),
        _ => location,
    }
}

fn redacted_url(url: &Url) -> String {
    let mut redacted = url.clone();
    let _ = redacted.set_password(None);
    let _ = redacted.set_username("");
    redacted.set_query(None);
    redacted.set_fragment(None);
    redacted.to_string()
}

fn new_delta_error(condition: DeltaErrorCondition, parameters: Vec<DeltaErrorParameter>) -> Error {
    match DeltaError::new(condition, parameters) {
        Ok(error) => Error::Delta(error),
        Err(error) => Error::Kernel(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_delta_error(
        error: Error,
        expected_condition: DeltaErrorCondition,
        expected_sql_state: &str,
        expected_parameters: &[(&str, &str)],
        expected_message: &str,
    ) {
        let Error::Delta(error) = error else {
            panic!("expected Delta error");
        };
        assert_eq!(error.condition(), expected_condition);
        assert_eq!(error.sql_state(), Some(expected_sql_state));
        assert_eq!(
            error
                .parameters()
                .iter()
                .map(|parameter| (parameter.name(), parameter.value()))
                .collect::<Vec<_>>(),
            expected_parameters
        );
        assert_eq!(error.to_string(), expected_message);
    }

    #[test]
    fn snapshot_discovery_factories_render_catalog_errors() {
        let log_root = Url::parse("memory:///table/_delta_log/").unwrap();

        assert_delta_error(
            table_not_found("memory:///table/"),
            DeltaErrorCondition::DeltaTableNotFound,
            "42P01",
            &[("tableName", "memory:///table/")],
            "Delta table memory:///table/ doesn't exist.",
        );
        assert_delta_error(
            version_not_found(9, 1, 7),
            DeltaErrorCondition::DeltaVersionNotFound,
            "22003",
            &[("userVersion", "9"), ("earliest", "1"), ("latest", "7")],
            "Cannot time travel Delta table to version 9. Available versions: [1, 7].",
        );
        assert_delta_error(
            versions_not_contiguous(1, 3, Some(3)),
            DeltaErrorCondition::DeltaVersionsNotContiguous,
            "KD00C",
            &[
                ("versionList", "1, 3"),
                ("startVersion", "1"),
                ("endVersion", "3"),
                ("versionToLoad", "3"),
            ],
            "Versions (1, 3) are not contiguous. \nA gap in the delta log between versions 1 and \
             3 was detected while trying to load version 3.",
        );
        assert_delta_error(
            log_file_not_found(None, None, &log_root),
            DeltaErrorCondition::DeltaLogFileNotFound,
            "42K03",
            &[
                ("version", "LATEST"),
                ("checkpointVersion", "-1"),
                ("logPath", "memory:///table/_delta_log/"),
            ],
            "Unable to retrieve the delta log files to construct table version LATEST starting \
             from checkpoint version -1 at memory:///table/_delta_log/.",
        );
        assert_delta_error(
            state_recover_error("metadata", 7),
            DeltaErrorCondition::DeltaStateRecoverError,
            "XXKDS",
            &[("operation", "metadata"), ("version", "7")],
            "The metadata of your Delta table could not be recovered while Reconstructing\nversion: \
             7. Did you manually delete files in the _delta_log directory?",
        );
    }

    #[test]
    fn protocol_and_feature_factories_render_catalog_errors() {
        let table_root = Url::parse("memory:///table/").unwrap();

        assert_delta_error(
            invalid_protocol_version(&table_root, 4, 8),
            DeltaErrorCondition::DeltaInvalidProtocolVersion,
            "KD004",
            &[
                ("tableNameOrPath", "memory:///table/"),
                ("readerRequired", "4"),
                ("writerRequired", "8"),
                ("deltaVersion", env!("CARGO_PKG_VERSION")),
                ("supportedReaders", "1, 2, 3"),
                ("supportedWriters", "1, 2, 3, 4, 5, 6, 7"),
            ],
            &format!(
                "Unsupported Delta protocol version: table \"memory:///table/\" requires reader \
                 version 4 and writer version 8, but Delta Lake \"{}\" supports reader versions \
                 1, 2, 3 and writer versions 1, 2, 3, 4, 5, 6, 7. Please upgrade to a newer \
                 release.",
                env!("CARGO_PKG_VERSION")
            ),
        );
        assert_delta_error(
            unsupported_features_for_read(
                &table_root,
                ["variantType", "columnMapping", "variantType"],
            ),
            DeltaErrorCondition::DeltaUnsupportedFeaturesForRead,
            "56038",
            &[
                ("tableNameOrPath", "memory:///table/"),
                ("deltaVersion", env!("CARGO_PKG_VERSION")),
                ("unsupported", "columnMapping, variantType"),
            ],
            &format!(
                "Unsupported Delta read feature: table \"memory:///table/\" requires reader table \
                 feature(s) that are unsupported by Delta Lake \"{}\": columnMapping, \
                 variantType.",
                env!("CARGO_PKG_VERSION")
            ),
        );
        assert_delta_error(
            unsupported_features_for_write(
                &table_root,
                ["identityColumns", "generatedColumns", "identityColumns"],
            ),
            DeltaErrorCondition::DeltaUnsupportedFeaturesForWrite,
            "56038",
            &[
                ("tableNameOrPath", "memory:///table/"),
                ("deltaVersion", env!("CARGO_PKG_VERSION")),
                ("unsupported", "generatedColumns, identityColumns"),
            ],
            &format!(
                "Unsupported Delta write feature: table \"memory:///table/\" requires writer table \
                 feature(s) that are unsupported by Delta Lake \"{}\": generatedColumns, \
                 identityColumns.",
                env!("CARGO_PKG_VERSION")
            ),
        );
        assert_delta_error(
            features_protocol_metadata_mismatch(["timestampNtz", "geospatial", "timestampNtz"]),
            DeltaErrorCondition::DeltaFeaturesProtocolMetadataMismatch,
            "KD004",
            &[("features", "geospatial, timestampNtz")],
            "Unable to operate on this table because the following table features are enabled in \
             metadata but not listed in protocol: geospatial, timestampNtz.",
        );
        assert_delta_error(
            read_feature_protocol_requires_write(),
            DeltaErrorCondition::DeltaReadFeatureProtocolRequiresWrite,
            "KD004",
            &[("writerVersion", "7"), ("docLink", TABLE_FEATURES_DOC_LINK)],
            &format!(
                "Unable to upgrade only the reader protocol version to use table features. Writer \
                 protocol version must be at least 7 to proceed. Refer to \
                 {TABLE_FEATURES_DOC_LINK} for more information on table protocol versions."
            ),
        );
        assert_delta_error(
            adaptive_metadata_requires_column_mapping_id_mode("name"),
            DeltaErrorCondition::DeltaAdaptiveMetadataRequiresColumnMappingIdMode,
            "42000",
            &[
                ("feature", "adaptiveMetadata-preview"),
                ("prop", "delta.columnMapping.mode"),
                ("mode", "name"),
            ],
            "The table feature 'adaptiveMetadata-preview' requires column mapping mode 'id', but \
             the table property 'delta.columnMapping.mode' is set to 'name'. Recreate the table \
             with 'delta.columnMapping.mode' = 'id', or omit the property to have it set \
             automatically.",
        );
        assert_delta_error(
            path_based_access_to_catalog_managed_table_blocked(&table_root),
            DeltaErrorCondition::DeltaPathBasedAccessToCatalogManagedTableBlocked,
            "KD00G",
            &[("path", "memory:///table/")],
            "Path-based access is not allowed for Catalog-Managed table: memory:///table/. Please \
             access the table via its name and retry.",
        );
    }

    #[test]
    fn location_parameters_omit_url_credentials() {
        let table_root =
            Url::parse("https://alice:secret@example.com/table/?sig=token#fragment").unwrap();
        let log_root = table_root.join("_delta_log/?sig=other-token").unwrap();

        for error in [
            table_not_found(table_root.as_str()),
            invalid_protocol_version(&table_root, 4, 8),
            unsupported_features_for_read(&table_root, ["futureFeature"]),
            unsupported_features_for_write(&table_root, ["futureFeature"]),
            path_based_access_to_catalog_managed_table_blocked(&table_root),
            log_file_not_found(None, None, &log_root),
        ] {
            let Error::Delta(error) = error else {
                panic!("expected Delta error");
            };
            let rendered_parameters = error
                .parameters()
                .iter()
                .map(DeltaErrorParameter::value)
                .collect::<Vec<_>>()
                .join(" ");
            assert!(!rendered_parameters.contains("alice"));
            assert!(!rendered_parameters.contains("secret"));
            assert!(!rendered_parameters.contains("token"));
            assert!(rendered_parameters.contains("https://example.com/table"));
        }
    }
}
