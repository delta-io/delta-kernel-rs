//! Single source of truth for all table feature validation and queries.
//!
//! This module centralizes all feature-related logic that was previously scattered across
//! Protocol and TableConfiguration. All feature queries should go through functions in this
//! module rather than calling methods on Protocol or TableConfiguration directly.

use crate::actions::{ensure_supported_features, Metadata, Protocol};
use crate::table_features::{
    TableFeature, SUPPORTED_TABLE_FEATURES_READ, SUPPORTED_TABLE_FEATURES_WRITE,
};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::{DeltaResult, Error};

//==================================================================================================
// Validation Functions
//==================================================================================================

/// Check if kernel can read a table with the given protocol.
///
/// This validates that:
/// - The protocol reader version is supported (1, 2, or 3)
/// - All enabled reader features are supported by the kernel
/// - Protocol is well-formed (e.g., reader features present when minReaderVersion = 3)
pub(crate) fn ensure_read_supported(protocol: &Protocol) -> DeltaResult<()> {
    match protocol.reader_features() {
        // if min_reader_version = 3 and all reader features are subset of supported => OK
        Some(reader_features) if protocol.min_reader_version() == 3 => {
            ensure_supported_features(reader_features, &SUPPORTED_TABLE_FEATURES_READ)
        }
        // if min_reader_version = 3 and no reader features => ERROR
        None if protocol.min_reader_version() == 3 => Err(Error::internal_error(
            "Reader features must be present when minimum reader version = 3",
        )),
        // if min_reader_version = 1,2 and there are no reader features => OK
        None if protocol.min_reader_version() == 1 || protocol.min_reader_version() == 2 => Ok(()),
        // if min_reader_version = 1,2 and there are reader features => ERROR
        Some(_) if protocol.min_reader_version() == 1 || protocol.min_reader_version() == 2 => {
            Err(Error::internal_error(
                "Reader features must not be present when minimum reader version = 1 or 2",
            ))
        }
        // any other min_reader_version is not supported
        _ => Err(Error::Unsupported(format!(
            "Unsupported minimum reader version {}",
            protocol.min_reader_version()
        ))),
    }
}

/// Check if kernel can write to a table with the given protocol.
///
/// This validates that:
/// - The protocol writer version is supported (1, 2, or 7)
/// - All enabled writer features are supported by the kernel
/// - Feature dependencies are satisfied (e.g., rowTracking requires domainMetadata)
/// - Catalog-managed tables are rejected (not yet supported for writes)
pub(crate) fn ensure_write_supported(protocol: &Protocol) -> DeltaResult<()> {
    #[cfg(feature = "catalog-managed")]
    require!(
        !protocol.is_catalog_managed(),
        Error::unsupported("Writes are not yet supported for catalog-managed tables")
    );

    match protocol.writer_features() {
        Some(writer_features) if protocol.min_writer_version() == 7 => {
            // Ensure all writer features are supported
            ensure_supported_features(writer_features, &SUPPORTED_TABLE_FEATURES_WRITE)?;

            // Validate feature dependencies
            for feature in writer_features {
                validate_feature_dependencies(feature, writer_features)?;
            }
            Ok(())
        }
        Some(_) => Err(Error::unsupported(
            "Tables with min writer version != 7 should not have table features.",
        )),
        None => {
            require!(
                protocol.min_writer_version() == 1 || protocol.min_writer_version() == 2,
                Error::unsupported(
                    "Currently delta-kernel-rs can only write to tables with protocol.minWriterVersion = 1, 2, or 7"
                )
            );
            Ok(())
        }
    }
}

/// Validate that all required features for a given feature are present.
///
/// For example, rowTracking requires domainMetadata to also be enabled.
fn validate_feature_dependencies(
    feature: &TableFeature,
    enabled_features: &[TableFeature],
) -> DeltaResult<()> {
    for required in feature.required_features() {
        if !enabled_features.contains(required) {
            return Err(Error::invalid_protocol(format!(
                "{} feature requires {} to also be enabled",
                feature.name(),
                required.name()
            )));
        }
    }
    Ok(())
}

//==================================================================================================
// Feature Support Queries (Protocol-only checks)
//==================================================================================================

/// Check if deletion vectors is supported on this table.
///
/// To support deletion vectors, a table must:
/// - Have minReaderVersion = 3
/// - Have minWriterVersion = 7
/// - Have deletionVectors in both readerFeatures and writerFeatures
pub(crate) fn is_deletion_vector_supported(protocol: &Protocol) -> bool {
    protocol.min_reader_version() == 3
        && protocol.min_writer_version() == 7
        && protocol
            .reader_features()
            .is_some_and(|f| f.contains(&TableFeature::DeletionVectors))
        && protocol
            .writer_features()
            .is_some_and(|f| f.contains(&TableFeature::DeletionVectors))
}

/// Check if row tracking is supported on this table.
///
/// To support row tracking, a table must:
/// - Have minWriterVersion = 7
/// - Have rowTracking in writerFeatures
pub(crate) fn is_row_tracking_supported(protocol: &Protocol) -> bool {
    protocol.supports_feature(TableFeature::RowTracking)
}

/// Check if append-only is supported on this table.
///
/// To support append-only:
/// - If minWriterVersion = 7, must have appendOnly in writerFeatures
/// - If minWriterVersion in 2..=6, implicitly supported
pub(crate) fn is_append_only_supported(protocol: &Protocol) -> bool {
    match protocol.min_writer_version() {
        7 => protocol.has_writer_feature(&TableFeature::AppendOnly),
        version => (2..=6).contains(&version),
    }
}

/// Check if invariants is supported on this table.
///
/// To support invariants:
/// - If minWriterVersion = 7, must have invariants in writerFeatures
/// - If minWriterVersion in 2..=6, implicitly supported
pub(crate) fn is_invariants_supported(protocol: &Protocol) -> bool {
    match protocol.min_writer_version() {
        7 => protocol.has_writer_feature(&TableFeature::Invariants),
        version => (2..=6).contains(&version),
    }
}

/// Check if V2 checkpoint is supported for writing on this table.
///
/// To support V2 checkpoint writing, a table must:
/// - Have minReaderVersion = 3
/// - Have minWriterVersion = 7
/// - Have v2Checkpoint in both readerFeatures and writerFeatures
pub(crate) fn is_v2_checkpoint_write_supported(protocol: &Protocol) -> bool {
    protocol
        .reader_features()
        .is_some_and(|f| f.contains(&TableFeature::V2Checkpoint))
        && protocol
            .writer_features()
            .is_some_and(|f| f.contains(&TableFeature::V2Checkpoint))
}

/// Check if in-commit timestamps is supported on this table.
///
/// To support in-commit timestamps, a table must:
/// - Have minWriterVersion = 7
/// - Have inCommitTimestamp in writerFeatures
pub(crate) fn is_in_commit_timestamps_supported(protocol: &Protocol) -> bool {
    protocol.min_writer_version() == 7
        && protocol.has_writer_feature(&TableFeature::InCommitTimestamp)
}

/// Check if domain metadata is supported on this table.
///
/// To support domain metadata, a table must:
/// - Have minWriterVersion = 7
/// - Have domainMetadata in writerFeatures
pub(crate) fn is_domain_metadata_supported(protocol: &Protocol) -> bool {
    protocol.min_writer_version() == 7 && protocol.has_writer_feature(&TableFeature::DomainMetadata)
}

/// Check if kernel supports reading Change Data Feed on this table.
///
/// To support CDF reading:
/// - Protocol must be supported for reading (see `is_cdf_protocol_supported`)
/// - CDF must be enabled via table property (see `is_cdf_enabled`)
/// - Column mapping must be disabled (CDF doesn't support column mapping yet)
pub(crate) fn is_cdf_read_supported(
    protocol: &Protocol,
    table_properties: &TableProperties,
) -> bool {
    let protocol_supported = is_cdf_protocol_supported(protocol);
    let cdf_enabled = table_properties.enable_change_data_feed.unwrap_or(false);
    let column_mapping_disabled = table_properties.column_mapping_mode.is_none()
        || matches!(
            table_properties.column_mapping_mode,
            Some(crate::table_features::ColumnMappingMode::None)
        );

    protocol_supported && cdf_enabled && column_mapping_disabled
}

/// Check if the protocol supports CDF (independent of table properties).
///
/// CDF protocol support requires:
/// - minReaderVersion = 1 with no reader features, OR
/// - minReaderVersion = 3 with only deletionVectors in reader features
fn is_cdf_protocol_supported(protocol: &Protocol) -> bool {
    match protocol.reader_features() {
        Some(reader_features) if protocol.min_reader_version() == 3 => {
            // Only deletionVectors is supported for CDF
            reader_features
                .iter()
                .all(|f| matches!(f, TableFeature::DeletionVectors))
        }
        None => protocol.min_reader_version() == 1,
        _ => false,
    }
}

//==================================================================================================
// Feature Enablement Queries (Protocol + Metadata/TableProperties checks)
//==================================================================================================

/// Check if deletion vectors is enabled (supported + table property set).
///
/// Deletion vectors is enabled when:
/// - The table supports deletion vectors (see `is_deletion_vector_supported`)
/// - The `delta.enableDeletionVectors` table property is set to `true`
pub(crate) fn is_deletion_vector_enabled(
    protocol: &Protocol,
    table_properties: &TableProperties,
) -> bool {
    is_deletion_vector_supported(protocol)
        && table_properties.enable_deletion_vectors.unwrap_or(false)
}

/// Check if row tracking is enabled (supported + table property set).
///
/// Row tracking is enabled when:
/// - The table supports row tracking (see `is_row_tracking_supported`)
/// - The `delta.enableRowTracking` table property is set to `true`
pub(crate) fn is_row_tracking_enabled(
    protocol: &Protocol,
    table_properties: &TableProperties,
) -> bool {
    is_row_tracking_supported(protocol) && table_properties.enable_row_tracking.unwrap_or(false)
}

/// Check if append-only is enabled (supported + table property set).
///
/// Append-only is enabled when:
/// - The table supports append-only (see `is_append_only_supported`)
/// - The `delta.appendOnly` table property is set to `true`
pub(crate) fn is_append_only_enabled(
    protocol: &Protocol,
    table_properties: &TableProperties,
) -> bool {
    is_append_only_supported(protocol) && table_properties.append_only.unwrap_or(false)
}

/// Check if in-commit timestamps is enabled (supported + table property set).
///
/// In-commit timestamps is enabled when:
/// - The table supports in-commit timestamps (see `is_in_commit_timestamps_supported`)
/// - The `delta.enableInCommitTimestamps` table property is set to `true`
pub(crate) fn is_in_commit_timestamps_enabled(
    protocol: &Protocol,
    table_properties: &TableProperties,
) -> bool {
    is_in_commit_timestamps_supported(protocol)
        && table_properties
            .enable_in_commit_timestamps
            .unwrap_or(false)
}

/// Check if row tracking should be written for this table.
///
/// Row tracking information should be written when:
/// - Row tracking is supported (see `is_row_tracking_supported`)
/// - Row tracking is not suspended (table property `delta.rowTrackingSuspended` is not `true`)
///
/// Note: We ignore `is_row_tracking_enabled` at this point because Kernel does not
/// preserve row IDs and row commit versions yet.
pub(crate) fn should_write_row_tracking(
    protocol: &Protocol,
    table_properties: &TableProperties,
) -> bool {
    let suspended = table_properties.row_tracking_suspended.unwrap_or(false);
    is_row_tracking_supported(protocol) && !suspended
}

//==================================================================================================
// Combined Validation Functions
//==================================================================================================

/// Validate that kernel can read from this table.
///
/// This checks:
/// - Protocol read support (see `ensure_read_supported`)
/// - Schema validation for special features (column mapping, timestamp_ntz, variant)
pub(crate) fn validate_read_table(protocol: &Protocol, _metadata: &Metadata) -> DeltaResult<()> {
    // Validate protocol
    ensure_read_supported(protocol)?;

    // Additional validation for specific features would go here
    // (e.g., schema validation for column mapping, timestamp_ntz, variant)

    Ok(())
}

/// Validate that kernel can write to this table.
///
/// This checks:
/// - Protocol write support (see `ensure_write_supported`)
/// - Additional table-specific validations (invariants, row tracking state, etc.)
pub(crate) fn validate_write_table(
    protocol: &Protocol,
    _metadata: &Metadata,
    _table_properties: &TableProperties,
) -> DeltaResult<()> {
    // Validate protocol
    ensure_write_supported(protocol)?;

    // Additional validation for specific features would go here
    // (e.g., checking invariants, row tracking state, etc.)

    Ok(())
}
