//! V2 FFI functions for DuckDB Delta Lake integration
//!
//! This module exposes Delta Lake V2 features through FFI including:
//! - Deletion Vector functions
//! - Row Tracking functions
//! - Liquid Clustering functions
//! - Protocol V2 functions

use crate::handle::Handle;
use crate::SharedSnapshot;
use delta_kernel::table_features::TableFeature;

// ============================================================================
// Deletion Vector Functions
// ============================================================================

// Note: get_dv_cardinality is implemented in scan.rs where CDvInfo is defined
// because it needs access to private fields.

/// Check if the snapshot's table has deletion vectors enabled.
///
/// Returns true if the table supports deletion vectors, false otherwise.
///
/// # Safety
///
/// Caller is responsible for passing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_has_deletion_vectors(
    snapshot: Handle<SharedSnapshot>,
) -> bool {
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::DeletionVectors)
}

// ============================================================================
// Row Tracking Functions
// ============================================================================

/// Information about row tracking for a file in a scan.
///
/// This struct contains the base row ID and default row commit version
/// which are needed to reconstruct row identifiers for files with row tracking.
#[repr(C)]
pub struct RowTrackingInfo {
    /// The base row ID for this file. This is the starting row ID for the first row
    /// in the file. Subsequent rows have IDs: base_row_id + row_index.
    /// A value of -1 indicates row tracking info is not available.
    pub base_row_id: i64,
    /// The commit version when this file was first added to the table.
    /// A value of -1 indicates this information is not available.
    pub default_row_commit_version: i64,
}

impl Default for RowTrackingInfo {
    fn default() -> Self {
        Self {
            base_row_id: -1,
            default_row_commit_version: -1,
        }
    }
}

/// Check if the snapshot's table has row tracking enabled.
///
/// Returns true if the table has row tracking enabled, false otherwise.
///
/// # Safety
///
/// Caller is responsible for passing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_has_row_tracking(snapshot: Handle<SharedSnapshot>) -> bool {
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::RowTracking)
}

// ============================================================================
// Liquid Clustering Functions
// ============================================================================

/// Check if the snapshot's table has liquid clustering enabled.
///
/// Returns true if the table has liquid clustering (the `clustering` feature) enabled,
/// false otherwise.
///
/// # Safety
///
/// Caller is responsible for passing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_has_liquid_clustering(
    snapshot: Handle<SharedSnapshot>,
) -> bool {
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::ClusteredTable)
}

// ============================================================================
// Protocol V2 Functions
// ============================================================================

/// Extended protocol information including feature bitmasks.
///
/// This struct provides detailed protocol information including reader/writer
/// versions and bitmasks for V2 features.
#[repr(C)]
pub struct TableProtocolV2 {
    /// Minimum reader version required to read this table.
    pub min_reader_version: i32,
    /// Minimum writer version required to write to this table.
    pub min_writer_version: i32,
    /// Bitmask of reader features. Each bit represents a specific feature.
    /// See `ReaderFeatureFlag` for the mapping.
    pub reader_features: u32,
    /// Bitmask of writer features. Each bit represents a specific feature.
    /// See `WriterFeatureFlag` for the mapping.
    pub writer_features: u32,
}

/// Flags for reader features in the protocol.
/// These are used as bit positions in the reader_features bitmask.
#[repr(u32)]
pub enum ReaderFeatureFlag {
    /// Column mapping feature
    ColumnMapping = 1 << 0,
    /// Deletion vectors feature
    DeletionVectors = 1 << 1,
    /// Timestamp without timezone feature
    TimestampNtz = 1 << 2,
    /// V2 checkpoint feature
    V2Checkpoint = 1 << 3,
    /// Type widening feature
    TypeWidening = 1 << 4,
    /// Vacuum protocol check feature
    VacuumProtocolCheck = 1 << 5,
    /// Variant type feature
    VariantType = 1 << 6,
}

/// Flags for writer features in the protocol.
/// These are used as bit positions in the writer_features bitmask.
#[repr(u32)]
pub enum WriterFeatureFlag {
    /// Append only feature
    AppendOnly = 1 << 0,
    /// Invariants feature
    Invariants = 1 << 1,
    /// Check constraints feature
    CheckConstraints = 1 << 2,
    /// Change data feed feature
    ChangeDataFeed = 1 << 3,
    /// Generated columns feature
    GeneratedColumns = 1 << 4,
    /// Column mapping feature
    ColumnMapping = 1 << 5,
    /// Identity columns feature
    IdentityColumns = 1 << 6,
    /// Deletion vectors feature
    DeletionVectors = 1 << 7,
    /// Row tracking feature
    RowTracking = 1 << 8,
    /// Timestamp without timezone feature
    TimestampNtz = 1 << 9,
    /// Domain metadata feature
    DomainMetadata = 1 << 10,
    /// V2 checkpoint feature
    V2Checkpoint = 1 << 11,
    /// Iceberg compat V1 feature
    IcebergCompatV1 = 1 << 12,
    /// Iceberg compat V2 feature
    IcebergCompatV2 = 1 << 13,
    /// Clustering (liquid clustering) feature
    Clustering = 1 << 14,
    /// Type widening feature
    TypeWidening = 1 << 15,
    /// Vacuum protocol check feature
    VacuumProtocolCheck = 1 << 16,
    /// In-commit timestamp feature
    InCommitTimestamp = 1 << 17,
    /// Variant type feature
    VariantType = 1 << 18,
}

/// Convert a TableFeature to a reader feature flag bit.
fn table_feature_to_reader_flag(feature: &TableFeature) -> u32 {
    match feature {
        TableFeature::ColumnMapping => ReaderFeatureFlag::ColumnMapping as u32,
        TableFeature::DeletionVectors => ReaderFeatureFlag::DeletionVectors as u32,
        TableFeature::TimestampWithoutTimezone => ReaderFeatureFlag::TimestampNtz as u32,
        TableFeature::V2Checkpoint => ReaderFeatureFlag::V2Checkpoint as u32,
        TableFeature::TypeWidening | TableFeature::TypeWideningPreview => {
            ReaderFeatureFlag::TypeWidening as u32
        }
        TableFeature::VacuumProtocolCheck => ReaderFeatureFlag::VacuumProtocolCheck as u32,
        TableFeature::VariantType
        | TableFeature::VariantTypePreview
        | TableFeature::VariantShreddingPreview => ReaderFeatureFlag::VariantType as u32,
        _ => 0, // Writer-only features don't have reader flags
    }
}

/// Convert a TableFeature to a writer feature flag bit.
fn table_feature_to_writer_flag(feature: &TableFeature) -> u32 {
    match feature {
        TableFeature::AppendOnly => WriterFeatureFlag::AppendOnly as u32,
        TableFeature::Invariants => WriterFeatureFlag::Invariants as u32,
        TableFeature::CheckConstraints => WriterFeatureFlag::CheckConstraints as u32,
        TableFeature::ChangeDataFeed => WriterFeatureFlag::ChangeDataFeed as u32,
        TableFeature::GeneratedColumns => WriterFeatureFlag::GeneratedColumns as u32,
        TableFeature::ColumnMapping => WriterFeatureFlag::ColumnMapping as u32,
        TableFeature::IdentityColumns => WriterFeatureFlag::IdentityColumns as u32,
        TableFeature::DeletionVectors => WriterFeatureFlag::DeletionVectors as u32,
        TableFeature::RowTracking => WriterFeatureFlag::RowTracking as u32,
        TableFeature::TimestampWithoutTimezone => WriterFeatureFlag::TimestampNtz as u32,
        TableFeature::DomainMetadata => WriterFeatureFlag::DomainMetadata as u32,
        TableFeature::V2Checkpoint => WriterFeatureFlag::V2Checkpoint as u32,
        TableFeature::IcebergCompatV1 => WriterFeatureFlag::IcebergCompatV1 as u32,
        TableFeature::IcebergCompatV2 => WriterFeatureFlag::IcebergCompatV2 as u32,
        TableFeature::ClusteredTable => WriterFeatureFlag::Clustering as u32,
        TableFeature::TypeWidening | TableFeature::TypeWideningPreview => {
            WriterFeatureFlag::TypeWidening as u32
        }
        TableFeature::VacuumProtocolCheck => WriterFeatureFlag::VacuumProtocolCheck as u32,
        TableFeature::InCommitTimestamp => WriterFeatureFlag::InCommitTimestamp as u32,
        TableFeature::VariantType
        | TableFeature::VariantTypePreview
        | TableFeature::VariantShreddingPreview => WriterFeatureFlag::VariantType as u32,
        TableFeature::CatalogManaged | TableFeature::CatalogOwnedPreview => 0, // Not exposed in V2 bitmask
        TableFeature::Unknown(_) => 0, // Unknown features not mapped
    }
}

/// Get the protocol information with V2 feature bitmasks.
///
/// Returns the protocol information including reader/writer versions and
/// feature bitmasks.
///
/// # Safety
///
/// Caller is responsible for passing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn get_table_protocol_v2(
    snapshot: Handle<SharedSnapshot>,
) -> TableProtocolV2 {
    let snapshot = unsafe { snapshot.as_ref() };
    let protocol = snapshot.table_configuration().protocol();

    let mut reader_features: u32 = 0;
    let mut writer_features: u32 = 0;

    // Convert reader features to bitmask
    if let Some(features) = protocol.reader_features() {
        for feature in features {
            reader_features |= table_feature_to_reader_flag(feature);
        }
    }

    // Convert writer features to bitmask
    if let Some(features) = protocol.writer_features() {
        for feature in features {
            writer_features |= table_feature_to_writer_flag(feature);
        }
    }

    TableProtocolV2 {
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features,
        writer_features,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_tracking_info_default() {
        let info = RowTrackingInfo::default();
        assert_eq!(info.base_row_id, -1);
        assert_eq!(info.default_row_commit_version, -1);
    }

    #[test]
    fn test_feature_flag_values() {
        // Ensure flag values are powers of 2 and unique
        assert_eq!(ReaderFeatureFlag::ColumnMapping as u32, 1);
        assert_eq!(ReaderFeatureFlag::DeletionVectors as u32, 2);
        assert_eq!(ReaderFeatureFlag::TimestampNtz as u32, 4);

        assert_eq!(WriterFeatureFlag::AppendOnly as u32, 1);
        assert_eq!(WriterFeatureFlag::Invariants as u32, 2);
        assert_eq!(WriterFeatureFlag::CheckConstraints as u32, 4);
    }
}

