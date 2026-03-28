//! Shared constants for UC catalog-managed table operations.

/// Property key for the UC table ID, stored in Delta metadata configuration.
pub(crate) const UC_TABLE_ID_KEY: &str = "io.unitycatalog.tableId";
/// Property key to enable in-commit timestamps.
pub(crate) const ENABLE_IN_COMMIT_TIMESTAMPS: &str = "delta.enableInCommitTimestamps";
/// Feature supported value.
pub(crate) const FEATURE_SUPPORTED: &str = "supported";
/// The property key prefix for table feature signals.
pub(crate) const FEATURE_PREFIX: &str = "delta.feature.";
/// Feature signal key for catalog-managed tables.
pub(crate) const CATALOG_MANAGED_FEATURE_KEY: &str = "delta.feature.catalogManaged";
/// Feature signal key for vacuum protocol check.
pub(crate) const VACUUM_PROTOCOL_CHECK_FEATURE_KEY: &str = "delta.feature.vacuumProtocolCheck";
/// The property key for the minimum reader version.
pub(crate) const MIN_READER_VERSION_KEY: &str = "delta.minReaderVersion";
/// The property key for the minimum writer version.
pub(crate) const MIN_WRITER_VERSION_KEY: &str = "delta.minWriterVersion";
/// UC property for the last committed version.
pub(crate) const METASTORE_LAST_UPDATE_VERSION: &str = "delta.lastUpdateVersion";
/// UC property for the last commit timestamp.
pub(crate) const METASTORE_LAST_COMMIT_TIMESTAMP: &str = "delta.lastCommitTimestamp";
