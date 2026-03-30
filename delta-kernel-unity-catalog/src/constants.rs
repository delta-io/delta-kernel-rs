//! Shared constants for UC catalog-managed table operations.

/// Property key for the UC table ID, stored in Delta metadata configuration.
pub(crate) const UC_TABLE_ID_KEY: &str = "io.unitycatalog.tableId";
/// Feature supported value.
pub(crate) const FEATURE_SUPPORTED: &str = "supported";
/// Feature signal key for catalog-managed tables.
pub(crate) const CATALOG_MANAGED_FEATURE_KEY: &str = "delta.feature.catalogManaged";
/// Feature signal key for vacuum protocol check.
pub(crate) const VACUUM_PROTOCOL_CHECK_FEATURE_KEY: &str = "delta.feature.vacuumProtocolCheck";
/// UC property for the last committed version.
pub(crate) const METASTORE_LAST_UPDATE_VERSION: &str = "delta.lastUpdateVersion";
/// UC property for the last commit timestamp.
pub(crate) const METASTORE_LAST_COMMIT_TIMESTAMP: &str = "delta.lastCommitTimestamp";
/// Feature name for catalog-managed tables (wire format).
pub(crate) const CATALOG_MANAGED_FEATURE: &str = "catalogManaged";
/// Feature name for catalog-owned preview tables (wire format). This is the preview feature
/// used by delta-spark OSS. Tables with this feature are treated as catalog-managed.
pub(crate) const CATALOG_OWNED_PREVIEW_FEATURE: &str = "catalogOwned-preview";
/// Feature name for vacuum protocol check (wire format).
pub(crate) const VACUUM_PROTOCOL_CHECK_FEATURE: &str = "vacuumProtocolCheck";
/// Feature name for in-commit timestamps (wire format).
pub(crate) const IN_COMMIT_TIMESTAMP_FEATURE: &str = "inCommitTimestamp";
/// Domain name for clustering metadata.
pub(crate) const CLUSTERING_DOMAIN_NAME: &str = "delta.clustering";
