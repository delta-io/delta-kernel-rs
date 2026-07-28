//! Shared constants for UC catalog-managed table operations.

/// Property key for the UC table ID, stored in Delta metadata configuration.
pub(crate) const UC_TABLE_ID_KEY: &str = "io.unitycatalog.tableId";
/// Property key to enable in-commit timestamps.
pub(crate) const ENABLE_IN_COMMIT_TIMESTAMPS: &str = "delta.enableInCommitTimestamps";
/// Feature supported value.
pub(crate) const FEATURE_SUPPORTED: &str = "supported";
/// Feature signal key for catalog-managed tables.
pub(crate) const CATALOG_MANAGED_FEATURE_KEY: &str = "delta.feature.catalogManaged";
/// Feature signal key for vacuum protocol check.
pub(crate) const VACUUM_PROTOCOL_CHECK_FEATURE_KEY: &str = "delta.feature.vacuumProtocolCheck";
/// Feature signal key for v2 checkpoints.
pub(crate) const V2_CHECKPOINT_FEATURE_KEY: &str = "delta.feature.v2Checkpoint";
/// Feature signal key for deletion vectors.
pub(crate) const DELETION_VECTORS_FEATURE_KEY: &str = "delta.feature.deletionVectors";
/// Config property enabling deletion vectors.
pub(crate) const ENABLE_DELETION_VECTORS_KEY: &str = "delta.enableDeletionVectors";
/// Config property selecting the checkpoint policy.
pub(crate) const CHECKPOINT_POLICY_KEY: &str = "delta.checkpointPolicy";
/// Config property value selecting the v2 checkpoint policy.
pub(crate) const CHECKPOINT_POLICY_V2: &str = "v2";
/// Config property writing checkpoint stats as a struct.
pub(crate) const CHECKPOINT_WRITE_STATS_AS_STRUCT_KEY: &str = "delta.checkpoint.writeStatsAsStruct";
/// Config property writing checkpoint stats as JSON.
pub(crate) const CHECKPOINT_WRITE_STATS_AS_JSON_KEY: &str = "delta.checkpoint.writeStatsAsJson";
/// Boolean-true config property value.
pub(crate) const CONFIG_TRUE: &str = "true";
/// Feature name for catalog-managed tables (wire format).
pub(crate) const CATALOG_MANAGED_FEATURE: &str = "catalogManaged";
/// Feature name for vacuum protocol check (wire format).
pub(crate) const VACUUM_PROTOCOL_CHECK_FEATURE: &str = "vacuumProtocolCheck";
/// Feature name for in-commit timestamps (wire format).
pub(crate) const IN_COMMIT_TIMESTAMP_FEATURE: &str = "inCommitTimestamp";
/// Domain name for clustering metadata.
pub(crate) const CLUSTERING_DOMAIN_NAME: &str = "delta.clustering";
/// Domain name for row-tracking metadata.
pub(crate) const ROW_TRACKING_DOMAIN_NAME: &str = "delta.rowTracking";
