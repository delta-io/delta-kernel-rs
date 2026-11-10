//! Protocol preparation for existing-table metadata updates.
//!
//! When UPDATE TABLE sets feature-enablement properties (for example
//! `delta.enableDeletionVectors=true`), Kernel upgrades the protocol to support the required
//! features, matching Java Kernel's `autoUpgradeProtocolBasedOnMetadata` path for property-driven
//! enablement. Explicit `delta.feature.<name>=supported` signals are parsed by
//! [`parse_feature_signal`] and enabled through the same path.

use std::sync::Arc;

use uuid::Uuid;

use crate::actions::Protocol;
use crate::schema::{Schema, SchemaRef};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    ColumnMappingMode, FeatureRequirement, LEGACY_WRITER_FEATURES,
    SET_TABLE_FEATURE_SUPPORTED_PREFIX, SET_TABLE_FEATURE_SUPPORTED_VALUE,
    TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION, TableFeature,
    add_feature_to_lists, assign_column_mapping_metadata, auto_enable_property_driven_features,
    ensure_domain_metadata_dependency, extract_enabled_reader_features,
    find_max_column_id_in_schema, schema_contains_timestamp_ntz,
};
use crate::table_properties::{
    COLUMN_MAPPING_MAX_COLUMN_ID, COLUMN_MAPPING_MODE, CheckpointPolicy, ENABLE_DELETION_VECTORS,
    ENABLE_ICEBERG_COMPAT_V1, ENABLE_ICEBERG_COMPAT_V2, ENABLE_ICEBERG_COMPAT_V3,
    ENABLE_ICEBERG_NATIVE_V4, ENABLE_IN_COMMIT_TIMESTAMPS, ENABLE_ROW_TRACKING,
    IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP, IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
    MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME, MATERIALIZED_ROW_ID_COLUMN_NAME,
    ROW_TRACKING_SUSPENDED, TableProperties, UNIVERSAL_FORMAT_ENABLED_FORMATS,
};
use crate::{KernelError, KernelResult};

/// Table features that `delta.feature.<name>=supported` may enable, on CREATE/UPDATE TABLE
///
/// Feature signals (`delta.feature.X=supported`) are validated against this list.
/// Only features in this list can be enabled via feature signals.
pub(crate) const ALLOWED_DELTA_FEATURES: &[TableFeature] = &[
    // DomainMetadata is required for clustering and other system domain operations
    TableFeature::DomainMetadata,
    // ColumnMapping enables column mapping (name/id mode)
    TableFeature::ColumnMapping,
    // InCommitTimestamp enables in-commit timestamps (writer-only)
    TableFeature::InCommitTimestamp,
    // VacuumProtocolCheck ensures consistent protocol checks during VACUUM
    TableFeature::VacuumProtocolCheck,
    // CatalogManaged enables catalog-managed table support
    TableFeature::CatalogManaged,
    // Note: Clustering is NOT included here. Users should not enable clustering via
    // `delta.feature.clustering = supported`. Instead, clustering is enabled by
    // specifying clustering columns via `with_data_layout()`.
    TableFeature::DeletionVectors,
    TableFeature::V2Checkpoint,
    // Simple protocol-only features: enabling these only updates the protocol action.
    // They can also be auto-enabled via their enablement properties (e.g. delta.appendOnly=true)
    // through `maybe_auto_enable_property_driven_features`.
    TableFeature::AppendOnly,
    TableFeature::ChangeDataFeed,
    TableFeature::TypeWidening,
    TableFeature::RowTracking,
    TableFeature::VariantType,
    TableFeature::VariantShredding,
    // Also derived from any final schema containing timestamp-without-time-zone columns.
    TableFeature::TimestampWithoutTimezone,
    // Invariants is auto-enabled by `maybe_enable_invariants` when the schema has non-null
    // fields. Allowing explicit `delta.feature.invariants=supported` lets users pre-enable
    // the feature on an all-nullable table so a later ALTER TABLE ADD COLUMN NOT NULL does
    // not need a protocol upgrade.
    TableFeature::Invariants,
    // MaterializePartitionColumns keeps partition columns in the data files instead of
    // dropping them on write. There is no `delta.*` enablement property; the only opt-in at
    // create time is the explicit feature signal
    // `delta.feature.materializePartitionColumns=supported`.
    TableFeature::MaterializePartitionColumns,
    TableFeature::Collations,
    // IcebergCompatV2 is a writer-only feature that gates Iceberg V2 conversion compatibility.
    // Dependent features (ColumnMapping) are auto-added during create table.
    TableFeature::IcebergCompatV2,
    // IcebergCompatV3 is a writer-only feature that gates Iceberg V3 conversion compatibility.
    // Dependent features (ColumnMapping, RowTracking, DomainMetadata) are auto-added during
    // create table.
    TableFeature::IcebergCompatV3,
    #[cfg(feature = "adaptive-metadata-in-dev")]
    TableFeature::IcebergNativeV4Preview,
];

/// Features that UPDATE TABLE may auto-add to the protocol from enablement properties.
const UPDATE_PROPERTY_DRIVEN_FEATURES: &[TableFeature] = &[
    TableFeature::AppendOnly,
    TableFeature::ChangeDataFeed,
    TableFeature::DeletionVectors,
    TableFeature::TypeWidening,
    TableFeature::InCommitTimestamp,
    TableFeature::RowTracking,
    TableFeature::ColumnMapping,
    TableFeature::IcebergCompatV2,
    TableFeature::IcebergCompatV3,
    TableFeature::IcebergNativeV4Preview,
    // IcebergCompatV1 remains NotSupported for writes; enabling it fails ensure_write_supported.
    TableFeature::IcebergCompatV1,
];

/// Parses one `delta.feature.<name>=supported` signal. `operation` names the caller in the
/// rejection message.
pub(crate) fn parse_feature_signal(
    key: &str,
    value: &str,
    operation: &str,
) -> KernelResult<TableFeature> {
    if value != SET_TABLE_FEATURE_SUPPORTED_VALUE {
        return Err(KernelError::generic(format!(
            "Invalid value '{value}' for '{key}'. Only '{SET_TABLE_FEATURE_SUPPORTED_VALUE}' is allowed."
        )));
    }
    let Some(feature_name) = key.strip_prefix(SET_TABLE_FEATURE_SUPPORTED_PREFIX) else {
        return Err(KernelError::generic(format!(
            "'{key}' is not a feature signal"
        )));
    };
    // Unknown names become TableFeature::Unknown, which is never in the allowlist.
    let feature: TableFeature = feature_name
        .parse()
        .unwrap_or_else(|_| TableFeature::Unknown(feature_name.to_string()));
    if !ALLOWED_DELTA_FEATURES.contains(&feature) {
        return Err(KernelError::generic(format!(
            "Enabling feature '{feature_name}' via '{key}' is not supported during {operation}"
        )));
    }
    Ok(feature)
}

/// Returns `protocol` with `timestampNtz` added when `schema` has timestamp-without-timezone
/// columns, or `None` when no change is needed.
pub(crate) fn protocol_with_timestamp_ntz(
    protocol: &Protocol,
    schema: &Schema,
) -> KernelResult<Option<Protocol>> {
    if !schema_contains_timestamp_ntz(schema) {
        return Ok(None);
    }
    let mut reader = extract_enabled_reader_features(protocol);
    let mut writer = extract_enabled_writer_features(protocol);
    let (reader_len, writer_len) = (reader.len(), writer.len());
    add_feature_to_lists(
        TableFeature::TimestampWithoutTimezone,
        &mut reader,
        &mut writer,
    );
    if reader.len() == reader_len && writer.len() == writer_len {
        return Ok(None);
    }
    protocol_with_features(protocol, reader, writer).map(Some)
}

/// Result of preparing protocol (and possibly metadata) after an existing-table property update.
pub(crate) struct ProtocolPrepResult {
    pub table_config: TableConfiguration,
    pub should_emit_protocol: bool,
    /// True when protocol prep mutated metadata beyond the caller's prior property/schema changes.
    pub metadata_side_effects: bool,
}

/// Upgrades protocol (and applies metadata side effects) for property-driven feature enablement.
///
/// `before` is the table configuration prior to this transaction's property/schema changes.
/// `after` already includes those changes.
pub(crate) fn prepare_protocol_for_update(
    before: &TableConfiguration,
    after: TableConfiguration,
    feature_signals: &[TableFeature],
) -> KernelResult<ProtocolPrepResult> {
    reject_existing_table_toggles(before, &after, feature_signals)?;

    let mut metadata = after.metadata().clone();
    let mut logical_schema = after.logical_schema();
    let mut metadata_side_effects = false;

    metadata_side_effects |= apply_iceberg_compat_property_dependencies(&mut metadata)?;
    metadata_side_effects |=
        apply_column_mapping_schema_if_needed(before, &mut metadata, &mut logical_schema)?;
    metadata_side_effects |= apply_ict_disablement_cleanup(before, &mut metadata)?;
    metadata_side_effects |= apply_row_tracking_materialized_names(before, &mut metadata)?;

    // `after` may already carry protocol changes from schema evolution (timestampNtz), so the
    // change check compares against `before`. Features are only ever added below.
    let mut reader_features = extract_enabled_reader_features(after.protocol());
    let mut writer_features = extract_enabled_writer_features(after.protocol());
    let original_reader_len = extract_enabled_reader_features(before.protocol()).len();
    let original_writer_len = extract_enabled_writer_features(before.protocol()).len();
    for feature in feature_signals {
        add_feature_to_lists(feature.clone(), &mut reader_features, &mut writer_features);
    }
    if reader_features.contains(&TableFeature::CatalogManaged)
        || writer_features.contains(&TableFeature::CatalogManaged)
    {
        // ICT itself is added by the CatalogManaged feature requirement during dependency
        // expansion; its enablement is property-driven, so the property must be set here.
        match TableProperties::from(metadata.configuration().iter()).enable_in_commit_timestamps {
            Some(true) => {}
            Some(false) => {
                return Err(KernelError::generic(format!(
                    "Catalog-managed tables require '{ENABLE_IN_COMMIT_TIMESTAMPS}=true', \
                     but it was explicitly set to 'false'"
                )));
            }
            None => {
                metadata = metadata.with_configuration_entry(ENABLE_IN_COMMIT_TIMESTAMPS, "true");
                metadata_side_effects = true;
            }
        }
    }
    let table_properties = TableProperties::from(metadata.configuration().iter());

    auto_enable_property_driven_features(
        UPDATE_PROPERTY_DRIVEN_FEATURES,
        &table_properties,
        &mut reader_features,
        &mut writer_features,
    );
    // The preview feature is skipped when the GA feature is already in the protocol, so an
    // opted-in GA table keeps its protocol unchanged.
    let variant_shredding_ga_enabled = reader_features.contains(&TableFeature::VariantShredding)
        || writer_features.contains(&TableFeature::VariantShredding);
    if table_properties.enable_variant_shredding == Some(true) && !variant_shredding_ga_enabled {
        add_feature_to_lists(
            TableFeature::VariantShreddingPreview,
            &mut reader_features,
            &mut writer_features,
        );
    }
    maybe_enable_v2_checkpoint_for_policy(
        &table_properties,
        &mut reader_features,
        &mut writer_features,
    );
    expand_feature_dependencies(&mut reader_features, &mut writer_features);
    ensure_domain_metadata_dependency(&mut reader_features, &mut writer_features);

    let should_emit_protocol = reader_features.len() != original_reader_len
        || writer_features.len() != original_writer_len;
    let new_protocol = if should_emit_protocol {
        protocol_with_features(after.protocol(), reader_features, writer_features)?
    } else {
        after.protocol().clone()
    };

    let table_config = if should_emit_protocol || metadata_side_effects {
        TableConfiguration::try_new(
            metadata,
            new_protocol,
            after.table_root().clone(),
            after.version(),
        )?
    } else {
        after
    };
    validate_universal_format(&table_config)?;
    // Reject enablement that Kernel cannot write (for example IcebergCompatV1).
    table_config.ensure_write_supported()?;

    Ok(ProtocolPrepResult {
        table_config,
        should_emit_protocol,
        metadata_side_effects,
    })
}

/// Returns true when this commit newly enables ICT relative to `read_config`.
pub(crate) fn transaction_enables_ict(
    read_config: &TableConfiguration,
    effective_config: &TableConfiguration,
) -> bool {
    effective_config.is_feature_enabled(&TableFeature::InCommitTimestamp)
        && !read_config.is_feature_enabled(&TableFeature::InCommitTimestamp)
}

/// Stamps ICT enablement version/timestamp into metadata for a commit that newly enables ICT.
pub(crate) fn with_ict_enablement_info(
    metadata: crate::actions::Metadata,
    commit_version: crate::Version,
    in_commit_timestamp: i64,
) -> crate::actions::Metadata {
    metadata
        .with_configuration_entry(
            IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
            commit_version.to_string(),
        )
        .with_configuration_entry(
            IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
            in_commit_timestamp.to_string(),
        )
}

/// Rejects changes that Kernel cannot apply to an existing table: toggling row tracking or
/// Iceberg writer compatibility. Compares properties, not feature state, because the protocol has
/// not been upgraded yet.
fn reject_existing_table_toggles(
    before: &TableConfiguration,
    after: &TableConfiguration,
    feature_signals: &[TableFeature],
) -> KernelResult<()> {
    let (before_props, after_props) = (before.table_properties(), after.table_properties());
    let toggled = |before: Option<bool>, after: Option<bool>| {
        before.unwrap_or(false) != after.unwrap_or(false)
    };
    // A feature signal must not bypass the property check by enabling the feature directly.
    let signal_enables_row_tracking = feature_signals.contains(&TableFeature::RowTracking)
        && !before.is_feature_supported(&TableFeature::RowTracking);
    if toggled(
        before_props.enable_row_tracking,
        after_props.enable_row_tracking,
    ) || signal_enables_row_tracking
    {
        return Err(KernelError::unsupported(
            "Enabling or disabling row tracking on an existing table is not supported",
        ));
    }
    if toggled(
        before_props.enable_iceberg_compat_v3,
        after_props.enable_iceberg_compat_v3,
    ) {
        return Err(KernelError::unsupported(
            "Enabling or disabling IcebergCompatV3 on an existing table is not supported",
        ));
    }
    if toggled(
        before_props.enable_iceberg_writer_compat_v1,
        after_props.enable_iceberg_writer_compat_v1,
    ) || toggled(
        before_props.enable_iceberg_writer_compat_v3,
        after_props.enable_iceberg_writer_compat_v3,
    ) {
        return Err(KernelError::unsupported(
            "Enabling or disabling Iceberg writer compatibility on an existing table is not supported",
        ));
    }
    Ok(())
}

/// Mirrors Java's UniForm validation: when `delta.universalFormat.enabledFormats` is present,
/// at most one of IcebergCompatV2/V3 may be enabled, and the `iceberg` format needs one of them.
fn validate_universal_format(table_config: &TableConfiguration) -> KernelResult<()> {
    let Some(formats) = table_config
        .metadata()
        .configuration()
        .get(UNIVERSAL_FORMAT_ENABLED_FORMATS)
    else {
        return Ok(());
    };
    let props = table_config.table_properties();
    let compat_v2 = props.enable_iceberg_compat_v2 == Some(true);
    let compat_v3 = props.enable_iceberg_compat_v3 == Some(true);
    if compat_v2 && compat_v3 {
        return Err(KernelError::generic(format!(
            "'{}' and '{}' cannot be enabled at the same time.",
            ENABLE_ICEBERG_COMPAT_V2, ENABLE_ICEBERG_COMPAT_V3,
        )));
    }
    let iceberg_enabled = formats
        .split(',')
        .map(str::trim)
        .any(|format| format == "iceberg");
    if iceberg_enabled && !compat_v2 && !compat_v3 {
        return Err(KernelError::generic(format!(
            "One of {} or {} must be set to \"true\" to enable iceberg uniform format.",
            ENABLE_ICEBERG_COMPAT_V2, ENABLE_ICEBERG_COMPAT_V3,
        )));
    }
    Ok(())
}

fn apply_ict_disablement_cleanup(
    before: &TableConfiguration,
    metadata: &mut crate::actions::Metadata,
) -> KernelResult<bool> {
    let was_enabled = before.is_feature_enabled(&TableFeature::InCommitTimestamp);
    let now_enabled = metadata
        .configuration()
        .get(ENABLE_IN_COMMIT_TIMESTAMPS)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    if !(was_enabled && !now_enabled) {
        return Ok(false);
    }
    if before.is_feature_supported(&TableFeature::CatalogManaged)
        || before.is_feature_supported(&TableFeature::CatalogOwnedPreview)
    {
        return Err(KernelError::generic(
            "Cannot disable inCommitTimestamp on a catalogManaged table",
        ));
    }
    *metadata = metadata.clone().with_configuration_keys_unset([
        IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
        IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
    ]);
    Ok(true)
}

fn apply_row_tracking_materialized_names(
    before: &TableConfiguration,
    metadata: &mut crate::actions::Metadata,
) -> KernelResult<bool> {
    // Row-tracking toggle is rejected above; only fill missing names when already enabled.
    if !before.is_feature_enabled(&TableFeature::RowTracking) {
        return Ok(false);
    }
    let config = metadata.configuration();
    let missing_id = !config.contains_key(MATERIALIZED_ROW_ID_COLUMN_NAME);
    let missing_version = !config.contains_key(MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME);
    if !missing_id && !missing_version {
        return Ok(false);
    }
    let mut updated = metadata.clone();
    if missing_id {
        updated = updated.with_configuration_entry(
            MATERIALIZED_ROW_ID_COLUMN_NAME,
            format!("_row-id-col-{}", Uuid::new_v4()),
        );
    }
    if missing_version {
        updated = updated.with_configuration_entry(
            MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME,
            format!("_row-commit-version-col-{}", Uuid::new_v4()),
        );
    }
    *metadata = updated;
    Ok(true)
}

fn apply_iceberg_compat_property_dependencies(
    metadata: &mut crate::actions::Metadata,
) -> KernelResult<bool> {
    let mut changed = false;
    if is_true(metadata, ENABLE_ICEBERG_COMPAT_V2) {
        changed |= require_column_mapping_mode_name_or_id(metadata, "IcebergCompatV2")?;
        reject_true_properties(
            metadata,
            "IcebergCompatV2",
            &[
                ENABLE_ICEBERG_COMPAT_V1,
                ENABLE_ICEBERG_COMPAT_V3,
                ENABLE_DELETION_VECTORS,
            ],
        )?;
    }
    if is_true(metadata, ENABLE_ICEBERG_COMPAT_V3) {
        changed |= require_column_mapping_mode_name_or_id(metadata, "IcebergCompatV3")?;
        require_true_property(metadata, ENABLE_ROW_TRACKING, "IcebergCompatV3")?;
        if is_true(metadata, ROW_TRACKING_SUSPENDED) {
            return Err(KernelError::generic(format!(
                "IcebergCompatV3 cannot be enabled while '{ROW_TRACKING_SUSPENDED}' is 'true'"
            )));
        }
        reject_true_properties(
            metadata,
            "IcebergCompatV3",
            &[ENABLE_ICEBERG_COMPAT_V1, ENABLE_ICEBERG_COMPAT_V2],
        )?;
    }
    if is_true(metadata, ENABLE_ICEBERG_NATIVE_V4) {
        changed |= require_property_value(metadata, COLUMN_MAPPING_MODE, "id", "IcebergNativeV4")?;
        changed |=
            require_property_value(metadata, ENABLE_DELETION_VECTORS, "true", "IcebergNativeV4")?;
        changed |= require_property_value(
            metadata,
            ENABLE_IN_COMMIT_TIMESTAMPS,
            "true",
            "IcebergNativeV4",
        )?;
        require_true_property(metadata, ENABLE_ROW_TRACKING, "IcebergNativeV4")?;
        if is_true(metadata, ROW_TRACKING_SUSPENDED) {
            return Err(KernelError::generic(format!(
                "IcebergNativeV4 cannot be enabled while '{ROW_TRACKING_SUSPENDED}' is 'true'"
            )));
        }
        reject_true_properties(
            metadata,
            "IcebergNativeV4",
            &[
                ENABLE_ICEBERG_COMPAT_V1,
                ENABLE_ICEBERG_COMPAT_V2,
                ENABLE_ICEBERG_COMPAT_V3,
            ],
        )?;
    }
    Ok(changed)
}

fn apply_column_mapping_schema_if_needed(
    before: &TableConfiguration,
    metadata: &mut crate::actions::Metadata,
    logical_schema: &mut SchemaRef,
) -> KernelResult<bool> {
    let before_mode = before.table_properties().column_mapping_mode;
    let after_mode = metadata
        .configuration()
        .get(COLUMN_MAPPING_MODE)
        .and_then(|v| v.parse::<ColumnMappingMode>().ok())
        .unwrap_or(ColumnMappingMode::None);
    let enabling = after_mode != ColumnMappingMode::None
        && before_mode.unwrap_or(ColumnMappingMode::None) == ColumnMappingMode::None;
    if !enabling {
        return Ok(false);
    }

    let assign_nested = after_mode == ColumnMappingMode::Id;
    let mut max_id = find_max_column_id_in_schema(logical_schema).unwrap_or(0);
    let mapped = assign_column_mapping_metadata(logical_schema, &mut max_id, assign_nested)?;
    *logical_schema = Arc::new(mapped);
    *metadata = metadata
        .clone()
        .with_schema(logical_schema.clone())?
        .with_configuration_entry(COLUMN_MAPPING_MAX_COLUMN_ID, max_id.to_string());
    Ok(true)
}

fn maybe_enable_v2_checkpoint_for_policy(
    table_properties: &TableProperties,
    reader_features: &mut Vec<TableFeature>,
    writer_features: &mut Vec<TableFeature>,
) {
    if table_properties.checkpoint_policy == Some(CheckpointPolicy::V2) {
        add_feature_to_lists(TableFeature::V2Checkpoint, reader_features, writer_features);
    }
}

fn expand_feature_dependencies(
    reader_features: &mut Vec<TableFeature>,
    writer_features: &mut Vec<TableFeature>,
) {
    loop {
        let before_reader = reader_features.len();
        let before_writer = writer_features.len();
        let snapshot: Vec<TableFeature> = reader_features
            .iter()
            .chain(writer_features.iter())
            .cloned()
            .collect();
        for feature in snapshot {
            for requirement in feature.info().feature_requirements {
                match requirement {
                    FeatureRequirement::Supported(dep) | FeatureRequirement::Enabled(dep) => {
                        add_feature_to_lists(dep.clone(), reader_features, writer_features);
                    }
                    FeatureRequirement::NotSupported(_)
                    | FeatureRequirement::NotEnabled(_)
                    | FeatureRequirement::Custom(_) => {}
                }
            }
        }
        if reader_features.len() == before_reader && writer_features.len() == before_writer {
            break;
        }
    }
}

fn extract_enabled_writer_features(protocol: &Protocol) -> Vec<TableFeature> {
    match protocol.min_writer_version() {
        TABLE_FEATURES_MIN_WRITER_VERSION => protocol
            .writer_features()
            .map(|f| f.to_vec())
            .unwrap_or_default(),
        v if (1..=6).contains(&v) => LEGACY_WRITER_FEATURES
            .iter()
            .filter(|f| f.is_valid_for_legacy_writer(v))
            .cloned()
            .collect(),
        _ => Vec::new(),
    }
}

fn protocol_with_features(
    current: &Protocol,
    reader_features: Vec<TableFeature>,
    writer_features: Vec<TableFeature>,
) -> KernelResult<Protocol> {
    // Existing table-feature protocols keep unknown/custom features already present.
    let mut reader = reader_features;
    let mut writer = writer_features;
    if current.min_reader_version() == TABLE_FEATURES_MIN_READER_VERSION
        && let Some(existing) = current.reader_features()
    {
        for feature in existing {
            if !reader.contains(feature) {
                reader.push(feature.clone());
            }
        }
    }
    if current.min_writer_version() == TABLE_FEATURES_MIN_WRITER_VERSION
        && let Some(existing) = current.writer_features()
    {
        for feature in existing {
            if !writer.contains(feature) {
                writer.push(feature.clone());
            }
        }
    }
    Protocol::try_new_modern(reader, writer)
}

fn is_true(metadata: &crate::actions::Metadata, key: &str) -> bool {
    metadata
        .configuration()
        .get(key)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

fn require_column_mapping_mode_name_or_id(
    metadata: &mut crate::actions::Metadata,
    feature_name: &str,
) -> KernelResult<bool> {
    match metadata
        .configuration()
        .get(COLUMN_MAPPING_MODE)
        .map(String::as_str)
    {
        None => {
            *metadata = metadata
                .clone()
                .with_configuration_entry(COLUMN_MAPPING_MODE, "name");
            Ok(true)
        }
        Some("name") | Some("id") => Ok(false),
        Some(other) => Err(KernelError::generic(format!(
            "{feature_name} requires '{COLUMN_MAPPING_MODE}' to be 'name' or 'id', got '{other}'"
        ))),
    }
}

/// Fills `key` with `required` when absent, and errors when it is set to a different value.
fn require_property_value(
    metadata: &mut crate::actions::Metadata,
    key: &str,
    required: &str,
    feature_name: &str,
) -> KernelResult<bool> {
    match metadata.configuration().get(key).map(String::as_str) {
        None => {
            *metadata = metadata.clone().with_configuration_entry(key, required);
            Ok(true)
        }
        Some(value) if value == required => Ok(false),
        Some(other) => Err(KernelError::generic(format!(
            "{feature_name} requires '{key}' to be '{required}', got '{other}'"
        ))),
    }
}

/// Row tracking cannot be enabled on an existing table, so dependents require it to already be on.
fn require_true_property(
    metadata: &crate::actions::Metadata,
    key: &str,
    feature_name: &str,
) -> KernelResult<()> {
    if is_true(metadata, key) {
        return Ok(());
    }
    Err(KernelError::generic(format!(
        "{feature_name} requires '{key}' to be 'true'"
    )))
}

fn reject_true_properties(
    metadata: &crate::actions::Metadata,
    feature_name: &str,
    keys: &[&str],
) -> KernelResult<()> {
    for key in keys {
        if is_true(metadata, key) {
            return Err(KernelError::generic(format!(
                "{feature_name} cannot be enabled together with '{key}'"
            )));
        }
    }
    Ok(())
}
