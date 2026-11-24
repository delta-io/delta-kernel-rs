//! Protocol preparation for existing-table metadata updates.
//!
//! When UPDATE TABLE sets feature-enablement properties (for example
//! `delta.enableDeletionVectors=true`), Kernel upgrades the protocol to support the required
//! features, matching Java Kernel's `autoUpgradeProtocolBasedOnMetadata` path for property-driven
//! enablement. Explicit `delta.feature.<name>=supported` signals are parsed by
//! [`parse_feature_signal`] and enabled through the same path.

use uuid::Uuid;

use crate::actions::Protocol;
use crate::schema::Schema;
use crate::schema::validation::validate_cdf_column_names;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    ColumnMappingMode, FeatureRequirement, LEGACY_WRITER_FEATURES,
    SET_TABLE_FEATURE_SUPPORTED_PREFIX, SET_TABLE_FEATURE_SUPPORTED_VALUE,
    TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION, TableFeature,
    add_feature_to_lists, auto_enable_property_driven_features, ensure_domain_metadata_dependency,
    extract_enabled_reader_features, schema_contains_timestamp_ntz,
};
use crate::table_properties::{
    COLUMN_MAPPING_MODE, CheckpointPolicy, ENABLE_DELETION_VECTORS, ENABLE_ICEBERG_COMPAT_V1,
    ENABLE_ICEBERG_COMPAT_V2, ENABLE_ICEBERG_COMPAT_V3, ENABLE_ICEBERG_NATIVE_V4,
    ENABLE_IN_COMMIT_TIMESTAMPS, ENABLE_ROW_TRACKING, IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
    IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME,
    MATERIALIZED_ROW_ID_COLUMN_NAME, ROW_TRACKING_SUSPENDED, TableProperties,
    UNIVERSAL_FORMAT_ENABLED_FORMATS,
};
use crate::{KernelError, KernelResult, Result};

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
) -> Result<ProtocolPrepResult> {
    reject_existing_table_toggles(before, &after, feature_signals)?;

    let mut metadata = after.metadata().clone();
    let mut metadata_side_effects = false;

    metadata_side_effects |= apply_iceberg_compat_property_dependencies(&mut metadata)?;
    // Defense in depth: mode changes are rejected above, and dependency helpers no longer fill a
    // mode. Keep the check so a future fill cannot silently rewrite mapping metadata.
    reject_column_mapping_mode_change(before, &metadata)?;
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
                ))
                .into());
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
    // Enabling shredding opts in to GA variantShredding, which PROTOCOL.md requires in both
    // feature lists. A table already on GA or the preview keeps its protocol unchanged, so clients
    // that only know the preview are not locked out.
    let variant_shredding_satisfied = [
        TableFeature::VariantShredding,
        TableFeature::VariantShreddingPreview,
    ]
    .iter()
    .any(|f| reader_features.contains(f) || writer_features.contains(f));
    if table_properties.enable_variant_shredding == Some(true) && !variant_shredding_satisfied {
        add_feature_to_lists(
            TableFeature::VariantShredding,
            &mut reader_features,
            &mut writer_features,
        );
    }
    // Clients that only know typeWidening-preview cannot read GA typeWidening, so a table already
    // on the preview keeps its protocol.
    let type_widening_preview_enabled = reader_features
        .contains(&TableFeature::TypeWideningPreview)
        || writer_features.contains(&TableFeature::TypeWideningPreview);
    if table_properties.enable_type_widening == Some(true) && !type_widening_preview_enabled {
        add_feature_to_lists(
            TableFeature::TypeWidening,
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
    reject_newly_enabled_hudi_uniform(before, &table_config)?;
    reject_cdf_reserved_columns(before, &table_config)?;
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
    let signal_newly_enables = |feature: &TableFeature| {
        feature_signals.contains(feature) && !before.is_feature_supported(feature)
    };
    // A feature signal must not bypass the property check by enabling the feature directly.
    if toggled(
        before_props.enable_row_tracking,
        after_props.enable_row_tracking,
    ) || signal_newly_enables(&TableFeature::RowTracking)
    {
        return Err(KernelError::unsupported(
            "Enabling or disabling row tracking on an existing table is not supported",
        ));
    }
    let before_mode = before_props
        .column_mapping_mode
        .unwrap_or(ColumnMappingMode::None);
    let after_mode = after_props
        .column_mapping_mode
        .unwrap_or(ColumnMappingMode::None);
    if before_mode != after_mode {
        return Err(KernelError::unsupported(
            "Changing column mapping mode on an existing table is not supported",
        ));
    }
    for (before_enabled, after_enabled, name, feature) in [
        (
            before_props.enable_iceberg_compat_v1,
            after_props.enable_iceberg_compat_v1,
            "IcebergCompatV1",
            TableFeature::IcebergCompatV1,
        ),
        (
            before_props.enable_iceberg_compat_v2,
            after_props.enable_iceberg_compat_v2,
            "IcebergCompatV2",
            TableFeature::IcebergCompatV2,
        ),
        (
            before_props.enable_iceberg_compat_v3,
            after_props.enable_iceberg_compat_v3,
            "IcebergCompatV3",
            TableFeature::IcebergCompatV3,
        ),
    ] {
        if toggled(before_enabled, after_enabled) || signal_newly_enables(&feature) {
            return Err(KernelError::unsupported(format!(
                "Enabling or disabling {name} on an existing table is not supported"
            )));
        }
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

/// Rejects newly enabling the `hudi` UniForm format on an existing table.
///
/// Spark gates Hudi UniForm on checks Kernel does not implement yet (no deletion vectors; no
/// NullType, Byte, Short, TimestampNTZ or geospatial columns), so Kernel could otherwise commit a
/// state Spark considers invalid. Tables that already have `hudi` keep working.
fn reject_newly_enabled_hudi_uniform(
    before: &TableConfiguration,
    after: &TableConfiguration,
) -> KernelResult<()> {
    let has_hudi = |config: &TableConfiguration| {
        config
            .metadata()
            .configuration()
            .get(UNIVERSAL_FORMAT_ENABLED_FORMATS)
            .is_some_and(|formats| formats.split(',').map(str::trim).any(|f| f == "hudi"))
    };
    if has_hudi(after) && !has_hudi(before) {
        return Err(KernelError::unsupported(
            "Enabling 'hudi' universal format on an existing table is not supported yet",
        ));
    }
    Ok(())
}

/// Rejects CDF-reserved column names (`_change_type`, `_commit_version`, `_commit_timestamp`) in
/// the final schema when this transaction newly enables Change Data Feed. Without this check a
/// later `TableChanges` read fails on duplicate fields.
fn reject_cdf_reserved_columns(
    before: &TableConfiguration,
    after: &TableConfiguration,
) -> KernelResult<()> {
    let newly_enabled = after.is_feature_enabled(&TableFeature::ChangeDataFeed)
        && !before.is_feature_enabled(&TableFeature::ChangeDataFeed);
    if newly_enabled {
        validate_cdf_column_names(after.logical_schema().as_ref(), after.column_mapping_mode())?;
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
        require_column_mapping_mode_name_or_id(metadata, "IcebergCompatV2")?;
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
        require_column_mapping_mode_name_or_id(metadata, "IcebergCompatV3")?;
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
        require_column_mapping_mode(metadata, "id", "IcebergNativeV4")?;
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

fn reject_column_mapping_mode_change(
    before: &TableConfiguration,
    metadata: &crate::actions::Metadata,
) -> KernelResult<()> {
    let before_mode = before
        .table_properties()
        .column_mapping_mode
        .unwrap_or(ColumnMappingMode::None);
    let after_mode = metadata
        .configuration()
        .get(COLUMN_MAPPING_MODE)
        .and_then(|v| v.parse::<ColumnMappingMode>().ok())
        .unwrap_or(ColumnMappingMode::None);
    if before_mode == after_mode {
        return Ok(());
    }
    Err(KernelError::unsupported(
        "Changing column mapping mode on an existing table is not supported",
    ))
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
    // Writer-only enablement must not force a reader upgrade. When no reader features are being
    // recorded, keep the table's current reader version and leave `readerFeatures` absent, so a
    // legacy reader-v1/v2 client is not needlessly locked out (PROTOCOL.md: writer-only features
    // do not raise reader requirements). Promote to the table-features reader version only when
    // there is at least one reader feature to list.
    if reader.is_empty() && current.min_reader_version() < TABLE_FEATURES_MIN_READER_VERSION {
        return Protocol::try_new(
            current.min_reader_version(),
            TABLE_FEATURES_MIN_WRITER_VERSION,
            None::<Vec<TableFeature>>,
            Some(writer),
        );
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
    metadata: &crate::actions::Metadata,
    feature_name: &str,
) -> KernelResult<()> {
    match metadata
        .configuration()
        .get(COLUMN_MAPPING_MODE)
        .map(String::as_str)
    {
        Some("name") | Some("id") => Ok(()),
        Some(other) => Err(KernelError::generic(format!(
            "{feature_name} requires '{COLUMN_MAPPING_MODE}' to be 'name' or 'id', got '{other}'"
        ))),
        None => Err(KernelError::generic(format!(
            "{feature_name} requires '{COLUMN_MAPPING_MODE}' to be 'name' or 'id'"
        ))),
    }
}

/// Fills `key` with `required` when absent, and errors when it is set to a different value.
///
/// Column-mapping mode is excluded: existing tables cannot change mode, so callers must use
/// [`require_column_mapping_mode`] / [`require_column_mapping_mode_name_or_id`] instead.
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

fn require_column_mapping_mode(
    metadata: &crate::actions::Metadata,
    required: &str,
    feature_name: &str,
) -> KernelResult<()> {
    match metadata
        .configuration()
        .get(COLUMN_MAPPING_MODE)
        .map(String::as_str)
    {
        Some(value) if value == required => Ok(()),
        Some(other) => Err(KernelError::generic(format!(
            "{feature_name} requires '{COLUMN_MAPPING_MODE}' to be '{required}', got '{other}'"
        ))),
        None => Err(KernelError::generic(format!(
            "{feature_name} requires '{COLUMN_MAPPING_MODE}' to be '{required}'"
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;

    use super::*;
    use crate::actions::Metadata;
    use crate::schema::schema_ref;
    use crate::table_features::{
        SET_TABLE_FEATURE_SUPPORTED_PREFIX, SET_TABLE_FEATURE_SUPPORTED_VALUE, TableFeature,
    };
    use crate::table_properties::{ENABLE_CHANGE_DATA_FEED, ENABLE_VARIANT_SHREDDING};
    use crate::unit_test_utils::{
        MockProtocolBuilder, MockTableConfigurationBuilder, assert_result_error_with_message,
        test_schema_flat_with_column_mapping,
    };

    fn metadata_with(props: &[(&str, &str)]) -> Metadata {
        Metadata::try_new(
            None,
            None,
            schema_ref! { nullable "value": INTEGER },
            vec![],
            0,
            props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
        )
        .unwrap()
    }

    #[rstest]
    #[case::missing_prefix(
        "delta.enableDeletionVectors",
        SET_TABLE_FEATURE_SUPPORTED_VALUE,
        "is not a feature signal"
    )]
    #[case::wrong_value("delta.feature.deletionVectors", "enabled", "Only 'supported'")]
    #[case::unknown_feature(
        "delta.feature.notAFeature",
        SET_TABLE_FEATURE_SUPPORTED_VALUE,
        "Enabling feature"
    )]
    #[case::not_allowlisted(
        "delta.feature.identityColumns",
        SET_TABLE_FEATURE_SUPPORTED_VALUE,
        "during UPDATE TABLE"
    )]
    fn parse_feature_signal_rejects_invalid_inputs(
        #[case] key: &str,
        #[case] value: &str,
        #[case] expected: &str,
    ) {
        assert_result_error_with_message(
            parse_feature_signal(key, value, "UPDATE TABLE"),
            expected,
        );
    }

    #[test]
    fn parse_feature_signal_accepts_allowlisted_feature() {
        let feature = parse_feature_signal(
            &format!("{SET_TABLE_FEATURE_SUPPORTED_PREFIX}deletionVectors"),
            SET_TABLE_FEATURE_SUPPORTED_VALUE,
            "UPDATE TABLE",
        )
        .unwrap();
        assert_eq!(feature, TableFeature::DeletionVectors);
    }

    #[test]
    fn protocol_with_timestamp_ntz_is_noop_when_feature_already_present() {
        let protocol = MockProtocolBuilder::new()
            .with_features([TableFeature::TimestampWithoutTimezone])
            .build();
        let schema = schema_ref! { nullable "ts": TIMESTAMP_NTZ };
        let result = protocol_with_timestamp_ntz(&protocol, &schema).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn protocol_with_timestamp_ntz_returns_none_without_timestamp_ntz_columns() {
        let protocol = MockProtocolBuilder::new().build();
        let schema = schema_ref! { nullable "value": INTEGER };
        let result = protocol_with_timestamp_ntz(&protocol, &schema).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn protocol_with_timestamp_ntz_upgrades_legacy_protocol() {
        let protocol = MockProtocolBuilder::new().with_versions(1, 2).build();
        let schema = schema_ref! { nullable "ts": TIMESTAMP_NTZ };
        let upgraded = protocol_with_timestamp_ntz(&protocol, &schema)
            .unwrap()
            .expect("protocol should gain timestampNtz");
        assert!(
            upgraded
                .reader_features()
                .unwrap()
                .contains(&TableFeature::TimestampWithoutTimezone)
        );
        assert!(
            upgraded
                .writer_features()
                .unwrap()
                .contains(&TableFeature::TimestampWithoutTimezone)
        );
    }

    #[rstest]
    #[case::preview_present_keeps_protocol(&[TableFeature::TypeWideningPreview], false)]
    #[case::no_preview_adds_ga(&[], true)]
    fn prepare_protocol_for_update_type_widening_respects_preview(
        #[case] existing: &[TableFeature],
        #[case] expect_protocol_change: bool,
    ) -> Result<()> {
        let before = MockTableConfigurationBuilder::new()
            .with_properties([("delta.enableTypeWidening", "true")])
            .with_protocol(MockProtocolBuilder::new().with_features(existing).build())
            .build();
        let result = prepare_protocol_for_update(&before, before.clone(), &[])?;
        assert_eq!(result.should_emit_protocol, expect_protocol_change);
        assert_eq!(
            result
                .table_config
                .is_feature_supported(&TableFeature::TypeWidening),
            !existing.contains(&TableFeature::TypeWideningPreview),
        );
        Ok(())
    }

    #[test]
    fn prepare_protocol_for_update_writer_only_feature_keeps_legacy_reader_version() -> Result<()> {
        // Enabling a writer-only feature (CDF) on a legacy (1, 2) table must bump only the writer
        // version to the table-features version, leaving the reader version and readerFeatures
        // untouched so reader v1/v2 clients are not locked out.
        let before = MockTableConfigurationBuilder::new()
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 2).build())
            .build();
        let after = MockTableConfigurationBuilder::new()
            .with_properties([(ENABLE_CHANGE_DATA_FEED, "true")])
            .with_protocol(MockProtocolBuilder::new().with_versions(1, 2).build())
            .build();

        let result = prepare_protocol_for_update(&before, after, &[])?;
        assert!(result.should_emit_protocol);

        let protocol = result.table_config.protocol();
        assert_eq!(protocol.min_reader_version(), 1);
        assert_eq!(
            protocol.min_writer_version(),
            TABLE_FEATURES_MIN_WRITER_VERSION
        );
        assert!(protocol.reader_features().is_none());
        assert!(
            protocol
                .writer_features()
                .unwrap()
                .contains(&TableFeature::ChangeDataFeed)
        );
        Ok(())
    }

    #[test]
    fn prepare_protocol_for_update_variant_shredding_keeps_existing_preview() -> Result<()> {
        let before = MockTableConfigurationBuilder::new()
            .with_properties([(ENABLE_VARIANT_SHREDDING, "true")])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([
                        TableFeature::VariantType,
                        TableFeature::VariantShreddingPreview,
                    ])
                    .build(),
            )
            .build();
        let result = prepare_protocol_for_update(&before, before.clone(), &[])?;
        assert!(!result.should_emit_protocol);
        assert!(
            result
                .table_config
                .is_feature_supported(&TableFeature::VariantShreddingPreview)
        );
        assert!(
            !result
                .table_config
                .is_feature_supported(&TableFeature::VariantShredding)
        );
        Ok(())
    }

    #[test]
    fn prepare_protocol_for_update_rejects_cdf_reserved_column_when_newly_enabled() {
        let reserved_schema = schema_ref! { nullable "_change_type": STRING };
        let before = MockTableConfigurationBuilder::new()
            .with_schema(reserved_schema.clone())
            .build();
        let after = MockTableConfigurationBuilder::new()
            .with_schema(reserved_schema)
            .with_properties([(ENABLE_CHANGE_DATA_FEED, "true")])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::ChangeDataFeed])
                    .build(),
            )
            .build();
        assert_result_error_with_message(
            prepare_protocol_for_update(&before, after, &[]),
            "reserved for Change Data Feed",
        );
    }

    #[test]
    fn validate_universal_format_rejects_both_iceberg_compat_versions() {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(test_schema_flat_with_column_mapping())
            .with_column_mapping(ColumnMappingMode::Name)
            .with_properties([
                (UNIVERSAL_FORMAT_ENABLED_FORMATS, "iceberg"),
                (ENABLE_ICEBERG_COMPAT_V2, "true"),
                (ENABLE_ICEBERG_COMPAT_V3, "true"),
                (ENABLE_ROW_TRACKING, "true"),
            ])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_reader_features([TableFeature::ColumnMapping])
                    .with_writer_features([
                        TableFeature::IcebergCompatV2,
                        TableFeature::IcebergCompatV3,
                        TableFeature::ColumnMapping,
                        TableFeature::RowTracking,
                        TableFeature::DomainMetadata,
                    ])
                    .build(),
            )
            .build();
        assert_result_error_with_message(
            validate_universal_format(&config),
            "cannot be enabled at the same time",
        );
    }

    #[test]
    fn reject_column_mapping_mode_change_detects_mode_mismatch() {
        let before = MockTableConfigurationBuilder::new().build();
        let metadata = metadata_with(&[(COLUMN_MAPPING_MODE, "name")]);
        assert_result_error_with_message(
            reject_column_mapping_mode_change(&before, &metadata),
            "Changing column mapping mode on an existing table is not supported",
        );
    }

    #[test]
    fn reject_column_mapping_mode_change_allows_unchanged_mode() {
        let before = MockTableConfigurationBuilder::new().build();
        let metadata = metadata_with(&[]);
        reject_column_mapping_mode_change(&before, &metadata).unwrap();
    }

    #[rstest]
    #[case::missing_mode(&[(ENABLE_ICEBERG_COMPAT_V2, "true")], "name' or 'id")]
    #[case::invalid_mode(
        &[(ENABLE_ICEBERG_COMPAT_V2, "true"), (COLUMN_MAPPING_MODE, "none")],
        "got 'none'"
    )]
    #[case::v3_missing_row_tracking(
        &[
            (ENABLE_ICEBERG_COMPAT_V3, "true"),
            (COLUMN_MAPPING_MODE, "name"),
        ],
        "requires 'delta.enableRowTracking' to be 'true'"
    )]
    #[case::v3_row_tracking_suspended(
        &[
            (ENABLE_ICEBERG_COMPAT_V3, "true"),
            (COLUMN_MAPPING_MODE, "name"),
            (ENABLE_ROW_TRACKING, "true"),
            (ROW_TRACKING_SUSPENDED, "true"),
        ],
        "cannot be enabled while 'delta.rowTrackingSuspended' is 'true'"
    )]
    #[case::native_v4_missing_mode(&[(ENABLE_ICEBERG_NATIVE_V4, "true")], "to be 'id'")]
    #[case::native_v4_wrong_mode(
        &[(ENABLE_ICEBERG_NATIVE_V4, "true"), (COLUMN_MAPPING_MODE, "name")],
        "got 'name'"
    )]
    #[case::native_v4_missing_row_tracking(
        &[
            (ENABLE_ICEBERG_NATIVE_V4, "true"),
            (COLUMN_MAPPING_MODE, "id"),
            (ENABLE_DELETION_VECTORS, "true"),
            (ENABLE_IN_COMMIT_TIMESTAMPS, "true"),
        ],
        "IcebergNativeV4 requires 'delta.enableRowTracking' to be 'true'"
    )]
    #[case::native_v4_row_tracking_suspended(
        &[
            (ENABLE_ICEBERG_NATIVE_V4, "true"),
            (COLUMN_MAPPING_MODE, "id"),
            (ENABLE_DELETION_VECTORS, "true"),
            (ENABLE_IN_COMMIT_TIMESTAMPS, "true"),
            (ENABLE_ROW_TRACKING, "true"),
            (ROW_TRACKING_SUSPENDED, "true"),
        ],
        "IcebergNativeV4 cannot be enabled while 'delta.rowTrackingSuspended' is 'true'"
    )]
    #[case::native_v4_dv_disabled(
        &[
            (ENABLE_ICEBERG_NATIVE_V4, "true"),
            (COLUMN_MAPPING_MODE, "id"),
            (ENABLE_DELETION_VECTORS, "false"),
            (ENABLE_ROW_TRACKING, "true"),
        ],
        "requires 'delta.enableDeletionVectors' to be 'true', got 'false'"
    )]
    fn apply_iceberg_compat_property_dependencies_rejects_invalid_metadata(
        #[case] props: &[(&str, &str)],
        #[case] expected: &str,
    ) {
        let mut metadata = metadata_with(props);
        assert_result_error_with_message(
            apply_iceberg_compat_property_dependencies(&mut metadata),
            expected,
        );
    }

    #[test]
    fn apply_iceberg_compat_property_dependencies_fills_native_v4_defaults() {
        let mut metadata = metadata_with(&[
            (ENABLE_ICEBERG_NATIVE_V4, "true"),
            (COLUMN_MAPPING_MODE, "id"),
            (ENABLE_ROW_TRACKING, "true"),
        ]);
        assert!(apply_iceberg_compat_property_dependencies(&mut metadata).unwrap());
        assert_eq!(
            metadata
                .configuration()
                .get(ENABLE_DELETION_VECTORS)
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            metadata
                .configuration()
                .get(ENABLE_IN_COMMIT_TIMESTAMPS)
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn apply_row_tracking_materialized_names_fills_missing_entries() {
        let before = MockTableConfigurationBuilder::new()
            .with_properties([(ENABLE_ROW_TRACKING, "true")])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::RowTracking, TableFeature::DomainMetadata])
                    .build(),
            )
            .build();
        assert!(before.is_feature_enabled(&TableFeature::RowTracking));

        let mut metadata = before.metadata().clone();
        assert!(
            !metadata
                .configuration()
                .contains_key(MATERIALIZED_ROW_ID_COLUMN_NAME)
        );
        assert!(
            !metadata
                .configuration()
                .contains_key(MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME)
        );

        assert!(apply_row_tracking_materialized_names(&before, &mut metadata).unwrap());
        assert!(
            metadata
                .configuration()
                .get(MATERIALIZED_ROW_ID_COLUMN_NAME)
                .is_some_and(|v| v.starts_with("_row-id-col-"))
        );
        assert!(
            metadata
                .configuration()
                .get(MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME)
                .is_some_and(|v| v.starts_with("_row-commit-version-col-"))
        );
    }

    #[test]
    fn apply_row_tracking_materialized_names_fills_only_missing_id() {
        let before = MockTableConfigurationBuilder::new()
            .with_properties([
                (ENABLE_ROW_TRACKING, "true"),
                (
                    MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME,
                    "_existing-version",
                ),
            ])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::RowTracking, TableFeature::DomainMetadata])
                    .build(),
            )
            .build();
        let mut metadata = before.metadata().clone();
        assert!(apply_row_tracking_materialized_names(&before, &mut metadata).unwrap());
        assert!(
            metadata
                .configuration()
                .contains_key(MATERIALIZED_ROW_ID_COLUMN_NAME)
        );
        assert_eq!(
            metadata
                .configuration()
                .get(MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME)
                .map(String::as_str),
            Some("_existing-version")
        );
    }

    #[test]
    fn apply_row_tracking_materialized_names_noop_when_row_tracking_disabled() {
        let before = MockTableConfigurationBuilder::new().build();
        let mut metadata = before.metadata().clone();
        assert!(!apply_row_tracking_materialized_names(&before, &mut metadata).unwrap());
    }

    #[test]
    fn extract_enabled_writer_features_returns_empty_for_unknown_writer_version() {
        // Writer version 8 is valid to construct but outside legacy (1..=6) and table-features (7).
        let protocol =
            Protocol::try_new(1, 8, None::<Vec<TableFeature>>, None::<Vec<TableFeature>>).unwrap();
        assert!(extract_enabled_writer_features(&protocol).is_empty());
    }

    #[test]
    fn protocol_with_features_preserves_existing_features_absent_from_input_lists() {
        let current = MockProtocolBuilder::new()
            .with_features([TableFeature::DeletionVectors, TableFeature::AppendOnly])
            .build();
        // Pass incomplete lists so the merge path must re-add existing features.
        let merged = protocol_with_features(
            &current,
            vec![TableFeature::V2Checkpoint],
            vec![TableFeature::V2Checkpoint, TableFeature::ChangeDataFeed],
        )
        .unwrap();
        let readers = merged.reader_features().unwrap();
        let writers = merged.writer_features().unwrap();
        assert!(readers.contains(&TableFeature::DeletionVectors));
        assert!(writers.contains(&TableFeature::DeletionVectors));
        assert!(writers.contains(&TableFeature::AppendOnly));
        assert!(readers.contains(&TableFeature::V2Checkpoint));
        assert!(writers.contains(&TableFeature::V2Checkpoint));
        assert!(writers.contains(&TableFeature::ChangeDataFeed));
    }

    #[test]
    fn with_ict_enablement_info_stamps_version_and_timestamp() {
        let stamped = with_ict_enablement_info(metadata_with(&[]), 7, 1_700_000_000_000);
        assert_eq!(
            stamped
                .configuration()
                .get(IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION)
                .map(String::as_str),
            Some("7")
        );
        assert_eq!(
            stamped
                .configuration()
                .get(IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP)
                .map(String::as_str),
            Some("1700000000000")
        );
    }

    #[test]
    fn transaction_enables_ict_requires_property_transition() {
        let without = MockTableConfigurationBuilder::new().build();
        let with_ict = MockTableConfigurationBuilder::new()
            .with_properties([(ENABLE_IN_COMMIT_TIMESTAMPS, "true")])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::InCommitTimestamp])
                    .build(),
            )
            .build();
        assert!(transaction_enables_ict(&without, &with_ict));
        assert!(!transaction_enables_ict(&with_ict, &with_ict));
        assert!(!transaction_enables_ict(&without, &without));
    }

    #[test]
    fn require_true_property_and_column_mapping_helpers_error_directly() {
        let metadata = metadata_with(&[(COLUMN_MAPPING_MODE, "none")]);
        assert_result_error_with_message(
            require_true_property(&metadata, ENABLE_ROW_TRACKING, "IcebergCompatV3"),
            "requires 'delta.enableRowTracking' to be 'true'",
        );
        assert_result_error_with_message(
            require_column_mapping_mode_name_or_id(&metadata, "IcebergCompatV2"),
            "got 'none'",
        );
        assert_result_error_with_message(
            require_column_mapping_mode(&metadata, "id", "IcebergNativeV4"),
            "got 'none'",
        );
        let empty = metadata_with(&[]);
        assert_result_error_with_message(
            require_column_mapping_mode_name_or_id(&empty, "IcebergCompatV2"),
            "to be 'name' or 'id'",
        );
        assert_result_error_with_message(
            require_column_mapping_mode(&empty, "id", "IcebergNativeV4"),
            "to be 'id'",
        );
    }
}
