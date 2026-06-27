//! Translate kernel `ProtocolMetadataIntent`s into typed UC `Update`s.
//!
//! The kernel captures what an ALTER changed as explicit intents on the
//! `CommitMetadata`, so the committer never diffs read vs new protocol/metadata. This module is a
//! pure translation of those intents into the typed [`Update`]s the UC API understands
//! (`SetProtocol`, `SetProperties`, `RemoveProperties`, `SetColumns`), plus the domain-metadata
//! mirror. Whole-value payloads (the `SetProtocol` protocol and the `SetColumns` schema) are read
//! from `new_protocol`/`new_metadata`, which carry the committed value; the intents only say
//! *what* changed.

use std::collections::HashMap;

use delta_kernel::committer::{CommitMetadata, ProtocolMetadataIntent};
use delta_kernel::schema::StructType;
use delta_kernel::{DeltaResult, Error};
use unity_catalog_delta_client_api::Update;

use crate::conversions::{parse_or_string, to_api_protocol, UC_MIRRORED_DOMAINS};

/// Derive the typed `Update`s implied by `commit_metadata`'s captured intents and domain
/// metadata changes.
///
/// Returns updates in the order: `SetProtocol` (if any feature was enabled), `SetProperties`,
/// `RemoveProperties`, `SetColumns` (if the schema changed), then domain updates: at most one
/// `SetDomainMetadata` and at most one `RemoveDomainMetadata`, each covering only the
/// UC-mirrored domains (see [`UC_MIRRORED_DOMAINS`]); user domains are left in the commit log.
///
/// # Errors
///
/// Returns an error if a `SetProtocol`/`SetColumns` is implied but the corresponding whole-value
/// payload (`new_protocol`/`new_metadata`) is missing, or if the schema fails to serialize.
pub fn derive_updates(commit_metadata: &CommitMetadata) -> DeltaResult<Vec<Update>> {
    let intents = commit_metadata.protocol_metadata_intents();

    let mut enable_feature = false;
    let mut set_properties: HashMap<String, String> = HashMap::new();
    let mut remove_properties: Vec<String> = Vec::new();
    let mut set_columns = false;
    for intent in intents {
        match intent {
            ProtocolMetadataIntent::EnableFeature { .. } => enable_feature = true,
            ProtocolMetadataIntent::SetProperty { key, value } => {
                set_properties.insert(key.clone(), value.clone());
            }
            ProtocolMetadataIntent::UnsetProperty { key } => remove_properties.push(key.clone()),
            ProtocolMetadataIntent::AddColumn { .. }
            | ProtocolMetadataIntent::SetNullable { .. } => set_columns = true,
        }
    }
    // Ordering is immaterial to the server; sorting a Vec just yields stable, debuggable bodies.
    // The HashMap-backed SetProperties has no cheap stable order, so it is left as-is.
    remove_properties.sort();

    let mut updates = Vec::new();
    if enable_feature {
        updates.push(protocol_update(commit_metadata)?);
    }
    if !set_properties.is_empty() {
        updates.push(Update::SetProperties {
            updates: set_properties,
        });
    }
    if !remove_properties.is_empty() {
        updates.push(Update::RemoveProperties {
            removals: remove_properties,
        });
    }
    if set_columns {
        updates.push(column_update(commit_metadata)?);
    }
    updates.extend(derive_domain_updates(commit_metadata));

    Ok(updates)
}

/// Build a `SetProtocol` from the committed `new_protocol` whole value.
fn protocol_update(commit_metadata: &CommitMetadata) -> DeltaResult<Update> {
    let new = commit_metadata.new_protocol().ok_or_else(|| {
        Error::generic("EnableFeature intent present but commit has no new_protocol")
    })?;
    Ok(Update::SetProtocol {
        protocol: to_api_protocol(new),
    })
}

/// Build a whole-schema `SetColumns` from the committed `new_metadata` schema.
fn column_update(commit_metadata: &CommitMetadata) -> DeltaResult<Update> {
    let new = commit_metadata
        .new_metadata()
        .ok_or_else(|| Error::generic("schema intent present but commit has no new_metadata"))?;
    set_columns_from_schema(&new.parse_schema()?)
}

/// Build a whole-schema `SetColumns` update from the given schema.
fn set_columns_from_schema(schema: &StructType) -> DeltaResult<Update> {
    let columns = serde_json::to_value(schema)
        .map_err(|e| Error::generic(format!("failed to serialize schema for SetColumns: {e}")))?;
    Ok(Update::SetColumns { columns })
}

fn derive_domain_updates(commit_metadata: &CommitMetadata) -> Vec<Update> {
    let mut set: HashMap<String, serde_json::Value> = HashMap::new();
    let mut removals: Vec<String> = Vec::new();
    for dm in commit_metadata.domain_metadata_changes() {
        // Forward only UC-mirrored domains. Others live solely in the staged commit file;
        // sending them here would fail the whole commit server-side.
        if !UC_MIRRORED_DOMAINS.contains(&dm.domain()) {
            continue;
        }
        if dm.is_removed() {
            removals.push(dm.domain().to_string());
        } else {
            set.insert(dm.domain().to_string(), parse_or_string(dm.configuration()));
        }
    }

    removals.sort();

    let mut updates = Vec::new();
    if !set.is_empty() {
        updates.push(Update::SetDomainMetadata { updates: set });
    }
    if !removals.is_empty() {
        updates.push(Update::RemoveDomainMetadata { domains: removals });
    }
    updates
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use delta_kernel::actions::{Metadata, Protocol as KernelProtocol};
    use delta_kernel::committer::{CommitMetadata, ProtocolMetadataIntent};
    use delta_kernel::schema::{DataType, StructField, StructType};
    use rstest::rstest;
    use unity_catalog_delta_client_api::Update;

    use super::*;

    fn table_root() -> url::Url {
        url::Url::parse("memory:///test_table/").unwrap()
    }

    fn base_metadata() -> CommitMetadata {
        CommitMetadata::new_unchecked(table_root(), 1).unwrap()
    }

    fn metadata_with_config_and_schema(
        config: HashMap<String, String>,
        schema: Arc<StructType>,
    ) -> Metadata {
        Metadata::try_new(None, None, schema, vec![], 0, config).unwrap()
    }

    fn metadata_with_schema(schema: Arc<StructType>) -> Metadata {
        metadata_with_config_and_schema(HashMap::new(), schema)
    }

    fn set_property(key: &str, value: &str) -> ProtocolMetadataIntent {
        ProtocolMetadataIntent::SetProperty {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn unset_property(key: &str) -> ProtocolMetadataIntent {
        ProtocolMetadataIntent::UnsetProperty {
            key: key.to_string(),
        }
    }

    fn add_column(name: &str) -> ProtocolMetadataIntent {
        ProtocolMetadataIntent::AddColumn {
            field: StructField::nullable(name, DataType::STRING),
        }
    }

    fn enable_feature(feature: &str) -> ProtocolMetadataIntent {
        ProtocolMetadataIntent::EnableFeature {
            feature: feature.to_string(),
        }
    }

    fn with_intents(cm: CommitMetadata, intents: Vec<ProtocolMetadataIntent>) -> CommitMetadata {
        cm.with_protocol_metadata_intents_for_test(intents)
    }

    #[test]
    fn derive_updates_emits_set_properties_from_set_property_intents() {
        let cm = with_intents(
            base_metadata(),
            vec![set_property("a", "1"), set_property("b", "2")],
        );

        let updates = derive_updates(&cm).unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            Update::SetProperties { updates } => {
                assert_eq!(updates.len(), 2);
                assert_eq!(updates.get("a"), Some(&"1".to_string()));
                assert_eq!(updates.get("b"), Some(&"2".to_string()));
            }
            other => panic!("expected SetProperties, got {other:?}"),
        }
    }

    #[test]
    fn derive_updates_emits_remove_properties_from_unset_property_intents() {
        let cm = with_intents(
            base_metadata(),
            vec![unset_property("b"), unset_property("a")],
        );

        let updates = derive_updates(&cm).unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            // Removals are sorted for a deterministic request body.
            Update::RemoveProperties { removals } => {
                assert_eq!(removals, &vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected RemoveProperties, got {other:?}"),
        }
    }

    #[test]
    fn derive_updates_emits_both_set_and_remove_properties() {
        let cm = with_intents(
            base_metadata(),
            vec![set_property("a", "2"), unset_property("b")],
        );

        let updates = derive_updates(&cm).unwrap();
        assert_eq!(updates.len(), 2);
        match &updates[0] {
            Update::SetProperties { updates } => {
                assert_eq!(updates.get("a"), Some(&"2".to_string()));
            }
            other => panic!("expected SetProperties, got {other:?}"),
        }
        match &updates[1] {
            Update::RemoveProperties { removals } => assert_eq!(removals, &vec!["b".to_string()]),
            other => panic!("expected RemoveProperties, got {other:?}"),
        }
    }

    #[test]
    fn derive_updates_no_intents_emits_nothing() {
        assert!(derive_updates(&base_metadata()).unwrap().is_empty());
    }

    #[test]
    fn derive_updates_emits_set_protocol_on_enable_feature_intent() {
        let new = KernelProtocol::try_new_modern(
            vec!["catalogManaged"],
            vec!["catalogManaged", "changeDataFeed"],
        )
        .unwrap();
        let cm = with_intents(
            base_metadata().with_new_protocol(new),
            vec![enable_feature("changeDataFeed")],
        );

        let updates = derive_updates(&cm).unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            // The whole protocol value comes from new_protocol, not the intent.
            Update::SetProtocol { protocol } => {
                assert!(protocol
                    .writer_features
                    .contains(&"changeDataFeed".to_string()));
                assert!(protocol
                    .writer_features
                    .contains(&"catalogManaged".to_string()));
            }
            other => panic!("expected SetProtocol, got {other:?}"),
        }
    }

    #[test]
    fn derive_updates_enable_feature_without_new_protocol_errors() {
        let cm = with_intents(base_metadata(), vec![enable_feature("changeDataFeed")]);
        let err = derive_updates(&cm).unwrap_err();
        assert!(err.to_string().contains("new_protocol"), "got: {err}");
    }

    #[rstest]
    #[case::add_column(add_column("region"))]
    #[case::set_nullable(ProtocolMetadataIntent::SetNullable {
        column: delta_kernel::expressions::ColumnName::new(["id"]),
    })]
    fn derive_updates_emits_set_columns_on_schema_intent(#[case] intent: ProtocolMetadataIntent) {
        let new_schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("id", DataType::INTEGER),
                StructField::nullable("region", DataType::STRING),
            ])
            .unwrap(),
        );
        let cm = with_intents(
            base_metadata().with_new_metadata(metadata_with_schema(new_schema)),
            vec![intent],
        );

        let updates = derive_updates(&cm).unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            // The whole schema comes from new_metadata, not the intent.
            Update::SetColumns { columns } => {
                assert_eq!(columns.get("type").and_then(|v| v.as_str()), Some("struct"));
                let fields = columns
                    .get("fields")
                    .and_then(|v| v.as_array())
                    .expect("fields should be a JSON array");
                assert_eq!(fields.len(), 2);
            }
            other => panic!("expected SetColumns, got {other:?}"),
        }
    }

    #[test]
    fn derive_updates_schema_intent_without_new_metadata_errors() {
        let cm = with_intents(base_metadata(), vec![add_column("region")]);
        let err = derive_updates(&cm).unwrap_err();
        assert!(err.to_string().contains("new_metadata"), "got: {err}");
    }

    #[test]
    fn derive_updates_emits_all_on_mixed_commit() {
        // One ALTER carrying a property change, a schema change, and a feature enablement must
        // produce SetProtocol, SetProperties, and SetColumns in that order.
        let new_protocol = KernelProtocol::try_new_modern(
            vec!["catalogManaged"],
            vec!["catalogManaged", "changeDataFeed"],
        )
        .unwrap();
        let new_schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("id", DataType::INTEGER),
                StructField::nullable("added", DataType::STRING),
            ])
            .unwrap(),
        );
        let cm = with_intents(
            base_metadata()
                .with_new_protocol(new_protocol)
                .with_new_metadata(metadata_with_schema(new_schema)),
            vec![
                enable_feature("changeDataFeed"),
                set_property("delta.enableChangeDataFeed", "true"),
                add_column("added"),
            ],
        );

        let updates = derive_updates(&cm).unwrap();
        let names: Vec<&str> = updates.iter().map(update_name).collect();
        assert_eq!(names, vec!["SetProtocol", "SetProperties", "SetColumns"]);
    }

    #[rstest]
    #[case::json_object(r#"{"clusteringColumns":[["x"]]}"#)]
    #[case::plain_string("opaque-blob")]
    fn derive_updates_emits_set_domain_metadata(#[case] payload: &str) {
        let cm = base_metadata().with_domain_change_payload("delta.clustering", payload);
        let updates = derive_updates(&cm).unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            Update::SetDomainMetadata { updates } => {
                let parsed: serde_json::Value = serde_json::from_str(payload)
                    .unwrap_or_else(|_| serde_json::Value::String(payload.to_string()));
                assert_eq!(updates.get("delta.clustering"), Some(&parsed));
                assert_eq!(updates.len(), 1);
            }
            other => panic!("expected SetDomainMetadata, got {other:?}"),
        }
    }

    #[test]
    fn derive_updates_emits_remove_domain_metadata_for_tombstone() {
        let cm = base_metadata().with_domain_removal("delta.clustering");
        let updates = derive_updates(&cm).unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            Update::RemoveDomainMetadata { domains } => {
                assert_eq!(domains, &vec!["delta.clustering".to_string()]);
            }
            other => panic!("expected RemoveDomainMetadata, got {other:?}"),
        }
    }

    #[test]
    fn derive_updates_emits_set_for_empty_non_removed_domain_config() {
        // Empty configuration is NOT a removal; only the tombstone flag means remove.
        let cm = base_metadata().with_domain_change("delta.clustering");
        let updates = derive_updates(&cm).unwrap();
        assert_eq!(updates.len(), 1);
        match &updates[0] {
            Update::SetDomainMetadata { updates } => {
                assert!(updates.contains_key("delta.clustering"));
            }
            other => panic!("expected SetDomainMetadata, got {other:?}"),
        }
    }

    #[test]
    fn derive_updates_skips_unmirrored_user_domains() {
        // User domains live in the staged commit file; the UC server rejects them in typed
        // updates, so they must not be forwarded. A set and a removal both drop out.
        let cm = base_metadata()
            .with_domain_change("user.custom")
            .with_domain_removal("user.other");
        let updates = derive_updates(&cm).unwrap();
        assert!(updates.is_empty());
    }

    #[test]
    fn derive_updates_orders_updates_correctly() {
        let new_protocol = KernelProtocol::try_new_modern(
            vec!["catalogManaged"],
            vec!["catalogManaged", "changeDataFeed"],
        )
        .unwrap();
        let new_schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("id", DataType::INTEGER),
                StructField::nullable("added", DataType::STRING),
            ])
            .unwrap(),
        );

        let cm = with_intents(
            base_metadata()
                .with_new_protocol(new_protocol)
                .with_new_metadata(metadata_with_schema(new_schema))
                .with_domain_change_payload("delta.clustering", r#"{"clusteringColumns":[]}"#),
            vec![
                enable_feature("changeDataFeed"),
                set_property("added_prop", "y"),
                unset_property("drop_me"),
                add_column("added"),
            ],
        );

        let updates = derive_updates(&cm).unwrap();
        let names: Vec<&str> = updates.iter().map(update_name).collect();
        assert_eq!(
            names,
            vec![
                "SetProtocol",
                "SetProperties",
                "RemoveProperties",
                "SetColumns",
                "SetDomainMetadata",
            ]
        );
    }

    fn update_name(u: &Update) -> &'static str {
        match u {
            Update::SetProtocol { .. } => "SetProtocol",
            Update::SetProperties { .. } => "SetProperties",
            Update::RemoveProperties { .. } => "RemoveProperties",
            Update::SetColumns { .. } => "SetColumns",
            Update::SetTableComment { .. } => "SetTableComment",
            Update::SetPartitionColumns { .. } => "SetPartitionColumns",
            Update::UpdateMetadataSnapshotVersion { .. } => "UpdateMetadataSnapshotVersion",
            Update::SetDomainMetadata { .. } => "SetDomainMetadata",
            Update::RemoveDomainMetadata { .. } => "RemoveDomainMetadata",
            Update::AddCommit { .. } => "AddCommit",
            Update::SetLatestBackfilledVersion { .. } => "SetLatestBackfilledVersion",
        }
    }
}
