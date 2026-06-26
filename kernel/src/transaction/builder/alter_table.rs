//! Builder for ALTER TABLE (schema evolution) transactions.
//!
//! This module contains [`AlterTableTransactionBuilder`], which uses a type-state pattern to
//! enforce valid operation chaining at compile time.
//!
//! # Type States
//!
//! - [`Ready`]: Initial state. Operations are available, but `build()` is not (at least one
//!   operation is required).
//! - [`Modifying`]: After any chainable schema operation. More ops can be chained, and `build()` is
//!   available. See [`AlterTableTransactionBuilder<Modifying>`] for ops.
//!
//! # Transitions
//!
//! Each `impl` block below is gated by a state bound and documents which operations that
//! state enables. Chainable schema operations live on `impl<S: Chainable>` and transition
//! the builder to a chainable state; `build()` lives on states that are buildable.
//!
//! ```ignore
//! // Allowed: at least one op queued before build().
//! snapshot.alter_table().add_column(field).build(engine, committer)?;
//!
//! // Not allowed: build() is not defined on Ready (no ops queued).
//! snapshot.alter_table().build(engine, committer)?;  // compile error
//! ```

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::actions::Protocol;
use crate::committer::{Committer, ProtocolMetadataIntent};
use crate::expressions::ColumnName;
use crate::schema::StructField;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    auto_enable_property_driven_features, Operation, TableFeature,
    TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use crate::table_properties::{
    TableProperties, COLUMN_MAPPING_MAX_COLUMN_ID, COLUMN_MAPPING_MODE, DELTA_PROPERTY_PREFIX,
    ENABLE_ICEBERG_COMPAT_V3, ENABLE_IN_COMMIT_TIMESTAMPS, ENABLE_ROW_TRACKING,
};
use crate::transaction::alter_table::AlterTableTransaction;
use crate::transaction::builder::create_table::{ALLOWED_DELTA_FEATURES, ALLOWED_DELTA_PROPERTIES};
use crate::transaction::schema_evolution::{
    apply_schema_operations, SchemaEvolutionResult, SchemaOperation,
};
use crate::{DeltaResult, Engine, Error};

/// Initial state: `build()` is not yet available (at least one operation is required).
/// See [`Chainable`] for the operations available on this state.
pub struct Ready;

/// State after at least one operation has been added. `build()` is available.
/// See [`Chainable`] for the operations available on this state.
pub struct Modifying;

/// Marker trait for builder states that accept chainable schema operations. Grouping states
/// under one bound lets each op (like `add_column`) live on a single `impl<S: Chainable>`
/// block -- chainable states share the body rather than duplicating it per state.
///
/// Sealed: external types cannot implement this, keeping the set of chainable states closed.
pub trait Chainable: sealed::Sealed {}
impl Chainable for Ready {}
impl Chainable for Modifying {}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Ready {}
    impl Sealed for super::Modifying {}
}

/// A table-property change queued by the builder. Kept separate from [`SchemaOperation`] so
/// schema evolution stays property-agnostic; applied to the evolved metadata's configuration
/// in `build()`.
enum PropertyOperation {
    Set { key: String, value: String },
    Unset { key: String },
}

/// Builder for constructing an [`AlterTableTransaction`] with schema evolution operations.
///
/// Uses a type-state pattern (`S`) to enforce at compile time:
/// - At least one schema operation must be queued before `build()` is callable.
/// - Only operations valid for the current state can be chained, disallowing incompatible chaining.
pub struct AlterTableTransactionBuilder<S = Ready> {
    snapshot: SnapshotRef,
    operations: Vec<SchemaOperation>,
    property_operations: Vec<PropertyOperation>,
    // PhantomData marker for builder state (Ready or Modifying).
    // Zero-sized; only affects which methods are available at compile time.
    _state: PhantomData<S>,
}

impl<S> AlterTableTransactionBuilder<S> {
    // Reconstructs the builder with a different PhantomData marker, changing which methods
    // are available at compile time (e.g. Ready -> Modifying enables `build()`). All real
    // fields are moved as-is; only the zero-sized type state changes.
    //
    // `T` (distinct from the struct's `S`) lets the caller pick the target state:
    // `self.transition::<Modifying>()` returns `AlterTableTransactionBuilder<Modifying>`.
    fn transition<T>(self) -> AlterTableTransactionBuilder<T> {
        AlterTableTransactionBuilder {
            snapshot: self.snapshot,
            operations: self.operations,
            property_operations: self.property_operations,
            _state: PhantomData,
        }
    }
}

impl AlterTableTransactionBuilder<Ready> {
    /// Create a new builder from a snapshot.
    pub(crate) fn new(snapshot: SnapshotRef) -> Self {
        AlterTableTransactionBuilder {
            snapshot,
            operations: Vec::new(),
            property_operations: Vec::new(),
            _state: PhantomData,
        }
    }
}

impl<S: Chainable> AlterTableTransactionBuilder<S> {
    /// Add a new top-level column to the table schema.
    ///
    /// The field must not already exist in the schema (case-insensitive). The field must be
    /// nullable because existing data files do not contain this column and will read NULL for it.
    /// `field` and any of its nested fields must not carry `delta.columnMapping.id` or
    /// `delta.columnMapping.physicalName` annotations.
    ///
    /// These constraints are validated during [`build()`](AlterTableTransactionBuilder::build).
    pub fn add_column(mut self, field: StructField) -> AlterTableTransactionBuilder<Modifying> {
        self.operations.push(SchemaOperation::AddColumn { field });
        self.transition()
    }

    /// Change a column's nullability from NOT NULL to nullable. If the column is already
    /// nullable, the op is a no-op but still generates a commit.
    ///
    /// Note: this matches Spark's behavior.
    pub fn set_nullable(mut self, column: ColumnName) -> AlterTableTransactionBuilder<Modifying> {
        self.operations
            .push(SchemaOperation::SetNullable { column });
        self.transition()
    }

    /// Set a table property (`metaData.configuration`). The change is applied to the evolved
    /// metadata in [`build()`](AlterTableTransactionBuilder::build) and captured as commit intent.
    pub fn set_table_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> AlterTableTransactionBuilder<Modifying> {
        self.property_operations.push(PropertyOperation::Set {
            key: key.into(),
            value: value.into(),
        });
        self.transition()
    }

    /// Remove a table property (`metaData.configuration`). The change is applied to the evolved
    /// metadata in [`build()`](AlterTableTransactionBuilder::build) and captured as commit intent.
    pub fn unset_table_property(
        mut self,
        key: impl Into<String>,
    ) -> AlterTableTransactionBuilder<Modifying> {
        self.property_operations
            .push(PropertyOperation::Unset { key: key.into() });
        self.transition()
    }
}

impl AlterTableTransactionBuilder<Modifying> {
    /// Validate and apply schema operations, then build the [`AlterTableTransaction`].
    ///
    /// This method:
    /// 1. Validates the table supports writes
    /// 2. Applies each operation sequentially against the evolving schema
    /// 3. Constructs new Metadata action with evolved schema
    /// 4. Builds the evolved table configuration
    /// 5. Creates the transaction
    ///
    /// # Errors
    ///
    /// - Any individual operation fails validation (see per-method errors above)
    /// - Table does not support writes (unsupported features)
    /// - The evolved schema requires protocol features not enabled on the table (e.g. adding a
    ///   `timestampNtz` column without the `timestampNtz` feature)
    pub fn build(
        self,
        _engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<AlterTableTransaction> {
        let table_config = self.snapshot.table_configuration();
        // We don't support ALTER TABLE on tables with icebergCompatV3 enabled yet. See
        // [`crate::table_features::ICEBERG_COMPAT_V3_INFO`] for the tracking issue.
        if table_config.is_feature_enabled(&TableFeature::IcebergCompatV3) {
            return Err(Error::unsupported(
                "ALTER TABLE is not yet supported on tables with icebergCompatV3 enabled",
            ));
        }
        // Rejects writes to tables kernel can't safely commit to: writer version out of
        // kernel's supported range, unsupported writer features, or schemas with SQL-expression
        // invariants. Runs on the pre-alter snapshot; future ALTER variants that change the
        // protocol must also re-check this on the evolved `TableConfiguration`.
        table_config.ensure_operation_supported(Operation::Write)?;

        // Collapse property ops per key (last-write-wins) so a key set then unset (or vice versa)
        // yields a single op matching the final evolved configuration. Used for both intent capture
        // and metadata application below, keeping the two in lockstep.
        let property_operations = collapse_property_operations(self.property_operations);

        // Reject `delta.*` Set ops ALTER can't safely apply, before they reach the metadata. Unset
        // ops and user-namespace keys pass through unchanged.
        for op in &property_operations {
            if let PropertyOperation::Set { key, .. } = op {
                validate_set_delta_property(key)?;
            }
        }

        // Capture schema + property ops as commit intents so a committer can read the change
        // directly instead of diffing read vs new P&M. Feature intents are appended below once the
        // evolved protocol is known.
        let mut intents: Vec<ProtocolMetadataIntent> = self
            .operations
            .iter()
            .map(|op| match op {
                SchemaOperation::AddColumn { field } => ProtocolMetadataIntent::AddColumn {
                    field: field.clone(),
                },
                SchemaOperation::SetNullable { column } => ProtocolMetadataIntent::SetNullable {
                    column: column.clone(),
                },
            })
            .collect();
        for op in &property_operations {
            intents.push(match op {
                PropertyOperation::Set { key, value } => ProtocolMetadataIntent::SetProperty {
                    key: key.clone(),
                    value: value.clone(),
                },
                PropertyOperation::Unset { key } => {
                    ProtocolMetadataIntent::UnsetProperty { key: key.clone() }
                }
            });
        }

        let schema = Arc::unwrap_or_clone(table_config.logical_schema());
        let column_mapping_mode = table_config.column_mapping_mode();
        let current_max_column_id = table_config.table_properties().column_mapping_max_column_id;
        let SchemaEvolutionResult {
            schema: evolved_schema,
            new_max_column_id,
        } = apply_schema_operations(
            schema,
            self.operations,
            column_mapping_mode,
            current_max_column_id,
        )?;

        let mut evolved_metadata = table_config
            .metadata()
            .clone()
            .with_schema(evolved_schema.clone())?;
        if let Some(id) = new_max_column_id {
            evolved_metadata = evolved_metadata
                .with_configuration_entry(COLUMN_MAPPING_MAX_COLUMN_ID, id.to_string());
        }
        for op in &property_operations {
            evolved_metadata = match op {
                PropertyOperation::Set { key, value } => {
                    evolved_metadata.with_configuration_entry(key, value)
                }
                PropertyOperation::Unset { key } => {
                    evolved_metadata.without_configuration_entry(key)
                }
            };
        }

        // TODO(#2446): enforce partition-column immutability for non-create ALTERs.
        // Promote the protocol for any property-driven feature the alter just set (e.g.
        // `delta.enableChangeDataFeed=true` enables `changeDataFeed`). Feature-based protocols
        // only; legacy protocols are returned unchanged.
        let read_protocol = table_config.protocol();

        // A legacy (non-(3,7)) protocol cannot record table features, so promoting one via a
        // property-enablement key (e.g. `delta.enableChangeDataFeed=true`) would leave the protocol
        // inconsistent. Reject it. Only keys this ALTER sets are considered; pre-existing config
        // carried over from the read snapshot is left alone. Catalog-managed tables are always
        // (3,7) and thus unaffected.
        if !is_table_features_protocol(read_protocol) {
            let set_props: HashMap<String, String> = property_operations
                .iter()
                .filter_map(|op| match op {
                    PropertyOperation::Set { key, value } => Some((key.clone(), value.clone())),
                    PropertyOperation::Unset { .. } => None,
                })
                .collect();
            if let Some(key) = first_feature_enabling_property(&set_props) {
                return Err(Error::unsupported(format!(
                    "Enabling a table feature via '{key}' requires a (3,7) table-features protocol"
                )));
            }
        }

        let evolved_protocol = read_protocol.with_property_driven_features(
            ALLOWED_DELTA_FEATURES,
            &evolved_metadata.parse_table_properties(),
        )?;

        // Validates the evolved metadata against the (possibly promoted) protocol.
        let evolved_table_config = TableConfiguration::try_new_with_schema(
            table_config,
            evolved_metadata,
            evolved_protocol,
            evolved_schema,
        )?;

        // Capture feature-enablement intent: kernel owns enablement, so this single set-difference
        // (evolved features minus read features) is done once here rather than diffed in the
        // committer. No-op when the alter enables no new feature.
        for feature in newly_enabled_features(read_protocol, evolved_table_config.protocol()) {
            intents.push(ProtocolMetadataIntent::EnableFeature { feature });
        }

        // Emit a Protocol action (and populate new_protocol) whenever the evolved protocol differs
        // from the read protocol, so the log records the change and a catalog committer can forward
        // it. Otherwise the Protocol action is omitted (metadata-only commit).
        let emit_protocol = evolved_table_config.protocol() != read_protocol;

        AlterTableTransaction::try_new_alter_table(
            self.snapshot,
            evolved_table_config,
            committer,
            intents,
            emit_protocol,
        )
    }
}

/// Collapse property operations per key (last-write-wins): each key yields one op carrying its
/// final value, kept at the key's first-seen position. A key set then unset (or unset then set)
/// ends up with a single op reflecting the final intent, so emitted intents match the evolved
/// configuration. The relative order of distinct keys is immaterial since metadata application is
/// keyed.
fn collapse_property_operations(ops: Vec<PropertyOperation>) -> Vec<PropertyOperation> {
    // Index of each key's surviving op in the output, so a later op for the same key overwrites it
    // in place rather than appending a duplicate.
    let mut index_by_key: HashMap<String, usize> = HashMap::new();
    let mut collapsed: Vec<PropertyOperation> = Vec::new();
    for op in ops {
        let key = match &op {
            PropertyOperation::Set { key, .. } | PropertyOperation::Unset { key } => key.clone(),
        };
        match index_by_key.get(&key) {
            Some(&i) => collapsed[i] = op,
            None => {
                index_by_key.insert(key, collapsed.len());
                collapsed.push(op);
            }
        }
    }
    collapsed
}

/// `delta.*` Set ops ALTER TABLE cannot safely apply: each needs handling the ALTER path does not
/// perform: column-mapping physical-name/maxColumnId assignment, row-id materialization,
/// icebergCompatV3 feature cascade, and in-commit-timestamp enablement (post-creation ICT must
/// record the enablement version and timestamp companions, or readers misdate earlier versions).
const ALTER_DENIED_DELTA_PROPERTIES: &[&str] = &[
    COLUMN_MAPPING_MODE,
    ENABLE_ROW_TRACKING,
    ENABLE_ICEBERG_COMPAT_V3,
    ENABLE_IN_COMMIT_TIMESTAMPS,
];

/// Validate a single `delta.*` property `Set` op for ALTER TABLE. User-namespace keys are allowed
/// unconditionally; this only constrains the `delta.` namespace.
///
/// # Errors
///
/// Rejects keys in [`ALTER_DENIED_DELTA_PROPERTIES`] (require create-time transforms) and any
/// `delta.*` key not in [`ALLOWED_DELTA_PROPERTIES`].
fn validate_set_delta_property(key: &str) -> DeltaResult<()> {
    if !key.starts_with(DELTA_PROPERTY_PREFIX) {
        return Ok(());
    }
    if ALTER_DENIED_DELTA_PROPERTIES.contains(&key) {
        return Err(Error::unsupported(format!(
            "Setting delta property '{key}' via ALTER TABLE is not supported (requires \
             create-time handling)"
        )));
    }
    if !ALLOWED_DELTA_PROPERTIES.contains(&key) {
        return Err(Error::generic(format!(
            "'{key}' is not a recognized delta table property (via ALTER TABLE)"
        )));
    }
    Ok(())
}

/// Whether `protocol` is a (3,7) table-features protocol that can record explicit table features.
fn is_table_features_protocol(protocol: &Protocol) -> bool {
    protocol.min_reader_version() == TABLE_FEATURES_MIN_READER_VERSION
        && protocol.min_writer_version() == TABLE_FEATURES_MIN_WRITER_VERSION
}

/// The first key in `configuration` that would promote a property-driven feature (the
/// `delta.enable*`-style keys `auto_enable_property_driven_features` acts on), if any. Each key is
/// tested in isolation so the returned key is the one responsible. Used to reject feature
/// enablement on legacy protocols that cannot record table features.
fn first_feature_enabling_property(configuration: &HashMap<String, String>) -> Option<String> {
    configuration
        .iter()
        .find(|(key, value)| {
            let single = TableProperties::from(std::iter::once((key.as_str(), value.as_str())));
            let mut reader = Vec::new();
            let mut writer = Vec::new();
            auto_enable_property_driven_features(
                ALLOWED_DELTA_FEATURES,
                &single,
                &mut reader,
                &mut writer,
            );
            !reader.is_empty() || !writer.is_empty()
        })
        .map(|(key, _)| key.clone())
}

/// Returns the reader+writer feature names present in `new` but not in `read`, in a stable order
/// (reader features first, then writer features). A feature listed in both reader and writer
/// yields one intent per list, matching how the protocol stores it.
fn newly_enabled_features(read: &Protocol, new: &Protocol) -> Vec<String> {
    fn feature_names(protocol: &Protocol) -> impl Iterator<Item = &str> {
        let reader = protocol.reader_features().into_iter().flatten();
        let writer = protocol.writer_features().into_iter().flatten();
        reader.chain(writer).map(|f| f.as_ref())
    }
    let read_features: HashSet<&str> = feature_names(read).collect();
    feature_names(new)
        .filter(|name| !read_features.contains(name))
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(key: &str, value: &str) -> PropertyOperation {
        PropertyOperation::Set {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn unset(key: &str) -> PropertyOperation {
        PropertyOperation::Unset {
            key: key.to_string(),
        }
    }

    fn describe(ops: &[PropertyOperation]) -> Vec<(String, Option<String>)> {
        ops.iter()
            .map(|op| match op {
                PropertyOperation::Set { key, value } => (key.clone(), Some(value.clone())),
                PropertyOperation::Unset { key } => (key.clone(), None),
            })
            .collect()
    }

    #[test]
    fn collapse_set_then_unset_same_key_keeps_only_unset() {
        let collapsed = collapse_property_operations(vec![set("k", "v"), unset("k")]);
        assert_eq!(describe(&collapsed), vec![("k".to_string(), None)]);
    }

    #[test]
    fn collapse_unset_then_set_same_key_keeps_only_set() {
        let collapsed = collapse_property_operations(vec![unset("k"), set("k", "v2")]);
        assert_eq!(
            describe(&collapsed),
            vec![("k".to_string(), Some("v2".to_string()))]
        );
    }

    #[test]
    fn collapse_keeps_last_value_at_first_seen_position() {
        // a, b, then a again: a survives at its first-seen position with the last value.
        let collapsed =
            collapse_property_operations(vec![set("a", "1"), set("b", "2"), set("a", "3")]);
        assert_eq!(
            describe(&collapsed),
            vec![
                ("a".to_string(), Some("3".to_string())),
                ("b".to_string(), Some("2".to_string())),
            ]
        );
    }

    #[test]
    fn newly_enabled_features_lists_reader_writer_feature_in_both_lists() {
        let read = Protocol::try_new_modern(Vec::<&str>::new(), Vec::<&str>::new()).unwrap();
        // deletionVectors is a reader+writer feature; changeDataFeed is writer-only.
        let new = Protocol::try_new_modern(
            vec!["deletionVectors"],
            vec!["deletionVectors", "changeDataFeed"],
        )
        .unwrap();
        let enabled = newly_enabled_features(&read, &new);
        // deletionVectors appears once per list (reader + writer) and changeDataFeed once (writer).
        assert_eq!(
            enabled,
            vec![
                "deletionVectors".to_string(),
                "deletionVectors".to_string(),
                "changeDataFeed".to_string(),
            ]
        );
    }

    #[test]
    fn newly_enabled_features_empty_when_protocols_match() {
        let p = Protocol::try_new_modern(vec!["deletionVectors"], vec!["deletionVectors"]).unwrap();
        assert!(newly_enabled_features(&p, &p).is_empty());
    }
}
