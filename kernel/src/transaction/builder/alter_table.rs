//! Builder for ALTER TABLE transactions.
//!
//! This module contains [`AlterTableTransactionBuilder`], which uses a type-state pattern to
//! enforce valid operation chaining at compile time.
//!
//! # Type States
//!
//! - [`Ready`]: Initial state. Operations are available, but `build()` is not (at least one
//!   operation is required).
//! - [`Modifying`]: After any chainable operation. More ops can be chained, and `build()` is
//!   available. See [`AlterTableTransactionBuilder<Modifying>`] for ops.
//!
//! # Transitions
//!
//! Each `impl` block below is gated by a state bound and documents which operations that
//! state enables. Chainable operations live on `impl<S: Chainable>` and transition
//! the builder to a chainable state; `build()` lives on states that are buildable.
//!
//! ```ignore
//! // Allowed: at least one op queued before build().
//! snapshot.alter_table().add_column(field).build(engine, committer)?;
//!
//! // Not allowed: build() is not defined on Ready (no ops queued).
//! snapshot.alter_table().build(engine, committer)?;  // compile error
//! ```

use std::marker::PhantomData;
use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::StructField;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    protocol_with_added_features, schema_has_column_mapping_metadata,
    strip_stray_column_mapping_metadata, ColumnMappingMode, Operation, TableFeature,
};
use crate::table_properties::COLUMN_MAPPING_MAX_COLUMN_ID;
use crate::transaction::alter_table::AlterTableTransaction;
use crate::transaction::schema_evolution::{
    apply_schema_operations, SchemaEvolutionResult, SchemaOperation,
};
use crate::utils::FoldWithOption as _;
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

/// Builder for constructing an [`AlterTableTransaction`] with protocol or schema operations.
///
/// Uses a type-state pattern (`S`) to enforce at compile time:
/// - At least one schema operation must be queued before `build()` is callable.
/// - Only operations valid for the current state can be chained. This will disallow incompatible
///   chaining.
pub struct AlterTableTransactionBuilder<S = Ready> {
    snapshot: SnapshotRef,
    operations: Vec<SchemaOperation>,
    table_features: Vec<TableFeature>,
    allow_protocol_versions_increase: bool,
    correlation_id: Option<Arc<str>>,
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
            table_features: self.table_features,
            allow_protocol_versions_increase: self.allow_protocol_versions_increase,
            correlation_id: self.correlation_id,
            _state: PhantomData,
        }
    }

    /// Attach an opaque, caller-supplied correlation id for joining the alter-table commit's metric
    /// events to the caller's own request or operation id. An empty id is treated as unset.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<Arc<str>>) -> Self {
        self.correlation_id = Some(correlation_id.into()).filter(|id| !id.is_empty());
        self
    }

    /// Allow adding table features to increase the writer protocol to 7 and, when required by a
    /// requested reader-writer feature, the reader protocol to 3.
    #[internal_api]
    pub(crate) fn with_allow_protocol_versions_increase(mut self, allow: bool) -> Self {
        self.allow_protocol_versions_increase = allow;
        self
    }
}

impl AlterTableTransactionBuilder<Ready> {
    /// Create a new builder from a snapshot.
    pub(crate) fn new(snapshot: SnapshotRef) -> Self {
        AlterTableTransactionBuilder {
            snapshot,
            operations: Vec::new(),
            table_features: Vec::new(),
            allow_protocol_versions_increase: false,
            correlation_id: None,
            _state: PhantomData,
        }
    }
}

impl<S: Chainable> AlterTableTransactionBuilder<S> {
    /// Add a supported table feature while conservatively preserving current capabilities.
    #[internal_api]
    pub(crate) fn add_table_feature(
        mut self,
        feature: TableFeature,
    ) -> AlterTableTransactionBuilder<Modifying> {
        self.table_features.push(feature);
        self.transition()
    }

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
}

impl AlterTableTransactionBuilder<Modifying> {
    /// Validate and apply protocol or schema operations, then build the
    /// [`AlterTableTransaction`].
    ///
    /// This method:
    /// 1. Validates schema changes against the table's write capabilities
    /// 2. Applies schema operations sequentially when present
    /// 3. Conservatively evolves and extends the table-feature Protocol when features are requested
    /// 4. Validates the evolved table configuration
    /// 5. Creates the transaction with explicit Protocol and Metadata emission flags
    ///
    /// # Errors
    ///
    /// - The table enables `icebergCompatV3` or `allowColumnDefaults`, which ALTER TABLE does not
    ///   yet support
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
        let has_schema_operations = !self.operations.is_empty();
        let has_feature_operations = !self.table_features.is_empty();

        let evolved_metadata = if has_schema_operations {
            // Schema ALTER behavior retains the broad write gate and feature restrictions.
            if table_config.is_feature_enabled(&TableFeature::IcebergCompatV3) {
                return Err(Error::unsupported(
                    "ALTER TABLE is not yet supported on tables with icebergCompatV3 enabled",
                ));
            }
            // TODO(#2630): Support ALTER TABLE on tables with column defaults.
            if table_config.is_feature_enabled(&TableFeature::AllowColumnDefaults) {
                return Err(Error::unsupported(
                    "ALTER TABLE is not yet supported on tables with allowColumnDefaults enabled",
                ));
            }
            table_config.ensure_operation_supported(Operation::Write)?;

            let schema = Arc::unwrap_or_clone(table_config.logical_schema());
            let column_mapping_mode = table_config.column_mapping_mode();
            let current_max_column_id =
                table_config.table_properties().column_mapping_max_column_id;
            let current_has_cm = column_mapping_mode == ColumnMappingMode::None
                && schema_has_column_mapping_metadata(&schema);
            let SchemaEvolutionResult {
                schema: evolved_schema,
                new_max_column_id,
            } = apply_schema_operations(
                schema,
                self.operations,
                column_mapping_mode,
                current_max_column_id,
            )?;

            let evolved_schema = if column_mapping_mode == ColumnMappingMode::None {
                strip_stray_column_mapping_metadata(current_has_cm, &evolved_schema)
                    .map_or(evolved_schema, Arc::new)
            } else {
                evolved_schema
            };

            let metadata = table_config
                .metadata()
                .clone()
                .with_schema(evolved_schema.clone())?
                .fold_with(new_max_column_id, |evolved_metadata, id| {
                    evolved_metadata
                        .with_configuration_entry(COLUMN_MAPPING_MAX_COLUMN_ID, id.to_string())
                });
            Some((metadata, evolved_schema))
        } else {
            None
        };

        let evolved_protocol = has_feature_operations
            .then(|| {
                protocol_with_added_features(
                    table_config.protocol(),
                    self.table_features,
                    self.allow_protocol_versions_increase,
                )
            })
            .transpose()?;

        // Preserve the existing schema-only construction path. When Protocol also changes,
        // re-parse the combined target configuration before publication.
        let evolved_table_config = match (evolved_metadata.as_ref(), evolved_protocol.clone()) {
            (Some((metadata, _)), Some(protocol)) => TableConfiguration::try_new_from(
                table_config,
                Some(metadata.clone()),
                Some(protocol),
                table_config.version(),
            )?,
            (Some((metadata, schema)), None) => TableConfiguration::try_new_with_schema(
                table_config,
                metadata.clone(),
                schema.clone(),
            )?,
            (None, protocol) => TableConfiguration::try_new_from(
                table_config,
                None,
                protocol,
                table_config.version(),
            )?,
        };

        AlterTableTransaction::try_new_alter_table(
            self.snapshot,
            evolved_table_config,
            committer,
            evolved_protocol.is_some(),
            has_schema_operations,
            if has_feature_operations {
                "ADD FEATURE"
            } else {
                "ALTER TABLE"
            },
            self.correlation_id,
        )
    }
}
