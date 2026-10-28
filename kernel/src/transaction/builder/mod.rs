//! Type-specific transaction construction with shared configuration setters.

// Allow `pub` items in this module even though the module itself may be `pub(crate)`.
// The module visibility controls external access; items are `pub` for use within the crate
// and for tests. Also allow dead_code since these are used by integration tests.
#![allow(unreachable_pub, dead_code)]

pub mod create_table;
mod state;
pub mod update_table;

use std::marker::PhantomData;
use std::sync::Arc;

pub(super) use self::state::TransactionBuilderState;
use crate::EngineData;
use crate::actions::DomainMetadata;
use crate::schema::SchemaRef;
use crate::transaction::{
    CreateTable, ExecutionMode, Imperative, UpdateTable, collect_operation_metadata,
};

/// A transaction type supported by [`TransactionBuilder`].
///
/// Each type selects its builder state and specializes constructors, setters, and build
/// validation. This trait is sealed; Kernel provides the supported transaction types.
pub trait TransactionType: private::Sealed {
    /// Builder state specific to this transaction type. Use `()` when no extra state is needed.
    #[doc(hidden)]
    type BuilderState: Send;
}

/// Configures a transaction of type `T` and execution mode `MODE`.
///
/// Shared setters preserve the transaction type. Constructors, type-specific setters, and
/// `build` are provided by the specialized implementations for [`CreateTable`] and [`UpdateTable`].
/// Use [`create_table`](super::create_table::create_table) or
/// [`Snapshot::update_table`](crate::Snapshot::update_table) to obtain a builder.
/// Both entry points select [`Imperative`]. With `declarative-plans`, use `with_execution_mode`
/// to select another mode before building the transaction.
///
/// The operation cannot be changed on a builder:
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{TransactionBuilder, UpdateTableOperation};
///
/// fn configure(builder: TransactionBuilder) {
///     let _ = builder.with_operation(UpdateTableOperation::Write);
/// }
/// ```
///
/// Update-table builders cannot configure new-table properties:
///
/// ```compile_fail,E0599
/// use delta_kernel::transaction::{TransactionBuilder, UpdateTable};
///
/// fn configure(builder: TransactionBuilder<UpdateTable>) {
///     let _ = builder.with_table_properties([("app.version", "1")]);
/// }
/// ```
pub struct TransactionBuilder<T: TransactionType = UpdateTable, MODE: ExecutionMode = Imperative> {
    common_state: TransactionBuilderState,
    local_state: T::BuilderState,
    _mode: PhantomData<MODE>,
}

impl<T: TransactionType, MODE: ExecutionMode> TransactionBuilder<T, MODE> {
    /// Selects `NewMode` for the transaction, preserving all accumulated configuration.
    ///
    /// Returns a builder whose `build` method constructs a transaction in the selected mode.
    #[cfg(feature = "declarative-plans")]
    pub fn with_execution_mode<NewMode: ExecutionMode>(self) -> TransactionBuilder<T, NewMode> {
        TransactionBuilder {
            common_state: self.common_state,
            local_state: self.local_state,
            _mode: PhantomData,
        }
    }

    /// Adds an application transaction identifier to emit as a `txn` action.
    ///
    /// Returns the updated builder. The action's `lastUpdated` value uses the transaction's commit
    /// timestamp. An application id may occur only once; duplicate ids are rejected during build.
    pub fn with_transaction_id(mut self, app_id: impl Into<String>, version: i64) -> Self {
        self.common_state
            .transaction_ids
            .push((app_id.into(), version));
        self
    }

    /// Adds user-controlled domain metadata and returns the updated builder.
    ///
    /// `domain` identifies the metadata and `configuration` supplies its serialized value. Each
    /// domain may occur only once; conflicts are rejected during build.
    pub fn with_domain_metadata(
        mut self,
        domain: impl Into<String>,
        configuration: impl Into<String>,
    ) -> Self {
        self.common_state
            .domain_metadata_additions
            .push(DomainMetadata::new(domain.into(), configuration.into()));
        self
    }

    /// Attaches an opaque identifier to the transaction's metric events.
    ///
    /// Returns the updated builder. Consecutive calls replace the previous value; an empty
    /// `correlation_id` clears it.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<Arc<str>>) -> Self {
        self.common_state.correlation_id = Some(correlation_id.into()).filter(|id| !id.is_empty());
        self
    }

    /// Replaces the operation parameters recorded in `commitInfo`.
    ///
    /// Returns the updated builder. Values must already be stringified as expected in table
    /// history; `None` writes a null map value. The map replaces rather than merges with an earlier
    /// map, and the last value wins when a key occurs more than once. Dedicated parameters take
    /// precedence over a same-named nested field supplied by
    /// [`with_commit_info`](Self::with_commit_info).
    pub fn with_operation_parameters<I, K, V>(mut self, parameters: I) -> Self
    where
        I: IntoIterator<Item = (K, Option<V>)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.common_state.operation_parameters = Some(collect_operation_metadata(parameters));
        self
    }

    /// Replaces the operation metrics recorded in `commitInfo`.
    ///
    /// Returns the updated builder. Values must already be stringified as expected in table
    /// history; `None` writes a null map value. The map replaces rather than merges with an earlier
    /// map, and the last value wins when a key occurs more than once. Metrics supplied to the built
    /// transaction replace these values. Dedicated metrics take precedence over a same-named
    /// nested field supplied by [`with_commit_info`](Self::with_commit_info).
    pub fn with_operation_metrics<I, K, V>(mut self, metrics: I) -> Self
    where
        I: IntoIterator<Item = (K, Option<V>)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.common_state.operation_metrics = Some(collect_operation_metadata(metrics));
        self
    }

    /// Supplies one arbitrary connector-provided `commitInfo` row and its schema.
    ///
    /// Returns the updated builder. Consecutive calls replace the previous row. Kernel-owned
    /// fields and dedicated parameter or metric maps take precedence over same-named fields in
    /// this row. Build rejects `commit_info` if it does not contain exactly one row.
    pub fn with_commit_info(
        mut self,
        commit_info: Box<dyn EngineData>,
        commit_info_schema: SchemaRef,
    ) -> Self {
        self.common_state.engine_commit_info = Some((commit_info, commit_info_schema));
        self
    }
}

impl TransactionType for CreateTable {
    type BuilderState = create_table::CreateTableBuilderState;
}

impl TransactionType for UpdateTable {
    type BuilderState = update_table::UpdateTableBuilderState;
}

impl private::Sealed for CreateTable {}
impl private::Sealed for UpdateTable {}

mod private {
    pub trait Sealed {}
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use rstest::rstest;

    use super::{TransactionBuilder, TransactionBuilderState, TransactionType, private};
    use crate::arrow::array::StringArray;
    use crate::schema::schema_ref;
    #[cfg(feature = "declarative-plans")]
    use crate::transaction::Declarative;
    use crate::transaction::{ExecutionMode, Imperative};
    use crate::unit_test_utils::string_array_to_engine_data;

    struct CustomType;

    impl private::Sealed for CustomType {}

    impl TransactionType for CustomType {
        type BuilderState = ();
    }

    impl TransactionBuilder<CustomType> {
        fn new(engine_info: &str) -> Self {
            Self {
                common_state: TransactionBuilderState::new(engine_info.into(), false),
                local_state: (),
                _mode: PhantomData,
            }
        }
    }

    fn configure_shared_setters<T: TransactionType>(
        builder: TransactionBuilder<T>,
    ) -> TransactionBuilder<T> {
        builder
            .with_correlation_id("correlation")
            .with_operation_parameters([("mode", Some("Append"))])
            .with_operation_metrics([("numFiles", Some("3"))])
            .with_transaction_id("app", 7)
            .with_domain_metadata("app.domain", "{}")
            .with_commit_info(
                string_array_to_engine_data(StringArray::from(vec!["tag"])),
                schema_ref! { nullable "a": STRING },
            )
    }

    #[rstest]
    #[case::imperative(Imperative::default())]
    #[cfg_attr(
        feature = "declarative-plans",
        case::declarative(Declarative::default())
    )]
    fn custom_type_supports_shared_setters_without_custom_state<MODE: ExecutionMode>(
        #[case] _mode: MODE,
    ) {
        let builder = TransactionBuilder::<CustomType>::new("test-engine");
        let builder = configure_shared_setters(builder);
        #[cfg(feature = "declarative-plans")]
        let builder = builder.with_execution_mode::<MODE>();

        assert_eq!(builder.common_state.engine_info, "test-engine");
        assert_eq!(
            builder.common_state.correlation_id.as_deref(),
            Some("correlation")
        );
        assert_eq!(
            builder.common_state.operation_parameters.as_ref().unwrap()["mode"].as_deref(),
            Some("Append")
        );
        assert_eq!(
            builder.common_state.operation_metrics.as_ref().unwrap()["numFiles"].as_deref(),
            Some("3")
        );
        assert_eq!(builder.common_state.transaction_ids, [("app".into(), 7)]);
        assert_eq!(
            builder.common_state.domain_metadata_additions[0].domain(),
            "app.domain"
        );
        assert!(!builder.common_state.data_change);
        let (commit_info, schema) = builder.common_state.engine_commit_info.as_ref().unwrap();
        assert_eq!(commit_info.len(), 1);
        assert_eq!(
            schema.as_ref(),
            schema_ref! { nullable "a": STRING }.as_ref()
        );
        builder.common_state.validate().unwrap();
    }
}
