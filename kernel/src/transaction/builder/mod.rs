//! Type-specific transaction construction with shared configuration setters.

// Allow `pub` items in this module even though the module itself may be `pub(crate)`.
// The module visibility controls external access; items are `pub` for use within the crate
// and for tests. Also allow dead_code since these are used by integration tests.
#![allow(unreachable_pub, dead_code)]

pub mod create_table;
mod state;
pub mod update_table;

use std::sync::Arc;

pub(super) use self::state::TransactionBuilderState;
use crate::actions::DomainMetadata;
use crate::schema::SchemaRef;
use crate::transaction::commit_info::collect_operation_metadata;
use crate::transaction::{CreateTable, UpdateTable};
use crate::EngineData;

/// A transaction type supported by [`TransactionBuilder`].
///
/// Each type selects its builder state and specializes constructors, setters, and build
/// validation. This trait is sealed; Kernel provides the supported transaction types.
pub trait TransactionType: private::Sealed {
    /// Builder state specific to this transaction type. Use `()` when no extra state is needed.
    #[doc(hidden)]
    type BuilderState: Send;
}

/// Configures a transaction of type `T`.
///
/// Shared setters preserve the transaction type. Constructors, type-specific setters, and
/// `build` are provided by the specialized implementations for [`CreateTable`] and [`UpdateTable`].
/// Use [`create_table`](super::create_table::create_table) or
/// [`Snapshot::update_table`](crate::Snapshot::update_table) to obtain a builder.
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
pub struct TransactionBuilder<T: TransactionType = UpdateTable> {
    common_state: TransactionBuilderState,
    local_state: T::BuilderState,
}

impl<T: TransactionType> TransactionBuilder<T> {
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

    /// Adds a per-file `tags` column to the transaction's [`add_files_schema`] and returns the
    /// updated builder.
    ///
    /// The column is a nullable `tags: map<string, string>` with nullable values. It follows the
    /// base add-files columns, and precedes `clusteringProvider` when
    /// [`with_add_file_clustering_provider`](Self::with_add_file_clustering_provider) is also set.
    /// Each row's value becomes the `tags` field of that file's Add action.
    ///
    /// `DefaultEngine::write_parquet` does not produce this column, so the caller appends it to
    /// each batch passed to [`add_files`], e.g. with [`EngineData::append_columns`]. Commit fails
    /// for a batch without it.
    ///
    /// [`add_files_schema`]: crate::transaction::Transaction::add_files_schema
    /// [`add_files`]: crate::transaction::Transaction::add_files
    pub fn with_add_file_tags(mut self) -> Self {
        self.common_state.add_file_tags = true;
        self
    }

    /// Adds a per-file `clusteringProvider` column to the transaction's [`add_files_schema`] and
    /// returns the updated builder.
    ///
    /// The column is a nullable `clusteringProvider: string`. It is the last add-files column,
    /// after `tags` when [`with_add_file_tags`](Self::with_add_file_tags) is also set. Each row's
    /// value becomes the `clusteringProvider` field of that file's Add action. The Delta protocol
    /// requires: "When a clustering implementation clusters files, writers must set the name of
    /// the clustering implementation in the `clusteringProvider` field when adding `add` actions
    /// for clustered files." A null value marks the file as unclustered.
    ///
    /// `DefaultEngine::write_parquet` does not produce this column, so the caller appends it to
    /// each batch passed to [`add_files`], e.g. with [`EngineData::append_columns`]. Commit fails
    /// for a batch without it.
    ///
    /// Build rejects this option if the table's protocol does not support the `clustering` writer
    /// feature.
    ///
    /// [`add_files_schema`]: crate::transaction::Transaction::add_files_schema
    /// [`add_files`]: crate::transaction::Transaction::add_files
    pub fn with_add_file_clustering_provider(mut self) -> Self {
        self.common_state.add_file_clustering_provider = true;
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
    use super::{private, TransactionBuilder, TransactionBuilderState, TransactionType};
    use crate::arrow::array::StringArray;
    use crate::schema::schema_ref;
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
            .with_add_file_tags()
            .with_add_file_clustering_provider()
    }

    #[test]
    fn custom_type_supports_shared_setters_without_custom_state() {
        let builder = TransactionBuilder::<CustomType>::new("test-engine");
        let builder = configure_shared_setters(builder);

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
        assert!(builder.common_state.add_file_tags);
        assert!(builder.common_state.add_file_clustering_provider);
        let (commit_info, schema) = builder.common_state.engine_commit_info.as_ref().unwrap();
        assert_eq!(commit_info.len(), 1);
        assert_eq!(
            schema.as_ref(),
            schema_ref! { nullable "a": STRING }.as_ref()
        );
        builder.common_state.validate().unwrap();
    }
}
