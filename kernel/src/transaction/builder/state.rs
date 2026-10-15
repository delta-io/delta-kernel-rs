//! Shared configuration for create-table and update-table transaction builders.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::actions::{DomainMetadata, SetTransaction};
use crate::schema::SchemaRef;
use crate::transaction::domain_metadata::validate_unique_domains;
use crate::transaction::{ExecutionMode, Transaction};
use crate::utils::require;
use crate::{EngineData, KernelError, Result};

/// Shared transaction configuration collected before build.
#[derive(Default)]
pub(super) struct TransactionBuilderState {
    pub(super) correlation_id: Option<Arc<str>>,
    pub(super) operation_parameters: Option<HashMap<String, Option<String>>>,
    pub(super) operation_metrics: Option<HashMap<String, Option<String>>>,
    pub(super) engine_info: Option<String>,
    pub(super) engine_commit_info: Option<(Box<dyn EngineData>, SchemaRef)>,
    pub(super) transaction_ids: Vec<(String, i64)>,
    pub(super) domain_metadata_additions: Vec<DomainMetadata>,
    pub(super) data_change: Option<bool>,
    pub(super) skip_dedup_validation: bool,
}

impl std::fmt::Debug for TransactionBuilderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBuilderState")
            .field("correlation_id", &self.correlation_id)
            .field("operation_parameters", &self.operation_parameters)
            .field("operation_metrics", &self.operation_metrics)
            .field("engine_info", &self.engine_info)
            .field("engine_commit_info", &self.engine_commit_info.is_some())
            .field("transaction_ids", &self.transaction_ids)
            .field("domain_metadata_additions", &self.domain_metadata_additions)
            .field("data_change", &self.data_change)
            .field("skip_dedup_validation", &self.skip_dedup_validation)
            .finish()
    }
}

impl TransactionBuilderState {
    pub(super) fn for_update_table() -> Self {
        Self::default()
    }

    pub(super) fn for_create_table(engine_info: String) -> Self {
        Self {
            engine_info: Some(engine_info),
            data_change: Some(true),
            ..Self::default()
        }
    }

    pub(super) fn validate(&self) -> Result<()> {
        if let Some((commit_info, _)) = &self.engine_commit_info {
            require!(
                commit_info.len() == 1,
                KernelError::invalid_transaction_state(
                    "Connector commit info must contain exactly one row"
                )
            );
        }

        let mut app_ids = HashSet::with_capacity(self.transaction_ids.len());
        if let Some((app_id, _)) = self
            .transaction_ids
            .iter()
            .find(|(app_id, _)| !app_ids.insert(app_id.as_str()))
        {
            return Err(KernelError::invalid_transaction_state(format!(
                "app_id {app_id} appears more than once"
            )));
        }

        validate_unique_domains(
            self.domain_metadata_additions
                .iter()
                .map(DomainMetadata::domain),
            |domain| {
                KernelError::invalid_transaction_state(format!(
                    "domain metadata '{domain}' appears more than once in transaction builder"
                ))
            },
        )?;

        Ok(())
    }

    /// Transfers builder intent into the transaction's commit configuration.
    // TODO(#3149): Replace this helper with direct initialization of immutable transaction state.
    pub(super) fn apply_to_transaction<STATE, MODE: ExecutionMode>(
        self,
        mut transaction: Transaction<STATE, MODE>,
    ) -> Transaction<STATE, MODE> {
        let Self {
            correlation_id,
            operation_parameters,
            operation_metrics,
            engine_info,
            engine_commit_info,
            transaction_ids,
            domain_metadata_additions,
            data_change,
            skip_dedup_validation,
        } = self;

        transaction.correlation_id = correlation_id;
        transaction.operation_parameters = operation_parameters;
        transaction.operation_metrics = operation_metrics;
        transaction.engine_info = engine_info;
        transaction.engine_commit_info = engine_commit_info;
        transaction.set_transactions = transaction_ids
            .into_iter()
            .map(|(app_id, version)| {
                SetTransaction::new(app_id, version, Some(transaction.commit_timestamp))
            })
            .collect();
        transaction.user_domain_metadata_additions = domain_metadata_additions;
        transaction.infer_data_change = data_change.is_none();
        transaction.data_change = data_change.unwrap_or(true);
        transaction.dedup_validation_enabled = !skip_dedup_validation;
        transaction
    }
}
