//! Shared configuration for create-table and update-table transaction builders.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::actions::DomainMetadata;
use crate::schema::SchemaRef;
use crate::transaction::domain_metadata::validate_unique_domains;
use crate::utils::require;
use crate::{EngineData, KernelError, KernelResult};

/// Shared transaction configuration collected before build.
#[derive(Default)]
pub(in crate::transaction) struct TransactionBuilderState {
    pub(in crate::transaction) correlation_id: Option<Arc<str>>,
    pub(in crate::transaction) operation_parameters: Option<HashMap<String, Option<String>>>,
    pub(in crate::transaction) operation_metrics: Option<HashMap<String, Option<String>>>,
    pub(in crate::transaction) engine_info: String,
    pub(in crate::transaction) engine_commit_info: Option<(Box<dyn EngineData>, SchemaRef)>,
    pub(in crate::transaction) transaction_ids: Vec<(String, i64)>,
    pub(in crate::transaction) domain_metadata_additions: Vec<DomainMetadata>,
    pub(in crate::transaction) data_change: bool,
    pub(in crate::transaction) skip_dedup_validation: bool,
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
    pub(super) fn new(engine_info: String, data_change: bool) -> Self {
        Self {
            engine_info,
            data_change,
            ..Self::default()
        }
    }

    pub(super) fn validate(&self) -> KernelResult<()> {
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
}
