//! Shared configuration for create-table and update-table transaction builders.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::actions::DomainMetadata;
use crate::schema::SchemaRef;
use crate::transaction::domain_metadata::validate_unique_domains;
use crate::utils::require;
use crate::{EngineData, KernelError, Result};

/// Transaction intent collected before building a transaction.
#[derive(Default)]
pub(crate) struct TransactionBuilderState {
    pub(in crate::transaction) correlation_id: Option<Arc<str>>,
    pub(in crate::transaction) operation_parameters: Option<HashMap<String, Option<String>>>,
    pub(in crate::transaction) operation_metrics: Option<HashMap<String, Option<String>>>,
    pub(in crate::transaction) engine_info: Option<String>,
    pub(in crate::transaction) engine_commit_info: Option<(Box<dyn EngineData>, SchemaRef)>,
    pub(in crate::transaction) transaction_ids: Vec<(String, i64)>,
    pub(in crate::transaction) domain_metadata_additions: Vec<DomainMetadata>,
    pub(in crate::transaction) data_change: Option<bool>,
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
            .finish()
    }
}

impl TransactionBuilderState {
    pub(in crate::transaction) fn for_update_table() -> Self {
        Self::default()
    }

    pub(in crate::transaction) fn for_create_table(engine_info: String) -> Self {
        Self {
            engine_info: Some(engine_info),
            data_change: Some(true),
            ..Self::default()
        }
    }

    pub(in crate::transaction) fn with_engine_info(
        mut self,
        engine_info: impl Into<String>,
    ) -> Self {
        self.engine_info = Some(engine_info.into());
        self
    }

    pub(in crate::transaction) fn with_correlation_id(
        mut self,
        correlation_id: impl Into<Arc<str>>,
    ) -> Self {
        self.correlation_id = Some(correlation_id.into()).filter(|id| !id.is_empty());
        self
    }

    pub(in crate::transaction) fn with_operation_parameters<I, K, V>(
        mut self,
        parameters: I,
    ) -> Self
    where
        I: IntoIterator<Item = (K, Option<V>)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.operation_parameters = Some(collect_operation_metadata(parameters));
        self
    }

    pub(in crate::transaction) fn with_operation_metrics<I, K, V>(mut self, metrics: I) -> Self
    where
        I: IntoIterator<Item = (K, Option<V>)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.operation_metrics = Some(collect_operation_metadata(metrics));
        self
    }

    pub(in crate::transaction) fn with_commit_info(
        mut self,
        commit_info: Box<dyn EngineData>,
        commit_info_schema: SchemaRef,
    ) -> Self {
        self.engine_commit_info = Some((commit_info, commit_info_schema));
        self
    }

    pub(in crate::transaction) fn with_transaction_id(
        mut self,
        app_id: impl Into<String>,
        version: i64,
    ) -> Self {
        self.transaction_ids.push((app_id.into(), version));
        self
    }

    pub(in crate::transaction) fn with_domain_metadata(
        mut self,
        domain: impl Into<String>,
        configuration: impl Into<String>,
    ) -> Self {
        self.domain_metadata_additions
            .push(DomainMetadata::new(domain.into(), configuration.into()));
        self
    }

    pub(in crate::transaction) fn validate(&self) -> Result<()> {
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

pub(crate) fn collect_operation_metadata<I, K, V>(entries: I) -> HashMap<String, Option<String>>
where
    I: IntoIterator<Item = (K, Option<V>)>,
    K: Into<String>,
    V: Into<String>,
{
    entries
        .into_iter()
        .map(|(key, value)| (key.into(), value.map(Into::into)))
        .collect()
}
