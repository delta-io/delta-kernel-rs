use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::actions::DomainMetadata;
use crate::schema::SchemaRef;
use crate::utils::require;
use crate::{DeltaResult, EngineData, KernelError};

#[derive(Default)]
pub(crate) struct TransactionBuilderState {
    pub(in crate::transaction) correlation_id: Option<Arc<str>>,
    pub(in crate::transaction) operation_parameters: Option<HashMap<String, String>>,
    pub(in crate::transaction) operation_metrics: Option<HashMap<String, String>>,
    pub(in crate::transaction) engine_info: Option<String>,
    pub(in crate::transaction) engine_commit_info: Option<(Box<dyn EngineData>, SchemaRef)>,
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
            .field("domain_metadata_additions", &self.domain_metadata_additions)
            .field("data_change", &self.data_change)
            .finish()
    }
}

impl TransactionBuilderState {
    pub(in crate::transaction) fn new() -> Self {
        Self::default()
    }

    pub(in crate::transaction) fn for_create_table(engine_info: String) -> Self {
        Self {
            engine_info: Some(engine_info),
            data_change: Some(true),
            ..Self::new()
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
    ) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.operation_parameters = Some(collect_operation_metadata("parameter", parameters)?);
        Ok(self)
    }

    pub(in crate::transaction) fn with_operation_metrics<I, K, V>(
        mut self,
        metrics: I,
    ) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.operation_metrics = Some(collect_operation_metadata("metric", metrics)?);
        Ok(self)
    }

    pub(in crate::transaction) fn with_commit_info(
        mut self,
        commit_info: Box<dyn EngineData>,
        commit_info_schema: SchemaRef,
    ) -> Self {
        self.engine_commit_info = Some((commit_info, commit_info_schema));
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

    pub(in crate::transaction) fn validate(&self) -> DeltaResult<()> {
        let mut domains = HashSet::with_capacity(self.domain_metadata_additions.len());
        if let Some(domain) = self
            .domain_metadata_additions
            .iter()
            .map(DomainMetadata::domain)
            .find(|domain| !domains.insert(*domain))
        {
            return Err(KernelError::invalid_transaction_state(format!(
                "domain metadata '{domain}' appears more than once in transaction builder"
            )));
        }

        Ok(())
    }
}

pub(crate) fn collect_operation_metadata<I, K, V>(
    kind: &str,
    entries: I,
) -> DeltaResult<HashMap<String, String>>
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    let mut values = HashMap::new();
    for (key, value) in entries {
        let key = key.into();
        require!(
            !key.is_empty(),
            KernelError::invalid_transaction_state(format!("operation {kind} key cannot be empty"))
        );
        require!(
            values.insert(key.clone(), value.into()).is_none(),
            KernelError::invalid_transaction_state(format!(
                "operation {kind} key '{key}' appears more than once"
            ))
        );
    }
    Ok(values)
}
