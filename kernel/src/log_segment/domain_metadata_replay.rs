//! Domain metadata replay logic for [`LogSegment`].
//!
//! This module contains the method that performs a log replay to extract the latest domain
//! metadata actions from a [`LogSegment`].

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use tracing::instrument;

use crate::actions::get_log_domain_metadata_schema;
use crate::actions::visitors::DomainMetadataVisitor;
use crate::actions::{DomainMetadata, DOMAIN_METADATA_NAME};
use crate::log_replay::ActionsBatch;
use crate::{DeltaResult, Engine, Expression as Expr, PredicateRef, RowVisitor as _};

use super::LogSegment;

const DOMAIN_METADATA_DOMAIN_FIELD: &str = "domain";

pub(crate) type DomainMetadataMap = HashMap<String, DomainMetadata>;

impl LogSegment {
    /// Scan this log segment for domain metadata actions. If a specific set of domains is
    /// provided, terminate log replay early once all requested domains have been found. If no
    /// filter is given, replay the entire log to collect all domains.
    ///
    /// Returns the latest domain metadata for each domain, accounting for tombstones
    /// (`removed=true`) — removed domain metadatas will _never_ be present in the returned map.
    #[instrument(name = "domain_metadata.scan", skip_all, fields(domains = ?domains.map(|d| d.iter().collect::<Vec<_>>())), err)]
    pub(crate) fn scan_domain_metadatas(
        &self,
        domains: Option<&HashSet<&str>>,
        engine: &dyn Engine,
    ) -> DeltaResult<DomainMetadataMap> {
        let domain_filter = domains.map(|set| {
            set.iter()
                .map(|s| s.to_string())
                .collect::<HashSet<String>>()
        });
        let mut visitor = DomainMetadataVisitor::new(domain_filter);
        // If a specific set of domains is requested then we can terminate log replay early as
        // soon as all requested domains have been found. If all domains are requested then we
        // are forced to replay the entire log.
        for actions in self.read_domain_metadata_batches(engine)? {
            let domain_metadatas = actions?.actions;
            visitor.visit_rows_of(domain_metadatas.as_ref())?;
            // if all requested domains have been found, terminate early
            if visitor.filter_found() {
                break;
            }
        }

        Ok(visitor.into_domain_metadatas())
    }

    /// Read action batches from the log, projecting rows to only contain domain metadata columns.
    fn read_domain_metadata_batches(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        let schema = get_log_domain_metadata_schema();
        static META_PREDICATE: LazyLock<Option<PredicateRef>> = LazyLock::new(|| {
            Some(Arc::new(
                Expr::column([DOMAIN_METADATA_NAME, DOMAIN_METADATA_DOMAIN_FIELD]).is_not_null(),
            ))
        });
        self.read_actions(engine, schema.clone(), META_PREDICATE.clone())
    }
}
