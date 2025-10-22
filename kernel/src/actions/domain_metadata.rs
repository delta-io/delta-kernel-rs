//! This module includes support for reading DomainMetadata from the log. NB: it is similar to the
//! set_transaction module which reads SetTransaction actions from the log.
//!
//! For now, this module only exposes the ability to read a single domain at once from the log. In
//! the future this should allow for reading all domains from the log at once.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use crate::actions::get_log_domain_metadata_schema;
use crate::actions::visitors::DomainMetadataVisitor;
use crate::actions::{DomainMetadata, DOMAIN_METADATA_NAME};
use crate::log_replay::ActionsBatch;
use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine, Expression as Expr, PredicateRef, RowVisitor as _};

const DOMAIN_METADATA_DOMAIN_FIELD: &str = "domain";

pub(crate) type DomainMetadataMap = HashMap<String, DomainMetadata>;

/// Read the latest domain metadata for a given domain and return its `configuration`. This
/// accounts for 'removed' domain metadata: if the domain is removed, then the configuration is
/// `None`. Additionally, this includes 'internal' (delta.*) domains. The consumer must filter
/// these before returning domains to the user.
// TODO we should have some finer-grained unit tests here instead of relying on the top-level
// snapshot tests.
pub(crate) fn domain_metadata_configuration(
    log_segment: &LogSegment,
    domain: &str,
    engine: &dyn Engine,
) -> DeltaResult<Option<String>> {
    let mut domain_metadatas = scan_domain_metadatas(log_segment, Some(domain), engine)?;
    Ok(domain_metadatas
        .remove(domain)
        .map(|domain_metadata| domain_metadata.configuration))
}

/// Scan the entire log for all domain metadata actions but terminate early if a specific domain
/// is provided. Note that this returns the latest domain metadata for each domain, accounting for
/// tombstones (removed=true) - that is, removed domain metadatas will _never_ be returned.
pub(crate) fn scan_domain_metadatas(
    log_segment: &LogSegment,
    domain: Option<&str>,
    engine: &dyn Engine,
) -> DeltaResult<DomainMetadataMap> {
    let mut visitor = DomainMetadataVisitor::new(domain.map(|s| s.to_owned()));
    // If a specific domain is requested then we can terminate log replay early as soon as it was
    // found. If all domains are requested then we are forced to replay the entire log.
    for actions in replay_for_domain_metadatas(log_segment, engine)? {
        // throw away is_log_batch since we don't care
        let domain_metadatas = actions?.actions;
        visitor.visit_rows_of(domain_metadatas.as_ref())?;
        // if a specific domain is requested and it was found, then return. note that we don't need
        // to check if it was the one that was found since the visitor will only keep the requested
        // domain
        if visitor.filter_found() {
            break;
        }
    }

    Ok(visitor.into_domain_metadatas())
}

fn replay_for_domain_metadatas(
    log_segment: &LogSegment,
    engine: &dyn Engine,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
    let schema = get_log_domain_metadata_schema();
    static META_PREDICATE: LazyLock<Option<PredicateRef>> = LazyLock::new(|| {
        Some(Arc::new(
            Expr::column([DOMAIN_METADATA_NAME, DOMAIN_METADATA_DOMAIN_FIELD]).is_not_null(),
        ))
    });
    log_segment.read_actions(
        engine,
        schema.clone(), // Arc clone
        META_PREDICATE.clone(),
    )
}
