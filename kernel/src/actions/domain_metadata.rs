//! This module includes support for reading DomainMetadata from the log. NB: it is similar to the
//! set_transaction module which reads SetTransaction actions from the log.
//!
//! For now, this module only exposes the ability to read a single domain at once from the log. In
//! the future this should allow for reading all domains from the log at once.

use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use crate::actions::get_log_domain_metadata_schema;
use crate::actions::visitors::DomainMetadataVisitor;
use crate::actions::{DomainMetadata, DOMAIN_METADATA_NAME};
use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine, EngineData, Expression as Expr, ExpressionRef, RowVisitor as _};
use std::ops::Deref;

const DOMAIN_METADATA_DOMAIN_FIELD: &str = "domain";

pub(crate) type DomainMetadataSet = HashSet<DomainMetadataHashSetEntry>;

#[derive(Debug)]
pub(crate) struct DomainMetadataHashSetEntry(DomainMetadata);

impl PartialEq for DomainMetadataHashSetEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.domain == other.0.domain
    }
}

impl Eq for DomainMetadataHashSetEntry {}

impl Hash for DomainMetadataHashSetEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.domain.hash(state);
    }
}

impl Borrow<str> for DomainMetadataHashSetEntry {
    fn borrow(&self) -> &str {
        &self.0.domain
    }
}

impl Deref for DomainMetadataHashSetEntry {
    type Target = DomainMetadata;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<DomainMetadata> for DomainMetadataHashSetEntry {
    fn from(metadata: DomainMetadata) -> Self {
        DomainMetadataHashSetEntry(metadata)
    }
}

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
    // note that the resulting domain_metadatas includes removed domains, so we need to filter
    domain_metadatas.retain(|dm| !dm.removed);
    Ok(domain_metadatas
        .take(domain)
        .map(|domain_metadata| domain_metadata.0.configuration))
}

/// Scan the entire log for all domain metadata actions but terminate early if a specific domain
/// is provided. Note that this returns the latest domain metadata for each domain, including
/// tombstones (removed=true). It is up to the caller to filter out removed domains if needed.
fn scan_domain_metadatas(
    log_segment: &LogSegment,
    domain: Option<&str>,
    engine: &dyn Engine,
) -> DeltaResult<DomainMetadataSet> {
    let mut visitor = DomainMetadataVisitor::new(domain.map(|s| s.to_owned()));
    // If a specific domain is requested then we can terminate log replay early as soon as it was
    // found. If all domains are requested then we are forced to replay the entire log.
    for actions in replay_for_domain_metadatas(log_segment, engine)? {
        // throw away is_log_batch since we don't care
        let (domain_metadatas, _) = actions?;
        visitor.visit_rows_of(domain_metadatas.as_ref())?;
        // if a specific domain is requested and it was found, then return. note that we don't need
        // to check if it was the one that was found since the visitor will only keep the requested
        // domain
        if domain.is_some() && !visitor.domain_metadatas.is_empty() {
            break;
        }
    }

    Ok(visitor.domain_metadatas)
}

fn replay_for_domain_metadatas(
    log_segment: &LogSegment,
    engine: &dyn Engine,
) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
    let schema = get_log_domain_metadata_schema();
    static META_PREDICATE: LazyLock<Option<ExpressionRef>> = LazyLock::new(|| {
        Some(Arc::new(
            Expr::column([DOMAIN_METADATA_NAME, DOMAIN_METADATA_DOMAIN_FIELD]).is_not_null(),
        ))
    });
    log_segment.read_actions(
        engine,
        schema.clone(), // Arc clone
        schema.clone(), // Arc clone
        META_PREDICATE.clone(),
    )
}
