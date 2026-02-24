//! This module includes support for reading DomainMetadata from the log. NB: it is similar to the
//! set_transaction module which reads SetTransaction actions from the log.
//!
//! This module supports reading a single domain, a specific set of N domains (with early
//! termination), or all domains from the log.

use crate::actions::DomainMetadata;
use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine};
use delta_kernel_derive::internal_api;
use std::collections::HashSet;

pub(crate) use crate::log_segment::DomainMetadataMap;

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
    let domains = HashSet::from([domain]);
    let mut domain_metadatas = log_segment.scan_domain_metadatas(Some(&domains), engine)?;
    Ok(domain_metadatas
        .remove(domain)
        .map(|domain_metadata| domain_metadata.configuration))
}

#[allow(unused)]
#[internal_api]
pub(crate) fn all_domain_metadata_configuration(
    log_segment: &LogSegment,
    engine: &dyn Engine,
) -> DeltaResult<Vec<DomainMetadata>> {
    let domain_metadatas = log_segment.scan_domain_metadatas(None, engine)?;
    Ok(domain_metadatas.into_values().collect())
}
