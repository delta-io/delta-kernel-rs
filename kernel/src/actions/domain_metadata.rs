//! This module includes support for reading DomainMetadata from the log. NB: it is similar to the
//! set_transaction module which reads SetTransaction actions from the log.
//!
//! For now, this module only exposes the ability to read a single domain at once from the log. In
//! the future this should allow for reading all domains from the log at once.

use crate::actions::get_log_domain_metadata_schema;
use crate::actions::visitors::DomainMetadataVisitor;
use crate::actions::{DomainMetadata, DOMAIN_METADATA_NAME};
use crate::crc::{CrcLoadResult, LazyCrc};
use crate::log_replay::ActionsBatch;
use crate::log_segment::LogSegment;
use crate::{DeltaResult, Engine, Expression as Expr, PredicateRef, RowVisitor as _};
use delta_kernel_derive::internal_api;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tracing::{info, instrument, warn};

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

#[allow(unused)]
#[internal_api]
pub(crate) fn all_domain_metadata_configuration(
    log_segment: &LogSegment,
    engine: &dyn Engine,
) -> DeltaResult<Vec<DomainMetadata>> {
    let domain_metadatas = scan_domain_metadatas(log_segment, None, engine)?;
    Ok(domain_metadatas.into_values().collect())
}

/// Scan the entire log for all domain metadata actions but terminate early if a specific domain
/// is provided. Note that this returns the latest domain metadata for each domain, accounting for
/// tombstones (removed=true) - that is, removed domain metadatas will _never_ be returned.
#[instrument(name = "domain_metadata.scan", skip_all, fields(domain = ?domain), err)]
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

/// Scan domain metadata using CRC when available, following the same pattern as
/// protocol_metadata_replay.rs. Uses CRC to avoid or reduce log replay:
///
/// Case 1: CRC at target version with domain metadata -> use directly
/// Case 2: CRC at earlier version -> replay only commits after CRC, merge with CRC
/// Case 3/4: No CRC or CRC without domain metadata -> full log replay
#[allow(dead_code)] // Will be used by transaction commit flow for domain removal lookups
#[instrument(name = "domain_metadata.scan_with_crc", skip_all, fields(domain = ?domain), err)]
pub(crate) fn scan_domain_metadatas_with_crc(
    log_segment: &LogSegment,
    domain: Option<&str>,
    engine: &dyn Engine,
    lazy_crc: &LazyCrc,
) -> DeltaResult<DomainMetadataMap> {
    let crc_version = lazy_crc.crc_version();

    // Case 1: CRC at target version -> use its domain metadata directly
    if crc_version == Some(log_segment.end_version) {
        if let CrcLoadResult::Loaded(crc) = lazy_crc.get_or_load(engine) {
            if let Some(domains) = &crc.domain_metadata {
                info!(
                    "Domain metadata from CRC at target version {}",
                    log_segment.end_version
                );
                let map: DomainMetadataMap = domains
                    .iter()
                    .filter(|dm| !dm.is_removed())
                    .map(|dm| (dm.domain().to_owned(), dm.clone()))
                    .collect();
                return Ok(map);
            }
        }
        warn!(
            "CRC at target version {} failed to load or has no domain metadata, \
             falling back to log replay",
            log_segment.end_version
        );
    }

    // Case 2: CRC at an earlier version -> replay only commits after CRC, merge with CRC
    if let Some(crc_v) = crc_version.filter(|&v| v < log_segment.end_version) {
        info!(
            "Pruning domain metadata replay to commits after CRC version {}",
            crc_v
        );
        let pruned = log_segment.segment_after_crc(crc_v);
        let pruned_map = scan_domain_metadatas(&pruned, domain, engine)?;

        // If a specific domain was found in pruned replay, newer takes priority
        if let Some(d) = domain {
            if pruned_map.contains_key(d) {
                return Ok(pruned_map);
            }
        }

        // Merge with CRC: pruned results take priority (they're newer)
        if let CrcLoadResult::Loaded(crc) = lazy_crc.get_or_load(engine) {
            if let Some(domains) = &crc.domain_metadata {
                info!("Merging CRC domain metadata with pruned replay results");
                let mut merged = pruned_map;
                for dm in domains.iter().filter(|dm| !dm.is_removed()) {
                    // Only insert if not already found in pruned replay (newer takes priority)
                    merged.entry(dm.domain().to_owned()).or_insert(dm.clone());
                }
                return Ok(merged);
            }
        }

        // CRC failed -> replay remaining segment, carrying forward pruned results
        warn!(
            "CRC at version {} failed to load, replaying remaining segment",
            crc_v
        );
        let remaining = log_segment.segment_through_crc(crc_v);
        let remaining_map = scan_domain_metadatas(&remaining, domain, engine)?;
        // Merge: pruned results take priority over remaining (they're newer)
        let mut merged = remaining_map;
        merged.extend(pruned_map);
        return Ok(merged);
    }

    // Case 3/4: No CRC or CRC at target failed -> full log replay
    scan_domain_metadatas(log_segment, domain, engine)
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
