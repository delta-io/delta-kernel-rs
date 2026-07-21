//! Domain metadata replay logic for [`LogSegment`].
//!
//! Two entry points: [`LogSegment::scan_domain_metadatas`] replays the whole log for the latest
//! domain metadata, and [`LogSegment::scan_domain_metadatas_rooted_in_crc`] scans only the commits
//! after an authoritative stale CRC and reconciles them against its active-domain map.

use std::collections::{HashMap, HashSet};

use tracing::instrument;

use super::LogSegment;
use crate::actions::visitors::DomainMetadataVisitor;
use crate::actions::{DomainMetadata, LOG_DOMAIN_METADATA_SCHEMA};
use crate::crc::merge_domain_metadata;
use crate::log_replay::ActionsBatch;
use crate::{DeltaResult, Engine, RowVisitor as _, Version};

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
        Ok(self
            .visit_domain_metadatas(domains, engine)?
            .into_domain_metadatas())
    }

    /// Answer a domain-metadata query rooted in an authoritative (`Complete`) but stale CRC's
    /// `base_active` map at `base_version`, scanning ONLY the commits in `(base_version, end]`.
    ///
    /// The checkpoint and every commit at/below `base_version` are skipped via
    /// [`Self::segment_after_version`]. For each domain the newest action in the tail wins; a tail
    /// tombstone means the domain is absent at `end` even when `base_active` holds it. A domain the
    /// tail never mentions falls back to `base_active`. `domains == None` answers all active
    /// domains; `Some(filter)` answers only the requested ones. Returned maps never contain
    /// tombstones.
    pub(crate) fn scan_domain_metadatas_rooted_in_crc(
        &self,
        base_version: Version,
        base_active: &HashMap<String, DomainMetadata>,
        domains: Option<&HashSet<&str>>,
        engine: &dyn Engine,
    ) -> DeltaResult<DomainMetadataMap> {
        let tail = self
            .segment_after_version(base_version)
            .scan_tail_including_tombstones(domains, engine)?;
        let reconciled = match domains {
            // Filtered: the newest tail action per requested domain wins; a tombstone settles the
            // answer as absent. Fall back to `base_active` only when the tail never mentions it.
            Some(filter) => filter
                .iter()
                .filter_map(|&k| match tail.get(k) {
                    Some(dm) if dm.is_removed() => None,
                    Some(dm) => Some((k.to_string(), dm.clone())),
                    None => base_active.get(k).map(|dm| (k.to_string(), dm.clone())),
                })
                .collect(),
            // Unfiltered: merge the whole tail onto the base, dropping tombstoned domains.
            None => {
                let mut active = base_active.clone();
                merge_domain_metadata(&mut active, tail);
                active
            }
        };
        Ok(reconciled)
    }

    /// Reverse-replay this segment for domain metadata, keeping tombstones. Terminates early once
    /// every requested domain is decided (a tombstone counts as decided). The CRC-rooted path keeps
    /// tombstones so a removal in a newer commit can suppress a domain the base holds.
    fn scan_tail_including_tombstones(
        &self,
        domains: Option<&HashSet<&str>>,
        engine: &dyn Engine,
    ) -> DeltaResult<DomainMetadataMap> {
        Ok(self
            .visit_domain_metadatas(domains, engine)?
            .into_domain_metadatas_including_tombstones())
    }

    /// Reverse-replay this segment, folding domain metadata actions into a
    /// [`DomainMetadataVisitor`] (newest-first, first-seen-wins). A domain filter terminates
    /// the scan early once every requested domain is found; without one the whole segment is
    /// replayed. The caller chooses whether to keep or strip tombstones from the returned
    /// visitor.
    fn visit_domain_metadatas(
        &self,
        domains: Option<&HashSet<&str>>,
        engine: &dyn Engine,
    ) -> DeltaResult<DomainMetadataVisitor> {
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
        Ok(visitor)
    }

    /// Read action batches from the log, projecting rows to only contain domain metadata columns.
    fn read_domain_metadata_batches(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        self.read_actions(engine, LOG_DOMAIN_METADATA_SCHEMA.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use url::Url;

    use crate::actions::visitors::DomainMetadataVisitor;
    use crate::committer::FileSystemCommitter;
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::schema::{DataType, StructField, StructType};
    use crate::transaction::create_table::create_table as create_table_txn;
    use crate::{RowVisitor as _, Snapshot};

    /// Builds a two-commit in-memory Delta table:
    ///   commit 0: protocol + metadata (with domainMetadata feature) + "domainC"
    ///   commit 1: "domainA" + "domainB"
    ///
    /// Log replay visits commits newest-first, so commit 1 is the first batch and commit 0
    /// is the second batch.
    fn build_two_commit_log() -> (impl crate::Engine, std::sync::Arc<Snapshot>) {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store);
        let url = Url::parse("memory:///").unwrap();

        // Commit 0: CREATE TABLE (protocol + metadata) with "domainC" in the same commit.
        // The domainMetadata writer feature is enabled so domain metadata actions are valid.
        let _ = create_table_txn(
            url.as_str(),
            Arc::new(StructType::new_unchecked(vec![StructField::new(
                "id",
                DataType::INTEGER,
                true,
            )])),
            "test",
        )
        .with_table_properties([("delta.feature.domainMetadata", "supported")])
        .build(&engine, Box::new(FileSystemCommitter::new()))
        .unwrap()
        .with_domain_metadata("domainC".to_string(), "cfgC".to_string())
        .commit(&engine)
        .unwrap();

        // Commit 1: add domainA and domainB via an existing-table transaction.
        let snapshot = Snapshot::builder_for(url.clone()).build(&engine).unwrap();
        let _ = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)
            .unwrap()
            .with_domain_metadata("domainA".to_string(), "cfgA".to_string())
            .with_domain_metadata("domainB".to_string(), "cfgB".to_string())
            .commit(&engine)
            .unwrap();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        (engine, snapshot)
    }

    /// Proves early termination actually fires: when both requested domains are found in the
    /// first (newest) batch, the iterator is broken before the second (older) batch is consumed.
    ///
    /// Strategy: count total batches via `read_domain_metadata_batches`, then manually drive
    /// the same loop that `scan_domain_metadatas` uses and count how many batches are consumed
    /// before `filter_found()` triggers the break. Asserting consumed < total is the only way
    /// to confirm the iterator is abandoned early — the domain values alone cannot distinguish
    /// this because `or_insert` in the visitor makes results identical whether or not the second
    /// batch was read.
    #[tokio::test]
    async fn test_scan_domain_metadatas_early_termination() {
        let (engine, snapshot) = build_two_commit_log();
        let log_segment = snapshot.log_segment();

        // Sanity-check: the log has exactly 2 batches (one per commit).
        let total_batches = log_segment
            .read_domain_metadata_batches(&engine)
            .unwrap()
            .filter(|r| r.is_ok())
            .count();
        assert_eq!(
            total_batches, 2,
            "expected 2 total batches (one per commit)"
        );

        // Drive the loop manually — identical to the body of scan_domain_metadatas — and
        // count how many batches are consumed before filter_found() breaks the loop.
        let filter = HashSet::from(["domainA".to_string(), "domainB".to_string()]);
        let mut visitor = DomainMetadataVisitor::new(Some(filter));
        let mut batches_consumed = 0;
        for actions in log_segment.read_domain_metadata_batches(&engine).unwrap() {
            batches_consumed += 1;
            visitor
                .visit_rows_of(actions.unwrap().actions.as_ref())
                .unwrap();
            if visitor.filter_found() {
                break;
            }
        }

        // The key assertion: only 1 of the 2 batches was consumed — early termination worked.
        assert_eq!(
            batches_consumed, 1,
            "should break after the first (newest) batch once both domains are found"
        );
        assert!(
            batches_consumed < total_batches,
            "early termination must consume fewer batches than the total"
        );

        // Also verify correct results: domainA and domainB present, domainC absent.
        let result = visitor.into_domain_metadatas();
        assert_eq!(result.len(), 2);
        assert_eq!(result["domainA"].configuration(), "cfgA");
        assert_eq!(result["domainB"].configuration(), "cfgB");
        assert!(
            !result.contains_key("domainC"),
            "domainC must not appear — second batch was not read"
        );
    }

    #[tokio::test]
    async fn test_scan_domain_metadatas_with_single_domain_filter_returns_only_that_domain() {
        let (engine, snapshot) = build_two_commit_log();
        let result = snapshot
            .log_segment()
            .scan_domain_metadatas(Some(&HashSet::from(["domainA"])), &engine)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result["domainA"].configuration(), "cfgA");
    }

    #[tokio::test]
    async fn test_scan_domain_metadatas_with_subset_filter_returns_matching_domains() {
        let (engine, snapshot) = build_two_commit_log();
        let result = snapshot
            .log_segment()
            .scan_domain_metadatas(Some(&HashSet::from(["domainA", "domainC"])), &engine)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result["domainA"].configuration(), "cfgA");
        assert_eq!(result["domainC"].configuration(), "cfgC");
    }

    #[tokio::test]
    async fn test_scan_domain_metadatas_with_no_filter_returns_all_domains() {
        let (engine, snapshot) = build_two_commit_log();
        let result = snapshot
            .log_segment()
            .scan_domain_metadatas(None, &engine)
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result["domainA"].configuration(), "cfgA");
        assert_eq!(result["domainB"].configuration(), "cfgB");
        assert_eq!(result["domainC"].configuration(), "cfgC");
    }

    #[tokio::test]
    async fn test_scan_domain_metadatas_with_split_domains_does_not_terminate_early() {
        let (engine, snapshot) = build_two_commit_log();
        let log_segment = snapshot.log_segment();

        // domainA is in commit 1 (batch 0), domainC is in commit 0 (batch 1).
        // filter_found() must not trigger after batch 0 alone.
        let filter = HashSet::from(["domainA".to_string(), "domainC".to_string()]);
        let mut visitor = DomainMetadataVisitor::new(Some(filter));
        let mut batches_consumed = 0;
        for actions in log_segment.read_domain_metadata_batches(&engine).unwrap() {
            batches_consumed += 1;
            visitor
                .visit_rows_of(actions.unwrap().actions.as_ref())
                .unwrap();
            if visitor.filter_found() {
                break;
            }
        }

        assert_eq!(
            batches_consumed, 2,
            "must read both batches when requested domains span two commits"
        );
        let result = visitor.into_domain_metadatas();
        assert_eq!(result["domainA"].configuration(), "cfgA");
        assert_eq!(result["domainC"].configuration(), "cfgC");
    }

    /// Adds a third commit to [`build_two_commit_log`] that tombstones "domainA".
    fn build_log_with_tombstone() -> (impl crate::Engine, std::sync::Arc<Snapshot>) {
        let (engine, snapshot) = build_two_commit_log();
        let table_root = snapshot.table_root().clone();
        let _ = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), &engine)
            .unwrap()
            .with_domain_metadata_removed("domainA".to_string())
            .commit(&engine)
            .unwrap();
        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
        (engine, snapshot)
    }

    #[tokio::test]
    async fn test_scan_tail_keeps_tombstone_that_scan_domain_metadatas_drops() {
        let (engine, snapshot) = build_log_with_tombstone();
        let log_segment = snapshot.log_segment();

        // scan_domain_metadatas strips the tombstone: "domainA" is gone.
        let active = log_segment.scan_domain_metadatas(None, &engine).unwrap();
        assert!(!active.contains_key("domainA"));
        assert!(active.contains_key("domainB"));
        assert!(active.contains_key("domainC"));

        // scan_tail_including_tombstones retains it as a tombstone so it can suppress a base
        // domain.
        let with_tombstones = log_segment
            .scan_tail_including_tombstones(None, &engine)
            .unwrap();
        assert!(with_tombstones["domainA"].is_removed());
        assert!(!with_tombstones["domainB"].is_removed());
        assert!(!with_tombstones["domainC"].is_removed());
    }
}
