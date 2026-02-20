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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use url::Url;

    use crate::actions::visitors::DomainMetadataVisitor;
    use crate::committer::FileSystemCommitter;
    use crate::engine::default::DefaultEngineBuilder;
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
        let engine = DefaultEngineBuilder::new(store).build();
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

    /// Proves that when requested domains span two commits, both batches ARE consumed
    /// (i.e., we don't terminate early until all N domains are found).
    #[tokio::test]
    async fn test_scan_domain_metadatas_no_early_termination_when_split_across_commits() {
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
}
