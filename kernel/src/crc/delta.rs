//! Incremental CRC state updates via commit deltas.
//!
//! A [`CrcDelta`] aggregates the CRC-relevant changes from commits `(X, Y]`. Applying it to a
//! base CRC at `X` yields the CRC at `Y`: `Crc[X] + CrcDelta = Crc[Y]`. [`Crc::apply`] is the
//! consumer.

use std::collections::HashMap;

use tracing::warn;

use super::file_stats::FileStatsDelta;
use super::{
    Crc, DomainMetadataState, FileSizeHistogram, FileStats, FileStatsState, SetTransactionState,
};
use crate::actions::{DomainMetadata, Metadata, Protocol, SetTransaction};
use crate::Version;

/// CRC-relevant changes aggregated over commits `(X, Y]`.
#[derive(Debug, Clone, Default)]
pub(crate) struct CrcDelta {
    /// Net file count, size changes and histograms.
    pub(crate) file_stats: FileStatsDelta,
    /// Newest protocol over `(X, Y]`, if any.
    pub(crate) protocol: Option<Protocol>,
    /// Newest metadata over `(X, Y]`, if any.
    pub(crate) metadata: Option<Metadata>,
    /// DomainMetadata actions keyed by domain, including tombstones (`removed=true`).
    pub(crate) domain_metadata: HashMap<String, DomainMetadata>,
    /// SetTransaction actions keyed by app_id.
    pub(crate) set_transactions: HashMap<String, SetTransaction>,
    /// In-commit timestamp at `Y`. Replaces the base's ICT unconditionally
    /// (whether `Some` or `None`).
    pub(crate) in_commit_timestamp: Option<i64>,
    /// Whether the file-stats portion of this delta can be applied incrementally. When `false`,
    /// [`Crc::apply`] transitions [`FileStatsState`] to `Indeterminate`. Producers set this to
    /// `false` whenever they observe a signal that makes incremental tracking unsound (for
    /// example, a non-incremental operation such as `ANALYZE STATS`).
    pub(crate) is_incremental_safe: bool,
}

impl CrcDelta {
    /// Convert this delta into a fresh [`Crc`]. Used when the delta represents the entire table
    /// state (e.g. CREATE TABLE or the first commit in a forward replay from version zero).
    ///
    /// Returns `None` if protocol or metadata are missing (both are required for a valid CRC).
    pub(crate) fn into_crc_for_version_zero(self) -> Option<Crc> {
        let protocol = self.protocol?;
        let metadata = self.metadata?;
        // For CREATE TABLE we know the full domain metadata state: the transaction either
        // included domain metadata actions or it didn't. Always Complete. Drop tombstones
        // (a fresh table has no domains to remove).
        let domain_metadata_state = DomainMetadataState::Complete(
            self.domain_metadata
                .into_iter()
                .filter(|(_, dm)| !dm.is_removed())
                .collect(),
        );
        // CREATE TABLE starts with a known-complete set of transactions (possibly empty).
        let set_transaction_state = SetTransactionState::Complete(self.set_transactions);
        // For version zero the delta IS the full table histogram. Validate that all bins
        // are non-negative (a real table can't have negative file counts). If validation
        // fails, drop the histogram.
        let initial_histogram = self.file_stats.net_histogram.and_then(|delta| {
            delta
                .check_non_negative()
                .inspect_err(|e| {
                    warn!("Non-negative file count check failed, dropping file size histogram for version zero: {e}");
                })
                .ok()
        });
        Some(Crc {
            version: 0,
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: self.file_stats.net_files,
                table_size_bytes: self.file_stats.net_bytes,
                file_size_histogram: initial_histogram,
            }),
            protocol,
            metadata,
            domain_metadata_state,
            set_transaction_state,
            in_commit_timestamp_opt: self.in_commit_timestamp,
            ..Default::default()
        })
    }
}

/// Commit delta application for [`Crc`]. See the [module-level docs](self) for details.
impl Crc {
    /// Apply a commit delta and advance to `new_version`.
    ///
    /// - Protocol / metadata: replaced when present in the delta, kept otherwise.
    /// - ICT: unconditional replace; the delta carries ICT at `Y` (whether `Some` or `None`).
    /// - Domain metadata: tombstones (`is_removed()`) drop the key from the base map; all others
    ///   upsert by domain. Set transactions: upsert by app_id. The `Complete`/`Partial` variant is
    ///   preserved in both cases.
    /// - File stats: governed by the [`FileStatsState`] state machine.
    pub(crate) fn apply(mut self, delta: CrcDelta, new_version: Version) -> Self {
        debug_assert!(
            new_version > self.version,
            "Crc::apply must advance version: self.version={}, new_version={}",
            self.version,
            new_version
        );
        // Protocol and metadata: replace if present.
        if let Some(p) = delta.protocol {
            self.protocol = p;
        }
        if let Some(m) = delta.metadata {
            self.metadata = m;
        }

        // Apply the delta onto the CRC's existing map: upsert each non-removed entry
        // (newest wins), drop each tombstone. The variant (Complete or Partial) stays the
        // same since a delta never changes whether the base was authoritative.
        let map = match &mut self.domain_metadata_state {
            DomainMetadataState::Complete(m) | DomainMetadataState::Partial(m) => m,
        };
        for (domain, dm) in delta.domain_metadata {
            if dm.is_removed() {
                map.remove(&domain);
            } else {
                map.insert(domain, dm);
            }
        }

        // Apply the delta onto the CRC's existing map: upsert each entry (newest wins).
        // The variant (Complete or Partial) stays the same since a delta never changes whether
        // the base was authoritative.
        let map = match &mut self.set_transaction_state {
            SetTransactionState::Complete(m) | SetTransactionState::Partial(m) => m,
        };
        map.extend(delta.set_transactions);

        // In-commit timestamp at `Y` (end of the range). Unconditional replace; `None` is a
        // legal value (ICT disabled at `Y`).
        self.in_commit_timestamp_opt = delta.in_commit_timestamp;

        self.file_stats_state = transition_file_stats(
            &self.file_stats_state,
            &delta.file_stats,
            delta.is_incremental_safe,
        );

        self.version = new_version;
        self
    }
}

/// Compute the next [`FileStatsState`] given the current state and an incoming delta.
///
/// See [`FileStatsState`] for the full transition table.
fn transition_file_stats(
    current: &FileStatsState,
    delta: &FileStatsDelta,
    is_incremental_safe: bool,
) -> FileStatsState {
    match current {
        // Indeterminate is terminal in incremental replay. A future full add/remove dedup
        // pass could recover Complete.
        FileStatsState::Indeterminate => FileStatsState::Indeterminate,
        // A non-incremental delta makes incremental tracking impossible; a full
        // reconciliation can recover.
        _ if !is_incremental_safe => FileStatsState::Indeterminate,
        FileStatsState::Complete(stats) => FileStatsState::Complete(FileStats {
            // Counts and bytes have no non-negative check.
            num_files: stats.num_files + delta.net_files,
            table_size_bytes: stats.table_size_bytes + delta.net_bytes,
            // Histogram: per-bin merge; drop on failure. See `merge_histogram`.
            file_size_histogram: merge_histogram(
                stats.file_size_histogram.as_ref(),
                delta.net_histogram.as_ref(),
            ),
        }),
    }
}

/// Merge a base histogram with a delta histogram. Returns `Some(merged)` only when both are
/// present and [`FileSizeHistogram::try_apply_delta`] succeeds; otherwise returns `None`.
/// `try_apply_delta` fails on boundary mismatch or negative per-bin sums.
fn merge_histogram(
    base: Option<&FileSizeHistogram>,
    delta: Option<&FileSizeHistogram>,
) -> Option<FileSizeHistogram> {
    match (base, delta) {
        (Some(base_hist), Some(delta_hist)) => match base_hist.try_apply_delta(delta_hist) {
            Ok(merged) => Some(merged),
            Err(e) => {
                warn!("Histogram merge failed, dropping file size histogram: {e}");
                None
            }
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;

    use super::*;
    use crate::actions::{DomainMetadata, Metadata, Protocol};
    use crate::crc::{is_incremental_safe_operation, FileSizeHistogram, SetTransactionState};

    fn base_crc() -> Crc {
        Crc {
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: 10,
                table_size_bytes: 1000,
                file_size_histogram: None,
            }),
            ..Default::default()
        }
    }

    fn dm_entry(domain: &str, config: &str) -> (String, DomainMetadata) {
        (
            domain.to_string(),
            DomainMetadata::new(domain.to_string(), config.to_string()),
        )
    }

    fn dm_remove_entry(domain: &str, config: &str) -> (String, DomainMetadata) {
        (
            domain.to_string(),
            DomainMetadata::remove(domain.to_string(), config.to_string()),
        )
    }

    fn txn_entry(
        app_id: &str,
        version: i64,
        last_updated: Option<i64>,
    ) -> (String, SetTransaction) {
        (
            app_id.to_string(),
            SetTransaction::new(app_id.to_string(), version, last_updated),
        )
    }

    fn seed_dm_map() -> HashMap<String, DomainMetadata> {
        HashMap::from([dm_entry("keep", "old"), dm_entry("drop", "x")])
    }

    fn write_delta(net_files: i64, net_bytes: i64) -> CrcDelta {
        CrcDelta {
            file_stats: FileStatsDelta {
                net_files,
                net_bytes,
                ..Default::default()
            },
            is_incremental_safe: true,
            ..Default::default()
        }
    }

    // ===== is_incremental_safe tests =====

    #[test]
    fn test_incremental_safe_operations() {
        for op in [
            "WRITE",
            "MERGE",
            "UPDATE",
            "DELETE",
            "OPTIMIZE",
            "CREATE TABLE",
            "REPLACE TABLE",
            "CREATE TABLE AS SELECT",
            "REPLACE TABLE AS SELECT",
            "CREATE OR REPLACE TABLE AS SELECT",
        ] {
            assert!(
                is_incremental_safe_operation(op),
                "{op} should be incremental-safe"
            );
        }
    }

    #[test]
    fn test_non_incremental_safe_operations() {
        assert!(!is_incremental_safe_operation("ANALYZE STATS"));
        assert!(!is_incremental_safe_operation("UNKNOWN"));
    }

    // ===== Crc deserialized from CRC file (default state) =====

    #[test]
    fn test_deserialized_crc_has_complete_stats() {
        let crc = base_crc();
        let stats = crc.file_stats().unwrap();
        assert!(crc.file_stats_state.is_complete());
        assert_eq!(stats.num_files(), 10);
        assert_eq!(stats.table_size_bytes(), 1000);
    }

    // ===== Crc::apply tests =====

    #[test]
    fn test_apply_updates_file_stats() {
        let crc = base_crc().apply(write_delta(3, 600), 1);
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files(), 13); // 10 + 3
        assert_eq!(stats.table_size_bytes(), 1600); // 1000 + 600
        assert!(crc.file_stats_state.is_complete());
        assert_eq!(crc.version, 1);
    }

    /// Applies multiple commit deltas sequentially.
    #[test]
    fn test_apply_multiple_deltas() {
        let crc = base_crc()
            .apply(write_delta(3, 600), 1)
            .apply(write_delta(-2, -400), 2);
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files(), 11); // 10 + 3 - 2
        assert_eq!(stats.table_size_bytes(), 1200); // 1000 + 600 - 400
        assert!(crc.file_stats_state.is_complete());
        assert_eq!(crc.version, 2);
    }

    #[test]
    fn test_apply_not_incremental_safe_transitions_to_indeterminate() {
        let unsafe_change = CrcDelta {
            is_incremental_safe: false,
            ..write_delta(1, 100)
        };
        let crc = base_crc().apply(unsafe_change, 1);
        assert!(crc.file_stats_state.is_indeterminate());
    }

    #[test]
    fn test_indeterminate_stays_indeterminate() {
        let unsafe_change = CrcDelta {
            is_incremental_safe: false,
            ..write_delta(1, 100)
        };
        let crc = base_crc().apply(unsafe_change, 1);
        assert!(crc.file_stats_state.is_indeterminate());

        // Subsequent safe op doesn't recover the state. Version still advances.
        let crc = crc.apply(write_delta(5, 500), 2);
        assert!(crc.file_stats_state.is_indeterminate());
        assert_eq!(crc.version, 2);
    }

    // ===== apply: non-file-stats field updates =====

    #[test]
    fn test_apply_replaces_protocol() {
        let new_protocol = Protocol::try_new(
            2,
            5,
            None::<Vec<crate::table_features::TableFeature>>,
            None::<Vec<crate::table_features::TableFeature>>,
        )
        .unwrap();
        let delta = CrcDelta {
            protocol: Some(new_protocol.clone()),
            ..write_delta(0, 0)
        };
        let crc = base_crc().apply(delta, 1);
        assert_eq!(crc.protocol, new_protocol);
        assert_eq!(crc.metadata, Metadata::default()); // unchanged
    }

    /// A delta carrying an upsert, an insert, and a tombstone exercises every apply
    /// semantic for domain metadata. Applied to either `Complete` or `Partial`, the map
    /// updates correctly and the variant is preserved.
    #[rstest]
    #[case::complete(DomainMetadataState::Complete(seed_dm_map()))]
    #[case::partial(DomainMetadataState::Partial(seed_dm_map()))]
    fn test_apply_dm_upserts_inserts_and_removes(#[case] base: DomainMetadataState) {
        let was_complete = matches!(base, DomainMetadataState::Complete(_));
        let delta = CrcDelta {
            domain_metadata: HashMap::from([
                dm_entry("keep", "new"),
                dm_entry("add", "y"),
                dm_remove_entry("drop", "x"),
            ]),
            ..write_delta(0, 0)
        };
        let crc = Crc {
            domain_metadata_state: base,
            ..base_crc()
        }
        .apply(delta, 1);

        // Bind the inner map AND panic if the variant flipped during apply.
        let map = match &crc.domain_metadata_state {
            DomainMetadataState::Complete(m) if was_complete => m,
            DomainMetadataState::Partial(m) if !was_complete => m,
            other => panic!("variant changed unexpectedly: {other:?}"),
        };
        assert_eq!(map.len(), 2);
        assert_eq!(map["keep"].configuration(), "new");
        assert_eq!(map["add"].configuration(), "y");
        assert!(!map.contains_key("drop"));
    }

    #[test]
    fn test_apply_replaces_in_commit_timestamp() {
        let delta = CrcDelta {
            in_commit_timestamp: Some(9999),
            ..write_delta(0, 0)
        };
        let crc = base_crc().apply(delta, 1);
        assert_eq!(crc.in_commit_timestamp_opt, Some(9999));
    }

    #[test]
    fn test_apply_clears_in_commit_timestamp_when_delta_is_none() {
        let delta = CrcDelta {
            in_commit_timestamp: None,
            ..write_delta(0, 0)
        };
        let crc = Crc {
            in_commit_timestamp_opt: Some(1000),
            ..base_crc()
        }
        .apply(delta, 1);
        assert_eq!(crc.in_commit_timestamp_opt, None);
    }

    // ===== CrcDelta::into_crc_for_version_zero tests =====

    fn test_protocol() -> Protocol {
        Protocol::try_new(
            1,
            2,
            None::<Vec<crate::table_features::TableFeature>>,
            None::<Vec<crate::table_features::TableFeature>>,
        )
        .unwrap()
    }

    #[test]
    fn test_into_crc_for_version_zero_with_protocol_and_metadata() {
        let protocol = test_protocol();
        let metadata = Metadata::default();
        let delta = CrcDelta {
            protocol: Some(protocol.clone()),
            metadata: Some(metadata.clone()),
            ..write_delta(5, 1000)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let stats = crc.file_stats().unwrap();
        assert_eq!(crc.version, 0);
        assert_eq!(crc.protocol, protocol);
        assert_eq!(crc.metadata, metadata);
        assert_eq!(stats.num_files(), 5);
        assert_eq!(stats.table_size_bytes(), 1000);
        assert!(crc.file_stats_state.is_complete());
        assert_eq!(
            crc.domain_metadata_state,
            DomainMetadataState::Complete(HashMap::new())
        );
        assert_eq!(
            crc.set_transaction_state,
            SetTransactionState::Complete(HashMap::new())
        );
        assert_eq!(crc.in_commit_timestamp_opt, None);
    }

    #[test]
    fn test_into_crc_for_version_zero_returns_none_without_protocol() {
        let delta = CrcDelta {
            metadata: Some(Metadata::default()),
            ..write_delta(5, 1000)
        };
        assert!(delta.into_crc_for_version_zero().is_none());
    }

    #[test]
    fn test_into_crc_for_version_zero_returns_none_without_metadata() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            ..write_delta(5, 1000)
        };
        assert!(delta.into_crc_for_version_zero().is_none());
    }

    #[test]
    fn test_into_crc_for_version_zero_with_domain_metadata() {
        let dm = DomainMetadata::new("my.domain".to_string(), "config1".to_string());
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            domain_metadata: HashMap::from([("my.domain".to_string(), dm)]),
            ..write_delta(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let map = crc.domain_metadata_state.expect_complete();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my.domain"].configuration(), "config1");
    }

    #[test]
    fn test_into_crc_for_version_zero_with_in_commit_timestamp() {
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            in_commit_timestamp: Some(12345),
            ..write_delta(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        assert_eq!(crc.in_commit_timestamp_opt, Some(12345));
    }

    // ===== apply: set transaction tests =====

    fn seed_txn_map() -> HashMap<String, SetTransaction> {
        HashMap::from([txn_entry("existing", 1, Some(1000))])
    }

    /// A delta carrying both an upsert and a new entry exercises all the apply semantics for
    /// set transactions (SetTransaction has no tombstone). Applied to either `Complete` or
    /// `Partial`, the map updates correctly and the variant is preserved.
    #[rstest]
    #[case::complete(SetTransactionState::Complete(seed_txn_map()))]
    #[case::partial(SetTransactionState::Partial(seed_txn_map()))]
    fn test_apply_upserts_and_inserts_set_transactions(#[case] base: SetTransactionState) {
        let was_complete = matches!(base, SetTransactionState::Complete(_));
        let delta = CrcDelta {
            set_transactions: HashMap::from([
                txn_entry("existing", 2, Some(2000)),
                txn_entry("new", 1, Some(1500)),
            ]),
            ..write_delta(0, 0)
        };
        let crc = Crc {
            set_transaction_state: base,
            ..base_crc()
        }
        .apply(delta, 1);

        let map = match &crc.set_transaction_state {
            SetTransactionState::Complete(m) if was_complete => m,
            SetTransactionState::Partial(m) if !was_complete => m,
            other => panic!("variant changed unexpectedly: {other:?}"),
        };
        assert_eq!(map.len(), 2);
        assert_eq!(map["existing"].version, 2);
        assert_eq!(map["existing"].last_updated, Some(2000));
        assert_eq!(map["new"].version, 1);
    }

    // ===== into_crc_for_version_zero: set transaction tests =====

    #[test]
    fn test_into_crc_for_version_zero_with_set_transactions() {
        let txn = SetTransaction::new("my-app".to_string(), 5, Some(3000));
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            set_transactions: HashMap::from([("my-app".to_string(), txn)]),
            ..write_delta(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let map = crc.set_transaction_state.expect_complete();
        assert_eq!(map.len(), 1);
        assert_eq!(map["my-app"].version, 5);
        assert_eq!(map["my-app"].last_updated, Some(3000));
    }

    // ===== Histogram tests =====

    /// Helper: creates a default-boundary histogram populated with the given file sizes.
    fn histogram_from_sizes(sizes: &[i64]) -> FileSizeHistogram {
        let mut hist = FileSizeHistogram::create_default();
        for &size in sizes {
            hist.insert(size).unwrap();
        }
        hist
    }

    /// Helper: creates a CRC with a histogram containing the given file sizes.
    fn base_crc_with_histogram(file_sizes: &[i64]) -> Crc {
        let hist = histogram_from_sizes(file_sizes);
        Crc {
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: file_sizes.len() as i64,
                table_size_bytes: file_sizes.iter().sum(),
                file_size_histogram: Some(hist),
            }),
            ..Default::default()
        }
    }

    /// Helper: creates a CrcDelta with a delta histogram built from adds and removes.
    fn write_delta_with_histograms(add_sizes: &[i64], remove_sizes: &[i64]) -> CrcDelta {
        let mut hist = FileSizeHistogram::create_default();
        for &s in add_sizes {
            hist.insert(s).unwrap();
        }
        for &s in remove_sizes {
            hist.remove(s).unwrap();
        }
        let net_files = add_sizes.len() as i64 - remove_sizes.len() as i64;
        let net_bytes: i64 = add_sizes.iter().sum::<i64>() - remove_sizes.iter().sum::<i64>();
        CrcDelta {
            file_stats: FileStatsDelta {
                net_files,
                net_bytes,
                net_histogram: Some(hist),
            },
            is_incremental_safe: true,
            ..Default::default()
        }
    }

    /// Histogram bins used in tests (default boundaries):
    ///   Bin 0: [0, 8KB)     -- e.g. 100, 200, 300, 500
    ///   Bin 1: [8KB, 16KB)  -- e.g. 10_000
    ///   Bin 2: [16KB, 32KB) -- e.g. 20_000
    ///   Bin 10: [4MB, 8MB)  -- e.g. 5_000_000
    #[rstest]
    #[case::single_bin(&[100, 200, 300], &[500], &[200], &[(0, 3, 900)])]
    #[case::adds_only(&[100], &[200, 300], &[], &[(0, 3, 600)])]
    #[case::removes_only(&[100, 200, 300], &[], &[100, 200], &[(0, 1, 300)])]
    #[case::empty_delta(&[100, 10_000], &[], &[], &[(0, 1, 100), (1, 1, 10_000)])]
    #[case::multi_bin(
        &[100, 10_000, 20_000],
        &[200, 10_500],
        &[100, 20_000],
        &[(0, 1, 200), (1, 2, 20_500), (2, 0, 0)]
    )]
    #[case::large_files(
        &[100, 5_000_000],
        &[10_000, 5_500_000],
        &[100],
        &[(0, 0, 0), (1, 1, 10_000), (10, 2, 10_500_000)]
    )]
    fn apply_merges_histogram(
        #[case] base: &[i64],
        #[case] add: &[i64],
        #[case] remove: &[i64],
        #[case] expected_bins: &[(usize, i64, i64)],
    ) {
        let delta = write_delta_with_histograms(add, remove);
        let crc = base_crc_with_histogram(base).apply(delta, 1);

        let stats = crc.file_stats().unwrap();
        let hist = stats.file_size_histogram().unwrap();
        for &(bin, count, bytes) in expected_bins {
            assert_eq!(hist.file_counts[bin], count, "file_counts[{bin}]");
            assert_eq!(hist.total_bytes[bin], bytes, "total_bytes[{bin}]");
        }
    }

    #[rstest]
    #[case::base_none_delta_none(None)]
    #[case::base_some_delta_none(Some(vec![100i64, 200]))]
    fn apply_drops_histogram_when_delta_missing_histogram(#[case] base_files: Option<Vec<i64>>) {
        let base = match &base_files {
            Some(sizes) => base_crc_with_histogram(sizes),
            None => base_crc(),
        };
        let delta = CrcDelta {
            file_stats: FileStatsDelta {
                net_files: 1,
                net_bytes: 100,
                net_histogram: None,
            },
            is_incremental_safe: true,
            ..Default::default()
        };
        let crc = base.apply(delta, 1);
        let stats = crc.file_stats().unwrap();
        assert!(
            stats.file_size_histogram().is_none(),
            "histogram should be None when delta doesn't provide a histogram"
        );
    }

    #[test]
    fn apply_drops_histogram_on_indeterminate() {
        let unsafe_delta = CrcDelta {
            is_incremental_safe: false,
            ..write_delta(1, 100)
        };
        let crc = base_crc_with_histogram(&[100, 200]).apply(unsafe_delta, 1);
        // Indeterminate has no histogram field; the histogram data is gone.
        assert!(crc.file_stats_state.is_indeterminate());
        assert!(crc.file_stats().is_none());
    }

    #[test]
    fn into_crc_for_version_zero_includes_histogram() {
        let delta_hist = histogram_from_sizes(&[500, 1000]);
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            file_stats: FileStatsDelta {
                net_files: 2,
                net_bytes: 1500,
                net_histogram: Some(delta_hist),
            },
            is_incremental_safe: true,
            ..Default::default()
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let stats = crc.file_stats().unwrap();
        let hist = stats.file_size_histogram().unwrap();
        assert_eq!(hist.file_counts[0], 2);
        assert_eq!(hist.total_bytes[0], 1500);
    }

    #[test]
    fn into_crc_for_version_zero_without_histogram() {
        // write_delta() produces a CrcDelta with no histogram delta, so
        // into_crc_for_version_zero cannot construct a file size histogram.
        let delta = CrcDelta {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            ..write_delta(0, 0)
        };
        let crc = delta.into_crc_for_version_zero().unwrap();
        let stats = crc.file_stats().unwrap();
        assert!(stats.file_size_histogram().is_none());
    }

    #[test]
    fn apply_merges_histogram_with_non_default_boundaries() {
        // Base CRC with custom 3-bin histogram: [0, 200) [200, 1000) [1000, inf)
        let boundaries = vec![0, 200, 1000];
        let base_hist = FileSizeHistogram::try_new(
            boundaries.clone(),
            vec![2, 1, 0], // 2 files in bin 0, 1 in bin 1
            vec![300, 500, 0],
        )
        .unwrap();
        let base = Crc {
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: 3,
                table_size_bytes: 800,
                file_size_histogram: Some(base_hist),
            }),
            ..Default::default()
        };

        // Delta with matching non-default boundaries: +100 and +1500, -150
        let mut delta_hist = FileSizeHistogram::create_empty_with_boundaries(boundaries).unwrap();
        delta_hist.insert(100).unwrap(); // bin 0
        delta_hist.insert(1500).unwrap(); // bin 2
        delta_hist.remove(150).unwrap(); // bin 0

        let delta = CrcDelta {
            file_stats: FileStatsDelta {
                net_files: 1,    // +2 - 1
                net_bytes: 1450, // (100 + 1500) - 150
                net_histogram: Some(delta_hist),
            },
            is_incremental_safe: true,
            ..Default::default()
        };

        let crc = base.apply(delta, 1);

        // Histogram should be preserved (boundaries match)
        let stats = crc.file_stats().unwrap();
        let hist = stats.file_size_histogram().unwrap();
        assert_eq!(hist.sorted_bin_boundaries, vec![0, 200, 1000]);
        assert_eq!(hist.file_counts, vec![2, 1, 1]); // (2+1-1), (1+0-0), (0+1-0)
        assert_eq!(hist.total_bytes, vec![250, 500, 1500]); // (300+100-150), (500+0-0), (0+1500-0)
        assert_eq!(stats.num_files(), 4);
        assert_eq!(stats.table_size_bytes(), 2250);
    }
}
