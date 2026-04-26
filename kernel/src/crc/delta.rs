//! Incremental CRC state updates.
//!
//! A [`CrcUpdate`] captures CRC-relevant changes from a single commit OR from a multi-commit
//! replay. [`Crc::apply`] is the universal mutator, applying an update onto a base CRC.
//!
//! This satisfies the fundamental invariant of the CRC system:
//!
//! ```text
//! Crc[N] + CrcUpdate(N→N+1) = Crc[N+1]
//! Crc[N] + CrcUpdate(N→M)   = Crc[M]    (M > N+1)
//! ```
//!
//! `apply` itself is *forward*: it always reads the equation left-to-right (older + delta =
//! newer). The update can be *produced* by either direction:
//!
//! - **Commit-time**: a single commit's actions are turned into one update; apply once.
//! - **Stale-CRC reverse-replay**: commits N+1..M read in reverse, accumulated into one update via
//!   [`ReverseCrcAccumulator`](crate::log_segment::CrcReplayAccumulator); apply once. (Reverse uses
//!   first-seen-wins per key; the resulting update is equivalent to `δ_{N+1} + δ_{N+2} + ... + δ_M`
//!   produced forward.)
//! - **Future: forward per-commit replay** (used by rebase): each commit produces an update,
//!   applied repeatedly. No accumulator needed.
//! - **Future: action reconciliation**: a wider visitor with deduplication produces one update,
//!   applied once. Same shape, different production.
//!
//! All four producers share `CrcUpdate` and feed `Crc::apply`.
//!
//! # Field semantics on apply
//!
//! - **Protocol / Metadata**: replace if `Some` (last-write-wins).
//! - **Domain metadata / Set transactions**: upsert from update map; tombstones (DM with
//!   `removed=true`) remove the entry. If base is [`Untracked`](super::DomainMetadataState),
//!   transition to [`Partial`](super::DomainMetadataState) when update has entries.
//! - **In-commit timestamp**: replace unconditionally (`None` clears, `Some` sets).
//! - **File stats**: state-machine transition based on update flags and base variant. See the table
//!   on [`FileStatsState`].

use std::collections::HashMap;

use tracing::warn;

use super::file_stats::FileStatsDelta;
use super::{Crc, DomainMetadataState, FileSizeHistogram, FileStatsState, SetTransactionState};
use crate::actions::{DomainMetadata, Metadata, Protocol, SetTransaction};
use crate::{DeltaResult, Error};

/// CRC-relevant changes ("delta") to apply to a base [`Crc`]. Universal type used by
/// commit-time, log-replay, and (future) rebase paths.
///
/// **Last-write-wins semantics**: every field in this struct represents "what should be set
/// on the resulting CRC at the new version." The producer is responsible for ensuring the
/// values are correct for the target version (e.g. reverse-replay producers use first-seen
/// wins to capture the newest values).
#[derive(Debug, Clone, Default)]
pub(crate) struct CrcUpdate {
    /// Net file count, size changes, and optional histogram (see [`FileStatsDelta`]).
    pub(crate) file_stats: FileStatsDelta,
    /// New protocol action, if changed in this update. `None` means "no change."
    pub(crate) protocol: Option<Protocol>,
    /// New metadata action, if changed. `None` means "no change."
    pub(crate) metadata: Option<Metadata>,
    /// Domain metadata changes keyed by domain name. Tombstones (DM with `removed=true`)
    /// remove the entry on apply. Pre-keyed for direct upsert -- producers (commit-time
    /// path, reverse-replay accumulator) own the dedup logic.
    pub(crate) domain_metadata: HashMap<String, DomainMetadata>,
    /// Set transaction changes keyed by `app_id`. Pre-keyed for direct upsert.
    pub(crate) set_transactions: HashMap<String, SetTransaction>,
    /// In-commit timestamp observed by the producer. The outer `Option` is "did the
    /// producer observe ICT data" (None = no observation; the apply leaves the base CRC's
    /// ICT untouched). The inner `Option` is the ICT value itself: `Some(v)` sets the new
    /// ICT, `None` clears it (e.g. ICT was disabled).
    ///
    /// In practice:
    /// - Commit-time path: always `Some(maybe_ict)` -- the transaction either has ICT enabled
    ///   (`Some(Some(v))`) or doesn't (`Some(None)`).
    /// - Reverse-replay accumulator: `Some(maybe_ict)` only when at least one commit's
    ///   `commitInfo.inCommitTimestamp` was observed; `None` when the segment was empty or
    ///   contained no commitInfo (trivially safe / checkpoint-only branches).
    pub(crate) in_commit_timestamp: Option<Option<i64>>,
    /// `true` iff every observed commit had an incremental-safe operation. `false` if any
    /// commit's `commitInfo.operation` was unrecognized, missing, or known-unsafe (e.g.
    /// `ANALYZE STATS`). Producers compute this once.
    pub(crate) operation_safe: bool,
    /// `true` iff any observed file action had a missing `size` field. Permanent: an update
    /// with this flag transitions file stats to [`Untrackable`](FileStatsState::Untrackable).
    pub(crate) has_missing_file_size: bool,
}

impl CrcUpdate {
    /// Convert this update into a fresh [`Crc`].
    ///
    /// Used when the update represents the *complete* table state (no base CRC to apply
    /// onto). Specifically: full log replay from version 0, or no-CRC bootstrap from a
    /// checkpoint + commits where the accumulator captured everything.
    ///
    /// Returns:
    /// - `FileStatsState::Untrackable` if `has_missing_file_size`
    /// - `FileStatsState::Indeterminate` if `!operation_safe`
    /// - `FileStatsState::Valid { ... }` otherwise (with histogram if present)
    /// - DM and txns: [`Complete`](DomainMetadataState::Complete) (full state was captured)
    /// - Filters DM tombstones from the resulting map
    ///
    /// # Errors
    ///
    /// Returns an error if `protocol` or `metadata` is missing -- both are required to
    /// construct a valid CRC.
    pub(crate) fn into_fresh_crc(self) -> DeltaResult<Crc> {
        let protocol = self.protocol.ok_or(Error::MissingProtocol)?;
        let metadata = self.metadata.ok_or(Error::MissingMetadata)?;

        let domain_metadata = DomainMetadataState::Complete(
            self.domain_metadata
                .into_iter()
                .filter(|(_, dm)| !dm.is_removed())
                .collect(),
        );
        let set_transactions = SetTransactionState::Complete(self.set_transactions);

        let file_stats = if self.has_missing_file_size {
            FileStatsState::Untrackable
        } else if !self.operation_safe {
            FileStatsState::Indeterminate
        } else {
            // For a full-replay update, file_stats.net_files / net_bytes / net_histogram are
            // the absolute totals (no negatives possible since we're summing checkpoint
            // adds + commit adds - commit removes from a complete history). If for any
            // reason the histogram has negative bins (corrupted log), drop it rather than
            // emit a Valid CRC with a bad histogram.
            let histogram = self.file_stats.net_histogram.and_then(|hist| {
                hist.check_non_negative()
                    .inspect_err(|e| {
                        warn!("into_fresh_crc: dropping histogram due to negative bins: {e}");
                    })
                    .ok()
            });
            FileStatsState::Valid {
                num_files: self.file_stats.net_files,
                table_size_bytes: self.file_stats.net_bytes,
                histogram,
            }
        };

        Ok(Crc {
            protocol,
            metadata,
            file_stats,
            domain_metadata,
            set_transactions,
            in_commit_timestamp_opt: self.in_commit_timestamp.flatten(),
        })
    }
}

/// Merge a base histogram with the update's net histogram. Returns `Some(merged)` on
/// success, `None` to drop the histogram (when the merge would fail or when partial data
/// would be misleading).
///
/// Behavior:
/// - `(Some(base), Some(net))` → merge via [`FileSizeHistogram::try_apply_delta`]; on failure
///   (boundary mismatch, negative counts after merge), drop and warn.
/// - `(Some(base), None)` → drop. The base had a histogram but the update could not provide one;
///   carrying the base forward without the update's contribution would be stale.
/// - `(None, Some(_))` → drop. The base lacked a histogram, so the update's net histogram alone (a
///   delta over an unknown baseline) cannot be turned into an absolute. The right place to seed an
///   initial histogram is [`CrcUpdate::into_fresh_crc`], which receives an absolute (full-replay)
///   histogram rather than a delta.
/// - `(None, None)` → stay `None`.
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

// ============================================================================
// Crc::apply -- the universal mutator
// ============================================================================

impl Crc {
    /// Apply a [`CrcUpdate`], advancing this CRC to the new version.
    ///
    /// Implements the fundamental invariant `Crc[N] + CrcUpdate(N→M) = Crc[M]`. Direction-
    /// agnostic: works for single-commit deltas (M = N+1) or accumulated multi-commit
    /// deltas (M > N+1) interchangeably.
    ///
    /// Field semantics: see the [module-level docs](self).
    pub(crate) fn apply(&mut self, update: CrcUpdate) {
        let CrcUpdate {
            file_stats,
            protocol,
            metadata,
            domain_metadata,
            set_transactions,
            in_commit_timestamp,
            operation_safe,
            has_missing_file_size,
        } = update;

        // Protocol / Metadata: replace if present.
        if let Some(p) = protocol {
            self.protocol = p;
        }
        if let Some(m) = metadata {
            self.metadata = m;
        }

        // Domain metadata: upsert from update map, removing tombstones. If base is
        // Untracked but the update has entries (e.g. older CRC didn't track DM but newer
        // commits do), transition to Partial.
        if !domain_metadata.is_empty() {
            match &mut self.domain_metadata {
                DomainMetadataState::Complete(map) | DomainMetadataState::Partial(map) => {
                    for (domain, dm) in domain_metadata {
                        if dm.is_removed() {
                            map.remove(&domain);
                        } else {
                            map.insert(domain, dm);
                        }
                    }
                }
                DomainMetadataState::Untracked => {
                    self.domain_metadata = DomainMetadataState::Partial(
                        domain_metadata
                            .into_iter()
                            .filter(|(_, dm)| !dm.is_removed())
                            .collect(),
                    );
                }
            }
        }

        // Set transactions: upsert from update map. No tombstone semantic for txns. Same
        // Untracked → Partial transition.
        if !set_transactions.is_empty() {
            match &mut self.set_transactions {
                SetTransactionState::Complete(map) | SetTransactionState::Partial(map) => {
                    map.extend(set_transactions);
                }
                SetTransactionState::Untracked => {
                    self.set_transactions = SetTransactionState::Partial(set_transactions);
                }
            }
        }

        // In-commit timestamp: replace ONLY when the producer observed it (Some(_)).
        // `None` means "no observation" -- leave base CRC's ICT untouched (this is the
        // empty-segment / checkpoint-only / no-commitInfo trivially-safe path). When the
        // producer did observe ICT, the inner Option is the new value (Some = enabled,
        // None = ICT disabled).
        if let Some(observed_ict) = in_commit_timestamp {
            self.in_commit_timestamp_opt = observed_ict;
        }

        self.file_stats = transition_file_stats(
            &self.file_stats,
            &file_stats,
            operation_safe,
            has_missing_file_size,
        );
    }
}

/// Compute the next [`FileStatsState`] given the current state and an incoming delta.
///
/// Implements the state-machine table on [`FileStatsState`]. Read top-to-bottom:
///
/// 1. `Untrackable` is terminal -- nothing recovers missing file size.
/// 2. Any delta with `has_missing_file_size` poisons the state to `Untrackable`.
/// 3. `Indeterminate` is terminal except for the `Untrackable` escalation handled above.
/// 4. Any delta with `!operation_safe` transitions to `Indeterminate`.
/// 5. Otherwise, the current state is `Valid` or `RequiresCheckpointRead` and we sum counts/bytes
///    and merge histograms.
fn transition_file_stats(
    current: &FileStatsState,
    delta: &FileStatsDelta,
    operation_safe: bool,
    has_missing_file_size: bool,
) -> FileStatsState {
    if matches!(current, FileStatsState::Untrackable) {
        return FileStatsState::Untrackable;
    }
    if has_missing_file_size {
        return FileStatsState::Untrackable;
    }
    if matches!(current, FileStatsState::Indeterminate) {
        return FileStatsState::Indeterminate;
    }
    if !operation_safe {
        return FileStatsState::Indeterminate;
    }
    match current {
        FileStatsState::Valid {
            num_files,
            table_size_bytes,
            histogram,
        } => FileStatsState::Valid {
            num_files: num_files + delta.net_files,
            table_size_bytes: table_size_bytes + delta.net_bytes,
            histogram: merge_histogram(histogram.as_ref(), delta.net_histogram.as_ref()),
        },
        FileStatsState::RequiresCheckpointRead {
            commit_delta_files,
            commit_delta_bytes,
            commit_delta_histogram,
        } => FileStatsState::RequiresCheckpointRead {
            commit_delta_files: commit_delta_files + delta.net_files,
            commit_delta_bytes: commit_delta_bytes + delta.net_bytes,
            commit_delta_histogram: merge_histogram(
                commit_delta_histogram.as_ref(),
                delta.net_histogram.as_ref(),
            ),
        },
        // Untrackable / Indeterminate handled by early returns above.
        FileStatsState::Untrackable | FileStatsState::Indeterminate => current.clone(),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;

    use super::*;
    use crate::actions::{DomainMetadata, Metadata, Protocol};
    use crate::crc::FileStatsValidity;

    fn write_update(net_files: i64, net_bytes: i64) -> CrcUpdate {
        CrcUpdate {
            file_stats: FileStatsDelta {
                net_files,
                net_bytes,
                net_histogram: None,
            },
            operation_safe: true,
            ..Default::default()
        }
    }

    fn base_crc() -> Crc {
        Crc {
            file_stats: FileStatsState::Valid {
                num_files: 10,
                table_size_bytes: 1000,
                histogram: None,
            },
            ..Default::default()
        }
    }

    fn test_protocol() -> Protocol {
        Protocol::try_new(
            1,
            2,
            None::<Vec<crate::table_features::TableFeature>>,
            None::<Vec<crate::table_features::TableFeature>>,
        )
        .unwrap()
    }

    // ===== Crc::apply: file stats state machine =====

    #[test]
    fn apply_safe_op_keeps_valid_and_sums() {
        let mut crc = base_crc();
        crc.apply(write_update(3, 600));
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files(), 13);
        assert_eq!(stats.table_size_bytes(), 1600);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Valid);
    }

    #[test]
    fn apply_multiple_safe_updates_sums_correctly() {
        // Demonstrates Crc[N] + δ + δ + δ = Crc[N+3] composition.
        let mut crc = base_crc();
        crc.apply(write_update(3, 600));
        crc.apply(write_update(-2, -400));
        crc.apply(write_update(1, 100));
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files(), 12); // 10 + 3 - 2 + 1
        assert_eq!(stats.table_size_bytes(), 1300); // 1000 + 600 - 400 + 100
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Valid);
    }

    #[test]
    fn apply_unsafe_op_transitions_valid_to_indeterminate() {
        let mut crc = base_crc();
        crc.apply(CrcUpdate {
            operation_safe: false,
            ..write_update(1, 100)
        });
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Indeterminate);
    }

    #[test]
    fn apply_indeterminate_stays_indeterminate_on_safe_update() {
        let mut crc = base_crc();
        crc.apply(CrcUpdate {
            operation_safe: false,
            ..write_update(1, 100)
        });
        crc.apply(write_update(5, 500));
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Indeterminate);
    }

    #[test]
    fn apply_missing_file_size_transitions_valid_to_untrackable() {
        let mut crc = base_crc();
        crc.apply(CrcUpdate {
            has_missing_file_size: true,
            ..write_update(1, 100)
        });
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Untrackable);
    }

    #[test]
    fn apply_untrackable_stays_untrackable() {
        let mut crc = base_crc();
        crc.apply(CrcUpdate {
            has_missing_file_size: true,
            ..write_update(1, 100)
        });
        crc.apply(write_update(5, 500));
        crc.apply(CrcUpdate {
            operation_safe: false,
            ..write_update(1, 100)
        });
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Untrackable);
    }

    #[test]
    fn apply_indeterminate_transitions_to_untrackable_on_missing_size() {
        let mut crc = base_crc();
        crc.apply(CrcUpdate {
            operation_safe: false,
            ..write_update(1, 100)
        });
        crc.apply(CrcUpdate {
            has_missing_file_size: true,
            ..write_update(1, 100)
        });
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Untrackable);
    }

    // ===== Crc::apply: P&M, DM, txn, ICT =====

    #[test]
    fn apply_replaces_protocol_and_metadata_when_present() {
        let mut crc = base_crc();
        let new_protocol = Protocol::try_new(
            2,
            5,
            None::<Vec<crate::table_features::TableFeature>>,
            None::<Vec<crate::table_features::TableFeature>>,
        )
        .unwrap();
        crc.apply(CrcUpdate {
            protocol: Some(new_protocol.clone()),
            ..write_update(0, 0)
        });
        assert_eq!(crc.protocol, new_protocol);
        assert_eq!(crc.metadata, Metadata::default()); // unchanged
    }

    #[test]
    fn apply_dm_to_complete_upserts_and_removes_tombstones() {
        let mut crc = base_crc();
        let mut dms = HashMap::new();
        dms.insert(
            "old".to_string(),
            DomainMetadata::new("old".to_string(), "v1".to_string()),
        );
        crc.domain_metadata = DomainMetadataState::Complete(dms);

        let mut update_map = HashMap::new();
        update_map.insert(
            "old".to_string(),
            DomainMetadata::new("old".to_string(), "v2".to_string()),
        ); // upsert
        update_map.insert(
            "new".to_string(),
            DomainMetadata::new("new".to_string(), "config".to_string()),
        ); // add
        crc.apply(CrcUpdate {
            domain_metadata: update_map,
            ..write_update(0, 0)
        });

        let map = crc.domain_metadata.map().unwrap();
        assert_eq!(map["old"].configuration(), "v2");
        assert_eq!(map["new"].configuration(), "config");
        assert!(crc.domain_metadata.is_complete());
    }

    #[test]
    fn apply_dm_tombstone_removes_from_complete_map() {
        let mut crc = base_crc();
        let mut dms = HashMap::new();
        dms.insert(
            "d".to_string(),
            DomainMetadata::new("d".to_string(), "v".to_string()),
        );
        crc.domain_metadata = DomainMetadataState::Complete(dms);

        let mut update_map = HashMap::new();
        update_map.insert(
            "d".to_string(),
            DomainMetadata::remove("d".to_string(), "v".to_string()),
        );
        crc.apply(CrcUpdate {
            domain_metadata: update_map,
            ..write_update(0, 0)
        });

        let map = crc.domain_metadata.map().unwrap();
        assert!(map.is_empty());
        assert!(crc.domain_metadata.is_complete()); // stays Complete
    }

    #[test]
    fn apply_dm_to_untracked_transitions_to_partial() {
        // Crucial: a CRC missing DM (Untracked) gets DM entries from newer commits via
        // apply. The result is Partial — entries are correct but the cache is incomplete.
        let mut crc = base_crc();
        assert!(matches!(
            crc.domain_metadata,
            DomainMetadataState::Untracked
        ));
        let mut update_map = HashMap::new();
        update_map.insert(
            "d".to_string(),
            DomainMetadata::new("d".to_string(), "v".to_string()),
        );
        crc.apply(CrcUpdate {
            domain_metadata: update_map,
            ..write_update(0, 0)
        });
        assert!(matches!(
            crc.domain_metadata,
            DomainMetadataState::Partial(_)
        ));
        let map = crc.domain_metadata.map().unwrap();
        assert_eq!(map["d"].configuration(), "v");
    }

    #[test]
    fn apply_txn_to_untracked_transitions_to_partial() {
        let mut crc = base_crc();
        let mut update_map = HashMap::new();
        update_map.insert(
            "app".to_string(),
            SetTransaction::new("app".to_string(), 5, None),
        );
        crc.apply(CrcUpdate {
            set_transactions: update_map,
            ..write_update(0, 0)
        });
        assert!(matches!(
            crc.set_transactions,
            SetTransactionState::Partial(_)
        ));
        let map = crc.set_transactions.map().unwrap();
        assert_eq!(map["app"].version, 5);
    }

    #[test]
    fn apply_ict_observation_semantics() {
        let mut crc = base_crc();
        crc.in_commit_timestamp_opt = Some(1000);

        // None outer = "did not observe ICT" -- leave base alone.
        crc.apply(CrcUpdate {
            in_commit_timestamp: None,
            ..write_update(0, 0)
        });
        assert_eq!(
            crc.in_commit_timestamp_opt,
            Some(1000),
            "None means 'no observation'; base is preserved"
        );

        // Some(None) = "observed: ICT disabled" -- clears base.
        crc.apply(CrcUpdate {
            in_commit_timestamp: Some(None),
            ..write_update(0, 0)
        });
        assert_eq!(
            crc.in_commit_timestamp_opt, None,
            "Some(None) clears the ICT (observed disabled)"
        );

        // Some(Some(v)) sets.
        crc.apply(CrcUpdate {
            in_commit_timestamp: Some(Some(2000)),
            ..write_update(0, 0)
        });
        assert_eq!(crc.in_commit_timestamp_opt, Some(2000));
    }

    // ===== into_fresh_crc =====

    #[test]
    fn into_fresh_crc_with_protocol_and_metadata_succeeds() {
        let update = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            ..write_update(5, 1000)
        };
        let crc = update.into_fresh_crc().unwrap();
        assert_eq!(crc.file_stats().unwrap().num_files(), 5);
        assert_eq!(crc.file_stats().unwrap().table_size_bytes(), 1000);
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Valid);
        assert!(crc.domain_metadata.is_complete());
        assert!(crc.set_transactions.is_complete());
    }

    #[rstest]
    #[case::missing_protocol(None, Some(Metadata::default()))]
    #[case::missing_metadata(Some(test_protocol()), None)]
    #[case::missing_both(None, None)]
    fn into_fresh_crc_requires_both_protocol_and_metadata(
        #[case] protocol: Option<Protocol>,
        #[case] metadata: Option<Metadata>,
    ) {
        let update = CrcUpdate {
            protocol,
            metadata,
            ..write_update(0, 0)
        };
        assert!(update.into_fresh_crc().is_err());
    }

    #[test]
    fn into_fresh_crc_unsafe_op_produces_indeterminate() {
        let update = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            operation_safe: false,
            ..Default::default()
        };
        let crc = update.into_fresh_crc().unwrap();
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Indeterminate);
    }

    #[test]
    fn into_fresh_crc_missing_size_produces_untrackable() {
        let update = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            has_missing_file_size: true,
            operation_safe: true,
            ..Default::default()
        };
        let crc = update.into_fresh_crc().unwrap();
        assert_eq!(crc.file_stats_validity(), FileStatsValidity::Untrackable);
    }

    #[test]
    fn into_fresh_crc_filters_dm_tombstones() {
        let mut dms = HashMap::new();
        dms.insert(
            "alive".to_string(),
            DomainMetadata::new("alive".to_string(), "v".to_string()),
        );
        dms.insert(
            "dead".to_string(),
            DomainMetadata::remove("dead".to_string(), "v".to_string()),
        );
        let update = CrcUpdate {
            protocol: Some(test_protocol()),
            metadata: Some(Metadata::default()),
            domain_metadata: dms,
            operation_safe: true,
            ..Default::default()
        };
        let crc = update.into_fresh_crc().unwrap();
        let map = crc.domain_metadata.map().unwrap();
        assert!(map.contains_key("alive"));
        assert!(!map.contains_key("dead"));
    }

    // ===== histogram merge =====

    #[test]
    fn apply_merges_histogram_when_both_present() {
        let mut hist = FileSizeHistogram::create_default();
        hist.insert(100).unwrap();
        hist.insert(200).unwrap();
        let mut crc = base_crc();
        crc.file_stats = FileStatsState::Valid {
            num_files: 2,
            table_size_bytes: 300,
            histogram: Some(hist),
        };

        let mut delta_hist = FileSizeHistogram::create_default();
        delta_hist.insert(500).unwrap();
        crc.apply(CrcUpdate {
            file_stats: FileStatsDelta {
                net_files: 1,
                net_bytes: 500,
                net_histogram: Some(delta_hist),
            },
            operation_safe: true,
            ..Default::default()
        });

        let stats = crc.file_stats().unwrap();
        let merged = stats.file_size_histogram().unwrap();
        assert_eq!(merged.file_counts()[0], 3); // bin 0: 100, 200, 500
        assert_eq!(merged.total_bytes()[0], 800);
    }

    #[test]
    fn apply_drops_histogram_when_base_some_delta_none() {
        let mut hist = FileSizeHistogram::create_default();
        hist.insert(100).unwrap();
        let mut crc = base_crc();
        crc.file_stats = FileStatsState::Valid {
            num_files: 1,
            table_size_bytes: 100,
            histogram: Some(hist),
        };

        crc.apply(write_update(0, 0)); // delta has no histogram

        let stats = crc.file_stats().unwrap();
        assert!(stats.file_size_histogram().is_none());
    }

    #[test]
    fn apply_drops_histogram_when_base_none_delta_some() {
        // A base CRC without a histogram + an update with a delta histogram → the
        // resulting CRC has no histogram. The delta is an *increment* over an unknown
        // baseline, so adopting it as the absolute histogram would be wrong (it would
        // describe only this commit's files, not the table's full file set). The right
        // way to seed an initial histogram is via CrcUpdate::into_fresh_crc when doing a
        // full replay.
        let mut crc = base_crc();
        crc.file_stats = FileStatsState::Valid {
            num_files: 0,
            table_size_bytes: 0,
            histogram: None,
        };

        let mut delta_hist = FileSizeHistogram::create_default();
        delta_hist.insert(500).unwrap();
        crc.apply(CrcUpdate {
            file_stats: FileStatsDelta {
                net_files: 1,
                net_bytes: 500,
                net_histogram: Some(delta_hist),
            },
            operation_safe: true,
            ..Default::default()
        });

        let stats = crc.file_stats().unwrap();
        assert!(
            stats.file_size_histogram().is_none(),
            "delta is not an absolute baseline"
        );
        assert_eq!(stats.num_files(), 1);
        assert_eq!(stats.table_size_bytes(), 500);
    }

    #[test]
    fn apply_drops_histogram_on_indeterminate_transition() {
        let mut hist = FileSizeHistogram::create_default();
        hist.insert(100).unwrap();
        let mut crc = base_crc();
        crc.file_stats = FileStatsState::Valid {
            num_files: 1,
            table_size_bytes: 100,
            histogram: Some(hist),
        };

        crc.apply(CrcUpdate {
            operation_safe: false,
            ..write_update(0, 0)
        });

        // Indeterminate has no histogram field — the data is gone.
        assert!(matches!(crc.file_stats, FileStatsState::Indeterminate));
        assert!(crc.file_stats().is_none());
    }

    #[test]
    fn apply_to_requires_checkpoint_read_accumulates_deltas() {
        let mut crc = Crc::minimal_from_pm(Protocol::default(), Metadata::default());
        assert_eq!(
            crc.file_stats_validity(),
            FileStatsValidity::RequiresCheckpointRead
        );
        crc.apply(write_update(3, 600));
        crc.apply(write_update(-1, -200));

        // Stays RequiresCheckpointRead, with accumulated deltas.
        assert_eq!(
            crc.file_stats_validity(),
            FileStatsValidity::RequiresCheckpointRead
        );
        let FileStatsState::RequiresCheckpointRead {
            commit_delta_files,
            commit_delta_bytes,
            ..
        } = &crc.file_stats
        else {
            panic!("expected RequiresCheckpointRead, got {:?}", crc.file_stats);
        };
        assert_eq!(*commit_delta_files, 2); // 3 - 1
        assert_eq!(*commit_delta_bytes, 400); // 600 - 200
    }
}
