//! Collapses the per-commit row-tracking change-feed listing to its NET effect per file path over
//! the whole version range, for [`crate::table_changes::TableChangesListingMode::NetChanges`].
//!
//! The per-commit listing ([`crate::table_changes::TableChangesListingMode::AllChanges`]) emits one
//! file per `add`/`remove` action in every commit. NetChanges reduces each file *path* to only its
//! boundary actions across the range: the earliest remove and the latest add. Intermediate actions
//! are dropped and never handed to the connector to scan.
//!
//! This is a scan-selection collapse, not the change-feed's correctness engine: the connector still
//! reconstructs the row-level feed (pairing an update's pre-image and post-image by row id) from
//! the files this returns. Its job is only to avoid listing files whose rows net to no change
//! across the range.

use std::collections::HashMap;

use crate::table_changes::scan_file::{TableChangesFileAction, TableChangesScanFile};
use crate::KernelResult;

// Variant order ensures removes sort before adds within the same commit.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ActionKind {
    Remove,
    Add,
}

// Track both endpoint of an initial add or final remove.
// Only endpoint files that can contribute to the output are retained.
struct Boundaries {
    first: (i64, ActionKind),
    last: (i64, ActionKind),
    first_remove: Option<TableChangesScanFile>,
    last_add: Option<TableChangesScanFile>,
}

impl Boundaries {
    fn new(kind: ActionKind, file: TableChangesScanFile) -> Self {
        let key = (file.commit_version, kind);
        let (first_remove, last_add) = match kind {
            ActionKind::Remove => (Some(file), None),
            ActionKind::Add => (None, Some(file)),
        };
        Self {
            first: key,
            last: key,
            first_remove,
            last_add,
        }
    }

    fn include(&mut self, kind: ActionKind, file: TableChangesScanFile) {
        let key = (file.commit_version, kind);
        if key < self.first {
            self.first = key;
            self.first_remove = (kind == ActionKind::Remove).then_some(file);
        } else if key >= self.last {
            self.last = key;
            self.last_add = (kind == ActionKind::Add).then_some(file);
        }
    }
}

/// Collapses per-commit [`TableChangesFileAction`]s to their net effect per path across the whole
/// range. Only the earliest remove-side action and the latest add-side action of a path survive, so
/// intermediate actions are dropped and never scanned.
///
/// Each input action is flattened to its side(s) -- a same-commit deletion-vector update
/// (`{add: Some, remove: Some}`) contributes both -- keyed by file path. A file path is globally
/// unique within a table's commit range (it carries the full partition prefix and a unique write
/// UUID), so keying on the path alone never merges actions from different files or partitions. Per
/// path, the global earliest and latest sides are ordered by `(commit_version, action_kind)` (a
/// remove-side precedes an add-side within a commit), and:
/// - earliest is a remove-side and latest is an add-side with differing deletion vectors -> a net
///   update: one output [`TableChangesFileAction`] carrying both boundary sides -- the pre-image
///   remove (with its DV) and the post-image add (with its DV). The connector reads each side under
///   its own DV and reconciles rows by row id;
/// - earliest is a remove-side with no later add-side -> a net delete (`{remove: Some}`);
/// - earliest and latest are both add-side -> a net insert (`{add: Some}`);
/// - added-then-removed within the range, or surviving deletion vectors identical (a carry-over
///   rewrite) -> dropped: the rows net to no change.
///
/// Deletion-vector equality here is structural (all descriptor fields), which is conservative: a
/// pair that is logically equivalent but structurally different still emits a net update. Only
/// structurally identical boundary DVs are dropped, so a surviving path is not a guarantee its rows
/// changed; the connector's row-level reconciliation is the authoritative carry-over check.
///
/// Each path yields at most one output action. The result is sorted by path (unique per output) for
/// a deterministic listing, since the intermediate path map has no defined iteration order.
/// Input errors are propagated before a listing is returned.
pub(super) fn collapse_net_changes(
    actions: impl IntoIterator<Item = KernelResult<TableChangesFileAction>>,
) -> KernelResult<Vec<TableChangesFileAction>> {
    let mut by_path: HashMap<String, Boundaries> = HashMap::new();
    for action in actions {
        let action = action?;
        if let Some(add) = action.add {
            if let Some(boundaries) = by_path.get_mut(&add.path) {
                boundaries.include(ActionKind::Add, add);
            } else {
                by_path.insert(add.path.clone(), Boundaries::new(ActionKind::Add, add));
            }
        }
        if let Some(remove) = action.remove {
            if let Some(boundaries) = by_path.get_mut(&remove.path) {
                boundaries.include(ActionKind::Remove, remove);
            } else {
                by_path.insert(remove.path.clone(), Boundaries::new(ActionKind::Remove, remove));
            }
        }
    }

    let mut out = Vec::new();
    for (path, boundaries) in by_path {
        let action = TableChangesFileAction {
            add: boundaries.last_add,
            remove: boundaries.first_remove,
        };

        if action.add.is_none() && action.remove.is_none() {
            continue;
        }
        if let (Some(remove), Some(add)) = (&action.remove, &action.add) {
            if remove.deletion_vector == add.deletion_vector {
                continue;
            }
        }

        out.push((path, action));
    }
    out.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
    Ok(out.into_iter().map(|(_, action)| action).collect())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use rstest::rstest;

    use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
    use crate::table_changes::net_changes::collapse_net_changes;
    use crate::table_changes::scan_file::{TableChangesFileAction, TableChangesScanFile};
    use crate::{KernelError, KernelResult};

    /// Builds a deletion-vector descriptor identified by `id`. Two descriptors with different ids
    /// compare unequal, which is how the collapse tells a real net update from a same-DV carry-over
    /// rewrite.
    fn test_dv(id: &str) -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: id.to_string(),
            offset: Some(1),
            size_in_bytes: 8,
            cardinality: 1,
        }
    }

    /// Builds one side (`TableChangesScanFile`) for the collapse tests. Only path, commit version,
    /// and this side's deletion vector vary across cases; the rest are fixed.
    fn side(
        path: &str,
        commit_version: i64,
        deletion_vector: Option<DeletionVectorDescriptor>,
    ) -> TableChangesScanFile {
        TableChangesScanFile {
            path: path.to_string(),
            deletion_vector,
            partition_values: HashMap::new(),
            size: Some(10),
            commit_version,
            commit_timestamp: 0,
            base_row_id: Some(0),
            default_row_commit_version: Some(1),
        }
    }

    /// A single-sided insert action for `path` at `commit_version`.
    fn insert(
        path: &str,
        commit_version: i64,
        dv: Option<DeletionVectorDescriptor>,
    ) -> TableChangesFileAction {
        TableChangesFileAction {
            add: Some(side(path, commit_version, dv)),
            remove: None,
        }
    }

    /// A single-sided delete action for `path` at `commit_version`.
    fn delete(
        path: &str,
        commit_version: i64,
        dv: Option<DeletionVectorDescriptor>,
    ) -> TableChangesFileAction {
        TableChangesFileAction {
            add: None,
            remove: Some(side(path, commit_version, dv)),
        }
    }

    /// A same-commit DV update: both sides at `commit_version`, the add carrying `add_dv` and the
    /// remove carrying `removed_dv`. This is the grouped shape `AllChanges` emits for an in-commit
    /// DV update.
    fn paired_dv_update(
        path: &str,
        commit_version: i64,
        add_dv: Option<DeletionVectorDescriptor>,
        removed_dv: Option<DeletionVectorDescriptor>,
    ) -> TableChangesFileAction {
        TableChangesFileAction {
            add: Some(side(path, commit_version, add_dv)),
            remove: Some(side(path, commit_version, removed_dv)),
        }
    }

    /// Summarizes an action as `(path, add commit + dv, remove commit + dv)` for assertions.
    type SideSummary = Option<(i64, Option<DeletionVectorDescriptor>)>;
    fn summarize(a: &TableChangesFileAction) -> (String, SideSummary, SideSummary) {
        let s = |side: &Option<TableChangesScanFile>| {
            side.as_ref()
                .map(|f| (f.commit_version, f.deletion_vector.clone()))
        };
        let path = a
            .add
            .as_ref()
            .or(a.remove.as_ref())
            .map(|f| f.path.clone())
            .expect("an action always has at least one side");
        (path, s(&a.add), s(&a.remove))
    }

    #[test]
    fn net_changes_collapse_reduces_each_path_to_its_net_effect() {
        let dv_a = test_dv("aaaaaaaaaaaaaaaaaaaa");
        let dv_b = test_dv("bbbbbbbbbbbbbbbbbbbb");
        let actions = vec![
            // insert.parquet: only ever added in the range -> net insert.
            insert("insert.parquet", 1, None),
            // delete.parquet: removed with no later add -> net delete.
            delete("delete.parquet", 1, Some(dv_a.clone())),
            // update.parquet: removed at v1 then re-added at v2 with a different DV -> net update,
            // grouped into a remove side at v1 and an add side at v2.
            delete("update.parquet", 1, Some(dv_a.clone())),
            insert("update.parquet", 2, Some(dv_b.clone())),
            // carryover.parquet: rewritten with the same DV on both sides -> no net change,
            // dropped.
            delete("carryover.parquet", 1, Some(dv_a.clone())),
            insert("carryover.parquet", 2, Some(dv_a.clone())),
            // churn.parquet: added then removed within the range -> no net change, dropped.
            insert("churn.parquet", 1, None),
            delete("churn.parquet", 2, None),
            // reinsert.parquet: added, removed, then re-added within the range -> net insert at
            // the last add (the (None, Some) arm reached via min/max boundaries, not a lone add).
            insert("reinsert.parquet", 1, None),
            delete("reinsert.parquet", 2, None),
            insert("reinsert.parquet", 3, None),
        ];

        let out = collapse_net_changes(actions.into_iter().map(Ok)).unwrap();

        // Only the three net-effect paths survive; the two no-net-change paths are dropped. Sorted
        // by path, each path yields a single grouped action.
        assert_eq!(
            out.iter().map(summarize).collect::<Vec<_>>(),
            vec![
                // delete: single remove side.
                (
                    "delete.parquet".to_string(),
                    None,
                    Some((1, Some(dv_a.clone())))
                ),
                // insert: single add side.
                ("insert.parquet".to_string(), Some((1, None)), None),
                // reinsert: single add side at the last add (v3).
                ("reinsert.parquet".to_string(), Some((3, None)), None),
                // update: grouped remove (v1, dv_a) + add (v2, dv_b).
                (
                    "update.parquet".to_string(),
                    Some((2, Some(dv_b))),
                    Some((1, Some(dv_a)))
                ),
            ]
        );
    }

    #[rstest]
    #[case::same_commit_dv_update(
        vec![paired_dv_update("update.parquet", 3, Some(test_dv("b")), Some(test_dv("a")))],
        vec![(
            "update.parquet".to_string(),
            Some((3, Some(test_dv("b")))),
            Some((3, Some(test_dv("a")))),
        )]
    )]
    #[case::multi_update_chain(
        vec![
            paired_dv_update("f.parquet", 1, Some(test_dv("b")), Some(test_dv("a"))),
            paired_dv_update("f.parquet", 2, Some(test_dv("c")), Some(test_dv("b"))),
        ],
        vec![(
            "f.parquet".to_string(),
            Some((2, Some(test_dv("c")))),
            Some((1, Some(test_dv("a")))),
        )]
    )]
    #[case::multi_commit_revert(
        vec![
            paired_dv_update("f.parquet", 1, Some(test_dv("b")), Some(test_dv("a"))),
            paired_dv_update("f.parquet", 2, Some(test_dv("a")), Some(test_dv("b"))),
        ],
        vec![]
    )]
    #[case::copy_on_write(
        vec![delete("old.parquet", 1, None), insert("new.parquet", 1, None)],
        vec![
            ("new.parquet".to_string(), Some((1, None)), None),
            ("old.parquet".to_string(), None, Some((1, None))),
        ]
    )]
    fn net_changes_collapse_reduces_paths_to_their_boundaries(
        #[case] actions: Vec<TableChangesFileAction>,
        #[case] expected: Vec<(String, SideSummary, SideSummary)>,
    ) {
        let out = collapse_net_changes(actions.into_iter().map(Ok)).unwrap();
        assert_eq!(out.iter().map(summarize).collect::<Vec<_>>(), expected);
    }

    /// The row-tracking metadata (`base_row_id`, `default_row_commit_version`) on each boundary
    /// side must survive the collapse unchanged -- it is what the connector uses to reconcile
    /// pre-image and post-image rows by row id. Regression guard for a net update across
    /// commits.
    #[test]
    fn net_changes_collapse_preserves_row_tracking_fields() {
        let dv_a = test_dv("aaaaaaaaaaaaaaaaaaaa");
        let dv_b = test_dv("bbbbbbbbbbbbbbbbbbbb");
        // Distinct row-tracking metadata on each side so a swap or drop would be caught. The
        // `side` helper's fixed defaults are overridden per side.
        let actions = vec![
            TableChangesFileAction {
                add: None,
                remove: Some(TableChangesScanFile {
                    base_row_id: Some(100),
                    default_row_commit_version: Some(1),
                    ..side("f.parquet", 1, Some(dv_a))
                }),
            },
            TableChangesFileAction {
                add: Some(TableChangesScanFile {
                    base_row_id: Some(200),
                    default_row_commit_version: Some(2),
                    ..side("f.parquet", 2, Some(dv_b))
                }),
                remove: None,
            },
        ];

        let out = collapse_net_changes(actions.into_iter().map(Ok)).unwrap();

        assert_eq!(out.len(), 1, "expected one grouped net update: {out:?}");
        let remove = out[0].remove.as_ref().expect("remove side");
        let add = out[0].add.as_ref().expect("add side");
        assert_eq!(remove.base_row_id, Some(100));
        assert_eq!(remove.default_row_commit_version, Some(1));
        assert_eq!(add.base_row_id, Some(200));
        assert_eq!(add.default_row_commit_version, Some(2));
    }
    // Use orginal implementation as a reference
    struct Side {
        is_add: bool,
        file: TableChangesScanFile,
    }

    fn reference_collapse(
        actions: Vec<TableChangesFileAction>,
    ) -> KernelResult<Vec<TableChangesFileAction>> {
        let mut by_path: HashMap<String, Vec<Side>> = HashMap::new();
        for action in actions {
            if let Some(add) = action.add {
                by_path.entry(add.path.clone()).or_default().push(Side {
                    is_add: true,
                    file: add,
                });
            }
            if let Some(remove) = action.remove {
                by_path.entry(remove.path.clone()).or_default().push(Side {
                    is_add: false,
                    file: remove,
                });
            }
        }

        let mut out: Vec<(String, TableChangesFileAction)> = Vec::with_capacity(by_path.len());
        for (path, sides) in by_path {
            let key = |s: &&Side| (s.file.commit_version, s.is_add);
            let (Some(earliest), Some(latest)) =
                (sides.iter().min_by_key(key), sides.iter().max_by_key(key))
            else {
                return Err(KernelError::internal_error(format!(
                    "net-changes collapse produced an empty side slot for path {path}"
                )));
            };
            // Only a leading remove and/or a trailing add mark a net change; clone the boundary
            // file(s) that survive.
            let first_remove = (!earliest.is_add).then(|| earliest.file.clone());
            let last_add = latest.is_add.then(|| latest.file.clone());
            let action = match (first_remove, last_add) {
                (Some(remove), None) => TableChangesFileAction {
                    add: None,
                    remove: Some(remove),
                },
                (None, Some(add)) => TableChangesFileAction {
                    add: Some(add),
                    remove: None,
                },
                (Some(remove), Some(add)) if remove.deletion_vector != add.deletion_vector => {
                    TableChangesFileAction {
                        add: Some(add),
                        remove: Some(remove),
                    }
                }
                (Some(_), Some(_)) | (None, None) => continue,
            };
            out.push((path, action));
        }

        out.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        Ok(out.into_iter().map(|(_, action)| action).collect())
    }

    /// Uses seeded, shuffled actions to compare against the collect-and-select reference algorithm.
    /// Distinct metadata detects selection of the wrong boundary when version and action kind tie.
    #[test]
    fn net_changes_matches_reference_for_shuffled_actions_and_equal_version_ties() {
        let mut rng = StdRng::seed_from_u64(3232);
        for len in [0, 1, 2, 10, 100, 1000] {
            for _ in 0..20 {
                let mut actions = Vec::new();
                for i in 0..len {
                    let path = format!("{}.parquet", rng.random_range(0..8));
                    let version = rng.random_range(0..10);
                    let dv = match rng.random_range(0..3) {
                        0 => None,
                        1 => Some(test_dv("a")),
                        _ => Some(test_dv("b")),
                    };
                    let mut action = match rng.random_range(0..4) {
                        0 => insert(&path, version, dv),
                        1 => delete(&path, version, dv),
                        2 => paired_dv_update(&path, version, dv, Some(test_dv("c"))),
                        _ => TableChangesFileAction {
                            add: None,
                            remove: None,
                        },
                    };
                    for file in [&mut action.add, &mut action.remove].into_iter().flatten() {
                        file.commit_timestamp = i;
                        file.partition_values.insert("p".into(), i.to_string());
                        file.base_row_id = Some(i);
                    }
                    actions.push(action);
                }
                actions.shuffle(&mut rng);
                let expected = reference_collapse(actions.clone()).unwrap();
                let actual = collapse_net_changes(actions.into_iter().map(Ok)).unwrap();
                assert_eq!(actual, expected);
            }
        }
    }

    /// Even when preceding actions cancel out, a later input error must fail the whole listing.
    #[test]
    fn net_changes_returns_late_input_error_instead_of_partial_listing() {
        let actions = [
            Ok(insert("a.parquet", 1, None)),
            Ok(delete("a.parquet", 2, None)),
            Err(crate::KernelError::generic("late input failure")),
        ];
        let error = collapse_net_changes(actions).unwrap_err();
        assert!(error.to_string().contains("late input failure"));
    }
}
