//! Integration tests for data skipping correctness.
//!
//! Tests cover partition-only pruning, data-stats-only pruning, mixed partition + stats
//! predicates (AND/OR), nested predicates, and unsupported predicate handling.
//!
//! Two test tables are used:
//!
//! `app-txn-checkpoint` (4 files, partitioned by `modified` (string)):
//!   - 2 files: modified="2021-02-01", value in [4, 11]
//!   - 2 files: modified="2021-02-02", value in [1, 3]
//!   - Version 0 (JSON) + version 1 (JSON + checkpoint) exercises both code paths.
//!
//! `parsed-stats` (6 files, non-partitioned):
//!   - File 1-6: id ranges [1,100]..[501,600], ts_col min values 1M..11M microseconds
//!   - Version 3 checkpoint + versions 4-5 JSON commits.

use std::path::PathBuf;
use std::sync::Arc;

use rstest::rstest;

use crate::engine::sync::SyncEngine;
use crate::expressions::{
    column_expr, Expression as Expr, Predicate as Pred, PredicateRef, Scalar,
};
use crate::Snapshot;

/// Counts files selected after data skipping for the given predicate and table.
fn count_selected(table_dir: &str, pred: PredicateRef) -> usize {
    let path = std::fs::canonicalize(PathBuf::from(table_dir)).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let scan = Snapshot::builder_for(url)
        .build(engine.as_ref())
        .unwrap()
        .scan_builder()
        .with_predicate(pred)
        .build()
        .unwrap();
    scan.scan_metadata(engine.as_ref())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .iter()
        .flat_map(|sm| sm.scan_files.selection_vector())
        .filter(|&&s| s)
        .count()
}

const PARTITIONED_TABLE: &str = "./tests/data/app-txn-checkpoint/";
const STATS_TABLE: &str = "./tests/data/parsed-stats/";

// -- Partition-only predicates (app-txn-checkpoint) ---------------------------

#[rstest]
#[case::eq_match(Pred::eq(column_expr!("modified"), Expr::literal("2021-02-01")), 2)]
#[case::eq_no_match(Pred::eq(column_expr!("modified"), Expr::literal("2099-01-01")), 0)]
#[case::neq(Pred::ne(column_expr!("modified"), Expr::literal("2021-02-01")), 2)]
#[case::gt(Pred::gt(column_expr!("modified"), Expr::literal("2021-02-01")), 2)]
#[case::lt(Pred::lt(column_expr!("modified"), Expr::literal("2021-02-02")), 2)]
#[case::gte_all(Pred::ge(column_expr!("modified"), Expr::literal("2021-02-01")), 4)]
#[case::lte_all(Pred::le(column_expr!("modified"), Expr::literal("2021-02-02")), 4)]
#[case::range_anded(
    Pred::and(
        Pred::ge(column_expr!("modified"), Expr::literal("2021-02-01")),
        Pred::le(column_expr!("modified"), Expr::literal("2021-02-01")),
    ),
    2
)]
fn partition_only_skipping(#[case] pred: Pred, #[case] expected: usize) {
    assert_eq!(count_selected(PARTITIONED_TABLE, Arc::new(pred)), expected);
}

// -- Data-stats-only predicates (app-txn-checkpoint) --------------------------

#[rstest]
#[case::gt_prunes_low(Pred::gt(column_expr!("value"), Expr::literal(9i32)), 2)]
#[case::lt_prunes_high(Pred::lt(column_expr!("value"), Expr::literal(4i32)), 2)]
#[case::gt_above_max(Pred::gt(column_expr!("value"), Expr::literal(11i32)), 0)]
#[case::le_at_max(Pred::le(column_expr!("value"), Expr::literal(11i32)), 4)]
#[case::range_anded(
    Pred::and(
        Pred::ge(column_expr!("value"), Expr::literal(1i32)),
        Pred::le(column_expr!("value"), Expr::literal(3i32)),
    ),
    2
)]
fn data_stats_only_skipping(#[case] pred: Pred, #[case] expected: usize) {
    assert_eq!(count_selected(PARTITIONED_TABLE, Arc::new(pred)), expected);
}

// -- Mixed AND: both partition and data conditions must hold -------------------

#[rstest]
#[case::partition_match_data_match(
    "2021-02-01",
    3i32,
    2,
    "partition prunes 02-02; data keeps 02-01 (max=11 > 3)"
)]
#[case::partition_match_data_miss(
    "2021-02-02",
    3i32,
    0,
    "partition keeps 02-02 but max=3 NOT >3; partition prunes 02-01"
)]
#[case::partition_miss("2099-01-01", 0i32, 0, "no files match partition")]
fn mixed_and_skipping(
    #[case] partition_val: &str,
    #[case] data_threshold: i32,
    #[case] expected: usize,
    #[case] _scenario: &str,
) {
    let pred = Arc::new(Pred::and(
        column_expr!("modified").eq(Expr::literal(partition_val)),
        column_expr!("value").gt(Expr::literal(data_threshold)),
    ));
    assert_eq!(count_selected(PARTITIONED_TABLE, pred), expected);
}

// -- Mixed OR: a file survives if either leg matches --------------------------

#[rstest]
#[case::both_match("2021-02-02", 9i32, 4, "02-02 matches partition; 02-01 has max=11 > 9")]
#[case::partition_saves_some(
    "2021-02-02",
    11i32,
    2,
    "02-02 matches partition; 02-01 max=11 NOT >11 -> pruned"
)]
#[case::data_saves_some(
    "2099-01-01", -1i32, 4,
    "no partition match; all files have max >= 0 so value > -1 keeps all"
)]
#[case::both_miss(
    "2099-01-01",
    11i32,
    0,
    "no partition match; max=11 NOT >11 -> all pruned"
)]
fn mixed_or_skipping(
    #[case] partition_val: &str,
    #[case] data_threshold: impl Into<Scalar>,
    #[case] expected: usize,
    #[case] _scenario: &str,
) {
    let pred = Arc::new(Pred::or(
        column_expr!("modified").eq(Expr::literal(partition_val)),
        column_expr!("value").gt(Expr::literal(data_threshold.into())),
    ));
    assert_eq!(count_selected(PARTITIONED_TABLE, pred), expected);
}

// -- Nested AND(partition, OR(data, data)) ------------------------------------

#[rstest]
#[case::loose_bound(10i32, 4, "max=11 > 10 keeps 02-01; min=1 < 2 keeps 02-02")]
#[case::strict_bound(11i32, 2, "max=11 NOT >11 prunes 02-01; min=1 < 2 keeps 02-02")]
fn nested_and_or_skipping(
    #[case] upper_bound: i32,
    #[case] expected: usize,
    #[case] _scenario: &str,
) {
    let pred = Arc::new(Pred::and(
        Pred::ge(column_expr!("modified"), Expr::literal("2021-02-01")),
        Pred::or(
            Pred::lt(column_expr!("value"), Expr::literal(2i32)),
            Pred::gt(column_expr!("value"), Expr::literal(upper_bound)),
        ),
    ));
    assert_eq!(count_selected(PARTITIONED_TABLE, pred), expected);
}

// -- Parsed stats skipping (non-partitioned table) ----------------------------

#[test]
fn parsed_stats_skipping() {
    // id > 400 should skip files 1-4 (max id: 100, 200, 300, 400) and keep files 5-6
    let pred = Arc::new(Pred::gt(column_expr!("id"), Expr::literal(400i64)));
    assert_eq!(count_selected(STATS_TABLE, pred), 2);
}

// -- Unsupported predicate handling (parsed-stats table) ----------------------
// Timestamp GT uses max stats which is unsupported for checkpoint skipping due to
// millisecond truncation. Timestamp LT uses min stats (supported).

#[rstest]
#[case::bare_ts_gt_returns_all(
    Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(2_000_000))),
    6
)]
#[case::bare_ts_lt_skips(
    Pred::lt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(3_000_000))),
    1
)]
#[case::and_mixed_supported_unsupported(
    Pred::and(
        Pred::gt(column_expr!("id"), Expr::literal(400i64)),
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(2_000_000))),
    ),
    2
)]
#[case::or_mixed_supported_unsupported(
    Pred::or(
        Pred::gt(column_expr!("id"), Expr::literal(400i64)),
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(2_000_000))),
    ),
    6
)]
#[case::and_all_unsupported(
    Pred::and(
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(2_000_000))),
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(5_000_000))),
    ),
    6
)]
#[case::or_all_unsupported(
    Pred::or(
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(2_000_000))),
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(5_000_000))),
    ),
    6
)]
fn unsupported_predicate_skipping(#[case] pred: Pred, #[case] expected: usize) {
    assert_eq!(count_selected(STATS_TABLE, Arc::new(pred)), expected);
}
