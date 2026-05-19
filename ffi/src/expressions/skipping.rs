//! Skipping-predicate logic for engine-named opaque ops.
//!
//! Kernel proper is op-agnostic -- it understands a fixed vocabulary (`<`,
//! `==`, `AND`, `IS NULL`, ...) and treats anything else as opaque.
//! [`NamedOpaquePredicateOp`] is the FFI's stub for engine-defined ops:
//! it carries only a name, so the engine can route the op back to its own
//! row-level evaluator. By default the stub opts out of every kernel-side
//! pruning pass, which costs FFI engines the ability to skip files for
//! predicates like `STARTS_WITH(col, "foo")`.
//!
//! This module is the single place where the FFI shim adopts opinions about
//! op semantics. For each recognized name it provides:
//!
//! - a scalar evaluator (partition pruning)
//! - a direct stats evaluator (parquet row-group skipping)
//! - a stats-rewrite (Delta log file pruning)
//!
//! Unrecognized names fall through to the no-support behavior preserved by
//! [`NamedOpaquePredicateOp`].
//!
//! [`NamedOpaquePredicateOp`]: super::NamedOpaquePredicateOp

use std::cmp::Ordering;

use delta_kernel::expressions::{
    ColumnName, Expression as Expr, JunctionPredicateOp, Predicate as Pred, Scalar,
    ScalarExpressionEvaluator,
};
use delta_kernel::kernel_predicates::{
    DataSkippingPredicateEvaluator, DirectDataSkippingPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use delta_kernel::{DeltaResult, Error};

// === Recognized op names =======================================================

/// Op name for prefix-match. Args: `[col, prefix_literal_string]`.
///
/// A string `s` starts with `prefix` iff `prefix <= s < next_prefix(prefix)`
/// under the binary collation kernel uses for string ordering. Skipping
/// reduces to a range-overlap test against the column's min/max stats.
pub(crate) const STARTS_WITH: &str = "STARTS_WITH";

// === Entry points dispatched on op name ========================================

/// Indirect rewrite for Delta log file pruning. The returned predicate is
/// evaluated by the engine's `EvaluationHandler` against a batch of Add-action
/// stats. Returns `None` for unrecognized ops or when the rewrite cannot be
/// applied (e.g. unexpected argument shape, missing stats).
pub(crate) fn rewrite_for_file_pruning(
    name: &str,
    evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
    exprs: &[Expr],
    inverted: bool,
) -> Option<Pred> {
    match name {
        STARTS_WITH => starts_with::skipping(evaluator, exprs, inverted),
        _ => None,
    }
}

/// Direct evaluation against parquet-footer min/max stats, used for row-group
/// skipping. Result is the predicate's truth value: `Some(true)` -> keep,
/// `Some(false)` -> skip, `None` -> unknown (keep). Returns `None` for
/// unrecognized ops.
pub(crate) fn evaluate_for_row_group_skipping(
    name: &str,
    evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
    exprs: &[Expr],
    inverted: bool,
) -> Option<bool> {
    match name {
        STARTS_WITH => starts_with::skipping(evaluator, exprs, inverted),
        _ => None,
    }
}

/// Scalar evaluation against resolved column references, used for partition
/// pruning. Returns outer `None` for unrecognized ops so the caller can fall
/// back to its "no support" behavior. When the name is recognized, returns
/// `Some(inner)` where `inner` matches [`OpaquePredicateOp::eval_pred_scalar`]
/// semantics:
/// - `Err` only for fundamental usage errors (wrong arity, non-string arg)
/// - `Ok(None)` for SQL NULL or arguments the partition-pruning evaluator could not resolve (e.g.
///   references to non-partition columns) -- both propagate as "unknown, keep the file"
/// - `Ok(Some(_))` for a concrete boolean result
///
/// [`OpaquePredicateOp::eval_pred_scalar`]: delta_kernel::expressions::OpaquePredicateOp::eval_pred_scalar
pub(crate) fn evaluate_on_partition_value(
    name: &str,
    eval_expr: &ScalarExpressionEvaluator<'_>,
    exprs: &[Expr],
    inverted: bool,
) -> Option<DeltaResult<Option<bool>>> {
    match name {
        STARTS_WITH => Some(starts_with::scalar(eval_expr, exprs, inverted)),
        _ => None,
    }
}

// === STARTS_WITH ===============================================================

mod starts_with {
    use super::*;

    /// Validates the arg shape and computes the range bounds for skipping.
    /// Factored out so the early-return logic is testable without a full
    /// `DataSkippingPredicateEvaluator` stub.
    ///
    /// Returns `(col, prefix_lower_bound, prefix_upper_bound)` -- a file may
    /// contain a matching row iff `[min, max]` overlaps
    /// `[prefix_lower_bound, prefix_upper_bound)`.
    pub(super) fn range_bounds(
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<(&ColumnName, Scalar, Scalar)> {
        // NOT STARTS_WITH can only safely skip a file when every row matches
        // the prefix, i.e. `[min, max]` lies entirely inside `[prefix,
        // next_prefix)`. That is much rarer than the non-inverted case (which
        // skips when `[min, max]` lies entirely outside that range), so this
        // shim opts out and engines fall back to row-level filtering.
        if inverted {
            return None;
        }
        let (col, prefix) = match exprs {
            [Expr::Column(col), Expr::Literal(Scalar::String(p))] => (col, p),
            _ => return None,
        };
        // Empty prefix matches every string -- pruning is impossible.
        if prefix.is_empty() {
            return None;
        }
        // No successor exists for strings made entirely of U+10FFFF; opt out
        // rather than incorrectly prune.
        let upper = next_prefix(prefix)?;
        Some((col, Scalar::String(prefix.clone()), Scalar::String(upper)))
    }

    /// Range-overlap skipping. Generic over [`DataSkippingPredicateEvaluator`]
    /// so the same logic serves both the indirect rewrite (`Output = Pred`)
    /// and direct row-group eval (`Output = bool`) paths.
    ///
    /// Two assumptions, both inherited from kernel's existing string-comparison
    /// skipping (see `KernelPredicateEvaluator::eval_pred_lt` / `eval_pred_gt`):
    /// - **Binary collation**: string ordering is byte-wise lexicographic. Delta's default
    ///   collation; non-default collations would need a separate rewrite.
    /// - **Wide max stat under truncation**: every known Delta writer (Spark, kernel default engine
    ///   -- see `engine::default::stats::truncate_max_string`) appends a tie-breaker after
    ///   truncating a long string max so the stored `maxValues.col >= actual max`. A writer that
    ///   truncated to a bare prefix would let `max >= prefix` evaluate false for a file that does
    ///   contain a matching row, producing an incorrect skip. The Delta protocol does not mandate
    ///   the tie-breaker; we rely on the universal convention.
    ///
    /// The column is also assumed to be STRING-typed; the engine is responsible
    /// for never producing `STARTS_WITH` over a non-string column.
    pub(super) fn skipping<E>(evaluator: &E, exprs: &[Expr], inverted: bool) -> Option<E::Output>
    where
        E: DataSkippingPredicateEvaluator + ?Sized,
    {
        let (col, lower, upper) = range_bounds(exprs, inverted)?;
        // `partial_cmp_X_stat(_, val, Less, inverted)` returns true when:
        //   inverted=false: stat < val
        //   inverted=true : NOT(stat < val), i.e. stat >= val
        // So `max >= prefix` is `partial_cmp_max_stat(_, _, Less, true)` and
        // `min < next_prefix` is `partial_cmp_min_stat(_, _, Less, false)`.
        let max_ge_prefix = evaluator.partial_cmp_max_stat(col, &lower, Ordering::Less, true);
        let min_lt_upper = evaluator.partial_cmp_min_stat(col, &upper, Ordering::Less, false);
        evaluator.finish_eval_pred_junction(
            JunctionPredicateOp::And,
            &mut [max_ge_prefix, min_lt_upper].into_iter(),
            false,
        )
    }

    /// Scalar evaluation: resolves both arguments to strings and computes
    /// `s.starts_with(prefix)`, applying the `inverted` flag.
    ///
    /// Returns:
    /// - `Err` only for shape / type errors (wrong arity, non-string argument)
    /// - `Ok(None)` when an argument could not be resolved (e.g. column ref to a non-partition
    ///   column during partition pruning) or is SQL NULL
    /// - `Ok(Some(_))` with the boolean result otherwise
    ///
    /// Unresolved must be `Ok(None)`, not `Err`: the kernel logs a `warn!`
    /// for every `Err` it sees during pruning, and partition-pruning routinely
    /// hits non-partition column refs for predicates like
    /// `STARTS_WITH(data_col, "x")` -- making `Err` here would mean one log
    /// line per Add action.
    pub(super) fn scalar(
        eval_expr: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expr],
        inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        let [s_expr, p_expr] = exprs else {
            return Err(Error::generic(
                "STARTS_WITH expects exactly 2 arguments (input, prefix)",
            ));
        };
        let (Some(s_val), Some(p_val)) = (eval_expr(s_expr), eval_expr(p_expr)) else {
            return Ok(None);
        };
        let Some(s) = require_string(s_val, "input")? else {
            return Ok(None);
        };
        let Some(p) = require_string(p_val, "prefix")? else {
            return Ok(None);
        };
        Ok(Some(s.starts_with(p.as_str()) != inverted))
    }

    /// Extracts the string from a scalar argument.
    /// `Ok(None)` for SQL NULL; `Err` only for non-string types.
    fn require_string(val: Scalar, role: &str) -> DeltaResult<Option<String>> {
        match val {
            Scalar::String(s) => Ok(Some(s)),
            Scalar::Null(_) => Ok(None),
            other => Err(Error::generic(format!(
                "STARTS_WITH expects string {role}, got {}",
                other.data_type()
            ))),
        }
    }
}

/// Lexicographic successor of `prefix` in the binary string collation kernel
/// uses for ordering.
///
/// For every string `s` that starts with `prefix`, `prefix <= s <
/// next_prefix(prefix)`. So a range-overlap test against this successor
/// captures exactly the strings beginning with `prefix`.
///
/// Returns `None` when:
/// - `prefix` is empty (matches every string -- no pruning possible)
/// - `prefix` consists entirely of `U+10FFFF` (no scalar value is greater)
///
/// The successor is found by incrementing the last code point. The surrogate
/// range `U+D800..=U+DFFF` is skipped (those are not valid Unicode scalar
/// values). If the last code point is `U+10FFFF` it is dropped and the next-
/// to-last is incremented; the loop continues through any trailing run of
/// `U+10FFFF`.
fn next_prefix(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        // `u32::from(char)` is in `0..=0x10FFFF`, so `+ 1` cannot overflow.
        let mut next_code = u32::from(last) + 1;
        if (0xD800..=0xDFFF).contains(&next_code) {
            next_code = 0xE000;
        }
        if let Some(c) = char::from_u32(next_code) {
            chars.push(c);
            return Some(chars.into_iter().collect());
        }
        // Reached U+10FFFF: drop and try to bump the prior char.
    }
    None
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::collections::HashMap;

    use delta_kernel::expressions::{column_expr, Expression};
    use delta_kernel::schema::DataType;

    use super::*;

    // === next_prefix ===========================================================

    #[test]
    fn next_prefix_increments_last_ascii_char() {
        assert_eq!(next_prefix("foo").as_deref(), Some("fop"));
        assert_eq!(next_prefix("a").as_deref(), Some("b"));
    }

    #[test]
    fn next_prefix_returns_none_for_empty_prefix() {
        assert_eq!(next_prefix(""), None);
    }

    #[test]
    fn next_prefix_drops_max_code_point_tail() {
        // "a\u{10FFFF}" -> drop max char, bump 'a' -> "b"
        let input = format!("a{}", '\u{10FFFF}');
        assert_eq!(next_prefix(&input).as_deref(), Some("b"));
    }

    #[test]
    fn next_prefix_returns_none_for_all_max_code_points() {
        let input = format!("{}{}", '\u{10FFFF}', '\u{10FFFF}');
        assert_eq!(next_prefix(&input), None);
    }

    #[test]
    fn next_prefix_steps_past_surrogate_range() {
        // U+D7FF is the last code point before the surrogate range; its
        // successor in Unicode scalar values is U+E000.
        let input = format!("a{}", '\u{D7FF}');
        let expected = format!("a{}", '\u{E000}');
        assert_eq!(next_prefix(&input).as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn next_prefix_produces_valid_utf8_for_multibyte_input() {
        // 'é' is U+00E9; successor is U+00EA = 'ê'.
        assert_eq!(next_prefix("é").as_deref(), Some("ê"));
    }

    // === starts_with::range_bounds (input validation) ==========================

    #[test]
    fn range_bounds_accepts_column_and_string_literal() {
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        let (col, lower, upper) = starts_with::range_bounds(&exprs, false).unwrap();
        assert_eq!(col.path()[0].as_str(), "name");
        assert_eq!(lower, Scalar::from("foo"));
        assert_eq!(upper, Scalar::from("fop"));
    }

    #[test]
    fn range_bounds_opts_out_for_inverted() {
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        assert!(starts_with::range_bounds(&exprs, true).is_none());
    }

    #[test]
    fn range_bounds_opts_out_for_empty_prefix() {
        let exprs = [column_expr!("name"), Expression::literal("")];
        assert!(starts_with::range_bounds(&exprs, false).is_none());
    }

    #[test]
    fn range_bounds_opts_out_when_prefix_has_no_successor() {
        let max = format!("{}", '\u{10FFFF}');
        let exprs = [column_expr!("name"), Expression::literal(max.as_str())];
        assert!(starts_with::range_bounds(&exprs, false).is_none());
    }

    #[test]
    fn range_bounds_opts_out_for_wrong_arg_shape() {
        // literal-first ordering: STARTS_WITH("foo", col) -- not the
        // canonical shape we support
        let exprs = [Expression::literal("foo"), column_expr!("name")];
        assert!(starts_with::range_bounds(&exprs, false).is_none());

        // wrong arity
        let exprs = [column_expr!("name")];
        assert!(starts_with::range_bounds(&exprs, false).is_none());

        // non-string prefix
        let exprs = [column_expr!("name"), Expression::literal(5_i32)];
        assert!(starts_with::range_bounds(&exprs, false).is_none());
    }

    // === starts_with::scalar (partition pruning) ===============================

    fn eval_with<'a>(
        partition: &'a HashMap<&'a str, Scalar>,
    ) -> impl Fn(&Expression) -> Option<Scalar> + 'a {
        |e: &Expression| match e {
            Expression::Column(c) => partition.get(c.path()[0].as_str()).cloned(),
            Expression::Literal(s) => Some(s.clone()),
            _ => None,
        }
    }

    fn run_scalar(
        partition: &HashMap<&str, Scalar>,
        exprs: &[Expression],
        inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        let f = eval_with(partition);
        starts_with::scalar(&f, exprs, inverted)
    }

    #[test]
    fn scalar_true_when_value_starts_with_prefix() {
        let p = HashMap::from([("name", Scalar::from("foobar"))]);
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        assert_eq!(run_scalar(&p, &exprs, false).unwrap(), Some(true));
    }

    #[test]
    fn scalar_false_when_value_does_not_start_with_prefix() {
        let p = HashMap::from([("name", Scalar::from("bazbar"))]);
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        assert_eq!(run_scalar(&p, &exprs, false).unwrap(), Some(false));
    }

    #[test]
    fn scalar_inverted_negates_result() {
        let p = HashMap::from([("name", Scalar::from("foobar"))]);
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        assert_eq!(run_scalar(&p, &exprs, true).unwrap(), Some(false));
    }

    #[test]
    fn scalar_null_input_produces_null_output() {
        let p = HashMap::from([("name", Scalar::Null(DataType::STRING))]);
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        assert_eq!(run_scalar(&p, &exprs, false).unwrap(), None);
    }

    #[test]
    fn scalar_null_prefix_produces_null_output() {
        let p = HashMap::from([("name", Scalar::from("foobar"))]);
        let exprs = [
            column_expr!("name"),
            Expression::literal(Scalar::Null(DataType::STRING)),
        ];
        assert_eq!(run_scalar(&p, &exprs, false).unwrap(), None);
    }

    #[test]
    fn scalar_unresolved_column_returns_none_not_err() {
        // `eval_expr` returns None for column refs the partition-pruning
        // evaluator can't resolve (e.g. data columns). That is the routine
        // case, not an error -- returning Err here would trigger one
        // `warn!` per Add action during pruning.
        let empty = HashMap::new();
        let exprs = [column_expr!("data_col"), Expression::literal("foo")];
        assert_eq!(run_scalar(&empty, &exprs, false).unwrap(), None);
    }

    #[test]
    fn scalar_wrong_arity_errors() {
        let p = HashMap::new();
        let exprs = [Expression::literal("foo")];
        assert!(run_scalar(&p, &exprs, false).is_err());
    }

    #[test]
    fn scalar_non_string_arg_errors() {
        let p = HashMap::from([("name", Scalar::from(5_i32))]);
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        assert!(run_scalar(&p, &exprs, false).is_err());
    }

    // === Dispatch ==============================================================

    #[test]
    fn unknown_op_falls_through_on_partition_path() {
        let partition = HashMap::from([("name", Scalar::from("foobar"))]);
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        let f = eval_with(&partition);
        assert!(evaluate_on_partition_value("UNKNOWN_OP", &f, &exprs, false).is_none());
    }

    #[test]
    fn starts_with_dispatched_by_name_on_partition_path() {
        let partition = HashMap::from([("name", Scalar::from("foobar"))]);
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        let f = eval_with(&partition);
        let result =
            evaluate_on_partition_value(STARTS_WITH, &f, &exprs, false).expect("recognized");
        assert_eq!(result.unwrap(), Some(true));
    }

    #[test]
    fn unknown_op_falls_through_on_row_group_path() {
        let stats = StubStats::new("foo", "foozzz");
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        assert_eq!(
            evaluate_for_row_group_skipping("UNKNOWN_OP", &stats, &exprs, false),
            None
        );
    }

    // === starts_with::skipping over direct row-group stats ====================
    //
    // Exercises the `Output = bool` instantiation of the generic `skipping`
    // helper. The `Output = Pred` side is covered by the integration tests.

    struct StubStats {
        min: Scalar,
        max: Scalar,
    }

    impl StubStats {
        fn new(min: &str, max: &str) -> Self {
            Self {
                min: Scalar::from(min),
                max: Scalar::from(max),
            }
        }
    }

    impl DataSkippingPredicateEvaluator for StubStats {
        type Output = bool;
        type ColumnStat = Scalar;

        fn get_min_stat(
            &self,
            _: &delta_kernel::expressions::ColumnName,
            dt: &DataType,
        ) -> Option<Scalar> {
            (self.min.data_type() == *dt).then(|| self.min.clone())
        }
        fn get_max_stat(
            &self,
            _: &delta_kernel::expressions::ColumnName,
            dt: &DataType,
        ) -> Option<Scalar> {
            (self.max.data_type() == *dt).then(|| self.max.clone())
        }
        fn get_nullcount_stat(&self, _: &delta_kernel::expressions::ColumnName) -> Option<Scalar> {
            None
        }
        fn get_rowcount_stat(&self) -> Option<Scalar> {
            None
        }
        fn eval_partial_cmp(
            &self,
            ord: Ordering,
            col: Scalar,
            val: &Scalar,
            inverted: bool,
        ) -> Option<bool> {
            delta_kernel::kernel_predicates::KernelPredicateEvaluatorDefaults::partial_cmp_scalars(
                ord, &col, val, inverted,
            )
        }
        fn finish_eval_pred_junction(
            &self,
            op: JunctionPredicateOp,
            preds: &mut dyn Iterator<Item = Option<bool>>,
            inverted: bool,
        ) -> Option<bool> {
            delta_kernel::kernel_predicates::KernelPredicateEvaluatorDefaults::finish_eval_pred_junction(op, preds, inverted)
        }
        // Unused on the STARTS_WITH skipping path -- stubs only need to compile.
        fn eval_pred_scalar(&self, _: &Scalar, _: bool) -> Option<bool> {
            None
        }
        fn eval_pred_scalar_is_null(&self, _: &Scalar, _: bool) -> Option<bool> {
            None
        }
        fn eval_pred_is_null(
            &self,
            _: &delta_kernel::expressions::ColumnName,
            _: bool,
        ) -> Option<bool> {
            None
        }
        fn eval_pred_binary_scalars(
            &self,
            _: delta_kernel::expressions::BinaryPredicateOp,
            _: &Scalar,
            _: &Scalar,
            _: bool,
        ) -> Option<bool> {
            None
        }
        fn eval_pred_opaque(
            &self,
            _: &delta_kernel::expressions::OpaquePredicateOpRef,
            _: &[Expr],
            _: bool,
        ) -> Option<bool> {
            None
        }
    }

    #[test]
    fn row_group_keep_when_prefix_overlaps_range() {
        // [foo, foozzz] overlaps [foo, fop) -> keep.
        let stats = StubStats::new("foo", "foozzz");
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        assert_eq!(
            evaluate_for_row_group_skipping(STARTS_WITH, &stats, &exprs, false),
            Some(true)
        );
    }

    #[test]
    fn row_group_skip_when_range_below_prefix() {
        // [apple, banana] strictly below "foo" -> max < foo -> skip.
        let stats = StubStats::new("apple", "banana");
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        assert_eq!(
            evaluate_for_row_group_skipping(STARTS_WITH, &stats, &exprs, false),
            Some(false)
        );
    }

    #[test]
    fn row_group_skip_when_range_above_prefix() {
        // [zebra, zoo] strictly above "fop" -> min >= fop -> skip.
        let stats = StubStats::new("zebra", "zoo");
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        assert_eq!(
            evaluate_for_row_group_skipping(STARTS_WITH, &stats, &exprs, false),
            Some(false)
        );
    }

    #[test]
    fn row_group_inverted_opts_out() {
        let stats = StubStats::new("foo", "foozzz");
        let exprs = [column_expr!("name"), Expression::literal("foo")];
        assert_eq!(
            evaluate_for_row_group_skipping(STARTS_WITH, &stats, &exprs, true),
            None
        );
    }
}

// === End-to-end skipping integration ==========================================
//
// Drives a real scan against an in-memory Delta log to verify `STARTS_WITH`
// pruning fires through the kernel data-skipping pipeline.

#[cfg(all(test, feature = "default-engine-rustls"))]
mod integration_tests {
    use std::sync::Arc;

    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::expressions::{column_expr, Expression, Predicate as Pred};
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::scan::state::ScanFile;
    use delta_kernel::snapshot::Snapshot;
    use test_utils::add_commit;

    use super::*;
    use crate::expressions::NamedOpaquePredicateOp;

    const TABLE_ROOT: &str = "memory:///";

    /// Schema and protocol shared by every test commit: a single `name` STRING column.
    const TABLE_PROTOCOL_AND_METADATA: &str = concat!(
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        "\n",
        r#"{"metaData":{"id":"starts-with-test","format":{"provider":"parquet","options":{}},"#,
        r#""schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","#,
        r#""partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#,
    );

    /// Build an `add` action JSON line for a file with the given min/max
    /// stats on the `name` column.
    fn add_action(path: &str, min: &str, max: &str) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":262,"modificationTime":1587968586000,"dataChange":true,"stats":"{{\"numRecords\":1,\"nullCount\":{{\"name\":0}},\"minValues\":{{\"name\":\"{min}\"}},\"maxValues\":{{\"name\":\"{max}\"}}}}"}}}}"#
        )
    }

    fn engine_and_storage() -> (
        Arc<
            delta_kernel::engine::default::DefaultEngine<
                delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor,
            >,
        >,
        Arc<InMemory>,
    ) {
        let storage = Arc::new(InMemory::new());
        let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
        (engine, storage)
    }

    async fn write_table(storage: &InMemory, files: &[(&str, &str, &str)]) {
        let mut commit = TABLE_PROTOCOL_AND_METADATA.to_string();
        for (path, min, max) in files {
            commit.push('\n');
            commit.push_str(&add_action(path, min, max));
        }
        add_commit(TABLE_ROOT, storage, 0, commit)
            .await
            .expect("write commit");
    }

    /// Build the scan and collect every file path the scan_metadata stream
    /// reports as surviving the predicate.
    fn surviving_files(
        engine: &dyn delta_kernel::Engine,
        snapshot: Arc<Snapshot>,
        predicate: Pred,
    ) -> Vec<String> {
        let scan = snapshot
            .scan_builder()
            .with_predicate(Arc::new(predicate))
            .build()
            .expect("build scan");
        let mut paths: Vec<String> = Vec::new();
        for metadata in scan.scan_metadata(engine).expect("scan_metadata") {
            let metadata = metadata.expect("scan_metadata batch");
            paths = metadata
                .visit_scan_files(paths, |paths: &mut Vec<String>, sf: ScanFile| {
                    paths.push(sf.path);
                })
                .expect("visit scan files");
        }
        paths
    }

    #[tokio::test]
    async fn starts_with_prunes_files_via_stats() {
        let (engine, storage) = engine_and_storage();
        // Three files whose `name` column ranges partition the keyspace
        // around the "foo" prefix. Range overlap means:
        //   apple..banana  ->  ends before "foo"   -> SKIP
        //   bar..foobar    ->  spans through "foo" -> KEEP
        //   foo..foozzz    ->  fully inside        -> KEEP
        //   zebra..zoo     ->  starts after "fop"  -> SKIP
        write_table(
            &storage,
            &[
                ("apple.parquet", "apple", "banana"),
                ("bar.parquet", "bar", "foobar"),
                ("foo.parquet", "foo", "foozzz"),
                ("zebra.parquet", "zebra", "zoo"),
            ],
        )
        .await;

        let snapshot = Snapshot::builder_for(TABLE_ROOT)
            .build(engine.as_ref())
            .expect("snapshot");

        let pred = Pred::opaque(
            NamedOpaquePredicateOp::new(STARTS_WITH),
            [column_expr!("name"), Expression::literal("foo")],
        );

        let mut surviving = surviving_files(engine.as_ref(), snapshot, pred);
        surviving.sort();

        assert_eq!(
            surviving,
            vec!["bar.parquet".to_string(), "foo.parquet".to_string()],
            "STARTS_WITH should keep exactly the files whose [min, max] range overlaps [\"foo\", \"fop\")"
        );
    }

    #[tokio::test]
    async fn starts_with_keeps_all_files_when_inverted() {
        // NOT STARTS_WITH opts out of skipping, so every file must survive
        // the metadata-level filter even when its stats clearly don't
        // satisfy the inverted predicate.
        let (engine, storage) = engine_and_storage();
        write_table(
            &storage,
            &[
                ("a.parquet", "apple", "banana"),
                ("b.parquet", "foo", "foozzz"),
            ],
        )
        .await;

        let snapshot = Snapshot::builder_for(TABLE_ROOT)
            .build(engine.as_ref())
            .expect("snapshot");

        let pred = Pred::not(Pred::opaque(
            NamedOpaquePredicateOp::new(STARTS_WITH),
            [column_expr!("name"), Expression::literal("foo")],
        ));

        let mut surviving = surviving_files(engine.as_ref(), snapshot, pred);
        surviving.sort();
        assert_eq!(
            surviving,
            vec!["a.parquet".to_string(), "b.parquet".to_string()],
            "inverted STARTS_WITH opts out of pruning"
        );
    }

    #[tokio::test]
    async fn opt_out_opaque_under_and_still_prunes_via_native_sibling() {
        // Mixed-AND predicate: an unrecognized opaque op (contributes None
        // to the rewrite) AND a native comparison that can prune. Kernel
        // must drop the None branch from the AND and use the native sibling
        // to skip files.
        let (engine, storage) = engine_and_storage();
        write_table(
            &storage,
            &[
                ("a.parquet", "apple", "banana"),
                ("b.parquet", "foo", "foozzz"),
            ],
        )
        .await;

        let snapshot = Snapshot::builder_for(TABLE_ROOT)
            .build(engine.as_ref())
            .expect("snapshot");

        let pred = Pred::and_from([
            Pred::opaque(
                NamedOpaquePredicateOp::new("MYSTERY_OP"),
                [column_expr!("name"), Expression::literal("ignored")],
            ),
            Pred::ge(column_expr!("name"), Expression::literal("foo")),
        ]);

        let mut surviving = surviving_files(engine.as_ref(), snapshot, pred);
        surviving.sort();
        assert_eq!(
            surviving,
            vec!["b.parquet".to_string()],
            "native >= sibling should still prune apple..banana even though MYSTERY_OP opts out"
        );
    }
}
