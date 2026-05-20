//! Arrow-based per-row evaluation of [`NamedOpaquePredicateOp`].
//!
//! [`ArrowNamedOpaquePredicateOp`] is a newtype wrapper around
//! [`NamedOpaquePredicateOp`] that adds an [`ArrowOpaquePredicateOp`] impl, so
//! the default engine's batch evaluator can run an opaque op directly against
//! the file-stats metadata batch. `eval_pred` iterates the batch rows, invokes
//! the engine's `eval_against_stats` callback per row with a
//! [`BatchRowStatsProvider`] that reads from the row's `stats_parsed.*`
//! columns, and packs the verdicts into a `BooleanArray`.
//!
//! This is the file-pruning path for engines using the default engine. It
//! avoids per-file JNI for the kernel-side native predicates (those are
//! still batch-evaluated as today) but does cross the FFI boundary once
//! per file for the opaque parts of the predicate.

use delta_kernel::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    RecordBatch, StringArray, StructArray,
};
use delta_kernel::engine::arrow_expression::opaque::{
    ArrowOpaquePredicate, ArrowOpaquePredicateOp,
};
use delta_kernel::expressions::{Expression, Predicate, Scalar, ScalarExpressionEvaluator};
use delta_kernel::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use delta_kernel::schema::DataType;
use delta_kernel::DeltaResult;

use super::pruning::{invoke_eval_against_stats, ChildAccessor, StatsAccessor, StatsProvider};
use super::NamedOpaquePredicateOp;

// === BatchRowStatsProvider =====================================================

/// `StatsProvider` that reads min/max/nullcount/rowcount for a single file
/// from one row of the data-skipping metadata batch.
///
/// The batch follows kernel's data-skipping schema convention: stats live
/// under a `stats_parsed` struct column with sub-fields `numRecords`,
/// `nullCount`, `minValues`, `maxValues`. Each of the latter three is
/// itself a struct keyed by physical column name.
pub(crate) struct BatchRowStatsProvider<'a> {
    batch: &'a RecordBatch,
    row: usize,
}

impl<'a> BatchRowStatsProvider<'a> {
    fn new(batch: &'a RecordBatch, row: usize) -> Self {
        Self { batch, row }
    }

    fn stats_parsed(&self) -> Option<&StructArray> {
        let col = self.batch.column_by_name("stats_parsed")?;
        col.as_any().downcast_ref::<StructArray>()
    }

    /// Read the value of `stats_parsed.<kind>.<col>` at the current row,
    /// matching the requested `dtype`.
    fn read_stat(&self, kind: &str, col: &str, dtype: &DataType) -> Option<Scalar> {
        let stats_parsed = self.stats_parsed()?;
        let kind_array = stats_parsed.column_by_name(kind)?;
        let kind_struct = kind_array.as_any().downcast_ref::<StructArray>()?;
        let value_array = kind_struct.column_by_name(col)?;
        if value_array.is_null(self.row) {
            return None;
        }
        scalar_from_array(value_array.as_ref(), self.row, dtype)
    }
}

impl<'a> StatsProvider for BatchRowStatsProvider<'a> {
    fn min(&self, col: &str, dtype: &DataType) -> Option<Scalar> {
        self.read_stat("minValues", col, dtype)
    }

    fn max(&self, col: &str, dtype: &DataType) -> Option<Scalar> {
        self.read_stat("maxValues", col, dtype)
    }

    fn null_count(&self, col: &str) -> Option<i64> {
        let scalar = self.read_stat("nullCount", col, &DataType::LONG)?;
        super::pruning::scalar_as_i64(&scalar)
    }

    fn row_count(&self) -> Option<i64> {
        let stats_parsed = self.stats_parsed()?;
        let array = stats_parsed.column_by_name("numRecords")?;
        if array.is_null(self.row) {
            return None;
        }
        let scalar = scalar_from_array(array.as_ref(), self.row, &DataType::LONG)?;
        super::pruning::scalar_as_i64(&scalar)
    }
}

/// Read a typed value out of `array` at index `row`, coercing to the
/// requested `dtype`. Returns `None` for unsupported types.
///
/// For an integer `dtype` (LONG / INTEGER / SHORT / BYTE) any narrower signed
/// integer array is accepted and reported as the corresponding `Scalar`
/// variant. For a `DOUBLE` request, both `Float64Array` and `Float32Array` are
/// accepted. Engines that need to know the original width should keep the
/// dispatch granular.
fn scalar_from_array(array: &dyn Array, row: usize, dtype: &DataType) -> Option<Scalar> {
    let any = array.as_any();
    match dtype {
        d if *d == DataType::STRING => any
            .downcast_ref::<StringArray>()
            .map(|arr| Scalar::String(arr.value(row).to_string())),
        d if *d == DataType::LONG => integer_scalar(any, row),
        d if *d == DataType::INTEGER => any
            .downcast_ref::<Int32Array>()
            .map(|arr| Scalar::Integer(arr.value(row))),
        d if *d == DataType::SHORT => any
            .downcast_ref::<Int16Array>()
            .map(|arr| Scalar::Short(arr.value(row))),
        d if *d == DataType::BYTE => any
            .downcast_ref::<Int8Array>()
            .map(|arr| Scalar::Byte(arr.value(row))),
        d if *d == DataType::DOUBLE => double_scalar(any, row),
        d if *d == DataType::FLOAT => any
            .downcast_ref::<Float32Array>()
            .map(|arr| Scalar::Float(arr.value(row))),
        _ => None,
    }
}

fn integer_scalar(any: &dyn std::any::Any, row: usize) -> Option<Scalar> {
    if let Some(arr) = any.downcast_ref::<Int64Array>() {
        return Some(Scalar::Long(arr.value(row)));
    }
    if let Some(arr) = any.downcast_ref::<Int32Array>() {
        return Some(Scalar::Integer(arr.value(row)));
    }
    if let Some(arr) = any.downcast_ref::<Int16Array>() {
        return Some(Scalar::Short(arr.value(row)));
    }
    any.downcast_ref::<Int8Array>()
        .map(|arr| Scalar::Byte(arr.value(row)))
}

fn double_scalar(any: &dyn std::any::Any, row: usize) -> Option<Scalar> {
    if let Some(arr) = any.downcast_ref::<Float64Array>() {
        return Some(Scalar::Double(arr.value(row)));
    }
    any.downcast_ref::<Float32Array>()
        .map(|arr| Scalar::Float(arr.value(row)))
}

// === ArrowNamedOpaquePredicateOp ==============================================

/// Newtype wrapper that adds [`ArrowOpaquePredicateOp`] capability to a
/// [`NamedOpaquePredicateOp`]. Use this only when constructing predicates
/// that will be evaluated by the default engine's arrow batch evaluator;
/// engines with their own `EvaluationHandler` should keep using plain
/// `NamedOpaquePredicateOp`.
///
/// Engine-facing FFI: see
/// [`visit_predicate_opaque_with_pruning_arrow`](crate::expressions::kernel_visitor::visit_predicate_opaque_with_pruning_arrow).
#[derive(Debug, Clone)]
pub struct ArrowNamedOpaquePredicateOp(NamedOpaquePredicateOp);

impl ArrowNamedOpaquePredicateOp {
    pub(crate) fn new(inner: NamedOpaquePredicateOp) -> Self {
        Self(inner)
    }

    fn inner(&self) -> &NamedOpaquePredicateOp {
        &self.0
    }
}

impl PartialEq for ArrowNamedOpaquePredicateOp {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for ArrowNamedOpaquePredicateOp {}

impl ArrowOpaquePredicateOp for ArrowNamedOpaquePredicateOp {
    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> DeltaResult<BooleanArray> {
        let Some(cb) = self.inner().callbacks_clone() else {
            // No callbacks registered: every-row unknown. Data-skipping treats
            // unknown as keep, so kernel won't prune any files.
            return Ok(BooleanArray::from(vec![None; batch.num_rows()]));
        };

        let op_name = self.inner().op_name();
        let children = ChildAccessor::new(args);
        let mut results: Vec<Option<bool>> = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let provider = BatchRowStatsProvider::new(batch, row);
            let stats = StatsAccessor::new(&provider);
            let verdict = invoke_eval_against_stats(&cb, op_name, &children, &stats, inverted);
            results.push(verdict);
        }
        Ok(BooleanArray::from(results))
    }

    fn name(&self) -> &str {
        self.inner().op_name()
    }

    fn eval_pred_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        eval_pred: &DirectPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        // Partition pruning dispatch lives on the inner op.
        <NamedOpaquePredicateOp as delta_kernel::expressions::OpaquePredicateOp>::eval_pred_scalar(
            self.inner(),
            eval_expr,
            eval_pred,
            exprs,
            inverted,
        )
    }

    fn eval_as_data_skipping_predicate(
        &self,
        predicate_evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        <NamedOpaquePredicateOp as delta_kernel::expressions::OpaquePredicateOp>::eval_as_data_skipping_predicate(
            self.inner(),
            predicate_evaluator,
            exprs,
            inverted,
        )
    }

    fn as_data_skipping_predicate(
        &self,
        _predicate_evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate> {
        // With callbacks present, keep the op live across the indirect rewrite
        // so the default engine's `evaluate_predicate` can drive `eval_pred`
        // per metadata-batch row. When `inverted`, wrap in `Not`; kernel's
        // batch evaluator pushes that down into the `eval_pred` call with
        // `inverted=true` so the engine sees a single (un-doubled) inversion.
        if !self.inner().has_callbacks() {
            return None;
        }
        let pred = Predicate::arrow_opaque(self.clone(), exprs.iter().cloned());
        Some(if inverted { Predicate::not(pred) } else { pred })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::cell::Cell;
    use std::sync::Arc;

    use delta_kernel::arrow::array::ArrayRef;
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use delta_kernel::expressions::column_expr;

    use super::*;
    use crate::expressions::pruning::OpaquePruningCallbacks;

    fn make_stats_batch(min: &str, max: &str, nulls: i64, rows: i64) -> RecordBatch {
        let min_struct = StructArray::from(vec![(
            Arc::new(Field::new("name", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec![min])) as ArrayRef,
        )]);
        let max_struct = StructArray::from(vec![(
            Arc::new(Field::new("name", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec![max])) as ArrayRef,
        )]);
        let nullcount_field = Field::new(
            "nullCount",
            ArrowDataType::Struct(vec![Field::new("name", ArrowDataType::Int64, true)].into()),
            true,
        );
        let nullcount_struct = StructArray::from(vec![(
            Arc::new(Field::new("name", ArrowDataType::Int64, true)),
            Arc::new(Int64Array::from(vec![nulls])) as ArrayRef,
        )]);

        let stats_parsed = StructArray::from(vec![
            (
                Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
                Arc::new(Int64Array::from(vec![rows])) as ArrayRef,
            ),
            (
                Arc::new(nullcount_field),
                Arc::new(nullcount_struct) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "minValues",
                    ArrowDataType::Struct(
                        vec![Field::new("name", ArrowDataType::Utf8, true)].into(),
                    ),
                    true,
                )),
                Arc::new(min_struct) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "maxValues",
                    ArrowDataType::Struct(
                        vec![Field::new("name", ArrowDataType::Utf8, true)].into(),
                    ),
                    true,
                )),
                Arc::new(max_struct) as ArrayRef,
            ),
        ]);
        let schema = Schema::new(vec![Field::new(
            "stats_parsed",
            stats_parsed.data_type().clone(),
            true,
        )]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(stats_parsed)]).unwrap()
    }

    #[test]
    fn batch_row_stats_provider_reads_string_min_max() {
        let batch = make_stats_batch("apple", "zebra", 0, 100);
        let provider = BatchRowStatsProvider::new(&batch, 0);
        let min = provider.min("name", &DataType::STRING).unwrap();
        let max = provider.max("name", &DataType::STRING).unwrap();
        match (min, max) {
            (Scalar::String(min_s), Scalar::String(max_s)) => {
                assert_eq!(min_s, "apple");
                assert_eq!(max_s, "zebra");
            }
            other => panic!("expected string scalars, got {other:?}"),
        }
        assert_eq!(provider.null_count("name"), Some(0));
        assert_eq!(provider.row_count(), Some(100));
    }

    // === End-to-end eval_pred =================================================

    // A toy STARTS_WITH evaluator wired as an engine callback. Computes the
    // skipping math against the row's min/max for "name" and writes a verdict.
    extern "C" fn starts_with_eval_stats(
        _state: *mut std::ffi::c_void,
        op_name: crate::KernelStringSlice,
        children: *const ChildAccessor<'_>,
        stats: *const StatsAccessor<'_>,
        inverted: bool,
        out: *mut crate::expressions::pruning::OpaquePruneResult,
    ) {
        if inverted {
            return;
        }
        let name = unsafe { <String as crate::TryFromStringSlice>::try_from_slice(&op_name) }
            .unwrap_or_default();
        if name != "STARTS_WITH" {
            return;
        }
        if unsafe { crate::expressions::pruning::child_accessor_count(children) } != 2 {
            return;
        }
        let mut col_slice = crate::KernelStringSlice {
            ptr: std::ptr::null(),
            len: 0,
        };
        if !unsafe {
            crate::expressions::pruning::child_accessor_column_name(children, 0, &mut col_slice)
        } {
            return;
        }
        // KernelStringSlice is consumed by value; copy so we can pass it twice.
        let col_name = unsafe { <String as crate::TryFromStringSlice>::try_from_slice(&col_slice) }
            .unwrap_or_default();

        let mut prefix_slice = crate::KernelStringSlice {
            ptr: std::ptr::null(),
            len: 0,
        };
        if !unsafe {
            crate::expressions::pruning::child_accessor_literal_string(
                children,
                1,
                &mut prefix_slice,
            )
        } {
            return;
        }
        let prefix =
            unsafe { <String as crate::TryFromStringSlice>::try_from_slice(&prefix_slice) }
                .unwrap_or_default();

        let mut min_slice = crate::KernelStringSlice {
            ptr: std::ptr::null(),
            len: 0,
        };
        if !unsafe {
            crate::expressions::pruning::stats_accessor_min_string(
                stats,
                crate::kernel_string_slice!(col_name),
                &mut min_slice,
            )
        } {
            return;
        }
        let min = unsafe { <String as crate::TryFromStringSlice>::try_from_slice(&min_slice) }
            .unwrap_or_default();

        let mut max_slice = crate::KernelStringSlice {
            ptr: std::ptr::null(),
            len: 0,
        };
        if !unsafe {
            crate::expressions::pruning::stats_accessor_max_string(
                stats,
                crate::kernel_string_slice!(col_name),
                &mut max_slice,
            )
        } {
            return;
        }
        let max = unsafe { <String as crate::TryFromStringSlice>::try_from_slice(&max_slice) }
            .unwrap_or_default();
        // Range overlap: max >= prefix AND min < next_prefix(prefix)
        if max.as_str() < prefix.as_str() {
            unsafe { crate::expressions::pruning::opaque_prune_result_skip(out) };
            return;
        }
        if let Some(upper) = next_prefix(&prefix) {
            if min.as_str() >= upper.as_str() {
                unsafe { crate::expressions::pruning::opaque_prune_result_skip(out) };
                return;
            }
        }
        unsafe { crate::expressions::pruning::opaque_prune_result_keep(out) };
    }

    extern "C" fn no_op_partition(
        _state: *mut std::ffi::c_void,
        _op_name: crate::KernelStringSlice,
        _children: *const ChildAccessor<'_>,
        _resolver: *const crate::expressions::pruning::ScalarResolver<'_>,
        _inverted: bool,
        _out: *mut crate::expressions::pruning::OpaquePruneResult,
    ) {
    }

    extern "C" fn no_op_free(_state: *mut std::ffi::c_void) {}

    fn next_prefix(prefix: &str) -> Option<String> {
        let mut chars: Vec<char> = prefix.chars().collect();
        while let Some(last) = chars.pop() {
            let mut next_code = u32::from(last) + 1;
            if (0xD800..=0xDFFF).contains(&next_code) {
                next_code = 0xE000;
            }
            if let Some(c) = char::from_u32(next_code) {
                chars.push(c);
                return Some(chars.into_iter().collect());
            }
        }
        None
    }

    fn callbacks() -> Arc<OpaquePruningCallbacks> {
        Arc::new(OpaquePruningCallbacks {
            engine_state: std::ptr::null_mut(),
            eval_against_stats: starts_with_eval_stats,
            eval_on_partition_values: no_op_partition,
            eval_on_row_group_stats: starts_with_eval_stats,
            free_state: no_op_free,
        })
    }

    fn three_file_batch() -> RecordBatch {
        // Three files with name ranges:
        //   row 0: apple..banana  (entirely below "foo" -> SKIP)
        //   row 1: foo..foozzz    (overlaps [foo, fop) -> KEEP)
        //   row 2: zebra..zoo     (entirely above "fop" -> SKIP)
        let min = StructArray::from(vec![(
            Arc::new(Field::new("name", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["apple", "foo", "zebra"])) as ArrayRef,
        )]);
        let max = StructArray::from(vec![(
            Arc::new(Field::new("name", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["banana", "foozzz", "zoo"])) as ArrayRef,
        )]);
        let nullcount = StructArray::from(vec![(
            Arc::new(Field::new("name", ArrowDataType::Int64, true)),
            Arc::new(Int64Array::from(vec![0_i64, 0, 0])) as ArrayRef,
        )]);
        let stats_parsed = StructArray::from(vec![
            (
                Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
                Arc::new(Int64Array::from(vec![10_i64, 20, 30])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "nullCount",
                    ArrowDataType::Struct(
                        vec![Field::new("name", ArrowDataType::Int64, true)].into(),
                    ),
                    true,
                )),
                Arc::new(nullcount) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "minValues",
                    ArrowDataType::Struct(
                        vec![Field::new("name", ArrowDataType::Utf8, true)].into(),
                    ),
                    true,
                )),
                Arc::new(min) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "maxValues",
                    ArrowDataType::Struct(
                        vec![Field::new("name", ArrowDataType::Utf8, true)].into(),
                    ),
                    true,
                )),
                Arc::new(max) as ArrayRef,
            ),
        ]);
        let schema = Schema::new(vec![Field::new(
            "stats_parsed",
            stats_parsed.data_type().clone(),
            true,
        )]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(stats_parsed)]).unwrap()
    }

    #[test]
    fn eval_pred_prunes_per_row_via_engine_callback() {
        let op = ArrowNamedOpaquePredicateOp::new(NamedOpaquePredicateOp::with_callbacks(
            "STARTS_WITH",
            callbacks(),
        ));
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();

        let result = ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, false).unwrap();
        assert_eq!(result.len(), 3);
        // row 0 (apple..banana) -> skip
        assert!(!result.value(0));
        // row 1 (foo..foozzz) -> keep
        assert!(result.value(1));
        // row 2 (zebra..zoo) -> skip
        assert!(!result.value(2));
    }

    #[test]
    fn eval_pred_without_callbacks_returns_all_unknown() {
        let op = ArrowNamedOpaquePredicateOp::new(NamedOpaquePredicateOp::new("STARTS_WITH"));
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();

        let result = ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, false).unwrap();
        assert_eq!(result.len(), 3);
        // All-null -> all unknown -> kernel treats as keep
        assert!(result.is_null(0));
        assert!(result.is_null(1));
        assert!(result.is_null(2));
    }

    // === Inverted-flag forwarding =============================================

    // Callback that records whether kernel passed `inverted=true` and returns
    // Skip unconditionally so we can verify both the inversion flag and the
    // verdict propagation in one shot.
    extern "C" fn inverted_recording_eval(
        _state: *mut std::ffi::c_void,
        _op_name: crate::KernelStringSlice,
        _children: *const ChildAccessor<'_>,
        _stats: *const StatsAccessor<'_>,
        inverted: bool,
        out: *mut crate::expressions::pruning::OpaquePruneResult,
    ) {
        SAW_INVERTED.with(|c| c.set(c.get() || inverted));
        unsafe { crate::expressions::pruning::opaque_prune_result_skip(out) };
    }

    thread_local! {
        static SAW_INVERTED: Cell<bool> = const { Cell::new(false) };
    }

    fn inverted_recording_callbacks() -> Arc<OpaquePruningCallbacks> {
        Arc::new(OpaquePruningCallbacks {
            engine_state: std::ptr::null_mut(),
            eval_against_stats: inverted_recording_eval,
            eval_on_partition_values: no_op_partition,
            eval_on_row_group_stats: inverted_recording_eval,
            free_state: no_op_free,
        })
    }

    #[test]
    fn eval_pred_forwards_inverted_flag_to_engine() {
        SAW_INVERTED.with(|c| c.set(false));
        let op = ArrowNamedOpaquePredicateOp::new(NamedOpaquePredicateOp::with_callbacks(
            "STARTS_WITH",
            inverted_recording_callbacks(),
        ));
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();

        let result = ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, true).unwrap();
        assert_eq!(result.len(), 3);
        // All Skip verdicts since the engine wrote Skip every time.
        for row in 0..3 {
            assert!(!result.value(row), "row {row} should be skipped");
        }
        assert!(
            SAW_INVERTED.with(Cell::get),
            "engine callback should have observed inverted=true"
        );
    }

    #[test]
    fn eval_pred_inverted_false_does_not_set_flag() {
        SAW_INVERTED.with(|c| c.set(false));
        let op = ArrowNamedOpaquePredicateOp::new(NamedOpaquePredicateOp::with_callbacks(
            "STARTS_WITH",
            inverted_recording_callbacks(),
        ));
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();

        ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, false).unwrap();
        assert!(
            !SAW_INVERTED.with(Cell::get),
            "engine callback should not have observed inverted=true"
        );
    }
}
