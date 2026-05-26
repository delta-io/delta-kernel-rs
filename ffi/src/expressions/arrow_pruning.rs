//! Arrow-backed batched evaluation of [`NamedOpaquePredicateOp`].
//!
//! Adds an [`ArrowOpaquePredicateOp`] impl so the default engine's batch
//! evaluator can run an opaque op against a stats metadata batch with a
//! single FFI upcall per batch.

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

use super::pruning::{
    invoke_eval_against_stats, BatchStatsAccessor, BatchStatsProvider, ChildAccessor,
    OpaquePruneVerdict,
};
use super::NamedOpaquePredicateOp;
use crate::engine_data::ArrowFFIData;

// === BatchedArrowStatsProvider =================================================

/// [`BatchStatsProvider`] reading from a stats metadata batch. Resolves
/// `stats_parsed.{minValues,maxValues,nullCount,numRecords}` at
/// construction; missing top-level columns yield `None` from every getter.
pub(crate) struct BatchedArrowStatsProvider<'a> {
    min_values: Option<&'a StructArray>,
    max_values: Option<&'a StructArray>,
    null_count: Option<&'a StructArray>,
    num_records: Option<&'a dyn Array>,
}

impl<'a> BatchedArrowStatsProvider<'a> {
    pub(crate) fn new(batch: &'a RecordBatch) -> Self {
        let stats_parsed = batch
            .column_by_name("stats_parsed")
            .and_then(|c| c.as_any().downcast_ref::<StructArray>());
        let sub_struct = |name: &str| -> Option<&StructArray> {
            stats_parsed?
                .column_by_name(name)?
                .as_any()
                .downcast_ref::<StructArray>()
        };
        let num_records = stats_parsed
            .and_then(|s| s.column_by_name("numRecords"))
            .map(|c| c.as_ref());
        Self {
            min_values: sub_struct("minValues"),
            max_values: sub_struct("maxValues"),
            null_count: sub_struct("nullCount"),
            num_records,
        }
    }

    fn read_stat(
        kind_struct: Option<&'a StructArray>,
        row: usize,
        col: &str,
        dtype: &DataType,
    ) -> Option<Scalar> {
        let kind_struct = kind_struct?;
        let value_array = kind_struct.column_by_name(col)?;
        if value_array.is_null(row) {
            return None;
        }
        scalar_from_array(value_array.as_ref(), row, dtype)
    }
}

impl<'a> BatchStatsProvider for BatchedArrowStatsProvider<'a> {
    fn min(&self, row: usize, col: &str, dtype: &DataType) -> Option<Scalar> {
        Self::read_stat(self.min_values, row, col, dtype)
    }

    fn max(&self, row: usize, col: &str, dtype: &DataType) -> Option<Scalar> {
        Self::read_stat(self.max_values, row, col, dtype)
    }

    fn null_count(&self, row: usize, col: &str) -> Option<i64> {
        let scalar = Self::read_stat(self.null_count, row, col, &DataType::LONG)?;
        super::pruning::scalar_as_i64(&scalar)
    }

    fn row_count(&self, row: usize) -> Option<i64> {
        let array = self.num_records?;
        if array.is_null(row) {
            return None;
        }
        let scalar = scalar_from_array(array, row, &DataType::LONG)?;
        super::pruning::scalar_as_i64(&scalar)
    }
}

/// Read `array[row]` as a [`Scalar`] of `dtype`. For [`DataType::LONG`],
/// accepts any signed integer array (`Int8`..`Int64`) and returns the
/// matching native-width [`Scalar`] variant; callers widen via
/// [`scalar_as_i64`](super::pruning::scalar_as_i64). For [`DataType::DOUBLE`],
/// accepts `Float64Array` or `Float32Array` and returns the matching
/// native-width variant.
fn scalar_from_array(array: &dyn Array, row: usize, dtype: &DataType) -> Option<Scalar> {
    let any = array.as_any();
    let DataType::Primitive(prim) = dtype else {
        return None;
    };
    use delta_kernel::schema::PrimitiveType::*;
    match prim {
        String => any
            .downcast_ref::<StringArray>()
            .map(|arr| Scalar::String(arr.value(row).to_string())),
        Long => integer_scalar(any, row),
        Integer => any
            .downcast_ref::<Int32Array>()
            .map(|arr| Scalar::Integer(arr.value(row))),
        Short => any
            .downcast_ref::<Int16Array>()
            .map(|arr| Scalar::Short(arr.value(row))),
        Byte => any
            .downcast_ref::<Int8Array>()
            .map(|arr| Scalar::Byte(arr.value(row))),
        Double => double_scalar(any, row),
        Float => any
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

// === ArrowOpaquePredicateOp impl ==============================================

impl ArrowOpaquePredicateOp for NamedOpaquePredicateOp {
    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> DeltaResult<BooleanArray> {
        let n_rows = batch.num_rows();
        let Some(cb) = self.callbacks_clone() else {
            // No callbacks -- yield all-null so kernel keeps every file.
            return Ok(BooleanArray::from(vec![None; n_rows]));
        };

        let provider = BatchedArrowStatsProvider::new(batch);
        // None on export failure: engines that imported successfully read
        // stats from their own runtime; the rest fall back to the scalar
        // lane.
        let arrow_ffi = ArrowFFIData::try_from_record_batch(batch).ok();
        let stats = BatchStatsAccessor::new(&provider).with_arrow_ffi(arrow_ffi);
        let children = ChildAccessor::new(args);
        let mut verdicts = vec![OpaquePruneVerdict::Unknown; n_rows];
        invoke_eval_against_stats(
            &cb,
            self.op_name(),
            &children,
            &stats,
            inverted,
            &mut verdicts,
        );

        let bools: Vec<Option<bool>> = verdicts
            .iter()
            .map(|v| match *v {
                OpaquePruneVerdict::Keep => Some(true),
                OpaquePruneVerdict::Skip => Some(false),
                OpaquePruneVerdict::Unknown => None,
            })
            .collect();
        Ok(BooleanArray::from(bools))
    }

    fn name(&self) -> &str {
        self.op_name()
    }

    fn eval_pred_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        eval_pred: &DirectPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        <Self as delta_kernel::expressions::OpaquePredicateOp>::eval_pred_scalar(
            self, eval_expr, eval_pred, exprs, inverted,
        )
    }

    fn eval_as_data_skipping_predicate(
        &self,
        predicate_evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        <Self as delta_kernel::expressions::OpaquePredicateOp>::eval_as_data_skipping_predicate(
            self,
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
        // Wrap with `Not` so the batch evaluator flips `inverted` when
        // descending; the engine callback then sees `inverted` exactly once.
        if !self.has_callbacks() {
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

    fn three_file_batch() -> RecordBatch {
        // row 0: apple..banana  (entirely below "foo" -> SKIP)
        // row 1: foo..foozzz    (overlaps [foo, fop) -> KEEP)
        // row 2: zebra..zoo     (entirely above "fop" -> SKIP)
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

    extern "C" fn no_op_free(_state: *mut std::ffi::c_void) {}

    // === BatchedArrowStatsProvider ============================================

    #[test]
    fn batched_arrow_stats_provider_reads_string_min_max() {
        let batch = three_file_batch();
        let provider = BatchedArrowStatsProvider::new(&batch);

        let min0 = provider.min(0, "name", &DataType::STRING).unwrap();
        let max0 = provider.max(0, "name", &DataType::STRING).unwrap();
        match (min0, max0) {
            (Scalar::String(min_s), Scalar::String(max_s)) => {
                assert_eq!(min_s, "apple");
                assert_eq!(max_s, "banana");
            }
            other => panic!("expected string scalars, got {other:?}"),
        }
        assert_eq!(provider.null_count(1, "name"), Some(0));
        assert_eq!(provider.row_count(2), Some(30));
    }

    #[test]
    fn batched_arrow_stats_provider_returns_none_when_stats_parsed_missing() {
        // Build a batch with no `stats_parsed` column; every provider getter must yield None.
        let dummy = StructArray::from(vec![(
            Arc::new(Field::new("x", ArrowDataType::Int64, true)),
            Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
        )]);
        let schema = Schema::new(vec![Field::new("other", dummy.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dummy)]).unwrap();

        let provider = BatchedArrowStatsProvider::new(&batch);
        assert!(provider.min(0, "any", &DataType::STRING).is_none());
        assert!(provider.max(0, "any", &DataType::STRING).is_none());
        assert!(provider.null_count(0, "any").is_none());
        assert!(provider.row_count(0).is_none());
    }

    // === Arrow C Data Interface passthrough ===================================

    #[test]
    fn batch_stats_as_arrow_exports_a_valid_ffi_struct() {
        thread_local! {
            static SAW_NON_NULL: Cell<bool> = const { Cell::new(false) };
            static SAW_ROW_COUNT: Cell<i64> = const { Cell::new(-1) };
        }

        extern "C" fn capture_arrow_eval(
            _state: *mut std::ffi::c_void,
            _op_name: crate::KernelStringSlice,
            _children: *const ChildAccessor<'_>,
            stats: *const BatchStatsAccessor<'_>,
            _n_rows: usize,
            _inverted: bool,
            _verdicts: *mut OpaquePruneVerdict,
        ) {
            let ffi = unsafe { crate::expressions::pruning::batch_stats_as_arrow(stats) };
            if ffi.is_null() {
                return;
            }
            SAW_NON_NULL.with(|c| c.set(true));
            let len = unsafe { (*ffi).array.len() } as i64;
            SAW_ROW_COUNT.with(|c| c.set(len));
        }

        let cb = Arc::new(OpaquePruningCallbacks {
            engine_state: std::ptr::null_mut(),
            eval_against_stats: capture_arrow_eval,
            free_state: no_op_free,
        });
        let op = NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", cb);
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();
        let _ = ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, false).unwrap();

        assert!(SAW_NON_NULL.with(Cell::get));
        assert_eq!(SAW_ROW_COUNT.with(Cell::get), 3);
    }

    #[test]
    fn batch_stats_as_arrow_returns_null_on_null_accessor() {
        let p = unsafe { crate::expressions::pruning::batch_stats_as_arrow(std::ptr::null()) };
        assert!(p.is_null());
    }

    // === End-to-end eval_pred (scalar lane) ===================================

    // Toy STARTS_WITH evaluator using the per-row scalar getters.
    extern "C" fn starts_with_eval_batch(
        _state: *mut std::ffi::c_void,
        op_name: crate::KernelStringSlice,
        children: *const ChildAccessor<'_>,
        stats: *const BatchStatsAccessor<'_>,
        n_rows: usize,
        inverted: bool,
        verdicts: *mut OpaquePruneVerdict,
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
        let upper = next_prefix(&prefix);
        let verdicts = unsafe { std::slice::from_raw_parts_mut(verdicts, n_rows) };
        for (row, verdict_slot) in verdicts.iter_mut().enumerate() {
            let mut min_slice = crate::KernelStringSlice {
                ptr: std::ptr::null(),
                len: 0,
            };
            if !unsafe {
                crate::expressions::pruning::batch_stats_min_string(
                    stats,
                    row,
                    crate::kernel_string_slice!(col_name),
                    &mut min_slice,
                )
            } {
                continue;
            }
            let min = unsafe { <String as crate::TryFromStringSlice>::try_from_slice(&min_slice) }
                .unwrap_or_default();
            let mut max_slice = crate::KernelStringSlice {
                ptr: std::ptr::null(),
                len: 0,
            };
            if !unsafe {
                crate::expressions::pruning::batch_stats_max_string(
                    stats,
                    row,
                    crate::kernel_string_slice!(col_name),
                    &mut max_slice,
                )
            } {
                continue;
            }
            let max = unsafe { <String as crate::TryFromStringSlice>::try_from_slice(&max_slice) }
                .unwrap_or_default();
            let skip = max.as_str() < prefix.as_str()
                || upper.as_ref().is_some_and(|u| min.as_str() >= u.as_str());
            *verdict_slot = if skip {
                OpaquePruneVerdict::Skip
            } else {
                OpaquePruneVerdict::Keep
            };
        }
    }

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

    #[test]
    fn eval_pred_prunes_per_row_via_engine_callback() {
        let cb = Arc::new(OpaquePruningCallbacks {
            engine_state: std::ptr::null_mut(),
            eval_against_stats: starts_with_eval_batch,
            free_state: no_op_free,
        });
        let op = NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", cb);
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();

        let result = ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, false).unwrap();
        assert_eq!(result.len(), 3);
        assert!(!result.value(0)); // apple..banana -> skip
        assert!(result.value(1)); // foo..foozzz -> keep
        assert!(!result.value(2)); // zebra..zoo -> skip
    }

    #[test]
    fn eval_pred_without_callbacks_returns_all_unknown() {
        let op = NamedOpaquePredicateOp::new("STARTS_WITH");
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();

        let result = ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, false).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.is_null(0));
        assert!(result.is_null(1));
        assert!(result.is_null(2));
    }

    // === Inverted-flag forwarding =============================================

    // Records the `inverted` flag and writes Skip on every row.
    extern "C" fn inverted_recording_eval(
        _state: *mut std::ffi::c_void,
        _op_name: crate::KernelStringSlice,
        _children: *const ChildAccessor<'_>,
        _stats: *const BatchStatsAccessor<'_>,
        n_rows: usize,
        inverted: bool,
        verdicts: *mut OpaquePruneVerdict,
    ) {
        SAW_INVERTED.with(|c| c.set(c.get() || inverted));
        let slots = unsafe { std::slice::from_raw_parts_mut(verdicts, n_rows) };
        for slot in slots.iter_mut() {
            *slot = OpaquePruneVerdict::Skip;
        }
    }

    thread_local! {
        static SAW_INVERTED: Cell<bool> = const { Cell::new(false) };
    }

    fn inverted_recording_callbacks() -> Arc<OpaquePruningCallbacks> {
        Arc::new(OpaquePruningCallbacks {
            engine_state: std::ptr::null_mut(),
            eval_against_stats: inverted_recording_eval,
            free_state: no_op_free,
        })
    }

    #[test]
    fn eval_pred_forwards_inverted_flag_to_engine() {
        SAW_INVERTED.with(|c| c.set(false));
        let op =
            NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", inverted_recording_callbacks());
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();

        let result = ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, true).unwrap();
        assert_eq!(result.len(), 3);
        for row in 0..3 {
            assert!(!result.value(row));
        }
        assert!(SAW_INVERTED.with(Cell::get));
    }

    #[test]
    fn eval_pred_inverted_false_does_not_set_flag() {
        SAW_INVERTED.with(|c| c.set(false));
        let op =
            NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", inverted_recording_callbacks());
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();

        ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, false).unwrap();
        assert!(!SAW_INVERTED.with(Cell::get));
    }

    // === Verdict -> Option<bool> mapping ========================================

    fn make_verdict_callbacks(verdict: OpaquePruneVerdict) -> Arc<OpaquePruningCallbacks> {
        // Per-test verdict via a thread-local so the extern callback can read it.
        VERDICT_TO_WRITE.with(|c| c.set(verdict));
        Arc::new(OpaquePruningCallbacks {
            engine_state: std::ptr::null_mut(),
            eval_against_stats: verdict_eval,
            free_state: no_op_free,
        })
    }

    thread_local! {
        static VERDICT_TO_WRITE: Cell<OpaquePruneVerdict> = const { Cell::new(OpaquePruneVerdict::Unknown) };
    }

    extern "C" fn verdict_eval(
        _state: *mut std::ffi::c_void,
        _op_name: crate::KernelStringSlice,
        _children: *const ChildAccessor<'_>,
        _stats: *const BatchStatsAccessor<'_>,
        n_rows: usize,
        _inverted: bool,
        verdicts: *mut OpaquePruneVerdict,
    ) {
        let v = VERDICT_TO_WRITE.with(Cell::get);
        let slots = unsafe { std::slice::from_raw_parts_mut(verdicts, n_rows) };
        for slot in slots.iter_mut() {
            *slot = v;
        }
    }

    #[test]
    fn eval_pred_maps_keep_verdict_to_true() {
        let op = NamedOpaquePredicateOp::with_callbacks(
            "STARTS_WITH",
            make_verdict_callbacks(OpaquePruneVerdict::Keep),
        );
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();

        let result = ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, false).unwrap();
        assert_eq!(result.len(), 3);
        for row in 0..3 {
            assert!(!result.is_null(row));
            assert!(result.value(row));
        }
    }

    #[test]
    fn eval_pred_maps_unknown_verdict_to_null() {
        let op = NamedOpaquePredicateOp::with_callbacks(
            "STARTS_WITH",
            make_verdict_callbacks(OpaquePruneVerdict::Unknown),
        );
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();

        let result = ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, false).unwrap();
        assert_eq!(result.len(), 3);
        for row in 0..3 {
            assert!(result.is_null(row));
        }
    }

    // === as_data_skipping_predicate branches ====================================
    //
    // The impl ignores the evaluator argument; we provide a stub that panics on
    // any trait method to make that contract explicit.
    struct PanickingEvaluator;
    impl delta_kernel::kernel_predicates::DataSkippingPredicateEvaluator for PanickingEvaluator {
        type Output = delta_kernel::expressions::Predicate;
        type ColumnStat = delta_kernel::expressions::Expression;

        fn get_min_stat(
            &self,
            _col: &delta_kernel::expressions::ColumnName,
            _data_type: &delta_kernel::schema::DataType,
        ) -> Option<Self::ColumnStat> {
            unimplemented!()
        }
        fn get_max_stat(
            &self,
            _col: &delta_kernel::expressions::ColumnName,
            _data_type: &delta_kernel::schema::DataType,
        ) -> Option<Self::ColumnStat> {
            unimplemented!()
        }
        fn get_nullcount_stat(
            &self,
            _col: &delta_kernel::expressions::ColumnName,
        ) -> Option<Self::ColumnStat> {
            unimplemented!()
        }
        fn get_rowcount_stat(&self) -> Option<Self::ColumnStat> {
            unimplemented!()
        }
        fn eval_pred_scalar(
            &self,
            _val: &delta_kernel::expressions::Scalar,
            _inverted: bool,
        ) -> Option<Self::Output> {
            unimplemented!()
        }
        fn eval_pred_scalar_is_null(
            &self,
            _val: &delta_kernel::expressions::Scalar,
            _inverted: bool,
        ) -> Option<Self::Output> {
            unimplemented!()
        }
        fn eval_pred_is_null(
            &self,
            _col: &delta_kernel::expressions::ColumnName,
            _inverted: bool,
        ) -> Option<Self::Output> {
            unimplemented!()
        }
        fn eval_pred_binary_scalars(
            &self,
            _op: delta_kernel::expressions::BinaryPredicateOp,
            _left: &delta_kernel::expressions::Scalar,
            _right: &delta_kernel::expressions::Scalar,
            _inverted: bool,
        ) -> Option<Self::Output> {
            unimplemented!()
        }
        fn eval_pred_opaque(
            &self,
            _op: &delta_kernel::expressions::OpaquePredicateOpRef,
            _exprs: &[delta_kernel::expressions::Expression],
            _inverted: bool,
        ) -> Option<Self::Output> {
            unimplemented!()
        }
        fn finish_eval_pred_junction(
            &self,
            _op: delta_kernel::expressions::JunctionPredicateOp,
            _preds: &mut dyn Iterator<Item = Option<Self::Output>>,
            _inverted: bool,
        ) -> Option<Self::Output> {
            unimplemented!()
        }
        fn eval_partial_cmp(
            &self,
            _ord: std::cmp::Ordering,
            _col: Self::ColumnStat,
            _val: &delta_kernel::expressions::Scalar,
            _inverted: bool,
        ) -> Option<Self::Output> {
            unimplemented!()
        }
    }

    #[test]
    fn as_data_skipping_predicate_without_callbacks_returns_none() {
        let op = NamedOpaquePredicateOp::new("STARTS_WITH");
        let evaluator: &dyn delta_kernel::kernel_predicates::DataSkippingPredicateEvaluator<
            Output = delta_kernel::expressions::Predicate,
            ColumnStat = delta_kernel::expressions::Expression,
        > = &PanickingEvaluator;
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let result = <NamedOpaquePredicateOp as ArrowOpaquePredicateOp>::as_data_skipping_predicate(
            &op, evaluator, &args, false,
        );
        assert!(result.is_none());
    }

    #[test]
    fn as_data_skipping_predicate_not_inverted_returns_arrow_opaque() {
        let op =
            NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", inverted_recording_callbacks());
        let evaluator: &dyn delta_kernel::kernel_predicates::DataSkippingPredicateEvaluator<
            Output = delta_kernel::expressions::Predicate,
            ColumnStat = delta_kernel::expressions::Expression,
        > = &PanickingEvaluator;
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let result =
            <NamedOpaquePredicateOp as ArrowOpaquePredicateOp>::as_data_skipping_predicate(
                &op, evaluator, &args, false,
            )
            .unwrap();
        // `Predicate::arrow_opaque` produces an `Opaque` variant wrapping the
        // arrow adaptor. Confirm the outer shape; the inner adaptor is module-private.
        assert!(matches!(result, Predicate::Opaque(_)));
    }

    #[test]
    fn as_data_skipping_predicate_inverted_wraps_with_not() {
        let op =
            NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", inverted_recording_callbacks());
        let evaluator: &dyn delta_kernel::kernel_predicates::DataSkippingPredicateEvaluator<
            Output = delta_kernel::expressions::Predicate,
            ColumnStat = delta_kernel::expressions::Expression,
        > = &PanickingEvaluator;
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let result =
            <NamedOpaquePredicateOp as ArrowOpaquePredicateOp>::as_data_skipping_predicate(
                &op, evaluator, &args, true,
            )
            .unwrap();
        // Inverted should wrap with `Not(Opaque(_))`.
        match result {
            Predicate::Not(inner) => {
                assert!(matches!(*inner, Predicate::Opaque(_)));
            }
            other => panic!("expected Predicate::Not(Opaque(_)), got {other:?}"),
        }
    }
}
