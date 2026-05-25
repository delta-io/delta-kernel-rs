//! Arrow-backed batched evaluation of [`NamedOpaquePredicateOp`].
//!
//! Adds an [`ArrowOpaquePredicateOp`] impl so the default engine's batch
//! evaluator can run an opaque op against a stats metadata batch with a
//! single FFI upcall per batch.

use delta_kernel::arrow::array::{BooleanArray, RecordBatch};
use delta_kernel::engine::arrow_expression::opaque::{
    ArrowOpaquePredicate, ArrowOpaquePredicateOp,
};
use delta_kernel::expressions::{Expression, Predicate, ScalarExpressionEvaluator};
use delta_kernel::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use delta_kernel::DeltaResult;

use super::pruning::{
    invoke_eval_against_stats, BatchStatsAccessor, ChildAccessor, OpaquePruneVerdict,
};
use super::NamedOpaquePredicateOp;
use crate::engine_data::ArrowFFIData;

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

        // None on export failure: engines that imported successfully read
        // stats from their own runtime; the rest leave verdicts at Unknown.
        let arrow_ffi = ArrowFFIData::try_from_record_batch(batch).ok();
        let stats = BatchStatsAccessor::new().with_arrow_ffi(arrow_ffi);
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

    use delta_kernel::arrow::array::{Array, ArrayRef, Int64Array, StringArray, StructArray};
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use delta_kernel::expressions::column_expr;

    use super::*;
    use crate::expressions::pruning::OpaquePruningCallbacks;

    fn three_file_batch() -> RecordBatch {
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

    #[test]
    fn eval_pred_on_empty_batch_does_not_panic() {
        thread_local! {
            static SAW_CALLBACK: Cell<bool> = const { Cell::new(false) };
        }

        extern "C" fn note_eval(
            _state: *mut std::ffi::c_void,
            _op_name: crate::KernelStringSlice,
            _children: *const ChildAccessor<'_>,
            _stats: *const BatchStatsAccessor<'_>,
            _n_rows: usize,
            _inverted: bool,
            _verdicts: *mut OpaquePruneVerdict,
        ) {
            SAW_CALLBACK.with(|c| c.set(true));
        }

        let empty_nullcount = StructArray::from(vec![(
            Arc::new(Field::new("size", ArrowDataType::Int64, true)),
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
        )]);
        let stats_parsed = StructArray::from(vec![(
            Arc::new(Field::new(
                "nullCount",
                ArrowDataType::Struct(vec![Field::new("size", ArrowDataType::Int64, true)].into()),
                true,
            )),
            Arc::new(empty_nullcount) as ArrayRef,
        )]);
        let schema = Schema::new(vec![Field::new(
            "stats_parsed",
            stats_parsed.data_type().clone(),
            true,
        )]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(stats_parsed)]).unwrap();

        let cb = Arc::new(OpaquePruningCallbacks {
            engine_state: std::ptr::null_mut(),
            eval_against_stats: note_eval,
            free_state: no_op_free,
        });
        let op = NamedOpaquePredicateOp::with_callbacks("FOO", cb);
        let result = ArrowOpaquePredicateOp::eval_pred(&op, &[], &batch, false).unwrap();
        assert_eq!(result.len(), 0);
        assert!(SAW_CALLBACK.with(Cell::get));
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
            assert!(!result.value(row), "row {row} should be skipped");
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
