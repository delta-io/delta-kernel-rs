//! `ArrowOpaquePredicateOp` impl for `NamedOpaquePredicateOp`.
//!
//! Kernel's `evaluate_predicate` recurses through compound shapes (Binary, Junction, etc.)
//! natively. This module is the leaf: when kernel hits an opaque predicate, we evaluate each arg
//! via `evaluate_expression`, bundle the resulting `ArrayRef`s as a `RecordBatch`, export through
//! Arrow C Data Interface, and hand them to the engine's callback. Engine receives pre-computed
//! columns, never the AST.
//!
//! Available only under `default-engine-base` (requires the kernel-side arrow expression
//! evaluator).

use std::sync::Arc;

use delta_kernel::arrow::array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use delta_kernel::arrow::array::{make_array, Array, ArrayRef, BooleanArray, RecordBatch};
use delta_kernel::arrow::datatypes::{Field, Schema};
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_expression;
use delta_kernel::engine::arrow_expression::opaque::ArrowOpaquePredicateOp;
use delta_kernel::expressions::{Expression, ScalarExpressionEvaluator};
use delta_kernel::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use delta_kernel::{DeltaResult, Error, Predicate};

use super::opaque_eval::OpaqueEvalCallbacks;
use super::NamedOpaquePredicateOp;
use crate::engine_data::ArrowFFIData;
use crate::kernel_string_slice;

/// Evaluate each arg using kernel's standard evaluator, then bundle the
/// resulting `ArrayRef`s as a single-batch `RecordBatch` keyed by
/// `arg0..argN`.
fn evaluate_args(args: &[Expression], batch: &RecordBatch) -> DeltaResult<RecordBatch> {
    let arrays: Vec<ArrayRef> = args
        .iter()
        .map(|arg| evaluate_expression(arg, batch, None))
        .collect::<DeltaResult<_>>()?;

    let fields: Vec<Field> = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("arg{i}"), a.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Zero-arg ops (e.g. NOW(), RAND()): empty-schema batch with the correct
    // row count so the engine knows how many rows to emit.
    if arrays.is_empty() {
        let n_rows = batch.num_rows();
        return RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &delta_kernel::arrow::array::RecordBatchOptions::new().with_row_count(Some(n_rows)),
        )
        .map_err(|e| Error::Generic(format!("zero-arg opaque eval batch construction: {e}")));
    }

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| Error::Generic(format!("opaque eval batch construction: {e}")))
}

/// Import an `ArrowFFIData` written by the engine back into an `ArrayRef`.
fn import_ffi_array(ffi: &mut ArrowFFIData) -> DeltaResult<ArrayRef> {
    // Engine returned success but never populated result_out: an empty
    // FFI_ArrowArray has no `release` callback (per Arrow C Data Interface,
    // every populated array carries a release fn). Detect this before
    // handing the empties to `from_ffi`, which would otherwise be UB-adjacent.
    if ffi.array.is_released() {
        return Err(Error::Generic(
            "engine callback returned success but wrote no result array".into(),
        ));
    }

    // Take ownership of the FFI structs out of the result buffer so the
    // engine's release callbacks fire exactly once.
    let array = std::mem::replace(&mut ffi.array, FFI_ArrowArray::empty());
    let schema = std::mem::replace(&mut ffi.schema, FFI_ArrowSchema::empty());

    // SAFETY: the engine promised these structs are valid Arrow C Data
    // Interface payloads it produced and handed to us.
    let array_data = unsafe { from_ffi(array, &schema) }
        .map_err(|e| Error::Generic(format!("from_ffi: {e}")))?;
    Ok(make_array(array_data))
}

/// Engine returned an `ArrayRef` of the wrong top-level shape -- raise an
/// error rather than silently coercing.
fn require_boolean_array(arr: ArrayRef) -> DeltaResult<BooleanArray> {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| {
            Error::Generic(format!(
                "opaque predicate eval_pred returned non-boolean array of type {:?}",
                arr.data_type()
            ))
        })
}

fn call_eval_pred(
    cb: &OpaqueEvalCallbacks,
    op_name: &str,
    args: &[Expression],
    batch: &RecordBatch,
    inverted: bool,
) -> DeltaResult<BooleanArray> {
    let args_batch = evaluate_args(args, batch)?;
    let mut args_ffi = ArrowFFIData::try_from_record_batch(&args_batch)?;
    let mut result_ffi = ArrowFFIData::empty();

    // SAFETY: callback was supplied by the engine. We pass a writable
    // args slot (engine takes ownership of the Arrow FFI handles) and a
    // writable result slot (engine writes its output handles here).
    let ok = unsafe {
        (cb.eval_pred)(
            cb.engine_state,
            kernel_string_slice!(op_name),
            &mut args_ffi,
            inverted,
            &mut result_ffi,
        )
    };
    if !ok {
        return Err(Error::Generic(format!(
            "engine eval_pred reported failure for `{op_name}`"
        )));
    }

    let arr = import_ffi_array(&mut result_ffi)?;
    require_boolean_array(arr)
}

// === ArrowOpaquePredicateOp ====================================================

impl ArrowOpaquePredicateOp for NamedOpaquePredicateOp {
    fn name(&self) -> &str {
        self.op_name()
    }

    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> DeltaResult<BooleanArray> {
        let cb = self.callbacks_clone().ok_or_else(|| {
            Error::Generic(format!(
                "opaque predicate `{}` has no eval context attached",
                self.op_name()
            ))
        })?;
        call_eval_pred(&cb, self.op_name(), args, batch, inverted)
    }

    fn eval_pred_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _eval_pred: &DirectPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        Ok(None)
    }

    fn eval_as_data_skipping_predicate(
        &self,
        evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        // Defer to the kernel-side OpaquePredicateOp impl on Self so that
        // ops constructed via `Predicate::arrow_opaque` (i.e. wrapped in the
        // ArrowOpaquePredicateOpAdaptor) still consult the skipping_decomp
        // facet through the Adaptor's delegating impl.
        <Self as delta_kernel::expressions::OpaquePredicateOp>::eval_as_data_skipping_predicate(
            self, evaluator, exprs, inverted,
        )
    }

    fn as_data_skipping_predicate(
        &self,
        evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate> {
        <Self as delta_kernel::expressions::OpaquePredicateOp>::as_data_skipping_predicate(
            self, evaluator, exprs, inverted,
        )
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::ffi::c_void;
    use std::ptr;
    use std::sync::Arc;

    use delta_kernel::arrow::array::ffi::from_ffi;
    use delta_kernel::arrow::array::{
        ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray, StructArray,
    };
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
    use delta_kernel::engine::arrow_expression::opaque::ArrowOpaquePredicate as _;
    use delta_kernel::expressions::{column_expr, Expression, Predicate};

    use super::*;
    use crate::expressions::opaque_eval::OpaqueEvalCallbacks;
    use crate::KernelStringSlice;

    // === Engine-side test stubs ===================================================
    //
    // Tiny "engine" written in Rust that demonstrates composite opaque predicates.
    // `STARTS_WITH_LOWER(col, prefix)` folds the LOWER step into the predicate
    // callback, so no `Expression::Opaque` ever appears in the tree.

    /// Take ownership of args FFI handles, leaving the slot empty so kernel's
    /// drop is a no-op.
    unsafe fn take_ffi_record_batch(args_in: *mut ArrowFFIData) -> RecordBatch {
        let array = std::mem::replace(unsafe { &mut (*args_in).array }, FFI_ArrowArray::empty());
        let schema = std::mem::replace(unsafe { &mut (*args_in).schema }, FFI_ArrowSchema::empty());
        let array_data = unsafe { from_ffi(array, &schema) }.unwrap();
        let sa = StructArray::from(array_data);
        sa.into()
    }

    /// Write a result `ArrayRef` back into `result_out` as a top-level array.
    fn write_result(result_out: *mut ArrowFFIData, arr: ArrayRef) {
        let array_data = arr.to_data();
        let array_ffi = delta_kernel::arrow::array::ffi::FFI_ArrowArray::new(&array_data);
        let schema_ffi =
            delta_kernel::arrow::array::ffi::FFI_ArrowSchema::try_from(array_data.data_type())
                .unwrap();
        unsafe {
            (*result_out).array = array_ffi;
            (*result_out).schema = schema_ffi;
        }
    }

    /// Composite opaque predicate: `STARTS_WITH(LOWER(args[0]), args[1])`. Both
    /// args arrive as pre-evaluated String columns; the engine applies LOWER
    /// then prefix-match per row.
    unsafe extern "C" fn engine_starts_with_lower(
        _state: *mut c_void,
        _op_name: KernelStringSlice,
        args_in: *mut ArrowFFIData,
        inverted: bool,
        result_out: *mut ArrowFFIData,
    ) -> bool {
        let batch = unsafe { take_ffi_record_batch(args_in) };
        assert_eq!(batch.num_columns(), 2);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arg0 should be StringArray");
        let prefix = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arg1 should be StringArray");
        let out: BooleanArray = (0..batch.num_rows())
            .map(|i| match (col.is_valid(i), prefix.is_valid(i)) {
                (true, true) => {
                    let m = col.value(i).to_lowercase().starts_with(prefix.value(i));
                    Some(if inverted { !m } else { m })
                }
                _ => None,
            })
            .collect();
        write_result(result_out, Arc::new(out));
        true
    }

    /// Simpler opaque predicate: `STARTS_WITH(args[0], args[1])` with no LOWER.
    /// Used by the composition test where we need two ops sharing the same op
    /// name function.
    unsafe extern "C" fn engine_starts_with(
        _state: *mut c_void,
        _op_name: KernelStringSlice,
        args_in: *mut ArrowFFIData,
        inverted: bool,
        result_out: *mut ArrowFFIData,
    ) -> bool {
        let batch = unsafe { take_ffi_record_batch(args_in) };
        let lhs = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arg0 should be StringArray");
        let rhs = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arg1 should be StringArray");
        let out: BooleanArray = (0..batch.num_rows())
            .map(|i| match (lhs.is_valid(i), rhs.is_valid(i)) {
                (true, true) => {
                    let m = lhs.value(i).starts_with(rhs.value(i));
                    Some(if inverted { !m } else { m })
                }
                _ => None,
            })
            .collect();
        write_result(result_out, Arc::new(out));
        true
    }

    unsafe extern "C" fn noop_free(_state: *mut c_void) {}

    fn callbacks_for(
        eval_pred: unsafe extern "C" fn(
            *mut c_void,
            KernelStringSlice,
            *mut ArrowFFIData,
            bool,
            *mut ArrowFFIData,
        ) -> bool,
    ) -> Arc<OpaqueEvalCallbacks> {
        Arc::new(OpaqueEvalCallbacks {
            engine_state: ptr::null_mut(),
            eval_pred,
            free_state: noop_free,
        })
    }

    fn batch_with_col(values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            ArrowDataType::Utf8,
            true,
        )]));
        let arr: ArrayRef = Arc::new(StringArray::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    // === End-to-end: STARTS_WITH(LOWER(col), "foo") as one composite op =========

    #[test]
    fn composite_starts_with_lower_predicate_round_trips() {
        let pred = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks(
                "STARTS_WITH_LOWER",
                callbacks_for(engine_starts_with_lower),
            ),
            [column_expr!("col"), Expression::literal("foo")],
        );

        let batch = batch_with_col(vec![Some("FOOBAR"), Some("Apple"), None, Some("foozzz")]);
        let result = evaluate_predicate(&pred, &batch, false).unwrap();

        assert_eq!(result.len(), 4);
        assert!(result.value(0), "FOOBAR -> foobar starts_with foo");
        assert!(!result.value(1), "Apple -> apple does not start_with foo");
        assert!(result.is_null(2), "null col -> null verdict");
        assert!(result.value(3), "foozzz starts_with foo");
    }

    #[test]
    fn inversion_flips_verdicts() {
        let pred = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks(
                "STARTS_WITH_LOWER",
                callbacks_for(engine_starts_with_lower),
            ),
            [column_expr!("col"), Expression::literal("foo")],
        );
        let batch = batch_with_col(vec![Some("FOOBAR"), Some("Apple")]);
        let result = evaluate_predicate(&pred, &batch, true).unwrap();
        assert!(!result.value(0), "inverted: FOOBAR no longer matches");
        assert!(result.value(1), "inverted: Apple now matches");
    }

    // === Composition: AND of two opaque predicates ==============================

    #[test]
    fn composition_and_of_two_opaque_predicates() {
        let cb = callbacks_for(engine_starts_with);
        let lhs = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", cb.clone()),
            [column_expr!("col"), Expression::literal("F")],
        );
        let rhs = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", cb),
            [column_expr!("col"), Expression::literal("FO")],
        );
        let and_pred = Predicate::and_from([lhs, rhs]);

        let batch = batch_with_col(vec![Some("FOOBAR"), Some("FBAR"), Some("BAR")]);
        let result = evaluate_predicate(&and_pred, &batch, false).unwrap();
        assert!(result.value(0), "FOOBAR starts with F AND FO");
        assert!(!result.value(1), "FBAR starts with F but not FO");
        assert!(!result.value(2), "BAR starts with neither");
    }

    // === Negative paths ==========================================================

    #[test]
    fn engine_returning_false_surfaces_error() {
        unsafe extern "C" fn engine_fail(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            _args_in: *mut ArrowFFIData,
            _inverted: bool,
            _result_out: *mut ArrowFFIData,
        ) -> bool {
            false
        }
        let pred = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks("ALWAYS_FAIL", callbacks_for(engine_fail)),
            [column_expr!("col")],
        );
        let batch = batch_with_col(vec![Some("x")]);
        let err = evaluate_predicate(&pred, &batch, false).unwrap_err();
        assert!(format!("{err}").contains("engine eval_pred reported failure"));
    }

    #[test]
    fn engine_returning_non_boolean_array_errors() {
        unsafe extern "C" fn engine_int(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            args_in: *mut ArrowFFIData,
            _inverted: bool,
            result_out: *mut ArrowFFIData,
        ) -> bool {
            let _ = unsafe { take_ffi_record_batch(args_in) };
            let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
            write_result(result_out, arr);
            true
        }
        let pred = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks("RETURNS_INT", callbacks_for(engine_int)),
            [column_expr!("col")],
        );
        let batch = batch_with_col(vec![Some("x"), Some("y"), Some("z")]);
        let err = evaluate_predicate(&pred, &batch, false).unwrap_err();
        assert!(format!("{err}").contains("non-boolean array"), "got: {err}");
    }

    #[test]
    fn engine_returning_success_without_result_errors() {
        unsafe extern "C" fn engine_empty(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            args_in: *mut ArrowFFIData,
            _inverted: bool,
            _result_out: *mut ArrowFFIData,
        ) -> bool {
            // Consume args but never populate result_out.
            let _ = unsafe { take_ffi_record_batch(args_in) };
            true
        }
        let pred = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks("EMPTY", callbacks_for(engine_empty)),
            [column_expr!("col")],
        );
        let batch = batch_with_col(vec![Some("x")]);
        let err = evaluate_predicate(&pred, &batch, false).unwrap_err();
        assert!(
            format!("{err}").contains("wrote no result array"),
            "got: {err}"
        );
    }

    // === Zero-arg opaque predicate ===============================================

    #[test]
    fn zero_arg_opaque_predicate_propagates_row_count() {
        unsafe extern "C" fn engine_always_true(
            _state: *mut c_void,
            _op_name: KernelStringSlice,
            args_in: *mut ArrowFFIData,
            _inverted: bool,
            result_out: *mut ArrowFFIData,
        ) -> bool {
            let batch = unsafe { take_ffi_record_batch(args_in) };
            assert_eq!(batch.num_columns(), 0, "zero-arg => no columns");
            let n = batch.num_rows();
            let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true; n]));
            write_result(result_out, arr);
            true
        }
        let pred = Predicate::arrow_opaque(
            NamedOpaquePredicateOp::with_callbacks("ZERO_ARG", callbacks_for(engine_always_true)),
            [] as [Expression; 0],
        );
        let batch = batch_with_col(vec![Some("a"); 5]);
        let result = evaluate_predicate(&pred, &batch, false).unwrap();
        assert_eq!(result.len(), 5);
        assert!((0..5).all(|i| result.value(i)));
    }
}
