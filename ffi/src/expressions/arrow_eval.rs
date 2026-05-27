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
use delta_kernel::arrow::array::{
    make_array, Array, ArrayRef, BooleanArray, RecordBatch, StructArray,
};
use delta_kernel::arrow::datatypes::{Field, Fields, Schema};
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_expression;
use delta_kernel::engine::arrow_expression::opaque::{
    ArrowOpaquePredicate as _, ArrowOpaquePredicateOp,
};
use delta_kernel::expressions::{Expression, ExpressionRef, ScalarExpressionEvaluator};
use delta_kernel::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator, KernelPredicateEvaluator,
};
use delta_kernel::schema::DataType;
use delta_kernel::{DeltaResult, Error, Predicate};

use super::opaque_eval::OpaqueEvalCallbacks;
use super::NamedOpaquePredicateOp;
use crate::engine_data::ArrowFFIData;
use crate::kernel_string_slice;

/// Evaluate each arg using kernel's standard evaluator, then bundle the
/// resulting `ArrayRef`s as a single-batch `RecordBatch` keyed by
/// `arg0..argN`.
///
/// `Expression::Struct` args produced by `as_data_skipping_predicate` are pre-evaluated
/// field-by-field and assembled into a `StructArray` here, since kernel's evaluator requires a
/// `DataType::Struct` result type for struct expressions and we don't know the field types
/// upfront (they depend on the underlying stats schema).
fn evaluate_args(args: &[Expression], batch: &RecordBatch) -> DeltaResult<RecordBatch> {
    let arrays: Vec<ArrayRef> = args
        .iter()
        .map(|arg| match arg {
            Expression::Struct(fields, _nullability) => evaluate_struct_arg(fields, batch),
            _ => evaluate_expression(arg, batch, None),
        })
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

/// Build a `StructArray` from a `Expression::Struct`'s child expressions, naming fields
/// positionally (`f0`, `f1`, ...). Engine callbacks crack the resulting struct by index, not
/// by name, so the names are just placeholders that satisfy Arrow's schema requirements.
fn evaluate_struct_arg(fields: &[ExpressionRef], batch: &RecordBatch) -> DeltaResult<ArrayRef> {
    let arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|f| evaluate_expression(f, batch, None))
        .collect::<DeltaResult<_>>()?;
    let arrow_fields: Fields = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("f{i}"), a.data_type().clone(), a.is_nullable()))
        .collect();
    let sa = StructArray::try_new(arrow_fields, arrays, None)
        .map_err(|e| Error::Generic(format!("struct arg construction: {e}")))?;
    Ok(Arc::new(sa))
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
        _evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<bool> {
        // The scalar-bool skipping path can't drive engine callbacks, so we always abstain;
        // file pruning happens via `as_data_skipping_predicate` instead.
        None
    }

    fn as_data_skipping_predicate(
        &self,
        evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate> {
        // Per-op negation semantics aren't expressible without engine input. Abstain to keep
        // pruning sound: kernel will fall back to "keep the file".
        if inverted {
            return None;
        }
        // Without callbacks the rewritten predicate would error at evaluation time (the
        // arrow eval_pred above requires callbacks). Abstain so the surrounding junction
        // logic correctly drops this branch from the stats predicate.
        self.callbacks_clone()?;

        // SENTINEL: kernel's `get_min_stat`/`get_max_stat` use the requested type only as a
        // gate against the stats schema -- columns whose min/max stats don't carry LONG
        // silently abstain. A follow-up commit will plumb the real per-column type through
        // the FFI so non-numeric columns (string, decimal, etc.) become prunable too.
        const SENTINEL_TYPE: DataType = DataType::LONG;

        let mut rewritten = Vec::with_capacity(exprs.len());
        for arg in exprs {
            let new_arg = match arg {
                Expression::Column(col) => {
                    let min = evaluator.get_min_stat(col, &SENTINEL_TYPE)?;
                    let max = evaluator.get_max_stat(col, &SENTINEL_TYPE)?;
                    // Positional struct: field 0 = min, field 1 = max. Field names come from
                    // the result schema at evaluation time, so engines crack by index.
                    Expression::struct_from([min, max])
                }
                Expression::Literal(_) => arg.clone(),
                Expression::Predicate(p) => {
                    let rw = evaluator.eval_pred(p, false)?;
                    Expression::Predicate(Box::new(rw))
                }
                _ => return None,
            };
            rewritten.push(new_arg);
        }
        // arrow_opaque (not bare opaque) so runtime dispatch reaches our callback via the
        // ArrowOpaquePredicateOpAdaptor.
        Some(Predicate::arrow_opaque(self.clone(), rewritten))
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

    // === Data-skipping rewrite ==================================================

    mod rewrite {
        use std::cmp::Ordering;

        use delta_kernel::engine::arrow_expression::opaque::ArrowOpaquePredicateOp as _;
        use delta_kernel::expressions::{
            column_expr, BinaryPredicateOp, ColumnName, Expression, JunctionPredicateOp,
            OpaquePredicateOpRef, Scalar,
        };
        use delta_kernel::kernel_predicates::DataSkippingPredicateEvaluator;
        use delta_kernel::schema::DataType;
        use delta_kernel::Predicate;

        use super::*;

        /// Stub evaluator that mirrors the real `DataSkippingPredicateCreator`:
        /// `get_min_stat`/`get_max_stat` return `stats_parsed.minValues.<col>` /
        /// `maxValues.<col>` references. Other methods are wired well enough for the
        /// `eval_pred(Predicate)` recursion the rewrite exercises.
        struct StatsRewriter {
            has_stats: bool,
        }

        fn stats_col(prefix: &str, col: &ColumnName) -> Expression {
            let mut name = ColumnName::new(["stats_parsed", prefix]);
            name = name.join(col);
            Expression::from(name)
        }

        impl DataSkippingPredicateEvaluator for StatsRewriter {
            type Output = Predicate;
            type ColumnStat = Expression;

            fn get_min_stat(&self, col: &ColumnName, _: &DataType) -> Option<Expression> {
                self.has_stats.then(|| stats_col("minValues", col))
            }
            fn get_max_stat(&self, col: &ColumnName, _: &DataType) -> Option<Expression> {
                self.has_stats.then(|| stats_col("maxValues", col))
            }
            fn get_nullcount_stat(&self, _: &ColumnName) -> Option<Expression> {
                None
            }
            fn get_rowcount_stat(&self) -> Option<Expression> {
                None
            }
            fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<Predicate> {
                if let Scalar::Boolean(b) = val {
                    Some(Predicate::literal(*b != inverted))
                } else {
                    None
                }
            }
            fn eval_pred_scalar_is_null(&self, _: &Scalar, _: bool) -> Option<Predicate> {
                None
            }
            fn eval_pred_is_null(&self, _: &ColumnName, _: bool) -> Option<Predicate> {
                None
            }
            fn eval_pred_binary_scalars(
                &self,
                _: BinaryPredicateOp,
                _: &Scalar,
                _: &Scalar,
                _: bool,
            ) -> Option<Predicate> {
                None
            }
            fn eval_pred_opaque(
                &self,
                _: &OpaquePredicateOpRef,
                _: &[Expression],
                _: bool,
            ) -> Option<Predicate> {
                None
            }
            fn finish_eval_pred_junction(
                &self,
                op: JunctionPredicateOp,
                preds: &mut dyn Iterator<Item = Option<Predicate>>,
                inverted: bool,
            ) -> Option<Predicate> {
                let collected: Vec<Predicate> = preds.collect::<Option<Vec<_>>>()?;
                let pred = Predicate::junction(op, collected);
                Some(if inverted { Predicate::not(pred) } else { pred })
            }
            fn eval_partial_cmp(
                &self,
                ord: Ordering,
                col: Expression,
                val: &Scalar,
                inverted: bool,
            ) -> Option<Predicate> {
                let base_op = match ord {
                    Ordering::Less => BinaryPredicateOp::LessThan,
                    Ordering::Greater => BinaryPredicateOp::GreaterThan,
                    Ordering::Equal => BinaryPredicateOp::Equal,
                };
                let pred = Predicate::binary(base_op, col, Expression::literal(val.clone()));
                Some(if inverted { Predicate::not(pred) } else { pred })
            }
        }

        fn rewriter() -> StatsRewriter {
            StatsRewriter { has_stats: true }
        }

        fn no_stats_rewriter() -> StatsRewriter {
            StatsRewriter { has_stats: false }
        }

        fn op_with_callbacks(name: &str) -> NamedOpaquePredicateOp {
            NamedOpaquePredicateOp::with_callbacks(name, callbacks_for(engine_starts_with))
        }

        fn rewrite(
            op: &NamedOpaquePredicateOp,
            args: &[Expression],
            rewriter: &StatsRewriter,
            inverted: bool,
        ) -> Option<Predicate> {
            op.as_data_skipping_predicate(
                rewriter
                    as &dyn DataSkippingPredicateEvaluator<
                        Output = Predicate,
                        ColumnStat = Expression,
                    >,
                args,
                inverted,
            )
        }

        #[test]
        fn abstains_when_inverted() {
            let op = op_with_callbacks("STARTS_WITH");
            let args = [column_expr!("col")];
            assert!(rewrite(&op, &args, &rewriter(), true).is_none());
        }

        #[test]
        fn abstains_when_no_callbacks() {
            let op = NamedOpaquePredicateOp::new("STARTS_WITH");
            let args = [column_expr!("col")];
            assert!(rewrite(&op, &args, &rewriter(), false).is_none());
        }

        #[test]
        fn wraps_column_in_min_max_struct() {
            let op = op_with_callbacks("STARTS_WITH");
            let args = [column_expr!("col")];
            let pred = rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");

            let Predicate::Opaque(opaque) = pred else {
                panic!("expected Opaque, got {pred:?}");
            };
            assert_eq!(opaque.exprs.len(), 1);
            let Expression::Struct(fields, _) = &opaque.exprs[0] else {
                panic!("expected Struct arg, got {:?}", opaque.exprs[0]);
            };
            assert_eq!(fields.len(), 2, "struct should have [min, max] fields");
            assert_eq!(
                *fields[0],
                stats_col("minValues", &ColumnName::new(["col"]))
            );
            assert_eq!(
                *fields[1],
                stats_col("maxValues", &ColumnName::new(["col"]))
            );
        }

        #[test]
        fn passes_literals_through_unchanged() {
            let op = op_with_callbacks("OP");
            let args = [Expression::literal("foo")];
            let pred = rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");
            let Predicate::Opaque(opaque) = pred else {
                panic!("expected Opaque");
            };
            assert_eq!(opaque.exprs.len(), 1, "arity preserved");
            assert_eq!(opaque.exprs[0], Expression::literal("foo"));
        }

        #[test]
        fn recursively_rewrites_predicate_child() {
            let op = op_with_callbacks("OP");
            let inner = Predicate::lt(column_expr!("col"), Expression::literal(5i64));
            let args = [Expression::Predicate(Box::new(inner))];

            let pred = rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");
            let Predicate::Opaque(opaque) = pred else {
                panic!("expected Opaque");
            };
            let Expression::Predicate(rewritten) = &opaque.exprs[0] else {
                panic!("expected Predicate arg, got {:?}", opaque.exprs[0]);
            };
            // The inner `col < 5` rewrites through `eval_partial_cmp` to a stats column ref.
            let serialized = format!("{rewritten:?}");
            assert!(
                serialized.contains("minValues") || serialized.contains("maxValues"),
                "predicate child should reference stats columns; got: {serialized}"
            );
        }

        #[test]
        fn abstains_on_unsupported_expr_kinds() {
            use delta_kernel::expressions::BinaryExpressionOp;
            let op = op_with_callbacks("OP");
            let args = [Expression::binary(
                BinaryExpressionOp::Plus,
                column_expr!("col"),
                Expression::literal(1i64),
            )];
            assert!(rewrite(&op, &args, &rewriter(), false).is_none());
        }

        #[test]
        fn abstains_when_column_has_no_stats() {
            let op = op_with_callbacks("OP");
            let args = [column_expr!("col")];
            assert!(rewrite(&op, &args, &no_stats_rewriter(), false).is_none());
        }

        #[test]
        fn preserves_arity_for_mixed_args() {
            let op = op_with_callbacks("OP");
            let inner = Predicate::lt(column_expr!("y"), Expression::literal(5i64));
            let args = [
                column_expr!("col"),
                Expression::literal(1i64),
                Expression::Predicate(Box::new(inner)),
            ];

            let pred = rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");
            let Predicate::Opaque(opaque) = pred else {
                panic!("expected Opaque");
            };
            assert_eq!(
                opaque.exprs.len(),
                3,
                "arity should be preserved (3 in, 3 out)"
            );
            assert!(matches!(opaque.exprs[0], Expression::Struct(_, _)));
            assert!(matches!(opaque.exprs[1], Expression::Literal(_)));
            assert!(matches!(opaque.exprs[2], Expression::Predicate(_)));
        }

        /// End-to-end: rewrite produces a predicate that evaluates against a synthesized
        /// stats batch -- proves the rewrite wraps via `arrow_opaque` (a bare opaque would
        /// fail the downcast inside the arrow evaluator and never invoke the callback).
        #[test]
        fn rewritten_predicate_dispatches_to_engine_callback() {
            use std::ffi::c_void;
            use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

            use delta_kernel::arrow::array::{Int64Array, StructArray};
            use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
            use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;

            static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

            unsafe extern "C" fn engine_count(
                _state: *mut c_void,
                _op_name: KernelStringSlice,
                args_in: *mut ArrowFFIData,
                _inverted: bool,
                result_out: *mut ArrowFFIData,
            ) -> bool {
                let batch = unsafe { take_ffi_record_batch(args_in) };
                CALL_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
                let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true; batch.num_rows()]));
                write_result(result_out, arr);
                true
            }

            let op = NamedOpaquePredicateOp::with_callbacks("OP", callbacks_for(engine_count));
            let args = [column_expr!("col")];
            let rewritten =
                rewrite(&op, &args, &rewriter(), false).expect("rewrite should succeed");

            // Build a stats batch matching the column refs produced by `StatsRewriter`:
            //   stats_parsed: { minValues: { col }, maxValues: { col } }
            let col_field = Arc::new(Field::new("col", ArrowDataType::Int64, true));
            let min_struct = StructArray::from(vec![(
                col_field.clone(),
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            )]);
            let max_struct = StructArray::from(vec![(
                col_field,
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
            )]);
            let stats_parsed = StructArray::from(vec![
                (
                    Arc::new(Field::new(
                        "minValues",
                        min_struct.data_type().clone(),
                        true,
                    )),
                    Arc::new(min_struct) as ArrayRef,
                ),
                (
                    Arc::new(Field::new(
                        "maxValues",
                        max_struct.data_type().clone(),
                        true,
                    )),
                    Arc::new(max_struct) as ArrayRef,
                ),
            ]);
            let schema = Arc::new(Schema::new(vec![Field::new(
                "stats_parsed",
                stats_parsed.data_type().clone(),
                true,
            )]));
            let stats_batch =
                RecordBatch::try_new(schema, vec![Arc::new(stats_parsed) as ArrayRef]).unwrap();

            CALL_COUNT.store(0, AtomicOrdering::SeqCst);
            let result = evaluate_predicate(&rewritten, &stats_batch, false).unwrap();
            assert_eq!(result.len(), 1);
            assert!(result.value(0));
            assert_eq!(
                CALL_COUNT.load(AtomicOrdering::SeqCst),
                1,
                "callback must fire exactly once"
            );
        }
    }
}
