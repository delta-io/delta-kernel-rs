//! Arrow-backed batched evaluation of [`NamedOpaquePredicateOp`].
//!
//! Adds an [`ArrowOpaquePredicateOp`] impl on [`NamedOpaquePredicateOp`] so the
//! default engine's batch evaluator can run an opaque op against the file-stats
//! metadata batch with a single FFI upcall: kernel hands the engine a
//! `BatchStatsAccessor` (backed by `BatchedArrowStatsProvider`) plus a verdicts
//! buffer of length `n_rows`, the engine writes per-row verdicts inline, and
//! kernel translates them back into a `BooleanArray`.
//!
//! `BatchedArrowStatsProvider` resolves the top-level `stats_parsed.*` columns
//! once at construction so per-row reads inside the callback skip the schema
//! walk.

use std::ffi::c_void;

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

/// `BatchStatsProvider` that serves the whole metadata batch. Column references
/// for `stats_parsed.minValues`, `.maxValues`, `.nullCount`, and `.numRecords`
/// are resolved once at construction; per-row reads skip the schema walk and
/// dispatch straight to the cached struct.
///
/// Missing top-level columns are tracked as `None`; reads return `None` (which
/// the FFI surface reports as "stat absent"). Per-column lookups inside each
/// stat-kind struct still happen on every call -- engines that need to amortize
/// those should add a column-level cache.
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

// === ArrowOpaquePredicateOp impl ==============================================

/// Adds arrow batch-evaluator capability to [`NamedOpaquePredicateOp`]. The
/// default engine's `evaluate_predicate` recovers this impl from the
/// `Predicate::Opaque` adaptor and invokes `eval_pred` once per metadata
/// batch. The engine receives a `BatchStatsAccessor` covering all rows and
/// writes one verdict per row; `eval_pred` maps the verdicts into a
/// `BooleanArray` for kernel. Partition and row-group dispatch reuses the
/// `OpaquePredicateOp` impl on the same type.
impl ArrowOpaquePredicateOp for NamedOpaquePredicateOp {
    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> DeltaResult<BooleanArray> {
        let n_rows = batch.num_rows();
        let Some(cb) = self.callbacks_clone() else {
            // No callbacks registered: every-row unknown. Data-skipping treats
            // unknown as keep, so kernel won't prune any files.
            return Ok(BooleanArray::from(vec![None; n_rows]));
        };

        let provider = BatchedArrowStatsProvider::new(batch);
        // Export the metadata batch over the Arrow C Data Interface; engines
        // that import it skip the scalar getters. Null on export failure
        // (unrepresentable schema) -- engines fall back to the scalar lane.
        // The Box must outlive `stats` (which holds a raw pointer to it);
        // declaring it first ensures drop order is stats -> arrow_ffi.
        let arrow_ffi: Option<Box<ArrowFFIData>> = ArrowFFIData::try_from_record_batch(batch)
            .ok()
            .map(Box::new);
        let arrow_ptr: *const c_void = arrow_ffi
            .as_deref()
            .map_or(std::ptr::null(), |d| d as *const _ as *const c_void);
        let stats = BatchStatsAccessor::new(&provider).with_arrow_ffi(arrow_ptr);
        let children = ChildAccessor::new(args);
        let mut verdicts = vec![OpaquePruneVerdict::Unknown; n_rows];
        invoke_eval_against_stats(
            &cb,
            self.op_name(),
            &children,
            &stats,
            n_rows,
            inverted,
            &mut verdicts,
        );
        // arrow_ffi drops here; release is idempotent.

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
        // Keep the op live across the indirect rewrite so the default engine's
        // `evaluate_predicate` can drive `eval_pred` per metadata-batch row.
        // The Not wrap when inverted is pushed back down by the batch
        // evaluator so the engine sees a single (un-doubled) inversion.
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
    fn batched_arrow_stats_provider_reads_string_min_max() {
        let batch = make_stats_batch("apple", "zebra", 0, 100);
        let provider = BatchedArrowStatsProvider::new(&batch);
        let min = provider.min(0, "name", &DataType::STRING).unwrap();
        let max = provider.max(0, "name", &DataType::STRING).unwrap();
        match (min, max) {
            (Scalar::String(min_s), Scalar::String(max_s)) => {
                assert_eq!(min_s, "apple");
                assert_eq!(max_s, "zebra");
            }
            other => panic!("expected string scalars, got {other:?}"),
        }
        assert_eq!(provider.null_count(0, "name"), Some(0));
        assert_eq!(provider.row_count(0), Some(100));
    }

    // === Arrow C Data Interface passthrough ==================================

    /// Verify `batch_stats_as_arrow` hands back a valid `ArrowFFIData` with
    /// the right row count.
    #[test]
    fn batch_stats_as_arrow_exports_a_valid_ffi_struct() {
        thread_local! {
            static SAW_NON_NULL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
            static SAW_ROW_COUNT: std::cell::Cell<i64> = const { std::cell::Cell::new(-1) };
        }

        extern "C" fn capture_arrow_eval(
            _state: *mut std::ffi::c_void,
            _op_name: crate::KernelStringSlice,
            _children: *const ChildAccessor<'_>,
            stats: *const BatchStatsAccessor<'_>,
            _n_rows: usize,
            _inverted: bool,
            _verdicts: *mut crate::expressions::pruning::OpaquePruneVerdict,
        ) {
            let p = unsafe { crate::expressions::pruning::batch_stats_as_arrow(stats) };
            if p.is_null() {
                return;
            }
            SAW_NON_NULL.with(|c| c.set(true));
            // Read-only peek; full import would consume the array.
            let ffi = p as *const crate::engine_data::ArrowFFIData;
            let len = unsafe { (*ffi).array.len() } as i64;
            SAW_ROW_COUNT.with(|c| c.set(len));
        }

        let cb = Arc::new(OpaquePruningCallbacks {
            engine_state: std::ptr::null_mut(),
            eval_against_stats: capture_arrow_eval,
            eval_on_partition_values: no_op_partition,
            eval_on_row_group_stats: no_op_row_group,
            free_state: no_op_free,
        });
        let op = NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", cb);
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();
        let _ = ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, false).unwrap();

        assert!(SAW_NON_NULL.with(std::cell::Cell::get));
        assert_eq!(SAW_ROW_COUNT.with(std::cell::Cell::get), 3);
    }

    #[test]
    fn eval_pred_on_empty_batch_does_not_panic() {
        use delta_kernel::arrow::array::Int64Array;

        thread_local! {
            static SAW_CALLBACK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        }

        extern "C" fn note_eval(
            _state: *mut std::ffi::c_void,
            _op_name: crate::KernelStringSlice,
            _children: *const ChildAccessor<'_>,
            _stats: *const BatchStatsAccessor<'_>,
            _n_rows: usize,
            _inverted: bool,
            _verdicts: *mut crate::expressions::pruning::OpaquePruneVerdict,
        ) {
            SAW_CALLBACK.with(|c| c.set(true));
        }

        // Empty 0-row stats_parsed struct with a single nullCount.size column.
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
            eval_on_partition_values: no_op_partition,
            eval_on_row_group_stats: no_op_row_group,
            free_state: no_op_free,
        });
        let op = NamedOpaquePredicateOp::with_callbacks("FOO", cb);
        let result = ArrowOpaquePredicateOp::eval_pred(&op, &[], &batch, false).unwrap();
        assert_eq!(result.len(), 0);
        assert!(SAW_CALLBACK.with(std::cell::Cell::get));
    }

    #[test]
    fn batch_stats_as_arrow_returns_null_on_null_accessor() {
        let p = unsafe { crate::expressions::pruning::batch_stats_as_arrow(std::ptr::null()) };
        assert!(p.is_null());
    }

    #[test]
    fn batch_stats_as_arrow_returns_null_for_non_arrow_provider() {
        struct DummyProvider;
        impl BatchStatsProvider for DummyProvider {
            fn min(&self, _row: usize, _col: &str, _dtype: &DataType) -> Option<Scalar> {
                None
            }
            fn max(&self, _row: usize, _col: &str, _dtype: &DataType) -> Option<Scalar> {
                None
            }
            fn null_count(&self, _row: usize, _col: &str) -> Option<i64> {
                None
            }
            fn row_count(&self, _row: usize) -> Option<i64> {
                None
            }
        }
        let p = DummyProvider;
        let acc = BatchStatsAccessor::new(&p);
        let ptr = unsafe { crate::expressions::pruning::batch_stats_as_arrow(&acc as *const _) };
        assert!(ptr.is_null());
    }

    // === End-to-end eval_pred =================================================

    // A toy STARTS_WITH evaluator wired as a batched engine callback. Loops
    // over `n_rows` and writes the per-row verdict via the
    // batch_stats_min/max_string accessors.
    extern "C" fn starts_with_eval_batch(
        _state: *mut std::ffi::c_void,
        op_name: crate::KernelStringSlice,
        children: *const ChildAccessor<'_>,
        stats: *const BatchStatsAccessor<'_>,
        n_rows: usize,
        inverted: bool,
        verdicts: *mut crate::expressions::pruning::OpaquePruneVerdict,
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
                crate::expressions::pruning::OpaquePruneVerdict::Skip
            } else {
                crate::expressions::pruning::OpaquePruneVerdict::Keep
            };
        }
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

    extern "C" fn no_op_row_group(
        _state: *mut std::ffi::c_void,
        _op_name: crate::KernelStringSlice,
        _children: *const ChildAccessor<'_>,
        _stats: *const crate::expressions::pruning::StatsAccessor<'_>,
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
            eval_against_stats: starts_with_eval_batch,
            eval_on_partition_values: no_op_partition,
            eval_on_row_group_stats: no_op_row_group,
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
        let op = NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", callbacks());
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
        let op = NamedOpaquePredicateOp::new("STARTS_WITH");
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

    // Callback that records whether kernel passed `inverted=true` and writes
    // `Skip` to every row so we can verify both the inversion flag and the
    // verdict propagation in one shot.
    extern "C" fn inverted_recording_eval(
        _state: *mut std::ffi::c_void,
        _op_name: crate::KernelStringSlice,
        _children: *const ChildAccessor<'_>,
        _stats: *const BatchStatsAccessor<'_>,
        n_rows: usize,
        inverted: bool,
        verdicts: *mut crate::expressions::pruning::OpaquePruneVerdict,
    ) {
        SAW_INVERTED.with(|c| c.set(c.get() || inverted));
        let verdicts = unsafe { std::slice::from_raw_parts_mut(verdicts, n_rows) };
        for slot in verdicts.iter_mut() {
            *slot = crate::expressions::pruning::OpaquePruneVerdict::Skip;
        }
    }

    thread_local! {
        static SAW_INVERTED: Cell<bool> = const { Cell::new(false) };
    }

    fn inverted_recording_callbacks() -> Arc<OpaquePruningCallbacks> {
        Arc::new(OpaquePruningCallbacks {
            engine_state: std::ptr::null_mut(),
            eval_against_stats: inverted_recording_eval,
            eval_on_partition_values: no_op_partition,
            eval_on_row_group_stats: no_op_row_group,
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
        let op =
            NamedOpaquePredicateOp::with_callbacks("STARTS_WITH", inverted_recording_callbacks());
        let args = vec![column_expr!("name"), Expression::literal("foo")];
        let batch = three_file_batch();

        ArrowOpaquePredicateOp::eval_pred(&op, &args, &batch, false).unwrap();
        assert!(
            !SAW_INVERTED.with(Cell::get),
            "engine callback should not have observed inverted=true"
        );
    }
}
