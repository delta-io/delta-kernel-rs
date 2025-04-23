use crate::arrow::array::{ArrayRef, BooleanArray};
use crate::arrow::compute::kernels::boolean::not;
use crate::arrow::record_batch::RecordBatch;
use crate::engine::arrow_expression::evaluate_expression;
use crate::expressions::{
    ArrayData as KernelArrayData, BinaryPredicateOp, Expression, JunctionPredicateOp,
    OpaqueExpression, OpaquePredicate, OpaquePredicateOp, Predicate, Scalar,
};
use crate::kernel_predicates::{DataSkippingPredicateEvaluator, KernelPredicateEvaluator as _};
use crate::{DeltaResult, Error};
use itertools::Itertools;

#[derive(Clone, Debug, PartialEq)]
pub enum ArrowOpaquePredicateOp {
    InList(KernelArrayData),
    NotInList(KernelArrayData),
}
impl ArrowOpaquePredicateOp {
    fn inlist_as_data_skipping_predicate<Output, ColumnStat>(
        &self,
        predicate_evaluator: &dyn DataSkippingPredicateEvaluator<
            Output = Output,
            ColumnStat = ColumnStat,
        >,
        exprs: &[Expression],
        values: &KernelArrayData,
        inverted: bool,
    ) -> Option<Output> {
        #[allow(deprecated)]
        let values = values.array_elements();
        let [ref arg] = exprs[..] else {
            return None; // wrong number of args
        };

        // We're not willing to search an unbounded list size
        if values.len() > 10 {
            return None;
        }

        // x IN(a, b, c) is just a shorthand for OR(x = a, x = b, x = c), so evaluate it as such.
        let mut values = values.iter().map(|value| {
            predicate_evaluator.eval_pred_binary(
                BinaryPredicateOp::Equal,
                arg,
                &Expression::from(value.clone()),
                inverted,
            )
        });
        predicate_evaluator.finish_eval_pred_junction(
            JunctionPredicateOp::Or,
            &mut values,
            inverted,
        )
    }
}

impl OpaquePredicateOp for ArrowOpaquePredicateOp {
    fn eval_pred_scalar(
        &self,
        values: &[Option<Scalar>],
        inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        use ArrowOpaquePredicateOp::*;
        match self {
            InList(ad) => eval_scalar_in_list(values, ad, inverted),
            NotInList(ad) => eval_scalar_in_list(values, ad, !inverted),
        }
    }

    fn as_data_skipping_predicate(
        &self,
        predicate_evaluator: &dyn DataSkippingPredicateEvaluator<
            Output = Predicate,
            ColumnStat = Expression,
        >,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate> {
        use ArrowOpaquePredicateOp::*;
        match self {
            InList(ad) => {
                self.inlist_as_data_skipping_predicate(predicate_evaluator, exprs, ad, inverted)
            }
            NotInList(ad) => {
                self.inlist_as_data_skipping_predicate(predicate_evaluator, exprs, ad, !inverted)
            }
        }
    }

    fn eval_as_data_skipping_predicate(
        &self,
        predicate_evaluator: &dyn DataSkippingPredicateEvaluator<
            Output = bool,
            ColumnStat = Scalar,
        >,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        use ArrowOpaquePredicateOp::*;
        match self {
            InList(ad) => {
                self.inlist_as_data_skipping_predicate(predicate_evaluator, exprs, ad, inverted)
            }
            NotInList(ad) => {
                self.inlist_as_data_skipping_predicate(predicate_evaluator, exprs, ad, !inverted)
            }
        }
    }
}
pub(crate) fn eval_expr_opaque(
    opaque: &OpaqueExpression,
    _batch: &RecordBatch,
) -> DeltaResult<ArrayRef> {
    // We don't currently define any opaque expressions
    Err(Error::generic(format!(
        "Unsupported opaque expression {:?}",
        opaque.op
    )))
}

pub(crate) fn eval_pred_opaque(
    opaque: &OpaquePredicate,
    inverted: bool,
    batch: &RecordBatch,
) -> DeltaResult<BooleanArray> {
    // Only evaluate the children if we actually know how to evaluate the requested operation.
    let op = opaque
        .op
        .any_ref()
        .downcast_ref::<ArrowOpaquePredicateOp>()
        .ok_or_else(|| Error::generic(format!("Unsupported opaque predicate: {:?}", opaque.op)))?;

    let arrays = opaque
        .exprs
        .iter()
        .map(|expr| evaluate_expression(expr, batch, None))
        .try_collect()?;

    use ArrowOpaquePredicateOp::*;
    let result = match (op, inverted) {
        (InList(ad), false) | (NotInList(ad), true) => eval_in_list(arrays, ad)?,
        (InList(ad), true) | (NotInList(ad), false) => not(&eval_in_list(arrays, ad)?)?,
    };
    Ok(result)
}

fn eval_scalar_in_list(
    args: &[Option<Scalar>],
    _inlist: &KernelArrayData,
    _inverted: bool,
) -> DeltaResult<Option<bool>> {
    let [ref _arg] = args[..] else {
        return Err(Error::generic(format!(
            "Invalid inlist arg count: expected 1, got {}",
            args.len()
        )));
    };
    todo!() // Hook into the existing in-list evaluation code
}

fn eval_in_list(args: Vec<ArrayRef>, _inlist: &KernelArrayData) -> DeltaResult<BooleanArray> {
    let [ref _arg] = args[..] else {
        return Err(Error::generic(format!(
            "Invalid inlist arg count: expected 1, got {}",
            args.len()
        )));
    };
    todo!() // Hook into the existing in-list evaluation code
}
