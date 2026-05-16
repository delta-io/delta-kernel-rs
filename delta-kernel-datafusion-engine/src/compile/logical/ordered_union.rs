//! Order-preserving Union lowering.

use datafusion_common::error::DataFusionError;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, LogicalPlanBuilder};

use super::scan::{append_constant_i64_column, drop_named_column, plan_column_by_name};

/// Build an ordered union of [`LogicalPlan`]s using stock DataFusion operators: project a
/// literal i64 ordinal onto each child, [`LogicalPlanBuilder::union`] the lot, sort by ordinal,
/// then project to drop it. Same recipe as the physical-layer ordered-union helper but emitted
/// at the logical layer so the optimizer sees it.
pub(super) fn compile_ordered_union(
    children: Vec<LogicalPlan>,
) -> Result<LogicalPlan, DataFusionError> {
    const ORDINAL_COL: &str = "__dk_ord";
    let mut tagged: Vec<LogicalPlan> = Vec::with_capacity(children.len());
    for (idx, child) in children.into_iter().enumerate() {
        tagged.push(append_constant_i64_column(child, ORDINAL_COL, idx as i64)?);
    }
    let mut iter = tagged.into_iter();
    let first = iter
        .next()
        .expect("compile_ordered_union: at least one child");
    let unioned = iter.try_fold(first, |acc, right| {
        LogicalPlanBuilder::from(acc).union(right)?.build()
    })?;
    let ordinal_col = plan_column_by_name(&unioned, ORDINAL_COL)?;
    let sorted = LogicalPlanBuilder::from(unioned)
        .sort(vec![Expr::Column(ordinal_col).sort(true, true)])?
        .build()?;
    drop_named_column(sorted, ORDINAL_COL)
}
