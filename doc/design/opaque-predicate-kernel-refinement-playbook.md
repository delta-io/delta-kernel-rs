# Playbook: kernel-side per-file refinement for opaque predicates

**Status:** Deferred follow-up
**Related design:** [`opaque-predicate-data-skipping.md`](opaque-predicate-data-skipping.md)
**Why deferred:** The PR that introduced `OpaquePruningCallbacks` shipped an
arrow-adapter approach instead (callback invoked per row of the metadata
batch via the default engine's `evaluate_predicate` path). This document
preserves the alternative for anyone who wants the kernel restructure
later, e.g. to support engines that don't go through the default engine.

## What this implements

The arrow-adapter approach in the current PR routes file-pruning callbacks
through the default engine's `evaluate_predicate`. That's fine for the
default engine, but two scenarios may want a different path:

1. **Non-default engines.** A pure-Rust connector with its own
   `EvaluationHandler` doesn't go through the default engine's arrow
   evaluator. For those engines, the opaque op's `as_data_skipping_predicate`
   returning a Some(arrow-opaque-pred) doesn't help -- their evaluator can't
   downcast to `ArrowOpaquePredicateOpAdaptor`.
2. **Engines that want kernel-driven pruning loop semantics.** Some
   connectors prefer to invoke pruning inline (per-file, per-row-group)
   rather than batch-driven. A kernel-side refinement loop gives them that.

For both, the kernel needs to evaluate the opaque op per file using direct
stats reads from the metadata batch.

## Implementation plan

### Step 1: Detect predicates that need refinement

Add a recursive helper that returns `true` if the predicate contains any
[`Predicate::Opaque`]. Store on `DataSkippingFilter` so `apply()` knows
whether to run the refinement pass.

```rust
fn predicate_has_opaque(pred: &Predicate) -> bool {
    match pred {
        Predicate::Opaque(_) => true,
        Predicate::Junction(j) => j.preds.iter().any(predicate_has_opaque),
        Predicate::Not(p) => predicate_has_opaque(p),
        Predicate::BooleanExpression(expr) => expression_has_opaque(expr),
        Predicate::Unary(u) => expression_has_opaque(&u.expr),
        Predicate::Binary(b) => {
            expression_has_opaque(&b.left) || expression_has_opaque(&b.right)
        }
        _ => false,
    }
}

fn expression_has_opaque(expr: &Expression) -> bool {
    match expr {
        Expression::Opaque(_) => true,
        Expression::Predicate(p) => predicate_has_opaque(p),
        // ... recurse other variants
        _ => false,
    }
}
```

### Step 2: Collect columns referenced by opaque ops

Walk the predicate, gather column names that appear inside opaque ops.
These are the columns the refinement pass needs stats for.

```rust
fn collect_opaque_columns(pred: &Predicate, out: &mut HashSet<String>) {
    match pred {
        Predicate::Opaque(op) => {
            for expr in &op.exprs {
                collect_columns_from_expr(expr, out);
            }
        }
        Predicate::Junction(j) => {
            for p in &j.preds {
                collect_opaque_columns(p, out);
            }
        }
        // ...
    }
}
```

These column names are *physical* (column mapping has already been applied
by the time `DataSkippingFilter::new` is called -- see
`scan/mod.rs::PhysicalPredicate::resolve`).

### Step 3: Implement the row visitor

Build a `RowVisitor` that declares interest in `stats_parsed.minValues.X`,
`stats_parsed.maxValues.X`, `stats_parsed.nullCount.X` for each referenced
column X, plus `stats_parsed.numRecords` for row count.

The visitor's `selected_column_names_and_types()` returns a static slice,
but the columns are dynamic (depend on the predicate). Two options:

(a) Build the static slice eagerly at `DataSkippingFilter::new` time and
    store it. Less elegant but practical.

(b) Use one of kernel's lower-level row-iteration helpers. Search
    `kernel/src/row_visitor` or `kernel/src/engine_data` for the right
    abstraction.

```rust
struct OpaqueRefinementVisitor {
    // Static columns + types declared up-front.
    columns: Vec<ColumnName>,
    types: Vec<DataType>,
    // Per-file accumulator. Length = batch's row count.
    stats: Vec<PerFileStats>,
}

struct PerFileStats {
    min: HashMap<String, Scalar>,
    max: HashMap<String, Scalar>,
    null_count: HashMap<String, i64>,
    row_count: Option<i64>,
}

impl RowVisitor for OpaqueRefinementVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // ... return self.columns + self.types (need 'static or unsafe extension)
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for row in 0..row_count {
            let mut stats = PerFileStats::default();
            // For each (column, type) declared, read the value at `row`
            // into stats.min/max/null_count.
            // ...
            self.stats.push(stats);
        }
        Ok(())
    }
}
```

The tricky part is `selected_column_names_and_types` requires `&'static`.
Workarounds:

- Leak via `Box::leak` once at construction (acceptable since
  `DataSkippingFilter` lives for a query).
- Or use `OnceLock` if the columns are known at type-construction time
  (they aren't here).

### Step 4: Implement the per-file evaluator

Build a `DataSkippingPredicateEvaluator<Output = bool, ColumnStat = Scalar>`
that reads from a `&PerFileStats`. This is the same shape as
`RowGroupFilter` in `kernel/src/engine/parquet_row_group_skipping.rs:132`;
copy that pattern.

```rust
struct PerFileStatsEvaluator<'a> {
    stats: &'a PerFileStats,
}

impl DataSkippingPredicateEvaluator for PerFileStatsEvaluator<'_> {
    type Output = bool;
    type ColumnStat = Scalar;

    fn get_min_stat(&self, col: &ColumnName, dtype: &DataType) -> Option<Scalar> {
        let name = col.path().first()?.to_string();
        let scalar = self.stats.min.get(&name)?;
        // Type-check: returned scalar must match dtype.
        (scalar.data_type() == *dtype).then(|| scalar.clone())
    }

    // ... similar for max, nullcount, rowcount

    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted)
    }

    // ... use Defaults for other methods

    fn eval_pred_opaque(
        &self,
        op: &OpaquePredicateOpRef,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        // This dispatches to NamedOpaquePredicateOp::eval_as_data_skipping_predicate
        // which invokes the engine callback (already wired in this PR).
        op.eval_as_data_skipping_predicate(self, exprs, inverted)
    }
}
```

### Step 5: Wire into `DataSkippingFilter::apply`

```rust
pub(crate) fn apply(&self, batch: &dyn EngineData) -> DeltaResult<Vec<bool>> {
    // ... existing batch eval produces `selection_vector` ...

    let mut selection_vector = self.run_batch_eval(batch)?;

    if let Some(refinement) = &self.opaque_refinement {
        let mut visitor = OpaqueRefinementVisitor::new(&refinement.columns);
        visitor.visit_rows_of(stats_batch.as_ref())?;
        let per_file_stats = visitor.stats;

        for (idx, keep) in selection_vector.iter_mut().enumerate() {
            if !*keep { continue; }  // batch already said skip
            let stats = &per_file_stats[idx];
            let evaluator = PerFileStatsEvaluator { stats };
            if let Some(refined) = evaluator.eval(&refinement.original_predicate) {
                if !refined {
                    *keep = false;
                }
            }
        }
    }

    Ok(selection_vector)
}
```

Refinement is **conservative**: it can only flip KEEP -> SKIP. A file the
batch said SKIP stays skipped. A file the batch said KEEP can be skipped
if the per-file refinement (using direct stats + callbacks) says SKIP.

### Step 6: Tests

- Unit tests on `predicate_has_opaque`, `collect_opaque_columns`.
- Unit tests on `PerFileStatsEvaluator` with hand-built `PerFileStats`.
- Integration test: real Delta table with multiple files; predicate
  combining a native op and an opaque op; verify the opaque op prunes
  files that batch eval alone wouldn't.

## Estimated effort

- Step 1-2: ~60 lines
- Step 3: ~150 lines (the visitor is the most code; type-dispatched getters
  for string, long, double, bool min/max + nullcount)
- Step 4: ~100 lines
- Step 5: ~50 lines integration into `DataSkippingFilter`
- Step 6: ~200 lines of tests

Total: ~550 lines plus careful review of the visitor's column-path
handling (nested struct access for `stats_parsed.minValues.X`).

## Open questions

1. **Visitor's `selected_column_names_and_types` `'static` requirement.**
   Cleanest workaround?
2. **Partition columns in the refinement.** Partition pruning has its own
   path today (the engine callback fires via `eval_pred_scalar`). Does the
   per-file refinement also need partition column access for ops that mix
   partition and data columns? If so, the visitor needs to access
   `partitionValues_parsed.X` too.
3. **Column type discovery.** The visitor needs to know each column's
   DataType to call the right getter. Where does it learn the types?
   Probably from the unified schema built in
   `DataSkippingFilter::build_unified_schema_and_expr`.

## When this approach wins over the arrow adapter

- Engines that don't use the default engine's `evaluate_predicate` path.
- Workloads where per-file callback granularity matters (e.g., the engine
  wants to short-circuit aggressively per file rather than processing the
  whole batch).
- Future work to make pruning callbacks invocable during streaming /
  partial-batch evaluation.

## When the arrow adapter (current PR) is sufficient

- Engines using the default engine. Their batch evaluator handles the
  opaque op natively via the adapter; the callback fires per row.
- Per-row JNI cost is acceptable (typically yes for file pruning with
  thousands to hundreds of thousands of files).

If both paths are wanted simultaneously, they can coexist: the arrow
adapter handles default-engine cases; the kernel refinement handles
custom-engine cases. The trait methods on `NamedOpaquePredicateOp`
already dispatch correctly for both.
