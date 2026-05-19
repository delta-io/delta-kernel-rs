# Opaque-predicate data skipping in delta-kernel-rs

**Status:** Proposed
**Author:** chiinlquah
**Date:** 2026-05-19
**PR:** chiinlquah/delta-kernel-rs#chiin/ffi-opaque-skipping (stacked on PR #2527)

## TL;DR

`NamedOpaquePredicateOp` -- the FFI shim that carries engine-defined predicate
ops (`STARTS_WITH`, `LIKE`, etc.) through kernel -- currently opts out of every
pruning pass. This document proposes a framework that lets engines participate
in kernel's three pruning passes (partition pruning, stats-based file pruning,
parquet row-group skipping) for arbitrary op names, without kernel adopting
opinions about op semantics.

We considered three approaches and recommend **Option A: per-eval engine
callbacks**. Kernel invokes an engine-supplied callback for every pruning
decision involving an opaque op. The engine writes its own pruning math in
its own language. The tradeoff is JNI/FFM round-trips in the pruning hot loop;
the upside is total semantic ownership and a generic framework that supports
any op the engine can evaluate, including those that have no kernel-native
expression (regex, UDFs, collation-aware comparisons).

## Background

Kernel does not understand engine-defined predicate ops. Predicates that
contain ops like `STARTS_WITH`, `LIKE`, or custom UDFs flow through the
FFI as `Predicate::Opaque(op, exprs)` nodes, where the op carries only an
op-name string and the engine is responsible for evaluating it at row time.

The `OpaquePredicateOp` trait exposes three pruning hooks:

| Method | Purpose | Output |
|---|---|---|
| `eval_pred_scalar` | Partition pruning -- evaluate the op against resolved partition values | `Option<bool>` |
| `eval_as_data_skipping_predicate` | Parquet row-group skipping -- evaluate against direct min/max stats | `Option<bool>` |
| `as_data_skipping_predicate` | Delta log file pruning -- rewrite the op into a kernel-native predicate over stats columns | `Option<Predicate>` |

A pure-Rust engine writes a real `OpaquePredicateOp` for each of its ops and
implements all three methods. An FFI engine (Java, C, ...) cannot do this
directly -- Rust traits can't be implemented across an FFI boundary -- and so
the FFI ships a `NamedOpaquePredicateOp` that carries only a name. Today every
method returns "no support", which means FFI engines get zero kernel-side
pruning for their opaque predicates.

The cost is real. A typical Spark query like
`WHERE name LIKE 'foo%' AND price > 100` against a table with thousands of
files cannot prune any file based on the `LIKE` clause, even though the
clause is trivially translatable to range tests over the `name` column's
min/max stats. We pay the I/O cost of reading every file and rely on
row-level filtering to drop non-matching rows.

## Goals

1. Let FFI engines participate in all three pruning passes for arbitrary op
   names.
2. The framework must be **generic**: adding a new op (e.g. `ENDS_WITH`, a
   custom UDF) requires no kernel-side change.
3. The framework must respect engine semantics. Two engines may define the
   same op name differently; the engine must remain authoritative.
4. Soft-fail conservatively. If an engine declines or fails to evaluate, the
   pruning pass must keep the file (correctness preserved, pruning power
   sacrificed).
5. No regression in row-time filtering -- opaque predicates still cross to
   the engine via `EvaluationHandler` per batch, as today.

## Non-goals

1. Adding new ops to kernel proper. Kernel stays op-agnostic; the Delta
   protocol does not standardise op names.
2. Rewriting kernel's existing pruning machinery (`as_sql_data_skipping_
   predicate`, `parquet_row_group_skipping`, partition pruning). The
   framework slots into the existing trait hooks.
3. Performance parity with kernel-native predicates. The framework will be
   slower per file than native pruning; the goal is "much faster than no
   pruning", not "as fast as native".

## Design options considered

We evaluated three approaches.

### Option A: per-eval engine callback (recommended)

Engine registers callbacks at engine construction time. Kernel invokes the
appropriate callback every time it needs a pruning decision involving an
opaque op -- once per Add action for stats-based file pruning, once per
partition value for partition pruning, once per row group for parquet
skipping.

```c
// FFI: engine registers per-eval callbacks alongside its EvaluationHandler
typedef struct {
    void* engine_state;

    // Called once per Add action during stats-based file pruning.
    // Engine reads min/max/nullcount via the provided accessor and returns
    // tri-state: skip / keep / unknown.
    void (*eval_against_stats)(
        void* state,
        KernelStringSlice op_name,
        ChildAccessor* children,
        StatsAccessor* stats,
        bool inverted,
        OpaquePruneResult* out
    );

    // Called once per partition combination during partition pruning.
    void (*eval_on_partition_values)(
        void* state,
        KernelStringSlice op_name,
        ChildAccessor* children,
        ScalarResolver* resolve,
        bool inverted,
        OpaquePruneResult* out
    );

    // Called once per parquet row group during row-group skipping.
    void (*eval_on_row_group_stats)(
        void* state,
        KernelStringSlice op_name,
        ChildAccessor* children,
        DirectStatsAccessor* stats,
        bool inverted,
        OpaquePruneResult* out
    );

    void (*free_state)(void* state);
} OpaquePruningCallbacks;
```

Engine side (Java sketch):

```java
class JavaOpaquePruner {
    static void evalAgainstStats(long state, String op,
                                 ChildAccessor c, StatsAccessor s,
                                 boolean inverted, OpaquePruneResult out) {
        switch (op) {
            case "STARTS_WITH": {
                String colName = c.column(0);
                String prefix  = c.literalString(1);
                String min = s.minString(colName);
                String max = s.maxString(colName);
                if (min == null || max == null) {
                    out.unknown();
                    return;
                }
                String upper = nextPrefix(prefix);
                boolean keep = max.compareTo(prefix) >= 0
                            && min.compareTo(upper) < 0;
                out.write(inverted ? !keep : keep);
                return;
            }
            case "LIKE": /* ... */ break;
            default: out.unknown();
        }
    }
}
```

**Pros:**

- **Maximally flexible.** Engines implement arbitrary op semantics in their
  own language. No kernel-side opinions about LIKE escape characters, regex
  flavors, ICASE behavior, etc.
- **Engine owns correctness.** If Spark's `LIKE` and Postgres's `LIKE`
  disagree on `\`-escapes, each engine ships its own evaluator.
- **Adding a new op requires zero kernel changes.** Engine just adds a
  `switch` arm.
- **Supports ops kernel could never implement** -- UDFs, locale-sensitive
  comparisons, full-text search semantics, custom hash functions.
- **Engine never needs to know kernel internals** like the
  `stats_parsed.minValues.X` column path conventions. Accessors hide it.
- **Mental model is clean:** "kernel asks the engine; engine answers."

**Cons:**

- **JNI/FFM round-trip in the pruning hot loop.**
  - Stats-based file pruning: one callback per Add action. 100k-file table =
    100k callbacks per query.
  - Parquet row-group skipping: one callback per row group per file. Wide
    table with 10 row groups per file x 100k files = 1M callbacks.
  - Partition pruning is mild (one per partition value set; usually < 1k).
- **New FFI surface is substantial.** Accessor types (`ChildAccessor`,
  `StatsAccessor`, `ScalarResolver`, `DirectStatsAccessor`,
  `OpaquePruneResult`) need careful design and type-system round-tripping
  for Scalars across the boundary.
- **Re-entrancy hazards.** Kernel calls the engine during pruning; if the
  engine in turn calls kernel (e.g., to allocate a result), lifetimes and
  borrow safety must be reasoned through.
- **Lifetime ownership of accessors.** Kernel passes borrowed views valid
  only for the callback duration. Engine must not retain.
- **Profiling is harder.** A pruning slowdown is distributed across the FFI
  boundary; profiling tools see "time spent in callback" rather than
  attributing to specific op semantics.

**Performance estimate:** JNI round-trips are typically 50-500 ns each
depending on the JVM, GC pressure, and how much data crosses. At 250 ns/call
and 100k files, that's 25 ms of pure boundary overhead per query. For
queries that should prune in single-digit ms, this is significant but
generally tolerable -- the win from pruning even 1% of files (avoiding a
single parquet read at ~50 ms) more than pays it back. Row-group skipping
is the worst case at ~1M callbacks; that's ~250 ms per query of pure FFI
overhead, which is a real concern.

### Option B: engine pre-builds a kernel-native skipping companion

Engine builds two predicates at construction time: (1) the opaque op
carrying name + children (for row-time evaluation), and (2) a kernel-native
predicate over stats columns that expresses the skipping math. The
companion is a regular `Predicate` built from kernel primitives the engine
already has.

```c
uintptr_t visit_predicate_opaque_with_skipping(
    KernelExpressionVisitorState* state,
    KernelStringSlice op_name,
    EngineIterator* children,
    uintptr_t file_companion_pred_id,       // kernel-native pred over stats cols
    uintptr_t partition_companion_pred_id,  // optional, for partition cols
    AllocateErrorFn allocate_error
);
```

For `STARTS_WITH(name, "foo")` the engine builds the companion via existing
`visit_predicate_*` builders, referencing `stats_parsed.maxValues.name >=
"foo" AND stats_parsed.minValues.name < "fop"`.

Kernel stashes the companion on `NamedOpaquePredicateOp` and returns it from
`as_data_skipping_predicate`. Downstream pruning evaluates it identically to
any other native predicate -- no FFI crossings during pruning.

**Pros:**

- **Zero JNI in the pruning hot loop.** Companion is native; evaluated by
  Rust against the metadata batch or parquet stats.
- **New FFI surface is small** -- one builder function plus optional
  stats-column helpers.
- **Engine fully owns op semantics** by choosing the companion's math.

**Cons:**

- **Engine has to know kernel's stats schema convention** (`stats_parsed.
  minValues.X` etc.). Coupling between engine and kernel internals.
  Mitigated by stats-column helpers but not eliminated.
- **Doesn't handle Category 3 ops** -- regex, UDFs, anything that needs a
  function call kernel doesn't have (no `REGEX_MATCH` expression node).
  Engine has to pass "no companion" and pruning degrades to no-op.
- **Engine bears the burden of building math correctly.** Companion can
  disagree with row-time evaluator (e.g., one case-sensitive, other not);
  produces wrong results silently.
- **Inversion is tricky.** Engine builds the non-inverted companion; kernel
  wraps in NOT for the inverted case. For some ops (`NOT STARTS_WITH`) the
  precise inverted-form math is fundamentally different.

### Option C: curated FFI-shim library (rejected)

Kernel/FFI ships hardcoded rewriters for a curated list of well-known op
names (`STARTS_WITH`, maybe `LIKE 'prefix%'`). Engines just call
`visit_predicate_opaque("STARTS_WITH", ...)` and get pruning for free.

**Pros:**

- Zero engine work for supported ops.
- Zero FFI overhead.
- Smallest implementation effort.

**Cons:**

- **Doesn't generalize.** Engines that want pruning for any other op
  (`LIKE` with non-Spark escapes, `ENDS_WITH`, custom UDFs) get nothing.
- **Kernel adopts opinions about op semantics** -- `STARTS_WITH` isn't in
  the Delta protocol spec; making kernel/FFI authoritative about its
  meaning is a real coupling.
- **Doesn't compose with multi-engine usage.** Two engines defining
  `STARTS_WITH` slightly differently (e.g., one ICASE) can't coexist.

**Rejection rationale:** This is a special-case rather than a framework. It
solves the STARTS_WITH problem at the cost of every other op the engine
might want. The current PR shipped this as a starting point but the user
explicitly rejected it as not a framework.

## Recommended approach: Option A

We recommend Option A despite the JNI cost.

The decisive factor is **generality**. Option B handles only Category 1 and
Category 2 ops -- those whose skipping math reduces to expressions over
min/max stats. That covers a meaningful chunk of practical predicates
(prefix matches, range comparisons, basic LIKE patterns), but excludes:

- Engine-specific UDFs
- Regex matching (no `REGEX_MATCH` in kernel)
- Locale-sensitive string comparisons
- Functions the engine wants to evolve without coordinating with kernel
- Ops that depend on engine-side configuration (e.g., a collation table
  loaded at engine init)

For these, Option A is the only framework that works. The engine's row-time
evaluator already knows how to evaluate any op; Option A simply gives the
engine the chance to ask "could this file/row-group/partition possibly
contain a matching row?" -- which is a question the engine is uniquely
qualified to answer.

The JNI cost is real but bounded, and we have several mitigations:

1. **Partition pruning** has small fan-out (typically dozens to hundreds of
   partition values). Even at 1 us per callback, partition pruning overhead
   is a millisecond.
2. **File pruning** at 100k callbacks * 250 ns = 25 ms. Significant but a
   single parquet read pruned saves ~50 ms; pruning even 1% of files is a
   net win.
3. **Row-group skipping** is the worst case. We can opt out for the v1
   implementation (return None from the row-group callback unconditionally;
   row-group skipping for opaque ops stays disabled) and re-enable once
   we've measured the overhead and considered batching options.

Option B remains attractive for hot-path ops where the engine knows the
math. A future enhancement could let the engine register **both** a
callback **and** an optional companion -- kernel uses the companion when
present (zero FFI cost) and the callback otherwise. This gives Option B's
performance for ops that can express their math in kernel-native form, and
Option A's generality for everything else. Out of scope for this PR.

## Detailed design (Option A)

### Engine registration

The `OpaquePruningCallbacks` struct is registered at engine construction,
alongside the existing `Engine` callbacks. It can be `NULL` -- engines
that don't want kernel-side pruning for opaque ops simply pass null, and
behaviour matches today's `NamedOpaquePredicateOp` (no pruning).

```c
struct EngineHandle build_engine_with_callbacks(
    /* existing engine config */,
    OpaquePruningCallbacks* opaque_pruning  // nullable
);
```

The callbacks are stored on the `EngineHandle` and threaded through to
`NamedOpaquePredicateOp` instances when they are constructed.

### FFI accessor types

Each callback receives kernel-owned accessor handles that expose the data
the engine needs to make a pruning decision.

#### `ChildAccessor`

Iterates the opaque op's children. Each child is either a column reference
or a scalar literal; the accessor exposes type-tagged getters.

```c
typedef struct ChildAccessor ChildAccessor;

uintptr_t child_accessor_count(const ChildAccessor*);
enum ChildKind child_accessor_kind(const ChildAccessor*, uintptr_t idx);
KernelStringSlice child_accessor_column_name(const ChildAccessor*, uintptr_t idx);
// scalar getters parallel KernelStringSlice / int64 / ...
```

#### `StatsAccessor` (indirect file pruning)

Exposes min/max/nullcount/rowcount for the columns the engine queries.
Kernel knows the stats schema; engine queries by logical column name and
gets back a typed scalar (or null if the stat is missing).

```c
typedef struct StatsAccessor StatsAccessor;

bool stats_accessor_min_string(const StatsAccessor*, KernelStringSlice col,
                                KernelStringSlice* out);
bool stats_accessor_min_long(const StatsAccessor*, KernelStringSlice col,
                              int64_t* out);
// ... per type
bool stats_accessor_nullcount(const StatsAccessor*, KernelStringSlice col,
                               int64_t* out);
int64_t stats_accessor_rowcount(const StatsAccessor*);
```

The accessor is borrowed; engine must not retain past the callback.

#### `ScalarResolver` (partition pruning)

For partition pruning, kernel resolves column refs to partition values
itself; the accessor exposes "give me the partition value for this column
ref" by name.

#### `DirectStatsAccessor` (row-group skipping)

Same shape as `StatsAccessor` but reads from parquet footer stats. Kernel
owns the type-mapping from parquet types to Delta types.

#### `OpaquePruneResult`

Write-side handle for the engine to return its decision.

```c
void prune_result_write_keep(OpaquePruneResult*);   // pred = true
void prune_result_write_skip(OpaquePruneResult*);   // pred = false
void prune_result_write_unknown(OpaquePruneResult*); // null -> keep
```

The tri-state matches kernel's `Option<bool>` semantics. Unknown is
conservatively "keep" but kernel uses it to short-circuit junction
evaluation correctly (`AND` with unknown stays unknown unless the other
side is false; `OR` with unknown stays unknown unless the other side is
true).

### Rust-side wiring

`NamedOpaquePredicateOp` carries an optional reference to the engine's
pruning callbacks:

```rust
pub struct NamedOpaquePredicateOp {
    name: String,
    callbacks: Option<Arc<OpaquePruningCallbacks>>,
}
```

Each trait method dispatches to the corresponding callback:

```rust
impl OpaquePredicateOp for NamedOpaquePredicateOp {
    fn eval_pred_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        _: &DirectPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        let Some(callbacks) = &self.callbacks else {
            return Ok(None); // No engine pruning support; opt out gracefully.
        };
        let resolver = ScalarResolverImpl::new(eval_expr);
        let children = ChildAccessorImpl::new(exprs);
        let mut result = OpaquePruneResultImpl::default();
        unsafe {
            (callbacks.eval_on_partition_values)(
                callbacks.engine_state,
                kernel_string_slice!(self.name),
                &children as *const _ as *mut _,
                &resolver as *const _ as *mut _,
                inverted,
                &mut result as *mut _,
            );
        }
        Ok(result.into_option())
    }

    fn eval_as_data_skipping_predicate(/* ... */) -> Option<bool> { ... }
    fn as_data_skipping_predicate(/* ... */) -> Option<Predicate> {
        // For the indirect (rewrite) path, we have a choice: invoke the
        // callback at rewrite time (once per query, not per file), and the
        // callback returns a Predicate the engine wants kernel to evaluate.
        // OR: skip this path entirely and rely on
        // eval_as_data_skipping_predicate being called per file via the
        // direct path. v1: the latter (simpler, fewer FFI surface
        // requirements). v2: investigate the rewrite approach.
        None
    }
}
```

### Indirect vs direct file pruning

Kernel has two paths for stats-based file pruning:

- **Indirect rewrite** (`as_data_skipping_predicate`): kernel walks the
  predicate at scan-build time, rewrites every node into a stats-column
  predicate, then evaluates the rewritten predicate against a metadata
  batch via `EvaluationHandler`. This is the hot path today.
- **Direct evaluation** (`eval_as_data_skipping_predicate`): kernel
  evaluates the predicate node-by-node against direct stats accessors.
  Today only used for parquet row-group skipping.

In v1 we will:

- Invoke the engine callback for **direct** stats-based file pruning. Kernel
  walks the predicate per file, calling the callback for each opaque node.
- Invoke the engine callback for partition pruning (existing path,
  per-partition-value-set, low fan-out).
- **Not** support row-group skipping for opaque ops in v1 (return None
  unconditionally). Re-enable in v2 after measurement.
- **Not** support indirect rewrite for opaque ops in v1 (return None).
  Today's `as_sql_data_skipping_predicate` will drop opaque branches; the
  AND/OR junction handling already does the right thing.

This means kernel needs to be modified to **also call** the direct path
for opaque ops during file pruning, even though it currently uses the
indirect path. This is a kernel-internal change scoped to data_skipping.rs.

### Failure modes

Each callback can return "unknown" (kernel treats as keep). Each callback
can be null (kernel treats as keep). Engine crashes mid-callback are
undefined behaviour -- engine is responsible for not panicking across the
FFI boundary.

### Memory ownership and re-entrancy

- Accessor handles (`ChildAccessor`, `StatsAccessor`, `ScalarResolver`,
  `DirectStatsAccessor`) are owned by kernel and borrowed by the callback.
  Engine must not retain.
- `OpaquePruneResult` is owned by kernel, mutated by the callback in-place.
- The callback must not call back into kernel during evaluation. (Future
  enhancement: allow read-only kernel calls, e.g., to construct a sub-
  predicate. v1 forbids.)
- `engine_state` lifetime is bounded by `EngineHandle`. Engine drops it via
  `free_state` when kernel drops the engine.

## Implementation plan

All work lands in a single PR.

### FFI types

1. `OpaquePruningCallbacks` struct: three callbacks (stats / partition /
   row-group), engine state, free-state fn.
2. `OpaquePruningContext` Arc-backed shared handle so multiple opaque ops in
   one query share callbacks without duplicating engine state.
3. `create_opaque_pruning_context` / `free_opaque_pruning_context` lifecycle.
4. Accessor types -- each is an opaque kernel-owned handle the callback
   borrows for the duration of the call:
   - `ChildAccessor`: walks the op's children. Methods for `count`, `kind`,
     `column_name`, `literal_string`, `literal_long`, `literal_double`.
   - `StatsAccessor`: indirect (file pruning) stats. Methods for min/max
     per type (string/long/double), nullcount, rowcount.
   - `DirectStatsAccessor`: direct (row-group) stats. Same shape as
     `StatsAccessor` but reads parquet footer stats.
   - `ScalarResolver`: partition pruning. Resolves a column ref to a
     partition scalar value.
   - `OpaquePruneResult`: write-side handle. Tri-state: keep / skip /
     unknown.

### Builder

`visit_predicate_opaque_with_pruning(state, name, children, ctx,
allocate_error)`: same shape as the existing `visit_predicate_opaque` but
takes an `OpaquePruningContext` handle. The op stashes an Arc clone.

### `NamedOpaquePredicateOp` wiring

Add `callbacks: Option<Arc<OpaquePruningContext>>` field. All three trait
methods dispatch to the corresponding callback through the FFI accessors.
If `callbacks` is None (engine didn't register any), kernel falls back to
the current "opt out" behavior.

### Kernel-side `data_skipping.rs` restructuring

This is the largest piece. Today kernel's file-pruning flow compiles the
rewritten predicate via `EvaluationHandler` and evaluates it once against
the whole metadata batch. Opaque ops get rewritten to "no support" (no
Predicate produced), so the AND/OR junction code drops them.

To support per-file callbacks we add a refinement pass:

1. Batch eval runs as today (native-only after opaque drops). Result is a
   per-file selection vector.
2. For each file the batch said KEEP, run a refinement pass:
   - Walk the *original* (un-rewritten) predicate.
   - For native nodes, evaluate via a `DataSkippingPredicateEvaluator` that
     reads stats from the metadata batch row.
   - For opaque nodes, invoke the engine callback via `StatsAccessor`.
   - Combine via AND/OR/NOT semantics.
3. AND the refinement result with the batch selection vector.

Refinement can only flip KEEP -> SKIP, never the reverse. This preserves
correctness: a file the batch could definitively skip stays skipped; a
file the batch couldn't decide may be skipped after consulting the
engine's opaque eval.

For row-group skipping (parquet_row_group_skipping.rs) the path is
simpler: opaque ops already go through `eval_as_data_skipping_predicate`,
which now invokes the callback.

For partition pruning, `eval_pred_scalar` already invokes the trait method
once per partition value set; now it invokes the engine callback through
the `ScalarResolver`.

### Replacing the curated STARTS_WITH dispatch

The existing `skipping` module (Option C from this design) is removed.
Tests that exercised it are rewritten to register a callback that
implements STARTS_WITH on the engine side -- this is exactly the engine
integration pattern this PR enables.

### Tests

- Unit tests for each FFI accessor type (correctness + lifetime).
- `NamedOpaquePredicateOp` dispatch tests with mock callbacks.
- Integration tests:
  - File pruning via callback for STARTS_WITH.
  - Partition pruning via callback.
  - Row-group skipping via callback.
  - Engine returns `unknown` -> file kept.
  - No callbacks registered -> no pruning, all files kept.
- Rust-side test helper that wires up a STARTS_WITH callback so the
  integration tests don't have to handcraft FFI for every test.

### Out of scope (deferred to follow-up PRs)

- Per-op companion (Option B style) as an opt-in optimization. Useful for
  hot ops where the engine wants zero-FFI evaluation; we can layer it on
  later without breaking the callback API.
- Benchmark gates. We document the perf concern in this design but ship
  without per-row-group benchmarks; row-group skipping may need to be
  feature-gated or rate-limited if real workloads show pathological FFI
  cost. That can land as a follow-up tweak.

## Risks and open questions

### Performance: confirm with benchmark before merge

JNI overhead estimates above are back-of-envelope. We need to benchmark
against a realistic table (10k-100k files, mix of opaque and native
predicates) and confirm the overhead is acceptable. If it's not, we may
need to ship v1 with file pruning behind a flag (default off), or
accelerate the FFI accessors with batching (one callback per N files
instead of per file).

### Accessor API stability

The accessor API is new and externally visible. Once shipped, it has to
maintain backwards compatibility. We should design it conservatively (few
methods, no over-engineered abstractions) and gate it behind an
`unstable-opaque-pruning` feature for one release before stabilizing.

### Multi-language engine support

The accessor API uses standard FFI types (slices, raw pointers, function
pointers). Java (FFM), C, C++, and other FFI-capable languages should be
able to use it directly. We will validate with the Java connector before
merge.

### Interaction with column mapping

Engines pass logical column names to the accessors. Kernel translates to
physical names internally. This is the same convention used by
`visit_predicate_opaque`'s children and should reuse the same translation
pass (`ApplyColumnMappings::transform_pred`). v1 will verify this works
end-to-end via a test with column mapping enabled.

### Cancellation and timeouts

Kernel currently doesn't cancel an in-progress scan. If an engine callback
runs slowly, the whole scan blocks. No new risk introduced (the engine's
existing `EvaluationHandler` calls have the same property), but worth
noting.

## Alternatives reconsidered

If at any point during v1 implementation we find the FFI accessor surface
is unmanageably complex, the fallback is to ship a **hybrid v0**:

- Keep the curated STARTS_WITH dispatch from the current PR (Option C) for
  the common case.
- Add `visit_predicate_opaque_with_skipping` (Option B) for engines that
  want their own ops with kernel-native math.
- Defer Option A's per-call callbacks to a follow-up PR.

This is a defensive plan; we shouldn't fall back unless v1 hits a wall.

## Appendix: which ops can a framework actually prune?

Three categories.

**Category 1: cleanly expressible via min/max range overlap.**
`STARTS_WITH(col, prefix)`, `LIKE 'prefix%'`, `BETWEEN`, `=`, `IN`,
`col1 < col2` (multi-column comparisons). The math reduces to range tests
on min/max stats. Both A and B handle these well.

**Category 2: expressible but skipping is weak.**
`ENDS_WITH`, `CONTAINS`, `LIKE '%suffix'`, `LIKE '%infix%'`,
`LENGTH(col) > N`. Min/max stats give little information; the framework
returns "keep most files". A and B both handle these, just with limited
benefit.

**Category 3: not expressible without per-row evaluation.**
Regex, UDFs, locale-sensitive comparisons, full-text search. **Only Option
A can engage at all** -- the engine might still return "unknown" most of
the time, but it has the opportunity to recognize special cases (e.g., a
regex with a literal prefix can be partially evaluated). Option B forces
the engine to opt out entirely.

The decisive case for A is Category 3. Most ops in practice are Category 1
or 2, where B is equally capable. But the framework's value is in handling
the long tail, which B cannot reach.
