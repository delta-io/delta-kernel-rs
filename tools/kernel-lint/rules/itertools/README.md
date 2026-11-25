# Iterator rules

These rules run on Rust syntax with ast-grep 0.37.0. They do not perform Rust
type resolution. `metadata.json` therefore marks all automatic fixes disabled;
some rules still provide replacement previews for a reviewer to apply and compile.
This distinction matters for methods shared with `Option`, arbitrary custom
methods, collection/error inference, imports, closure captures, and drop timing.

The pack implements:

- `try-collect`: explicitly spelled fallible Vec collection.
- `collect-vec`: a necessary Vec collection when `Itertools` is already imported
  at file scope; elimination of unnecessary collections takes precedence.
- `map-ok`: nested mapping on a parameter explicitly declared as
  `impl KernelResultIterator<T>`.
- `iterator-join`: an immediately joined temporary Vec.
- `borrowed-join`: avoid cloning and collecting schema field names just to join.
- `preserve-result-errors`: explicit `Result::ok` / `Result::into_iter` pipelines
  that discard errors. A generic `.flatten()` is deliberately not diagnosed.
- `process-results-unzip`: a directly collected fallible Vec immediately unzipped.
- `exact-split-tuple`: an exact-two slice match over a collected string-like split.
- `filter-map-ok`: an explicit successful-value mapping that forwards every Err.

Rules without a replacement require a semantic decision. In particular, replacing
intentional best-effort error filtering changes behavior; no automatic rewrite is
offered. `collect_tuple` must preserve rejection of extra items, while a
`split_once` rewrite would not do so. It also consumes fewer excess elements, so
its exact-arity rule is suitable only after checking input purity.

Positive and negative fixtures live under `tools/kernel-lint/tests/itertools`.
They cover existing idioms, Option pipelines, retained collections, extra effects,
mapping callbacks, unrelated iterator parameters, and exact-arity rejection.
Snapshots record replacement previews where provided; no Rust source is changed
by running the fixture tests or the scan.
