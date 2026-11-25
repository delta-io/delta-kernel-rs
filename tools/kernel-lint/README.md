# Kernel lint rules

This is an executable, repository-specific Rust syntax linter using **ast-grep
0.37.0** and a Python **3.8+** runner. It reports structural candidates from the
Kernel conventions; it does **not** resolve Rust types, trait methods, control
flow, ownership or runtime contracts. Rust compilation, Clippy, tests and code
review remain necessary when applying a suggestion.

The rules cover expression/schema shorthand, test helpers, iterator combinators,
selected API conventions and narrow FFI patterns. [coverage.json](coverage.json)
maps every audited `DOC01`–`DOC20` and `USER01`–`USER04` convention to its actual
implementation or an explicit limitation/deferred semantic check. A matched
pattern is not proof that its replacement is correct.

## Clippy review checks

`cargo clippy-review` runs the workspace's existing strict Clippy check plus
`redundant_else`, `filter_map_next` and `flat_map_option`. CI uses this alias.
These checks encourage linear control flow and direct iterator combinators.

`cargo clippy-review-audit` also reports `manual_let_else`,
`redundant_closure_for_method_calls`, `map_unwrap_or`, `unnecessary_wraps`,
`or_fun_call`, `redundant_clone` and `needless_collect` in Kernel. This advisory
command exits successfully for warnings; inspect its diagnostics before changing
code. Some findings retain useful type inference, public API contracts or eager
evaluation. The Clippy configuration preserves exported APIs and allows let-else
suggestions for all matched types. Only the small enforced set is added to CI;
review the broader findings individually before promoting another check.

## Setup

Use an installed `ast-grep` 0.37.0, or install the pinned package explicitly:

```sh
npm install --prefix /tmp/kernel-lint-tools --no-save @ast-grep/cli@0.37.0
export AST_GREP=/tmp/kernel-lint-tools/node_modules/.bin/ast-grep
```

`AST_GREP` is an optional executable-path override, not a shell command. The
runner also discovers an installed binary and verifies the pinned version. It
does not download tools at runtime. Run the commands below from the repository
root; `--root /path/to/checkout` can precede the subcommand to use another checkout.

## Run and inspect

```sh
python3 tools/kernel-lint/run.py test
python3 -m unittest discover -s tools/kernel-lint -p 'test_*.py'
python3 tools/kernel-lint/run.py check
python3 tools/kernel-lint/run.py scan kernel/src kernel/tests ffi/src
python3 tools/kernel-lint/run.py scan kernel/src --rule 'kernel-expression-'
python3 tools/kernel-lint/run.py scan kernel/src kernel/tests ffi/src --format json --output /tmp/kernel-lint.json
python3 tools/kernel-lint/run.py scan kernel/src --format github
```

`scan` reports all findings, including accepted baseline entries. `check` compares
the current findings with the accepted baseline and fails on new unsuppressed
findings. Both accept paths, `--rule REGEX`, `--format text|json|github` and
`--output PATH`. JSON output supplies fingerprints and replacement previews for
rules that define them. Some warnings deliberately have no replacement.

The baseline is a ratchet over existing syntax findings, not an approval of every
legacy pattern. It is recorded after the reviewed pilot fixes. Constructor tests,
legacy FFI signatures and other existing exceptions can remain accepted while new
occurrences are flagged. Refresh accepted findings only after reviewing them:

```sh
python3 tools/kernel-lint/run.py baseline
```

CI installs the pinned npm package, runs the rule fixtures and runner unit tests,
then runs `check`. A new finding needs a code change, an explicit local exception,
or an intentionally reviewed baseline refresh. Existing warnings are not silently
rewritten or promoted into ABI migrations.

## Reviewed fixes

All current rule metadata has `safeFix: false`. Calling `fix` without a reviewed
manifest therefore makes no changes. Replacements need review for imports, type
inference, shadowing, literal/path semantics, evaluation order and errors.

For an exact replacement preview that has been reviewed, create a manifest:

```json
{
  "edits": [
    {
      "fingerprint": "<fingerprint from the JSON scan>",
      "file": "kernel/src/example.rs",
      "line": 123,
      "replacement": "<exact replacement from that finding>"
    }
  ]
}
```

```sh
python3 tools/kernel-lint/run.py fix --reviewed /tmp/reviewed-kernel-lint.json
```

`line` is one-based; `file` and `line` may be omitted only when the fingerprint
identifies one match. The replacement must equal the current scan preview.
Stale, ambiguous, overlapping or generated-source edits fail validation with exit
code 2 and no partial writes. There is no mass-review override. Format, compile,
run the relevant tests and run Clippy after applying fixes. Findings without a
preview require a separately reviewed source edit.

## Local exceptions

Put a reasoned suppression immediately before the diagnostic's starting line:

```rust
// kernel-lint: allow kernel-expression-column-macro -- exercises the constructor
let expression = Expression::column(["column"]);
```

Use the exact rule ID and a useful reason. Do not replace a type-sensitive test or
an ABI contract merely to remove a warning. The runner validates suppression
syntax; the reason still requires review.
Replace rejected native `ast-grep-ignore` directives with a reasoned `kernel-lint: allow` above.

## Important boundaries

- Keep `assert!(result.is_err())` when failure alone is the contract. Use
  `assert_result_error_with_message` when the test already checks a substring;
  retain exact-message, variant, multi-message and useful contextual checks.
- Use `assert_batches_sorted_eq!` only for existing unordered pretty-table
  comparisons. Sort expected lines separately: the macro sorts actual lines.
  Preserve typed schema/null/value/order and batch-boundary assertions.
- `get_column!` shortens test-only Arrow lookup/downcast chains; it is a panicking
  test helper, not a fallible production accessor.
- `column_name!("a.b")` and `col!("a.b")` split a path. A constructor containing
  one literal segment named `a.b` is not equivalent. `expr!` returns Expression;
  ordinary Rust `==` compares AST values rather than constructing a Predicate.
- Preserve checked/unchecked schemas, metadata and exact Scalar/DataType values.
- `map_ok`, `filter_ok`, `filter_map_ok` and `flatten_ok` preserve error channels
  in different shapes. `filter_map(Result::ok)` discards errors and needs an
  explicit policy, not an automatic substitution.
- Keep eager I/O, first-error behavior and observable partial state. `try_collect`
  can need type annotations or explicit `Itertools::try_collect` qualification.

Rule definitions and per-rule limitations live in `rules/*/*.yml` and
`rules/*/metadata.json`. Positive/negative fixtures and replacement snapshots live
under `tests/`. Adding a rule requires those tests plus an honest coverage entry;
do not mark a semantic convention covered merely because a related AST pattern
can be found.
