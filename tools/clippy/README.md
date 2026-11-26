# Clippy review profiles

## Clippy review checks

`cargo clippy-review` runs the workspace's existing strict Clippy check plus
`manual_let_else`, `redundant_else`, `filter_map_next` and `flat_map_option`.
CI uses this alias.
These checks encourage linear control flow and direct iterator combinators.

`cargo clippy-review-audit` also reports `redundant_closure_for_method_calls`,
`map_unwrap_or`, `unnecessary_wraps`, `or_fun_call`, `redundant_clone`,
`needless_collect`, `implicit_hasher`, `needless_pass_by_value` and `absolute_paths`
in Kernel. This advisory command exits successfully for warnings; inspect its
diagnostics before changing code. Some findings retain useful type inference, public API contracts or eager
evaluation. The Clippy configuration preserves exported APIs and allows let-else
suggestions for all matched types. Only the small enforced set is added to CI;
review the broader findings individually before promoting another check.

Clippy's built-in `absolute_paths` supplies module-import advice. The root
configuration sets `absolute-paths-max-segments = 1`, so the advisory profile also
reports a path such as `crate::KernelError`. Its default level remains allow;
ordinary strict CI does not enable it. The same setting is copied into the
isolated nesting configuration for callers combining audit profiles. Resolve
imports and check name collisions before replacing a qualified path.

## Nesting audit

Run the optional Clippy nesting profile with its isolated configuration:

```sh
CLIPPY_CONF_DIR="$PWD/tools/clippy/nesting" cargo clippy-nesting-audit
```

The configuration sets `excessive-nesting-threshold = 5` and adds advisory
`single_match_else` and `excessive_nesting` checks alongside the strict
`manual_let_else` and `redundant_else` checks. The standard Clippy checks still
apply. Without `CLIPPY_CONF_DIR`, the alias uses the ordinary repository
configuration; Clippy's default excessive nesting threshold is zero, which
disables that lint. This audit does not change
CI's strict profile or apply fixes.

Clippy counts enclosing module, impl and function blocks, so its threshold is
not a function-body depth limit. `single_match_else` can also report readable
matches whose alternatives both do work. Review each diagnostic for a useful
early return, let-else or loop guard while preserving error values, evaluation
order, partial state and work done before a skipped branch.
