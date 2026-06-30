# Derive macros guidelines

## What this crate is

`delta_kernel_derive` is the proc-macro crate backing kernel ergonomics. It provides the
`ToSchema` and `IntoEngineData` derives, the `#[internal_api]` attribute (feature-gates the
visibility of unstable APIs), and a column-name parsing macro. It is a compile-time dependency
of kernel; it has no runtime surface of its own.

## Invariants to uphold

- **`ToSchema` encodes the Delta naming convention.** It maps Rust `snake_case` fields to the
  protocol's `camelCase` schema names. Changing that mapping changes the on-disk/action schema
  for every deriving type -- treat it as protocol-affecting, not cosmetic.
- **Generated code must obey the no-panic-in-production rule.** A macro that can emit
  `unwrap`/`panic!` into kernel violates the root guideline at every expansion site; surface
  errors through the generated code's `Result` paths instead.
- **Attributes must validate their target.** `#[internal_api]` is meaningful only on items whose
  visibility is below `pub`; the container-null attribute is meaningful only on map-shaped
  fields. Emit a clear `compile_error!` on misuse rather than silently miscompiling.
- **A macro is the wrong tool for a one-off.** Proc macros are hard to read and debug; add or
  extend one only when several call sites share the generated shape. Prefer plain code otherwise.
