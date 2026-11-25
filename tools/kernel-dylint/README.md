# Kernel Dylint rules

These compiler lints resolve the defining crate, types, and functions. They catch aliases of
Kernel APIs and ignore unrelated APIs with the same names. The ast-grep rules in
`../kernel-lint` provide broader structural checks without compiling the code.

| Lint | Prefer |
|---|---|
| `kernel_named_nullability` | `StructField::nullable(...)` / `not_null(...)` for literal booleans |
| `kernel_expression_literal` | `lit(value)` over `Expression::literal(value)` |
| `kernel_null_literal` | `null_lit(data_type)` over `lit(Scalar::Null(data_type))` |
| `kernel_predicate_constant` | `Predicate::TRUE` / `FALSE` over `Predicate::literal(true/false)` |

The lints emit warnings with help text. They do not apply edits or add imports. Runtime boolean
values and macro expansions are excluded. Helper implementations that delegate to the longer
constructors are excluded to avoid recursive advice. Use a narrow `#[allow(kernel_...)]` when a
test intentionally exercises a constructor.

## Run

Install the pinned compiler and Dylint CLI:

```sh
rustup toolchain install nightly-2026-07-20 --profile minimal \
  --component rustc-dev --component llvm-tools --component rustfmt
cargo +nightly-2026-07-20 install cargo-dylint dylint-link --version 6.4.0 --locked
```

Run from the repository root:

```sh
python3 tools/kernel-dylint/test.py
cargo dylint --path tools/kernel-dylint --fail-on-no-libraries \
  -p delta_kernel -- --lib --all-features --locked
```

Dylint builds and loads the library with its pinned nightly. The library and fixture have
independent Cargo workspaces and lockfiles so this compiler dependency does not change Kernel's
MSRV or root lockfile. `CARGO_TARGET_DIR` can keep these builds separate from normal Kernel builds.

The fixture test fails if any expected warning is missing, if a negative example warns, or if
Dylint changes either standalone lockfile. It uses the real Kernel crate and covers aliased
types/functions, local lookalikes, runtime booleans, preferred constructors, explicit suppression,
and generated code.

The repository check above is advisory: warn-level findings do not fail it. CI rejects these four
warnings in the Kernel library by setting `DYLINT_RUSTFLAGS` for its production check:

```sh
DYLINT_RUSTFLAGS='-Dkernel_named_nullability -Dkernel_expression_literal -Dkernel_null_literal -Dkernel_predicate_constant' \
  cargo dylint --path tools/kernel-dylint --fail-on-no-libraries \
  -p delta_kernel -- --lib --all-features --locked
```

The fixture step leaves these lints at warning level so it can check positive examples. The
production step denies only these custom lints; unrelated nightly warnings are unaffected. The
structural lint ratchet also covers syntax patterns enforced in `../kernel-lint`.
