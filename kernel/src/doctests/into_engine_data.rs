//! Doctests for the `IntoEngineData` derive macro.
//!
//! `IntoEngineData` converts a Rust struct into the `EngineData` representation.
//! See the `IntoEngineData` trait for details.
//! `#[derive(IntoEngineData)]` implements the `IntoEngineData` trait for the struct.
//!
//! What is valid:
//! - A **named-field struct** (a regular `struct Foo { a: T, b: U }`)
//!
//! What is not valid (and should fail to compile):
//! - A **unit struct** (`struct Foo;`) — no fields to convert into engine data.
//! - A **tuple struct** (`struct Foo(T, U);`) — the macro expects named fields.

/// ```
/// # use delta_kernel_derive::IntoEngineData;
/// #[derive(IntoEngineData)]
/// pub struct WithFields {
///     some_name: String,
///     count: i32,
/// }
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithField;

/// ```compile_fail
/// # use delta_kernel_derive::IntoEngineData;
/// #[derive(IntoEngineData)]
/// pub struct NoFields;
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithoutField;

/// ```compile_fail
/// # use delta_kernel_derive::IntoEngineData;
/// #[derive(IntoEngineData)]
/// pub struct TupleStruct(String, i32);
/// ```
#[cfg(doctest)]
pub struct MacroTestTupleStruct;
