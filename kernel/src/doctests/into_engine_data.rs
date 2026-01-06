//! Doctests for IntoEngineData derive macro

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
