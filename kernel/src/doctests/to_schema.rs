//! Doctests for ToSchema derive macro

/// ```
/// # use delta_kernel_derive::ToSchema;
/// #[derive(ToSchema)]
/// pub struct WithFields {
///     some_name: String,
/// }
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithField;

/// ```compile_fail
/// # use delta_kernel_derive::ToSchema;
/// #[derive(ToSchema)]
/// pub struct NoFields;
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithoutField;

/// ```
/// # use delta_kernel_derive::ToSchema;
/// # use std::collections::HashMap;
/// #[derive(ToSchema)]
/// pub struct WithAngleBracketPath {
///     map_field: HashMap<String, String>,
/// }
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithAngleBracketedPathField;

/// ```
/// # use delta_kernel_derive::ToSchema;
/// # use std::collections::HashMap;
/// #[derive(ToSchema)]
/// pub struct WithAttributedField {
///     #[allow_null_container_values]
///     map_field: HashMap<String, String>,
/// }
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithAttributedField;

/// ```compile_fail
/// # use delta_kernel_derive::ToSchema;
/// #[derive(ToSchema)]
/// pub struct WithInvalidAttributeTarget {
///     #[allow_null_container_values]
///     some_name: String,
/// }
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithInvalidAttributeTarget;

/// ```compile_fail
/// # use delta_kernel_derive::ToSchema;
/// # use syn::Token;
/// #[derive(ToSchema)]
/// pub struct WithInvalidFieldType {
///     token: Token![struct],
/// }
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithInvalidFieldType;
