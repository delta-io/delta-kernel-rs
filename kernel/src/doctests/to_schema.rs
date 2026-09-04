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

/// Verify that `#[allow_null_container_values]` works on `Option<HashMap<_, _>>` fields.
/// This is needed for optional map fields like `Remove.partition_values` that can contain
/// null values.
/// ```
/// # use delta_kernel_derive::ToSchema;
/// # use std::collections::HashMap;
/// #[derive(ToSchema)]
/// pub struct WithOptionalAttributedField {
///     #[allow_null_container_values]
///     map_field: Option<HashMap<String, String>>,
/// }
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithOptionalAttributedField;

/// Verify that `#[allow_null_container_values]` fails on `Option<_>` fields that are not maps.
/// ```compile_fail
/// # use delta_kernel_derive::ToSchema;
/// #[derive(ToSchema)]
/// pub struct WithInvalidOptionalAttributeTarget {
///     #[allow_null_container_values]
///     some_name: Option<String>,
/// }
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithInvalidOptionalAttributeTarget;

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

/// Verify that `#[field_id]` / `#[nested_field_id]` on a list field produce the expected
/// `parquet.field.id` and `delta.columnMapping.nested.ids` metadata on the generated `StructField`.
/// ```
/// # use delta_kernel_derive::ToSchema;
/// # use delta_kernel::schema::{ToSchema, ColumnMetadataKey, MetadataValue};
/// #[derive(ToSchema)]
/// pub struct WithFieldIds {
///     #[field_id = 132]
///     #[nested_field_id = 133]
///     split_offsets: Option<Vec<i64>>,
/// }
/// let schema = WithFieldIds::to_schema();
/// let field = schema.field("splitOffsets").unwrap();
/// assert_eq!(
///     field.metadata.get(ColumnMetadataKey::ParquetFieldId.as_ref()),
///     Some(&MetadataValue::Number(132)),
/// );
/// let nested = field
///     .metadata
///     .get(ColumnMetadataKey::ColumnMappingNestedIds.as_ref())
///     .unwrap();
/// assert_eq!(nested.to_string(), r#"{"splitOffsets.element":133}"#);
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithFieldIds;

/// Verify that `#[nested_field_id]` is rejected on a non-list field.
/// ```compile_fail
/// # use delta_kernel_derive::ToSchema;
/// #[derive(ToSchema)]
/// pub struct WithNestedFieldIdOnScalar {
///     #[nested_field_id = 5]
///     some_name: String,
/// }
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithNestedFieldIdOnScalar;

/// Verify that an out-of-range `#[field_id]` (must be `1..=i32::MAX`) is rejected.
/// ```compile_fail
/// # use delta_kernel_derive::ToSchema;
/// #[derive(ToSchema)]
/// pub struct WithOutOfRangeFieldId {
///     #[field_id = 9223372036854775807]
///     some_name: String,
/// }
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithOutOfRangeFieldId;

/// Verify that a malformed `#[field_id]` shape (bare path, no value) is rejected.
/// ```compile_fail
/// # use delta_kernel_derive::ToSchema;
/// #[derive(ToSchema)]
/// pub struct WithMalformedFieldId {
///     #[field_id]
///     some_name: String,
/// }
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithMalformedFieldId;

/// Verify that a `#[skip_schema]` field is excluded from the generated schema while its siblings
/// remain.
/// ```
/// # use delta_kernel_derive::ToSchema;
/// # use delta_kernel::schema::ToSchema;
/// #[derive(ToSchema)]
/// pub struct WithSkippedField {
///     #[skip_schema]
///     skipped: String,
///     kept: i32,
/// }
/// let schema = WithSkippedField::to_schema();
/// assert!(schema.field("kept").is_some());
/// assert!(schema.field("skipped").is_none());
/// ```
#[cfg(doctest)]
pub struct MacroTestStructWithSkippedField;
