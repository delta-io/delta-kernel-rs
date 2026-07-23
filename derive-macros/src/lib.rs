use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, quote_spanned, ToTokens};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, Attribute, Data, DataStruct, DeriveInput, Error, Expr, ExprLit, Field,
    Fields, Item, Lit, Meta, PathArguments, Token, Type, Visibility,
};

mod schema_macro;

// Builds a `StructType`; see `delta_kernel::schema::schema` re-export for details.
#[doc(hidden)]
#[proc_macro]
pub fn schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    schema_macro::parse_schema(input, false, |block| block)
}

// Fallible version of `schema!`; see `delta_kernel::schema::try_schema` re-export for details.
#[doc(hidden)]
#[proc_macro]
pub fn try_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Wrap the block in a closure that anchors the block's `?` operators
    schema_macro::parse_schema(input, true, |block| {
        quote! { (|| -> delta_kernel::DeltaResult<_> #block)() }
    })
}

// `Arc`-wrapped `schema!`; see `delta_kernel::schema::schema_ref` for details.
#[doc(hidden)]
#[proc_macro]
pub fn schema_ref(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    schema_macro::parse_schema(input, false, |block| quote!(::std::sync::Arc::new(#block)))
}

// `LazyLock<SchemaRef>`-wrapped `schema!`; see `delta_kernel::schema::lazy_schema_ref`.
#[doc(hidden)]
#[proc_macro]
pub fn lazy_schema_ref(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    schema_macro::parse_schema(
        input,
        false,
        |block| quote!(::std::sync::LazyLock::new(|| ::std::sync::Arc::new(#block))),
    )
}

/// Builds `&[&str]` from the arguments to `column_name!`. Each **string-literal** argument is a
/// dot-separated path, split into individual segments; every other (constant) argument is taken as
/// a single segment, validated at const-eval time. All segments must match `[a-zA-Z0-9_]+`.
#[proc_macro]
pub fn column_name_segments(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    column_name_segments_impl(input.into())
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn column_name_segments_impl(input: TokenStream) -> Result<TokenStream, Error> {
    // `column_name!` requires at least one argument, so no empty check is needed here.
    let mut emitted = Vec::new();
    for expr in Punctuated::<Expr, Token![,]>::parse_terminated.parse2(input)? {
        // Fragment-forwarding macros (col!, column_expr!, joined_column_name!, ...) capture with
        // `:literal`/`:expr` and re-emit, which wraps the argument in an invisible (none-delimited)
        // group. Peel away those groups to recover an underlying string literal.
        let mut inner = &expr;
        while let Expr::Group(group) = inner {
            inner = &group.expr;
        }
        match inner {
            // A string literal is a dot-separated path: split it and validate each segment now.
            Expr::Lit(ExprLit {
                lit: Lit::Str(lit_str),
                ..
            }) => {
                for segment in lit_str.value().split('.') {
                    validate_single_segment(segment, lit_str.span())?;
                    let literal = proc_macro2::Literal::string(segment);
                    emitted.push(quote_spanned! { lit_str.span() => #literal });
                }
            }
            // A string constant's value isn't visible until const-eval (after this macro expands),
            // so wrap the value in a const fn validator with failure as a compile-time panic.
            _ => emitted.push(quote_spanned! { expr.span() =>
                match ::delta_kernel::expressions::__require_valid_simple_column_segment(#expr) {
                    Some(segment) => segment,
                    None => panic!("String constants passed to column_name! must be simple names \
                                    matching [a-zA-Z0-9_]+; use a string literal for dot-separated \
                                    paths, or ColumnName::new() to disambiguate"),
                }
            }),
        }
    }

    Ok(quote! { &[ #(#emitted),* ] })
}

fn validate_single_segment(segment: &str, span: Span) -> Result<(), Error> {
    if segment.is_empty() {
        return Err(Error::new(span, "empty column name segment"));
    }
    if let Some(bad) = segment
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_'))
    {
        return Err(Error::new(
            span,
            format!("invalid character {bad:?} in column name segment {segment:?}"),
        ));
    }
    Ok(())
}

/// Derive a `delta_kernel::schemas::ToSchema` implementation for the annotated struct. The actual
/// field names in the schema (and therefore of the struct members) are all mandated by the Delta
/// spec, and so the user of this macro is responsible for ensuring that
/// e.g. `Metadata::schema_string` is the snake_case-ified version of `schemaString` from [Delta's
/// Change Metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata)
/// action (this macro allows the use of standard rust snake_case, and will convert to the correct
/// delta schema camelCase version).
///
/// Supported field attributes:
/// - `#[field_id = N]`: Sets the Parquet field ID for this field. `N` must be in `1..=i32::MAX`.
/// - `#[nested_field_id = N]`: Sets the Parquet field ID for the element of a list field (`Vec<T>`
///   or `Option<Vec<T>>`; rejected on any other type). Stored as `ColumnMappingNestedIds` metadata
///   (`{"fieldName.element": N}`) on the parent `StructField` and propagated to the inner Arrow
///   list element during schema conversion. `N` must be in `1..=i32::MAX`.
/// - `#[allow_null_container_values]`: Marks the value field of a container as nullable, i.e. the
///   underlying data can contain null in the values of the container (a `key` -> `null` in a
///   `HashMap`). Those mappings will be dropped when converting to an actual rust `HashMap`.
///   Currently this can _only_ be set on `HashMap` fields.
/// - `#[skip_schema]`: Excludes this field from the generated schema (and, on a struct that also
///   derives `IntoEngineData`, from the produced engine data).
#[proc_macro_derive(
    ToSchema,
    attributes(allow_null_container_values, field_id, nested_field_id, skip_schema)
)]
pub fn derive_to_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_ident = input.ident;

    let schema_fields = gen_schema_fields(&input.data);
    let output = quote! {
        #[automatically_derived]
        impl delta_kernel::schema::ToSchema for #struct_ident {
            fn to_schema() -> delta_kernel::schema::StructType {
                use delta_kernel::schema::derive_macro_utils::{
                    ToDataType as _, GetStructField as _, GetNullableContainerStructField as _,
                };
                delta_kernel::schema::StructType::new_unchecked([
                    #schema_fields
                ])
            }
        }
    };
    proc_macro::TokenStream::from(output)
}

// turn our struct name into the schema name, goes from snake_case to camelCase
fn get_schema_name(name: &Ident) -> Ident {
    let snake_name = name.to_string();
    let mut next_caps = false;
    let ret: String = snake_name
        .chars()
        .filter_map(|c| {
            if c == '_' {
                next_caps = true;
                None
            } else if next_caps {
                next_caps = false;
                // This assumes we're using ascii, should be okay
                Some(c.to_ascii_uppercase())
            } else {
                Some(c)
            }
        })
        .collect();
    Ident::new(&ret, name.span())
}

/// Reads a `#[<attr_name> = N]` field attribute, returning `Ok(None)` if absent. The value must be
/// an integer literal in `1..=i32::MAX` (the valid Parquet field-id range); a bare `#[<attr_name>]`
/// or `#[<attr_name>(..)]` shape, a non-integer, or an out-of-range value is a hard error. If the
/// attribute is repeated, the first occurrence wins.
fn get_named_attr_id(
    field_attributes: &[Attribute],
    attr_name: &str,
) -> Result<Option<i64>, Error> {
    // Match on the attribute path first so a malformed shape (bare path / list form) on the right
    // name is a diagnosable error rather than silently skipped.
    let Some(attr) = field_attributes
        .iter()
        .find(|attr| attr.path().is_ident(attr_name))
    else {
        return Ok(None);
    };
    let Meta::NameValue(nv) = &attr.meta else {
        return Err(Error::new(
            attr.span(),
            format!("{attr_name} error: expected `#[{attr_name} = N]`"),
        ));
    };
    let Expr::Lit(ExprLit {
        lit: Lit::Int(lit_int),
        ..
    }) = &nv.value
    else {
        return Err(Error::new(
            nv.value.span(),
            format!("{attr_name} error: expected an integer"),
        ));
    };
    let value: i64 = lit_int
        .base10_parse()
        .map_err(|e| Error::new(lit_int.span(), format!("{attr_name} error: {e}")))?;
    if !(1..=i64::from(i32::MAX)).contains(&value) {
        return Err(Error::new(
            lit_int.span(),
            format!("{attr_name} error: field id {value} must be in 1..=2147483647"),
        ));
    }
    Ok(Some(value))
}

/// Check if a path segment is `Option<{inner}<..>>` (e.g. `Option<HashMap<K, V>>`), matching on the
/// inner type's last segment identifier.
fn is_option_of(seg: &syn::PathSegment, inner: &str) -> bool {
    if seg.ident != "Option" {
        return false;
    }
    let PathArguments::AngleBracketed(angle_args) = &seg.arguments else {
        return false;
    };
    // Option has exactly one type argument
    let Some(syn::GenericArgument::Type(Type::Path(inner_type))) = angle_args.args.first() else {
        return false;
    };
    inner_type
        .path
        .segments
        .last()
        .is_some_and(|seg| seg.ident == inner)
}

/// Check if a path segment is a list-shaped type: `Vec<T>` or `Option<Vec<T>>`.
fn is_list_segment(seg: &syn::PathSegment) -> bool {
    seg.ident == "Vec" || is_option_of(seg, "Vec")
}

/// Check if any of `attrs` is a bare-path attribute named `name` (e.g. `#[skip_schema]`).
fn has_named_attr(attrs: &[Attribute], name: &str) -> bool {
    attrs.iter().any(|attr| match &attr.meta {
        Meta::Path(path) => path.is_ident(name),
        _ => false,
    })
}

fn gen_schema_field(field: &Field) -> TokenStream {
    let name = get_schema_name(field.ident.as_ref().unwrap());
    let have_schema_null = has_named_attr(&field.attrs, "allow_null_container_values");

    match field.ty {
        Type::Path(ref type_path) => {
            let type_path_quoted = type_path.path.segments.iter().map(|segment| {
                let segment_ident = &segment.ident;
                match &segment.arguments {
                    PathArguments::None => quote! { #segment_ident :: },
                    PathArguments::AngleBracketed(angle_args) => {
                        quote! { #segment_ident::#angle_args :: }
                    }
                    _ => Error::new(
                        segment.arguments.span(),
                        "Can only handle <> type path args",
                    )
                    .to_compile_error(),
                }
            });

            let base_call = if have_schema_null {
                if let Some(last_seg) = type_path.path.segments.last() {
                    let is_valid = last_seg.ident == "HashMap" || is_option_of(last_seg, "HashMap");
                    if !is_valid {
                        return Error::new(
                            last_seg.ident.span(),
                            format!(
                                "Can only use allow_null_container_values on HashMap or \
                                 Option<HashMap> fields, not {}",
                                last_seg.ident
                            ),
                        )
                        .to_compile_error();
                    }
                }
                quote_spanned! { field.span() => #(#type_path_quoted)* get_nullable_container_struct_field(stringify!(#name)) }
            } else {
                quote_spanned! { field.span() => #(#type_path_quoted)* get_struct_field(stringify!(#name)) }
            };

            let field_id = match get_named_attr_id(&field.attrs, "field_id") {
                Ok(v) => v,
                Err(e) => return e.to_compile_error(),
            };
            let nested_field_id = match get_named_attr_id(&field.attrs, "nested_field_id") {
                Ok(v) => v,
                Err(e) => return e.to_compile_error(),
            };

            let with_field_id = if let Some(id) = field_id {
                quote_spanned! { field.span() => #base_call.add_metadata([(delta_kernel::schema::ColumnMetadataKey::ParquetFieldId.as_ref(), #id)]) }
            } else {
                quote_spanned! { field.span() => #base_call }
            };

            if let Some(elem_id) = nested_field_id {
                // The `<field>.element` nested id is only meaningful for a list element; reject it
                // on any other type, mirroring the allow_null_container_values guard above.
                let is_list = type_path.path.segments.last().is_some_and(is_list_segment);
                if !is_list {
                    return Error::new(
                        field.span(),
                        format!(
                            "Can only use nested_field_id on Vec or Option<Vec> fields, not {}",
                            type_path.path.segments.last().map_or_else(
                                || "<empty path>".to_string(),
                                |seg| seg.ident.to_string()
                            )
                        ),
                    )
                    .to_compile_error();
                }
                let nested_key = format!("{}.element", name);
                quote_spanned! { field.span() =>
                    #with_field_id.add_metadata([(
                        delta_kernel::schema::ColumnMetadataKey::ColumnMappingNestedIds.as_ref().to_string(),
                        delta_kernel::schema::MetadataValue::Other(
                            ::serde_json::json!({ #nested_key: #elem_id })
                        )
                    )])
                }
            } else {
                with_field_id
            }
        }
        _ => Error::new(field.span(), format!("Can't handle type: {:?}", field.ty))
            .to_compile_error(),
    }
}

fn has_skip_schema(field: &Field) -> bool {
    has_named_attr(&field.attrs, "skip_schema")
}

fn gen_schema_fields(data: &Data) -> TokenStream {
    let fields = match data {
        Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) => &fields.named,
        _ => {
            return Error::new(
                Span::call_site(),
                "this derive macro only works on structs with named fields",
            )
            .to_compile_error()
        }
    };

    let schema_fields = fields
        .iter()
        .filter(|f| !has_skip_schema(f))
        .map(gen_schema_field);
    quote! { #(#schema_fields),* }
}

/// Derive an IntoEngineData trait for a struct that has all fields implement `TryInto<Scalar>`.
///
/// This is a relatively simple macro to produce the boilerplate for converting a struct into
/// EngineData using the `create_one` method. TODO: (doc)tests included in the delta_kernel crate:
/// `IntoEngineData` trait.
#[proc_macro_derive(IntoEngineData)]
pub fn into_engine_data_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    let Data::Struct(DataStruct {
        fields: Fields::Named(fields),
        ..
    }) = &input.data
    else {
        return Error::new(
            struct_name.span(),
            "IntoEngineData can only be derived for structs with named fields",
        )
        .to_compile_error()
        .into();
    };

    // Honor `#[skip_schema]` so the produced engine data matches the arity of the schema built by
    // the `ToSchema` derive (which filters the same fields).
    let kept: Vec<_> = fields
        .named
        .iter()
        .filter(|f| !has_skip_schema(f))
        .collect();
    let field_idents = kept.iter().map(|f| &f.ident);
    let field_types: Vec<_> = kept.iter().map(|f| &f.ty).collect();

    let expanded = quote! {
        #[automatically_derived]
        impl delta_kernel::IntoEngineData for #struct_name
        where
            #(#field_types: TryInto<delta_kernel::expressions::Scalar>,)*
            #(delta_kernel::Error: From<<#field_types as TryInto<delta_kernel::expressions::Scalar>>::Error>,)*
        {
            fn into_engine_data(
                self,
                schema: delta_kernel::schema::SchemaRef,
                engine: &dyn delta_kernel::Engine)
            -> delta_kernel::DeltaResult<Box<dyn delta_kernel::EngineData>> {
                // NB: we `use` here to avoid polluting the caller's namespace
                use delta_kernel::EvaluationHandlerExtension as _;
                let values = [
                    #(self.#field_idents.try_into()?),*
                ];
                let evaluator = engine.evaluation_handler();
                evaluator.create_one(schema, &values)
            }
        }
    };

    proc_macro::TokenStream::from(expanded)
}

/// Mark items as `internal_api` to make them public iff the `internal-api` feature is enabled.
///
/// NOTE: This macro does not support `mod` declarations because of nuances in how the mod expander
/// and proc macro system interact for non-inline modules such as `mod foo;`. Use explicit
/// cfg-gated `pub mod` / `pub(crate) mod` for module visibility control instead.
#[proc_macro_attribute]
pub fn internal_api(
    _attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input = parse_macro_input!(item as Item);

    // Create a version with public visibility for the unstable feature
    let public_version = make_public(input.clone());

    // The original item stays as-is for the non-unstable case
    let output = quote! {
        #[cfg(feature = "internal-api")]
        #public_version

        #[cfg(not(feature = "internal-api"))]
        #input
    };

    output.into()
}

fn make_public(mut item: Item) -> Item {
    /// Transforms the passed visibility to be `pub`. We pass the original span that the visibility
    /// came from, and attach it to the newly created pub token. This means that the compiler treats
    /// it as user-written code and normal lints apply. We want this because it allows us to catch
    /// "private_in_public" violations that are tricky to notice when just slapping
    /// `#[internal_api]` on something.
    fn set_pub(vis: &mut Visibility, span: Span) -> Result<(), syn::Error> {
        if matches!(vis, Visibility::Public(_)) {
            return Err(Error::new(
                vis.span(),
                "ineligible for #[internal_api]: item is already public",
            ));
        }
        *vis = Visibility::Public(syn::token::Pub { span });
        Ok(())
    }

    macro_rules! set_vis {
        ($item:ident) => {{
            let vis_span = $item.vis.span();
            set_pub(&mut $item.vis, vis_span)
        }};
    }

    let result = match &mut item {
        Item::Fn(f) => set_vis!(f),
        Item::Struct(s) => set_vis!(s),
        Item::Enum(e) => set_vis!(e),
        Item::Trait(t) => set_vis!(t),
        Item::Type(t) => set_vis!(t),
        Item::Use(u) => set_vis!(u),
        Item::Static(s) => set_vis!(s),
        Item::Const(c) => set_vis!(c),
        Item::Union(u) => set_vis!(u),
        // foreign mod, impl block, and all others not handled
        _ => Err(Error::new(
            item.span(),
            format!("unsupported item type for #[internal_api]: {item:?}"),
        )),
    };

    if let Err(err) = result {
        let error = err.to_compile_error();
        let mut tokens = item.to_token_stream();
        tokens.extend(error);
        return syn::parse_quote!(#tokens);
    }

    item
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Expand `gen_schema_fields` for `input` and return the generated tokens as a string. Macro
    /// errors are embedded as `compile_error!` tokens in that string; `Err` only signals that the
    /// input itself failed to parse as a `DeriveInput`.
    fn schema_fields_tokens(input: &str) -> Result<String, String> {
        let input = syn::parse_str::<DeriveInput>(input).map_err(|e| e.to_string())?;
        Ok(gen_schema_fields(&input.data).to_string())
    }

    #[test]
    fn test_valid_field_id_parsing() {
        let input = r#"
            struct TestStruct {
                #[field_id = 123]
                valid_field: String,

                #[field_id = 456]
                another_valid_field: i32,

                normal_field: bool,
            }
        "#;

        let tokens = schema_fields_tokens(input).unwrap();
        assert!(tokens.contains("123"));
        assert!(tokens.contains("456"));
        assert!(!tokens.contains("compile_error"));
    }

    #[rstest]
    #[case::one("1", "1i64")]
    #[case::max("2147483647", "2147483647i64")]
    fn field_id_in_range_renders_literal(#[case] id: &str, #[case] expected: &str) {
        let input = format!("struct S {{ #[field_id = {id}] f: String }}");
        let tokens = schema_fields_tokens(&input).unwrap();
        let needle = format!(
            "(delta_kernel :: schema :: ColumnMetadataKey :: ParquetFieldId . as_ref () , {expected})"
        );
        assert!(tokens.contains(&needle), "found: {tokens}");
    }

    #[rstest]
    #[case::zero("#[field_id = 0]")]
    #[case::negative("#[field_id = -1]")]
    #[case::overflow_i32("#[field_id = 2147483648]")]
    #[case::overflow_i64("#[field_id = 9223372036854775808]")]
    #[case::not_an_integer(r#"#[field_id = "x"]"#)]
    #[case::bare_path("#[field_id]")]
    #[case::list_form("#[field_id(1)]")]
    fn field_id_rejected(#[case] attr: &str) {
        let input = format!("struct S {{ {attr} f: String }}");
        let tokens = schema_fields_tokens(&input).unwrap();
        assert!(
            tokens.contains("compile_error"),
            "expected rejection, found: {tokens}"
        );
    }

    #[rstest]
    // With a companion field_id, both metadata keys are emitted.
    #[case::with_field_id("#[field_id = 132]\n#[nested_field_id = 133]", true)]
    // Element id alone emits only the nested id, not ParquetFieldId.
    #[case::without_field_id("#[nested_field_id = 133]", false)]
    fn nested_field_id_on_list(#[case] attrs: &str, #[case] expect_parquet_field_id: bool) {
        let input = format!("struct S {{ {attrs}\nsplit_offsets: Option<Vec<i64>> }}");
        let tokens = schema_fields_tokens(&input).unwrap();
        assert!(!tokens.contains("compile_error"), "found: {tokens}");
        assert!(tokens.contains("ColumnMappingNestedIds"), "found: {tokens}");
        assert!(tokens.contains("splitOffsets.element"), "found: {tokens}");
        assert!(tokens.contains("133i64"), "found: {tokens}");
        assert_eq!(
            tokens.contains("ParquetFieldId . as_ref"),
            expect_parquet_field_id,
            "found: {tokens}"
        );
    }

    #[rstest]
    #[case::scalar("f: String")]
    #[case::map("f: std::collections::HashMap<String, i64>")]
    fn nested_field_id_rejected_on_non_list(#[case] field: &str) {
        let input = format!("struct S {{ #[nested_field_id = 5] {field} }}");
        let tokens = schema_fields_tokens(&input).unwrap();
        assert!(
            tokens.contains("compile_error"),
            "expected rejection, found: {tokens}"
        );
    }

    #[test]
    fn skip_schema_excludes_field() {
        let input = r#"
            struct TestStruct {
                #[skip_schema]
                skipped: String,
                kept: i32,
            }
        "#;
        let tokens = schema_fields_tokens(input).unwrap();
        assert!(tokens.contains("kept"), "kept field must remain: {tokens}");
        assert!(
            !tokens.contains("skipped"),
            "skipped field must be absent: {tokens}"
        );
    }

    #[rstest]
    #[case::hashmap("std::collections::HashMap<String, i64>")]
    #[case::option_hashmap("Option<std::collections::HashMap<String, i64>>")]
    fn allow_null_container_values_on_map(#[case] ty: &str) {
        let input = format!("struct S {{ #[allow_null_container_values] f: {ty} }}");
        let tokens = schema_fields_tokens(&input).unwrap();
        assert!(!tokens.contains("compile_error"), "found: {tokens}");
        assert!(
            tokens.contains("get_nullable_container_struct_field"),
            "found: {tokens}"
        );
    }

    #[rstest]
    #[case::scalar("String")]
    #[case::vec("Vec<i64>")]
    fn allow_null_container_values_rejected_on_non_map(#[case] ty: &str) {
        let input = format!("struct S {{ #[allow_null_container_values] f: {ty} }}");
        let tokens = schema_fields_tokens(&input).unwrap();
        assert!(
            tokens.contains("compile_error"),
            "expected rejection, found: {tokens}"
        );
    }

    #[rstest]
    #[case::tuple("f: (i32, i32)")]
    #[case::array("f: [i32; 4]")]
    fn non_path_field_type_rejected(#[case] field: &str) {
        let input = format!("struct S {{ {field} }}");
        let tokens = schema_fields_tokens(&input).unwrap();
        assert!(
            tokens.contains("compile_error"),
            "expected rejection, found: {tokens}"
        );
    }
}
