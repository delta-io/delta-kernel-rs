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
/// If a field sets `allow_null_container_values`, it means the underlying data can contain null in
/// the values of the container (i.e. a `key` -> `null` in a `HashMap`). Therefore the schema should
/// mark the value field as nullable, but those mappings will be dropped when converting to an
/// actual rust `HashMap`. Currently this can _only_ be set on `HashMap` fields.
///
/// Supported field attributes:
/// - `#[field_id = N]`: Sets the Parquet field ID for this field.
/// - `#[element_field_id = N]`: Sets the Parquet field ID for the element of a list field. Stored
///   as `ColumnMappingNestedIds` metadata (`{"fieldName.element": N}`) on the parent `StructField`
///   and propagated to the inner Arrow list element during schema conversion. The generated code
///   references `serde_json::json!`, which must be available at the expansion site.
/// - `#[allow_null_container_values]`: Marks the value field of a `HashMap` as nullable.
/// - `#[skip_schema]`: Excludes this field from the generated schema.
#[proc_macro_derive(
    ToSchema,
    attributes(allow_null_container_values, field_id, element_field_id, skip_schema)
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

fn get_named_attr_id(
    field_attributes: &[Attribute],
    attr_name: &str,
) -> Result<Option<i64>, Error> {
    field_attributes
        .iter()
        .filter_map(|attr| match &attr.meta {
            Meta::NameValue(nv) => Some(nv),
            _ => None,
        })
        .find(|nv| matches!(nv.path.get_ident(), Some(ident) if ident == attr_name))
        .map(|nv| {
            let span = nv.value.span();
            match &nv.value {
                syn::Expr::Lit(syn::ExprLit {
                    lit: Lit::Int(lit_int),
                    ..
                }) => lit_int.base10_parse().map_err(|e| {
                    Error::new(
                        lit_int.span(),
                        format!("{} error: Failed to parse integer: {}", attr_name, e),
                    )
                }),
                _ => Err(Error::new(
                    span,
                    format!("{} error: Expected an integer", attr_name),
                )),
            }
        })
        .transpose() // Convert Option<Result<T, E>> to Result<Option<T>, E>
}

fn get_field_id(field_attributes: &[Attribute]) -> Result<Option<i64>, Error> {
    get_named_attr_id(field_attributes, "field_id")
}

fn get_element_field_id(field_attributes: &[Attribute]) -> Result<Option<i64>, Error> {
    get_named_attr_id(field_attributes, "element_field_id")
}

/// Check if a path segment is `Option<HashMap<K, V>>`.
fn is_option_of_hashmap(seg: &syn::PathSegment) -> bool {
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
    // Check if the inner type's last segment is HashMap
    inner_type
        .path
        .segments
        .last()
        .is_some_and(|seg| seg.ident == "HashMap")
}

fn gen_schema_field(field: &Field) -> TokenStream {
    let name = get_schema_name(field.ident.as_ref().unwrap());
    let have_schema_null = field.attrs.iter().any(|attr| {
        // check if we have allow_null_container_values attr
        match &attr.meta {
            Meta::Path(path) => path
                .get_ident()
                .is_some_and(|ident| ident == "allow_null_container_values"),
            _ => false,
        }
    });

    match field.ty {
        Type::Path(ref type_path) => {
            // Convert the type path segments into a single quoted string
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

            // First, determine which base function to call based on schema_null setting
            let base_call = if have_schema_null {
                if let Some(last_seg) = type_path.path.segments.last() {
                    let is_valid = last_seg.ident == "HashMap" || is_option_of_hashmap(last_seg);
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

            // Then, add field-id and element-field-id metadata if present
            let field_id = match get_field_id(&field.attrs) {
                Ok(v) => v,
                Err(e) => return e.to_compile_error(),
            };
            let element_field_id = match get_element_field_id(&field.attrs) {
                Ok(v) => v,
                Err(e) => return e.to_compile_error(),
            };

            let with_field_id = if let Some(id) = field_id {
                quote_spanned! { field.span() => #base_call.add_metadata([(delta_kernel::schema::ColumnMetadataKey::ParquetFieldId.as_ref(), #id)]) }
            } else {
                quote_spanned! { field.span() => #base_call }
            };

            if let Some(elem_id) = element_field_id {
                let nested_key = format!("{}.element", name);
                quote_spanned! { field.span() =>
                    #with_field_id.add_metadata([(
                        delta_kernel::schema::ColumnMetadataKey::ColumnMappingNestedIds.as_ref().to_string(),
                        delta_kernel::schema::MetadataValue::Other(
                            serde_json::json!({ #nested_key: #elem_id })
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
    field.attrs.iter().any(|attr| match &attr.meta {
        Meta::Path(path) => path.get_ident().is_some_and(|ident| ident == "skip_schema"),
        _ => false,
    })
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

    let fields = &fields.named;
    let field_idents = fields.iter().map(|f| &f.ident);
    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();

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
