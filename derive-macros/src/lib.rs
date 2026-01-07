use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, quote_spanned, ToTokens};
use syn::parse_macro_input;
use syn::spanned::Spanned;
use syn::{
    Data, DataStruct, DeriveInput, Error, Fields, Item, Meta, PathArguments, Type, Visibility,
};

/// Parses a dot-delimited column name into an array of field names. See
/// `delta_kernel::expressions::column_name::column_name` macro for details.
#[proc_macro]
pub fn parse_column_name(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let is_valid = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '.';
    let err = match syn::parse(input) {
        Ok(syn::Lit::Str(name)) => match name.value().chars().find(|c| !is_valid(*c)) {
            Some(bad_char) => Error::new(name.span(), format!("Invalid character: {bad_char:?}")),
            _ => {
                let path = name.value();
                let path = path.split('.').map(proc_macro2::Literal::string);
                return quote_spanned! { name.span() => [#(#path),*] }.into();
            }
        },
        Ok(lit) => Error::new(lit.span(), "Expected a string literal"),
        Err(err) => err,
    };
    err.into_compile_error().into()
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
#[proc_macro_derive(ToSchema, attributes(allow_null_container_values))]
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

    let schema_fields = fields.iter().map(|field| {
        let name = field.ident.as_ref().unwrap(); // we know these are named fields
        let name = get_schema_name(name);
        let have_schema_null = field.attrs.iter().any(|attr| {
            // check if we have allow_null_container_values attr
            match &attr.meta {
                Meta::Path(path) => path.get_ident().is_some_and(|ident| ident == "allow_null_container_values"),
                _ => false,
            }
        });

        match field.ty {
            Type::Path(ref type_path) => {
                let type_path_quoted = type_path.path.segments.iter().map(|segment| {
                    let segment_ident = &segment.ident;
                    match &segment.arguments {
                        PathArguments::None => quote! { #segment_ident :: },
                        PathArguments::AngleBracketed(angle_args) => quote! { #segment_ident::#angle_args :: },
                        _ => Error::new(segment.arguments.span(), "Can only handle <> type path args").to_compile_error()
                    }
                });
                if have_schema_null {
                    if let Some(last_ident) = type_path.path.segments.last().map(|seg| &seg.ident) {
                        if last_ident != "HashMap" {
                           return Error::new(
                                last_ident.span(),
                                format!("Can only use allow_null_container_values on HashMap fields, not {last_ident}")
                            ).to_compile_error()
                        }
                    }
                    quote_spanned! { field.span() => #(#type_path_quoted)* get_nullable_container_struct_field(stringify!(#name))}
                } else {
                    quote_spanned! { field.span() => #(#type_path_quoted)* get_struct_field(stringify!(#name))}
                }
            }
            _ => Error::new(field.span(), format!("Can't handle type: {:?}", field.ty)).to_compile_error()
        }
    });
    quote! { #(#schema_fields),* }
}

/// Derive an IntoEngineData trait for a struct that has all fields implement `Into<Scalar>`.
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
    let field_types = fields.iter().map(|f| &f.ty);

    let expanded = quote! {
        #[automatically_derived]
        impl crate::IntoEngineData for #struct_name
        where
            #(#field_types: Into<crate::expressions::Scalar>),*
        {
            fn into_engine_data(
                self,
                schema: crate::schema::SchemaRef,
                engine: &dyn crate::Engine)
            -> crate::DeltaResult<Box<dyn crate::EngineData>> {
                // NB: we `use` here to avoid polluting the caller's namespace
                use crate::EvaluationHandlerExtension as _;
                let values = [
                    #(self.#field_idents.into()),*
                ];
                let evaluator = engine.evaluation_handler();
                evaluator.create_one(schema, &values)
            }
        }
    };

    proc_macro::TokenStream::from(expanded)
}

/// Mark items as `internal_api` to make them public iff the `internal-api` feature is enabled.
/// Note this doesn't work for inline module definitions (see `internal_mod!` macro in delta_kernel
/// crate - can't export macro_rules! from proc macro crate).
/// Ref: <https://github.com/rust-lang/rust/issues/54727>
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
    fn set_pub(vis: &mut Visibility) -> Result<(), syn::Error> {
        if matches!(vis, Visibility::Public(_)) {
            return Err(Error::new(
                vis.span(),
                "ineligible for #[internal_api]: item is already public",
            ));
        }
        *vis = syn::parse_quote!(pub);
        Ok(())
    }

    let result = match &mut item {
        Item::Fn(f) => set_pub(&mut f.vis),
        Item::Struct(s) => set_pub(&mut s.vis),
        Item::Enum(e) => set_pub(&mut e.vis),
        Item::Trait(t) => set_pub(&mut t.vis),
        Item::Type(t) => set_pub(&mut t.vis),
        Item::Mod(m) => set_pub(&mut m.vis),
        Item::Static(s) => set_pub(&mut s.vis),
        Item::Const(c) => set_pub(&mut c.vis),
        Item::Union(u) => set_pub(&mut u.vis),
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
