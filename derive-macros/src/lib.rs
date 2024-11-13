use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, Data, DataStruct, DeriveInput, Error, Fields, Meta, PathArguments, Type,
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

/// Derive a `delta_kernel::schemas::ToDataType` implementation for the annotated struct. The actual
/// field names in the schema (and therefore of the struct members) are all mandated by the Delta
/// spec, and so the user of this macro is responsible for ensuring that
/// e.g. `Metadata::schema_string` is the snake_case-ified version of `schemaString` from [Delta's
/// Change Metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata)
/// action (this macro allows the use of standard rust snake_case, and will convert to the correct
/// delta schema camelCase version).
///
/// If a field sets `drop_null_container_values`, it means the underlying data can contain null in
/// the values of the container (i.e. a `key` -> `null` in a `HashMap`). Therefore the schema should
/// mark the value field as nullable, but those mappings will be dropped when converting to an
/// actual rust `HashMap`. Currently this can _only_ be set on `HashMap` fields.
#[proc_macro_derive(Schema, attributes(drop_null_container_values))]
pub fn derive_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_ident = input.ident;

    let schema_fields = gen_schema_fields(&input.data);
    let output = quote! {
        #[automatically_derived]
        impl crate::actions::schemas::ToDataType for #struct_ident {
            fn to_data_type() -> crate::schema::DataType {
                use crate::actions::schemas::{ToDataType, GetStructField, GetNullableContainerStructField};
                crate::schema::DataType::struct_type([
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
            // check if we have drop_null_container_values attr
            match &attr.meta {
                Meta::Path(path) => path.get_ident().is_some_and(|ident| ident == "drop_null_container_values"),
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
                    if let Some(first_ident) = type_path.path.segments.first().map(|seg| &seg.ident) {
                        if first_ident != "HashMap" {
                           return Error::new(
                                first_ident.span(),
                                format!("Can only use drop_null_container_values on HashMap fields, not {first_ident}")
                            ).to_compile_error()
                        }
                    }
                    quote_spanned! { field.span() => #(#type_path_quoted),* get_nullable_container_struct_field(stringify!(#name))}
                } else {
                    quote_spanned! { field.span() => #(#type_path_quoted),* get_struct_field(stringify!(#name))}
                }
            }
            _ => Error::new(field.span(), format!("Can't handle type: {:?}", field.ty)).to_compile_error()
        }
    });
    quote! { #(#schema_fields),* }
}
