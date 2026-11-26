use std::collections::HashSet;

use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::ext::IdentExt;
use syn::{Attribute, Data, DeriveInput, Error, Fields, LitStr, Type};

pub(super) struct Variant<'a> {
    ident: &'a Ident,
    payload: &'a Type,
    name: LitStr,
}

pub(super) fn variants(input: &DeriveInput) -> Result<Vec<Variant<'_>>, Error> {
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "struct-data enums do not support generic parameters",
        ));
    }
    let Data::Enum(data) = &input.data else {
        return Err(Error::new_spanned(input, "expected an enum"));
    };
    if data.variants.is_empty() {
        return Err(Error::new_spanned(
            input,
            "struct-data enums require a variant",
        ));
    }
    let mut names = HashSet::new();
    data.variants
        .iter()
        .map(|variant| {
            let payload = match &variant.fields {
                Fields::Unnamed(fields) if fields.unnamed.len() == 1 => &fields.unnamed[0].ty,
                _ => {
                    return Err(Error::new_spanned(
                        variant,
                        "enum variants must contain exactly one unnamed struct payload",
                    ));
                }
            };
            let name = schema_name(&variant.ident, &variant.attrs)?;
            if !names.insert(name.value().to_lowercase()) {
                return Err(Error::new(name.span(), "duplicate enum schema field name"));
            }
            Ok(Variant {
                ident: &variant.ident,
                payload,
                name,
            })
        })
        .collect()
}

pub(super) fn schema_fields(variants: &[Variant<'_>]) -> TokenStream {
    let fields = variants.iter().map(|variant| {
        let field_name = &variant.name;
        let payload = variant.payload;
        quote! {
            delta_kernel::schema::StructField::nullable(
                #field_name,
                <#payload as delta_kernel::schema::ToSchema>::schema_ref().clone(),
            )
        }
    });
    quote! { #(#fields),* }
}

pub(super) fn into_struct_data(input: &DeriveInput) -> Result<TokenStream, Error> {
    let struct_name = &input.ident;
    let variants = variants(input)?;
    let payloads = variants.iter().map(|v| v.payload);
    let arms = variants.iter().map(|variant| {
        let ident = variant.ident;
        let name = &variant.name;
        quote! { #struct_name::#ident(value) =>
        (#name, delta_kernel::expressions::Scalar::Struct(value.into())), }
    });
    let bounds = quote! { #(#payloads: Into<delta_kernel::expressions::StructData>,)* };
    let values = quote! {{
        let (name, value) = match value { #(#arms)* };
        let mut value = ::std::option::Option::Some(value);
        schema
            .fields()
            .map(|field| {
                value.take_if(|_| field.name().as_str() == name).unwrap_or_else(|| {
                    delta_kernel::expressions::Scalar::Null(field.data_type().clone())
                })
            })
            .collect::<::std::vec::Vec<_>>()
    }};
    let output = super::struct_data_encoder(struct_name, bounds, values);
    Ok(output)
}

pub(super) fn try_from_struct_data(struct_name: &Ident, variants: &[Variant<'_>]) -> TokenStream {
    let arms = variants.iter().map(|variant| {
        let ident = variant.ident;
        let payload = variant.payload;
        let name = &variant.name;
        quote! {
            #name => <#payload as TryFrom<delta_kernel::expressions::Scalar>>::try_from(value)
                .map(Self::#ident),
        }
    });
    let payloads = variants.iter().map(|v| v.payload);
    let result = quote! {{
        let (_, name, value) = fields.take_variant()?;
        match name {
            #(#arms)*
            _ => Err(delta_kernel::KernelError::internal_error(format!("Unknown enum variant field {name:?}"))),
        }.map_err(|error| delta_kernel::error::add_scalar_path_context(error, name))?
    }};
    super::struct_data_decoder(struct_name, payloads, result)
}

fn schema_name(ident: &Ident, attributes: &[Attribute]) -> Result<LitStr, Error> {
    let mut name = None;
    for attribute in attributes
        .iter()
        .filter(|a| a.path().is_ident("schema_rename"))
    {
        let value = attribute.meta.require_name_value().map_err(|_| {
            Error::new_spanned(attribute, "expected #[schema_rename = \"fieldName\"]")
        })?;
        let expr = &value.value;
        let value = syn::parse2::<LitStr>(quote! { #expr })
            .map_err(|_| Error::new_spanned(attribute, "schema name must be a string"))?;
        if value.value().is_empty() || name.replace(value).is_some() {
            return Err(Error::new_spanned(
                attribute,
                "schema name must be nonempty and specified once",
            ));
        }
    }
    if let Some(name) = name {
        return Ok(name);
    }
    let ident = ident.unraw();
    let camel = super::get_schema_name(&ident).to_string();
    let mut characters = camel.chars();
    let lower_camel = characters
        .next()
        .map(|c| c.to_lowercase().collect::<String>() + characters.as_str())
        .unwrap_or_default();
    Ok(LitStr::new(&lower_camel, ident.span()))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("struct AddFile {}", Some("addFile"))]
    #[case(r#"#[schema_rename = "add"] struct AddedFile {}"#, Some("add"))]
    #[case(r#"#[schema_rename = ""] struct Row {}"#, None)]
    #[case("#[schema_rename = 3] struct Row {}", None)]
    #[case(r#"#[schema_rename = "a"] #[schema_rename = "b"] struct Row {}"#, None)]
    fn enum_variant_name(#[case] input: &str, #[case] expected: Option<&str>) {
        let input = syn::parse_str::<DeriveInput>(input).unwrap();
        assert_eq!(
            schema_name(&input.ident, &input.attrs)
                .ok()
                .map(|name| name.value())
                .as_deref(),
            expected
        );
    }

    const ONE_PAYLOAD: &str = "enum variants must contain exactly one unnamed struct payload";
    const NO_GENERICS: &str = "struct-data enums do not support generic parameters";
    const UNIQUE_NAMES: &str = "duplicate enum schema field name";

    #[rstest]
    #[case::unit_variant("enum Action { Add }", ONE_PAYLOAD)]
    #[case::named_fields("enum Action { Add { id: i32 } }", ONE_PAYLOAD)]
    #[case::multiple_fields("enum Action { Add(i32, i64) }", ONE_PAYLOAD)]
    #[case::empty_variant("enum Action { Add() }", ONE_PAYLOAD)]
    #[case::type_parameter("enum Action<T> { Add(T) }", NO_GENERICS)]
    #[case::lifetime("enum Action<'a> { Add(&'a str) }", NO_GENERICS)]
    #[case::const_parameter("enum Action<const N: usize> { Add([u8; N]) }", NO_GENERICS)]
    #[case::same_name(
        "enum Action { Add(Add), #[schema_rename = \"add\"] Remove(Remove) }",
        UNIQUE_NAMES
    )]
    #[case::case_insensitive(
        "enum Action { Add(Add), #[schema_rename = \"ADD\"] Remove(Remove) }",
        UNIQUE_NAMES
    )]
    #[case::no_variants("enum Action {}", "struct-data enums require a variant")]
    fn invalid_enum_definitions_are_rejected(#[case] input: &str, #[case] expected: &str) {
        let input = syn::parse_str::<DeriveInput>(input).unwrap();
        assert_eq!(variants(&input).err().unwrap().to_string(), expected);
    }
}
