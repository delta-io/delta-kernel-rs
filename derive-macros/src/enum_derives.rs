use std::collections::HashSet;

use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, LitStr, Type};

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
            let name = crate::schema_name(&variant.ident, &variant.attrs)?;
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
    let arms = variants.iter().enumerate().map(|(index, variant)| {
        let ident = variant.ident;
        quote! { #struct_name::#ident(value) => values[#index] =
        delta_kernel::expressions::Scalar::Struct(value.into()), }
    });
    Ok(super::struct_data_encoder(
        struct_name,
        quote! { #(#payloads: Into<delta_kernel::expressions::StructData>,)* },
        quote! {{
            let mut values: ::std::vec::Vec<_> = schema.fields().map(|field|
                delta_kernel::expressions::Scalar::Null(field.data_type().clone())
            ).collect();
            match value { #(#arms)* }
            values
        }},
    ))
}

pub(super) fn try_from_struct_data(struct_name: &Ident, variants: &[Variant<'_>]) -> TokenStream {
    let conversions = variants.iter().map(|variant| {
        let ident = variant.ident;
        let field_name = &variant.name;
        let payload = variant.payload;
        quote! {
            .or(fields.take_field::<Option<#payload>>(#field_name)?.map(Self::#ident))
        }
    });
    super::struct_data_decoder(
        struct_name,
        variants.iter().map(|v| v.payload),
        quote! {
            delta_kernel::schema::derive_macro_utils::validate_variant(value.values())?;
        },
        quote! {
            None #(#conversions)* .ok_or_else(||
                delta_kernel::KernelError::invalid_struct_data(
                    "Expected exactly one non-null variant, found 0",
                )
            )?
        },
    )
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

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
