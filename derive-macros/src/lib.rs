use proc_macro2::{Ident, Spacing, TokenStream, TokenTree};
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, Attribute, Data, DataStruct, DeriveInput, Fields, Meta, PathArguments, Type,
};

static SCHEMA_ERR_STR: &str = "schema(...) only supports schema(name = name)";

// Return the ident to use as the schema name if it's been specified in the attributes of the struct
fn get_schema_name_from_attr<'a>(attrs: impl Iterator<Item = &'a Attribute>) -> Option<Ident> {
    for attr in attrs {
        if let Meta::List(list) = &attr.meta {
            if let Some(attr_name) = list.path.segments.iter().last() {
                if attr_name.ident == "schema" {
                    // We have some schema(...) attribute, see if we've specified a different name
                    let tokens: Vec<TokenTree> = list.tokens.clone().into_iter().collect();
                    match tokens[..] {
                        // we only support `name = name` style
                        [TokenTree::Ident(ref name_ident), TokenTree::Punct(ref punct), TokenTree::Ident(ref schema_ident)] =>
                        {
                            assert!(name_ident == "name", "{}", SCHEMA_ERR_STR);
                            assert!(punct.as_char() == '=', "{}", SCHEMA_ERR_STR);
                            assert!(punct.spacing() == Spacing::Alone, "{}", SCHEMA_ERR_STR);
                            return Some(schema_ident.clone());
                        }
                        _ => panic!("{}", SCHEMA_ERR_STR),
                    }
                } else {
                    panic!("Schema only accepts `schema` as an extra attribute")
                }
            }
        }
    }
    None
}

#[proc_macro_derive(Schema, attributes(schema))]
pub fn derive_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_ident = input.ident;
    let schema_name = get_schema_name_from_attr(input.attrs.iter()).unwrap_or_else(|| {
        // default to the struct name, but lowercased
        Ident::new(
            &struct_ident.to_string().to_lowercase(),
            struct_ident.span(),
        )
    });

    let schema_fields = gen_schema_fields(&input.data);
    let output = quote! {
        impl crate::actions::schemas::GetSchema for #struct_ident {
            fn get_schema() -> &'static crate::schema::StructField {
                use crate::actions::schemas::GetField;
                static SCHEMA_LOCK: std::sync::OnceLock<crate::schema::StructField> = std::sync::OnceLock::new();
                SCHEMA_LOCK.get_or_init(|| {
                    Self::get_field(stringify!(#schema_name))
                })
            }
        }

        impl crate::actions::schemas::GetField for #struct_ident {
            fn get_field(name: impl Into<String>) -> crate::schema::StructField {
                use crate::actions::schemas::GetField;
                crate::schema::StructField::new(
                    name,
                    crate::schema::StructType::new(vec![
                        #schema_fields
                    ]),
                    // TODO: Ensure correct. By default not nullable, only can be made nullable by
                    // being wrapped in an Option
                    false,
                )
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
        _ => panic!("this derive macro only works on structs with named fields"),
    };

    let schema_fields = fields.iter().map(|field| {
        let name = field.ident.as_ref().unwrap(); // we know these are named fields
        let name = get_schema_name(name);
        match field.ty {
            Type::Path(ref type_path) => {
                if let Some(fin) = type_path.path.segments.iter().last() {
                    let type_ident = &fin.ident;
                    if let PathArguments::AngleBracketed(angle_args) = &fin.arguments {
                        quote_spanned! {field.span()=>
                                        #type_ident::#angle_args::get_field(stringify!(#name))
                        }
                    } else {
                        quote_spanned! {field.span()=>
                                        #type_ident::get_field(stringify!(#name))
                        }
                    }
                } else {
                    panic!("Couldn't get type");
                }
            }
            _ => {
                panic!("Can't handle type: {:?}", field.ty);
            }
        }
    });
    quote! { #(#schema_fields),* }
}
