use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, Data, DataStruct, DeriveInput, Fields, GenericArgument, PathArguments,
    PathSegment, Type,
};

#[proc_macro_derive(Schema)]
pub fn derive_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_ident = input.ident;
    let schema_fields = gen_schema_fields(&input.data);
    let output = quote! {
        impl crate::actions::GetSchema for #struct_ident {
            fn get_schema() -> crate::schema::StructField {
                crate::schema::StructField::new(
                    // TODO: this is wrong, we probably need an attribute for this since it doesn't
                    // exactly follow the struct name
                    stringify!(#struct_ident),
                    crate::schema::StructType::new(vec![
                        #schema_fields
                    ]),
                    true, // TODO: how to determine nullable
                )
            }
        }
    };
    proc_macro::TokenStream::from(output)
}

/// Get the generic types out of something like Option<Generic> or HashMap<Generic, Generic>
fn get_generic_types(path_args: &PathArguments) -> TokenStream {
    if let PathArguments::AngleBracketed(gen_args) = path_args {
        let types = gen_args.args.iter().map(|arg| {
            if let GenericArgument::Type(typ) = arg {
                match typ {
                    Type::Path(ref type_path) => {
                        if let Some(fin) = type_path.path.segments.iter().last() {
                            get_data_type(fin)
                        } else {
                            panic!("Path for generic type must have last")
                        }
                    }
                    _ => panic!("Generic needs type path"),
                }
            } else {
                panic!("Only support schema for literal generic types")
            }
        });
        quote! {
            #(#types),*
        }
    } else {
        panic!("Can only handle angle bracketed (i.e. <>) generic args")
    }
}

fn get_data_type(path_segment: &PathSegment) -> Option<TokenStream> {
    let name = path_segment.ident.to_string();
    match name.as_str() {
        "String" => Some(quote! { crate::schema::DataType::STRING }),
        "i64" => Some(quote! { crate::schema::DataType::LONG }),
        "i32" => Some(quote! { crate::schema::DataType::INTEGER }),
        "i16" => Some(quote! { crate::schema::DataType::SHORT }),
        "char" => Some(quote! { crate::schema::DataType::BYTE }), // TODO: Correct rust type
        "f32" => Some(quote! { crate::schema::DataType::FLOAT }),
        "f64" => Some(quote! { crate::schema::DataType::DOUBLE }),
        "bool" => Some(quote! { crate::schema::DataType::BOOLEAN }),
        // TODO: Binary, Date and Timestamp rust types
        "HashMap" => {
            let types = get_generic_types(&path_segment.arguments);
            Some(quote! {
                crate::schema::MapType::new(#types, true) // TODO (how to determine if value contains null)
            })
        }
        "Option" => {
            let option_type = get_generic_types(&path_segment.arguments);
            if option_type.to_string() == "" {
                // This indicates that the thing in the option didn't have a directly known data type
                // TODO get_generic_types should probably return an Option
                None
            } else {
                Some(quote! { #option_type })
            }
        }
        "Vec" => {
            let vec_type = get_generic_types(&path_segment.arguments);
            Some(quote! {
                crate::schema::ArrayType::new(#vec_type, false) // TODO (how to determine if contains null)
            })
        }
        _ => {
            // assume it's a struct type that implements get_schema, will be handled in called
            None
        }
    }
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
        match field.ty {
            Type::Path(ref type_path) => {
                if let Some(fin) = type_path.path.segments.iter().last() {
                    let optional = fin.ident == "Option";
                    if let Some(schema_type) = get_data_type(fin) {
                        quote_spanned! {field.span()=>
                                        crate::schema::StructField::new(stringify!(#name), #schema_type, #optional)
                        }
                    } else {
                        // Return of None means it's some type we don't support directly, just
                        // assume it implements GetSchema
                        let ident = if optional {
                            // if optional, use name of inside thing
                            // TODO: this is similar to get_generic_types, unify
                            if let PathArguments::AngleBracketed(gen_args) = &fin.arguments {
                                assert!(gen_args.args.iter().len() == 1);
                                match gen_args.args.iter().next().unwrap() {
                                    GenericArgument::Type(typ) => {
                                        match typ {
                                            Type::Path(ref type_path) => {
                                                if let Some(fin) = type_path.path.segments.iter().last() {
                                                    fin.ident.clone()
                                                } else {
                                                    panic!("Path for generic type must have last")
                                                }
                                            }
                                            _ => panic!("Generic needs type path")
                                        }
                                    }
                                    _ => panic!("Option must be a generic arg"),
                                }
                            } else {
                                panic!("Need angle bracketed Option types");
                            }
                        } else {
                            fin.ident.clone()
                        };
                        quote_spanned! {field.span()=>
                                        #ident::get_schema()
                        }
                    }
                } else {
                    panic!("Cound't get type");
                }
            }
            _ => { panic!("Can't handle type: {:?}", field.ty); }
        }
    });
    quote! { #(#schema_fields),* }
}
