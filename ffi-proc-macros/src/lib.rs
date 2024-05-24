use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, Error, Fields, FieldsNamed, FieldsUnnamed, Ident, ItemStruct, LitBool,
    Result, Token, Type,
};

struct HandleDescriptorParams {
    target: Type,
    mutable: bool,
    sized: bool,
}

impl Parse for HandleDescriptorParams {
    fn parse(input: ParseStream) -> Result<Self> {
        // Parse the "target=..." field (always first)
        let key: Ident = input.parse()?;
        if let "target" = key.to_string().as_str() {
            let _: Token![=] = input.parse()?;
        } else {
            return Err(Error::new(key.span(), "expected `target` field"));
        }
        let target: Type = input.parse()?;
        let is_trait = match target {
            Type::Path(_) => false,
            Type::TraitObject(_) => true,
            _ => {
                return Err(Error::new(
                    target.span(),
                    "expected a type or a trait object",
                ))
            }
        };

        // Parse the remaining fields (unordered)
        let mut mutable = None;
        let mut sized = None;
        while !input.is_empty() {
            let _: Token![,] = input.parse()?;
            let ident: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            let value: LitBool = input.parse()?;
            match ident.to_string().as_str() {
                "mutable" if mutable.is_none() => {
                    mutable = Some(value.value());
                }
                "sized" if sized.is_none() => {
                    if value.value() && is_trait {
                        return Err(Error::new(value.span(), "trait objects are never Sized"));
                    }
                    sized = Some(value.value());
                }
                field => {
                    return Err(Error::new(
                        ident.span(),
                        format!("unknown or duplicated field `{}`", field),
                    ));
                }
            }
        }
        let mutable = mutable.ok_or_else(|| input.error("missing `mutable` field"))?;
        if sized.is_none() && is_trait {
            sized = Some(false);
        }
        let sized = sized.ok_or_else(|| input.error("missing `sized` field"))?;
        Ok(Self {
            target,
            mutable,
            sized,
        })
    }
}

fn bool_to_boolean(b: bool) -> TokenStream2 {
    let name = if b {
        quote! { True }
    } else {
        quote! { False }
    };
    quote! { delta_kernel_ffi::handle::#name }
}

/// Macro for conveniently deriving a `delta_kernel_ffi::handle::HandleDescriptor`.
///
/// When targeting a struct, it is invoked with three arguments:
/// ```ignore
/// #[handle_descriptor(target = Foo, mutable = false. sized = true)]
/// pub struct SharedFoo;
/// ```
///
/// When targeting a trait, two arguments suffice (`sized = false` is implied):
/// ```ignore
/// #[handle_descriptor(target = dyn Bar, mutable = true)]
/// pub struct MutableBar;
/// ```
#[proc_macro_attribute]
pub fn handle_descriptor(attr: TokenStream, item: TokenStream) -> TokenStream {
    let descriptor_params = parse_macro_input!(attr as HandleDescriptorParams);
    let mut st = parse_macro_input!(item as ItemStruct);
    match st.fields {
        Fields::Named(FieldsNamed {
            named: ref fields, ..
        })
        | Fields::Unnamed(FieldsUnnamed {
            unnamed: ref fields,
            ..
        }) if !fields.is_empty() => {
            return Error::new(fields.span(), "A handle struct must not define any fields")
                .into_compile_error()
                .into();
        }
        _ => {}
    };

    // Inject a single unconstructible member to produce a NZST
    let ident = &st.ident;
    let new_struct = quote! { struct #ident(delta_kernel_ffi::handle::Unconstructable); };
    let new_struct = new_struct.into();
    let new_struct = parse_macro_input!(new_struct as ItemStruct);
    st.fields = new_struct.fields;

    // Emit the modified struct and also derive the impl HandleDescriptor for it.
    let mutable = bool_to_boolean(descriptor_params.mutable);
    let sized = bool_to_boolean(descriptor_params.sized);
    let target = &descriptor_params.target;
    let descriptor_impl = quote! {
        #[automatically_derived]
        impl delta_kernel_ffi::handle::HandleDescriptor for #ident {
            type Target = #target;
            type Mutable = #mutable;
            type Sized = #sized;
        }
    };
    quote_spanned! { st.span() => #st #descriptor_impl }.into()
}
