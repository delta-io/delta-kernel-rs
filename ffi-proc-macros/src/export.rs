use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::parse::Parser;
use syn::{Error, Expr, GenericArgument, Ident, ItemFn, PathArguments, Result, ReturnType, Type};

pub(super) fn expand(args: TokenStream, item: TokenStream) -> Result<TokenStream> {
    let mut allocator = None;
    syn::meta::parser(|meta| {
        if !meta.path.is_ident("error_allocator") || allocator.is_some() {
            return Err(meta.error("expected one `error_allocator`"));
        }
        let source: Expr = meta.value()?.parse()?;
        allocator = Some(match source {
            Expr::Field(field) => {
                let owner = field.base;
                let member = field.member;
                quote!(unsafe { (#owner).as_ref() }.#member.clone())
            }
            Expr::Path(path) => quote!(unsafe { (#path).as_ref() }),
            expression => quote!(#expression),
        });
        Ok(())
    })
    .parse2(args)?;
    let allocator =
        allocator.ok_or_else(|| Error::new(Span::call_site(), "missing `error_allocator`"))?;
    let mut function: ItemFn = syn::parse2(item)?;
    let signature = &function.sig;
    if signature
        .abi
        .as_ref()
        .and_then(|abi| abi.name.as_ref())
        .map(|name| name.value())
        != Some("C".into())
        || signature.asyncness.is_some()
        || signature.constness.is_some()
        || !signature.generics.params.is_empty()
        || signature.variadic.is_some()
    {
        return Err(Error::new_spanned(
            signature,
            "expected a nongeneric `extern \"C\"` function",
        ));
    }
    let ReturnType::Type(_, result) = &signature.output else {
        return Err(Error::new_spanned(signature, "expected `ExternResult<T>`"));
    };
    let Type::Path(path) = result.as_ref() else {
        return Err(Error::new_spanned(result, "expected `ExternResult<T>`"));
    };
    let segment = path
        .path
        .segments
        .last()
        .filter(|segment| segment.ident == "ExternResult")
        .ok_or_else(|| Error::new_spanned(result, "expected `ExternResult<T>`"))?;
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(Error::new_spanned(result, "expected `ExternResult<T>`"));
    };
    if !matches!(args.args.first(), Some(GenericArgument::Type(_))) || args.args.len() != 1 {
        return Err(Error::new_spanned(result, "expected `ExternResult<T>`"));
    }
    let body = &function.block;
    let retained = Ident::new("__ffi_allocator", Span::mixed_site());
    let caught = Ident::new("__ffi_result", Span::mixed_site());
    let catch = quote_spanned!(signature.ident.span() => crate::panic::catch_panic(|| #body));
    function.block = syn::parse2(quote!({
        let #retained = #allocator;
        let #caught = #catch;
        unsafe { crate::error::IntoExternResult::into_extern_result(#caught, &#retained) }
    }))?;
    Ok(quote!(#function))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("error_allocator = ()", "fn f() -> ExternResult<()> {}", "extern")]
    #[case(
        "error_allocator = ()",
        "extern \"C-unwind\" fn f() -> ExternResult<()> {}",
        "extern"
    )]
    #[case(
        "error_allocator = ()",
        "extern \"C\" fn f() -> usize {}",
        "ExternResult"
    )]
    #[case("", "extern \"C\" fn f() -> ExternResult<()> {}", "missing")]
    #[case(
        "error_allocator = (), error_allocator = ()",
        "extern \"C\" fn f() -> ExternResult<()> {}",
        "one"
    )]
    #[case(
        "error_allocator = ()",
        "extern \"C\" fn f<T>() -> ExternResult<()> {}",
        "nongeneric"
    )]
    fn rejects_invalid_exports(#[case] args: &str, #[case] function: &str, #[case] message: &str) {
        let error = expand(args.parse().unwrap(), function.parse().unwrap()).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }
}
