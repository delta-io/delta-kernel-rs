//! Translates the `expr!` grammar into Kernel expression builders. `syn` supplies Rust grouping
//! and comparison syntax; this module restricts calls to the constructors documented by `expr!`.
//! Arguments to `lit` and `path` remain Rust expressions, so callers can supply computed values.

use proc_macro2::{Delimiter, Group, Span, TokenStream, TokenTree};
use quote::{quote, quote_spanned};
use syn::ext::IdentExt;
use syn::parse::{ParseStream, Parser};
use syn::spanned::Spanned;
use syn::{BinOp, Error, Expr, ExprCall, ExprLit, Ident, Lit, Path, Token, UnOp};

/// Expands an expression preceded by the Kernel crate path supplied by the `macro_rules!` wrapper.
/// Keeping that path's tokens preserves `$crate` hygiene when a caller renames the dependency.
/// Returns builder tokens, or an error for unsupported syntax and invalid constructor arguments.
pub(crate) fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let parser = |input: ParseStream| {
        let kernel: Path = input.parse()?;
        input.parse::<Token![;]>()?;
        let expr: Expr = syn::parse2(escape_struct_constructor(input.parse()?))?;
        emit(&expr, &kernel)
    };
    parser.parse2(input)
}

fn emit(expr: &Expr, kernel: &Path) -> syn::Result<TokenStream> {
    match expr {
        Expr::Group(group) if group.attrs.is_empty() => emit(&group.expr, kernel),
        Expr::Paren(paren) if paren.attrs.is_empty() => emit(&paren.expr, kernel),
        Expr::Lit(literal) if literal.attrs.is_empty() => match &literal.lit {
            Lit::Str(_) | Lit::Bool(_) | Lit::Int(_) | Lit::Float(_) => {
                Ok(quote_spanned! { expr.span() =>
                    #kernel::expressions::lit(#expr)
                })
            }
            _ => Err(Error::new_spanned(
                expr,
                "expected a numeric, boolean, or string literal",
            )),
        },
        Expr::Unary(unary) if unary.attrs.is_empty() && matches!(unary.op, UnOp::Neg(_)) => {
            let numeric_literal = match ungroup(&unary.expr) {
                Expr::Lit(literal) if literal.attrs.is_empty() => {
                    matches!(literal.lit, Lit::Int(_) | Lit::Float(_))
                }
                _ => false,
            };
            if !numeric_literal {
                return Err(Error::new_spanned(
                    expr,
                    "only numeric literals can be negated; use lit() for Rust expressions",
                ));
            }
            Ok(quote_spanned! { expr.span() => #kernel::expressions::lit(#expr) })
        }
        Expr::Binary(binary) if binary.attrs.is_empty() => {
            let method = match binary.op {
                BinOp::Eq(_) => "eq",
                BinOp::Ne(_) => "ne",
                BinOp::Lt(_) => "lt",
                BinOp::Le(_) => "le",
                BinOp::Gt(_) => "gt",
                BinOp::Ge(_) => "ge",
                _ => {
                    return Err(Error::new_spanned(
                        binary.op,
                        "supported operators are ==, !=, <, <=, >, >=",
                    ));
                }
            };
            let method = Ident::new(method, binary.op.span());
            let left = emit(&binary.left, kernel)?;
            let right = emit(&binary.right, kernel)?;
            Ok(quote! {
                #kernel::expressions::Expression::from_pred(
                    #kernel::expressions::Predicate::#method(#left, #right),
                )
            })
        }
        Expr::Call(call) if call.attrs.is_empty() => emit_call(call, kernel),
        _ => Err(Error::new_spanned(
            expr,
            "expected an expr! constructor, literal, or comparison",
        )),
    }
}

fn emit_call(call: &ExprCall, kernel: &Path) -> syn::Result<TokenStream> {
    let method = match &*call.func {
        Expr::Path(path) if path.qself.is_none() && path.attrs.is_empty() => path.path.get_ident(),
        _ => None,
    }
    .ok_or_else(|| {
        Error::new_spanned(&call.func, "expected an unqualified expr! constructor name")
    })?;
    let name = method.unraw().to_string();
    let args = &call.args;
    if name == "col" {
        let path = match args.first().filter(|_| args.len() == 1).map(ungroup) {
            Some(Expr::Lit(ExprLit {
                attrs,
                lit: Lit::Str(path),
                ..
            })) if attrs.is_empty() => path,
            _ => {
                return Err(Error::new_spanned(
                    call,
                    "col() requires one string literal; \
                     use path() for explicit or computed column segments",
                ));
            }
        };
        return Ok(quote! { #kernel::expressions::col!(#path) });
    }
    let arity = match name.as_str() {
        "lit" | "null" | "not" | "is_null" | "is_not_null" => Some(1),
        "eq" | "ne" | "lt" | "le" | "gt" | "ge" | "distinct" => Some(2),
        "coalesce" | "and" | "or" | "path" | "struct" | "guarded_struct" => None,
        _ => {
            return Err(Error::new_spanned(
                method,
                format!("unsupported expression call: {name}"),
            ));
        }
    };
    if let Some(arity) = arity
        && args.len() != arity
    {
        return Err(Error::new_spanned(
            call,
            format!(
                "{name}() requires {arity} argument{}",
                if arity == 1 { "" } else { "s" }
            ),
        ));
    }
    match name.as_str() {
        "lit" => Ok(quote! { #kernel::expressions::lit(#args) }),
        "null" => {
            let ty = args
                .first()
                .and_then(|arg| match ungroup(arg) {
                    Expr::Path(path) if path.qself.is_none() && path.attrs.is_empty() => {
                        path.path.get_ident()
                    }
                    _ => None,
                })
                .ok_or_else(|| {
                    Error::new_spanned(
                        call,
                        "null() requires a DataType constant name, e.g. null(LONG)",
                    )
                })?;
            Ok(quote! { #kernel::expressions::null_lit(#kernel::schema::DataType::#ty) })
        }
        "path" => {
            if args.is_empty() {
                return Err(Error::new_spanned(
                    call,
                    "path() requires at least one column segment",
                ));
            }
            let segments = args.iter();
            Ok(quote! {
                #kernel::expressions::Expression::column([
                    #(::std::convert::Into::<::std::string::String>::into(#segments)),*
                ])
            })
        }
        "guarded_struct" => {
            let mut args = args.iter();
            let guard = args.next().ok_or_else(|| {
                Error::new_spanned(call, "guarded_struct() requires a guard before its fields")
            })?;
            let guard = emit(guard, kernel)?;
            let fields = args
                .map(|arg| emit(arg, kernel))
                .collect::<syn::Result<Vec<_>>>()?;
            let guard_binding = Ident::new("__kernel_struct_guard", Span::mixed_site());
            // The builder takes fields first. Bind the guard first without capturing names in
            // caller-supplied field arguments.
            Ok(quote! {{
                let #guard_binding = #guard;
                #kernel::expressions::Expression::struct_with_nullability_from(
                    ::std::vec::Vec::<#kernel::expressions::Expression>::from([#(#fields),*]),
                    #guard_binding,
                )
            }})
        }
        _ => {
            let children = args
                .iter()
                .map(|arg| emit(arg, kernel))
                .collect::<syn::Result<Vec<_>>>()?;
            match name.as_str() {
                "coalesce" => Ok(quote! {
                    #kernel::expressions::Expression::coalesce(
                        ::std::vec::Vec::<#kernel::expressions::Expression>::from([#(#children),*]),
                    )
                }),
                "struct" => Ok(quote! {
                    #kernel::expressions::Expression::struct_from(
                        ::std::vec::Vec::<#kernel::expressions::Expression>::from([#(#children),*]),
                    )
                }),
                "and" | "or" => {
                    let junction = Ident::new(&format!("{name}_from"), method.span());
                    Ok(quote! {
                        #kernel::expressions::Expression::from_pred(
                            #kernel::expressions::Predicate::#junction([
                                #(#kernel::expressions::Predicate::from_expr(#children)),*
                            ]),
                        )
                    })
                }
                "not" => Ok(quote! {
                    #kernel::expressions::Expression::from_pred(
                        #kernel::expressions::Predicate::not(
                            #(#kernel::expressions::Predicate::from_expr(#children)),*
                        ),
                    )
                }),
                _ => Ok(quote! {
                    #kernel::expressions::Expression::from_pred(
                        #kernel::expressions::Predicate::#method(#(#children),*),
                    )
                }),
            }
        }
    }
}

// syn rejects `struct(...)`, so parse it as `r#struct(...)`. Keep Rust arguments of lit() and
// path() verbatim: their macros may depend on the original tokens.
fn escape_struct_constructor(input: TokenStream) -> TokenStream {
    let mut tokens = input.into_iter().peekable();
    let mut escaped = TokenStream::new();
    while let Some(token) = tokens.next() {
        match token {
            TokenTree::Ident(ident) => {
                let name = ident.unraw().to_string();
                let ident = if name == "struct" {
                    Ident::new_raw("struct", ident.span())
                } else {
                    ident
                };
                escaped.extend([TokenTree::Ident(ident)]);
                if matches!(name.as_str(), "lit" | "path")
                    && matches!(tokens.peek(), Some(TokenTree::Group(group))
                        if group.delimiter() == Delimiter::Parenthesis)
                {
                    escaped.extend(tokens.next());
                }
            }
            TokenTree::Group(group) => {
                let mut escaped_group =
                    Group::new(group.delimiter(), escape_struct_constructor(group.stream()));
                escaped_group.set_span(group.span());
                escaped.extend([TokenTree::Group(escaped_group)]);
            }
            other => escaped.extend([other]),
        }
    }
    escaped
}

// Forwarded `:literal` and `:expr` fragments arrive in invisible groups, which must preserve the
// same constructor rules as the original tokens. Explicit parentheses remain part of the syntax.
fn ungroup(mut expr: &Expr) -> &Expr {
    while let Expr::Group(group) = expr
        && group.attrs.is_empty()
    {
        expr = &group.expr;
    }
    expr
}

#[cfg(test)]
mod tests {
    use proc_macro2::TokenStream;
    use quote::quote;
    use rstest::rstest;

    use super::expand;

    #[rstest]
    #[case::unknown_call(quote!(custom_predicate("a")), "unsupported expression call")]
    #[case::unknown_nested_call(quote!("a" > custom_predicate("b")), "unsupported expression call")]
    #[case::missing_unary_arg(quote!(not()), "requires 1 argument")]
    #[case::extra_unary_arg(quote!(not("a", "b")), "requires 1 argument")]
    #[case::missing_binary_arg(quote!(gt("a")), "requires 2 argument")]
    #[case::extra_binary_arg(quote!(eq("a", 1, 2)), "requires 2 argument")]
    #[case::extra_null_predicate_arg(quote!(is_null("a", "b")), "requires 1 argument")]
    #[case::missing_struct_guard(quote!(guarded_struct()), "requires a guard")]
    #[case::invalid_struct_field(quote!(struct(value)), "expected an expr! constructor")]
    #[case::invalid_struct_guard(quote!(guarded_struct(guard, 1)), "expected an expr! constructor")]
    #[case::unknown_struct_field(quote!(struct(unknown(1))), "unsupported expression call")]
    #[case::unknown_struct_guard(quote!(guarded_struct(unknown(1))), "unsupported expression call")]
    #[case::empty_path(quote!(path()), "at least one column segment")]
    #[case::missing_literal_arg(quote!(lit()), "requires 1 argument")]
    #[case::extra_literal_arg(quote!(lit(1, 2)), "requires 1 argument")]
    #[case::computed_null_type(quote!(null(decimal(10, 2))), "DataType constant name")]
    #[case::qualified_null_type(quote!(null(DataType::LONG)), "DataType constant name")]
    #[case::add(quote!("a" + 1), "supported operators")]
    #[case::subtract(quote!("a" - 1), "supported operators")]
    #[case::multiply(quote!("a" * 1), "supported operators")]
    #[case::divide(quote!("a" / 1), "supported operators")]
    #[case::rust_and(quote!(true && false), "supported operators")]
    #[case::bit_or(quote!(true | false), "supported operators")]
    #[case::assignment(quote!("a" = 1), "expected an expr! constructor")]
    #[case::bare_variable(quote!("a" >= threshold), "expected an expr! constructor")]
    #[case::method_call(quote!("a".gt(1)), "expected an expr! constructor")]
    #[case::qualified_call(quote!(module::eq("a", 1)), "unqualified expr! constructor")]
    #[case::generic_call(quote!(eq::<i64>("a", 1)), "unqualified expr! constructor")]
    #[case::type_qualified_call(quote!(<T>::eq("a", 1)), "unqualified expr! constructor")]
    #[case::type_qualified_null(quote!(null(<T>::LONG)), "DataType constant name")]
    #[case::rust_not(quote!(!true), "expected an expr! constructor")]
    #[case::negated_string(quote!(-"a"), "only numeric literals can be negated")]
    #[case::negated_boolean(quote!(-true), "only numeric literals can be negated")]
    #[case::negated_call(quote!(-col("a")), "only numeric literals can be negated")]
    #[case::bare_char(quote!('a'), "numeric, boolean, or string literal")]
    #[case::bare_bytes(quote!(b"a"), "numeric, boolean, or string literal")]
    #[case::attribute(quote!(#[allow(unused)] "a"), "expected an expr! constructor")]
    fn rejects_invalid_syntax_with_diagnostic(
        #[case] input: TokenStream,
        #[case] diagnostic: &str,
    ) {
        let error = expand(quote! { kernel; #input }).expect_err("expected invalid expr! syntax");
        assert!(error.to_string().contains(diagnostic), "{error}");
    }

    #[rstest]
    #[case::computed(quote!(col(COLUMN)))]
    #[case::empty(quote!(col()))]
    #[case::extra_argument(quote!(col("a", "b")))]
    #[case::numeric(quote!(col(42)))]
    #[case::attribute(quote!(col(#[allow(unused)] "a")))]
    fn invalid_col_arguments_include_path_hint(#[case] input: TokenStream) {
        let error =
            expand(quote! { kernel; #input }).expect_err("expected invalid col() arguments");
        assert_eq!(
            error.to_string(),
            "col() requires one string literal; \
             use path() for explicit or computed column segments",
        );
    }

    #[rstest]
    #[case::empty(quote!())]
    #[case::chained_comparisons(quote!(0 < "a" < 10))]
    #[case::missing_operand(quote!("a" >=))]
    #[case::missing_comma(quote!(and("a" > 0 "b" < 1)))]
    #[case::extra_tokens(quote!("a" > 1; "b"))]
    #[case::missing_struct_comma(quote!(struct(1 2)))]
    #[case::missing_struct_guard(quote!(guarded_struct(, 1)))]
    fn rejects_structurally_invalid_input(#[case] input: TokenStream) {
        assert!(expand(quote! { kernel; #input }).is_err());
    }
}
