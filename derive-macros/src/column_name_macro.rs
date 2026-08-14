//! Parser and code generator for [`column_name!`](delta_kernel::expressions::column_name) /
//! [`col!`](delta_kernel::expressions::col). `(seg)` inserts one path segment; `..(path)` splices
//! the segments of anything [`ColumnName::new`](delta_kernel::expressions::ColumnName::new)
//! accepts.
//!
//! # Grammar
//!
//! ```text
//! args := (arg ',')* arg?                      // 1+ comma-separated args, optional trailing comma
//! arg  := STR_LITERAL                          // dot-separated path; each segment validated now
//!       | EXPR                                 // const &str segment (validated at const-eval)
//!       | '(' EXPR ')'                         // one runtime segment (`impl Into<String>`)
//!       | '..' '(' EXPR ')'                    // splice (`impl CollectInto<ColumnName>`)
//! ```

use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::parse::{ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token::Paren;
use syn::{parenthesized, Expr, ExprLit, Lit, LitStr, Token};

enum Arg {
    /// Compile-time string literal, already split and validated into segments.
    LiteralSegments(Vec<LitStr>),
    /// Bare expression treated as a const `&str` segment (validated in a `const` context).
    ConstSegment(Expr),
    /// `(expr)` — one runtime segment.
    RuntimeSegment(Expr),
    /// `..(expr)` — splice a path / segment iterator via `ColumnName::new`.
    Splice(Expr),
}

/// Emits a [`ColumnName`](delta_kernel::expressions::ColumnName) construction for the macro body.
pub(crate) fn parse_column_name(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    match try_parse_column_name(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn try_parse_column_name(input: TokenStream) -> syn::Result<TokenStream> {
    let parser = |input: ParseStream| {
        Ok(Vec::from_iter(
            Punctuated::<Arg, Token![,]>::parse_terminated_with(input, parse_arg)?,
        ))
    };
    let args = parser.parse2(input)?;
    if args.is_empty() {
        return Err(syn::Error::new(
            Span::call_site(),
            "column_name! requires at least one argument",
        ));
    }

    let has_runtime = args
        .iter()
        .any(|a| matches!(a, Arg::RuntimeSegment(_) | Arg::Splice(_)));
    if has_runtime {
        emit_runtime_builder(args)
    } else {
        emit_const_segments(args)
    }
}

fn parse_arg(input: ParseStream) -> syn::Result<Arg> {
    // `..(expr)` — splice. Require parens so `..` is not ambiguous with other token forms.
    if input.peek(Token![..]) {
        let dotdot = input.parse::<Token![..]>()?;
        if !input.peek(Paren) {
            return Err(syn::Error::new(
                dotdot.span(),
                "expected `(...)` after `..` (splice a ColumnName or segment iterator)",
            ));
        }
        let expr = parse_paren_expr(input)?;
        return Ok(Arg::Splice(expr));
    }
    // `(expr)` — one runtime segment.
    if input.peek(Paren) {
        return Ok(Arg::RuntimeSegment(parse_paren_expr(input)?));
    }

    let expr: Expr = input.parse()?;
    let mut inner = &expr;
    while let Expr::Group(group) = inner {
        inner = &group.expr;
    }
    match inner {
        Expr::Lit(ExprLit {
            lit: Lit::Str(lit_str),
            ..
        }) => {
            let mut segments = Vec::new();
            for segment in lit_str.value().split('.') {
                validate_single_segment(segment, lit_str.span())?;
                segments.push(LitStr::new(segment, lit_str.span()));
            }
            Ok(Arg::LiteralSegments(segments))
        }
        _ => Ok(Arg::ConstSegment(expr)),
    }
}

fn parse_paren_expr(input: ParseStream) -> syn::Result<Expr> {
    let content;
    parenthesized!(content in input);
    let expr: Expr = content.parse()?;
    if content.is_empty() {
        Ok(expr)
    } else {
        Err(content.error("unexpected tokens inside `(...)`"))
    }
}

fn validate_single_segment(segment: &str, span: Span) -> syn::Result<()> {
    if segment.is_empty() {
        return Err(syn::Error::new(span, "empty column name segment"));
    }
    if let Some(bad) = segment
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_'))
    {
        return Err(syn::Error::new(
            span,
            format!("invalid character {bad:?} in column name segment {segment:?}"),
        ));
    }
    Ok(())
}

/// Emits `__require_valid_simple_column_segment(expr)` (must be used in a `const` context).
fn emit_validated_const_segment(expr: &Expr) -> TokenStream {
    quote_spanned! { expr.span() =>
        match ::delta_kernel::expressions::__require_valid_simple_column_segment(#expr) {
            Some(segment) => segment,
            None => panic!(
                "String constants passed to column_name! must be simple names \
                 matching [a-zA-Z0-9_]+; use a string literal for dot-separated \
                 paths, `(seg)` for a runtime segment, or `..(path)` to splice a \
                 ColumnName"
            ),
        }
    }
}

/// All-compile-time path: emit `&[&str]` for `ColumnName::new(SEGMENTS.iter().copied())`.
fn emit_const_segments(args: Vec<Arg>) -> syn::Result<TokenStream> {
    let mut emitted = Vec::new();
    for arg in args {
        match arg {
            Arg::LiteralSegments(segments) => {
                for seg in segments {
                    emitted.push(quote_spanned! { seg.span() => #seg });
                }
            }
            Arg::ConstSegment(expr) => {
                emitted.push(emit_validated_const_segment(&expr));
            }
            Arg::RuntimeSegment(_) | Arg::Splice(_) => unreachable!("filtered by caller"),
        }
    }
    Ok(quote! {{
        const SEGMENTS: &[&str] = &[ #(#emitted),* ];
        ::delta_kernel::expressions::ColumnName::new(SEGMENTS.iter().copied())
    }})
}

/// Mixed/runtime path: build a `Vec<String>` with push / extend, then `ColumnName::new`.
fn emit_runtime_builder(args: Vec<Arg>) -> syn::Result<TokenStream> {
    // Use a single call-site ident for the scratch vec. `quote_spanned!` on push/extend stmts
    // would otherwise give `__segments` the interpolated expr's span, breaking hygiene when this
    // proc macro is reached through the `col!` / `column_expr!` macro_rules forwarders.
    let segments = syn::Ident::new("__dk_column_name_segments", Span::call_site());
    let mut stmts = Vec::new();
    for arg in args {
        match arg {
            Arg::LiteralSegments(segs) => {
                for seg in segs {
                    stmts.push(quote_spanned! { seg.span() =>
                        #segments.push(::std::convert::Into::into(#seg));
                    });
                }
            }
            Arg::ConstSegment(expr) => {
                // Keep bare args in a `const` context so non-constants still fail to compile.
                let validated = emit_validated_const_segment(&expr);
                stmts.push(quote_spanned! { expr.span() =>
                    {
                        const __SEG: &str = #validated;
                        #segments.push(::std::convert::Into::into(__SEG));
                    }
                });
            }
            Arg::RuntimeSegment(expr) => {
                stmts.push(quote_spanned! { expr.span() =>
                    #segments.push(::std::convert::Into::<::std::string::String>::into(#expr));
                });
            }
            Arg::Splice(expr) => {
                // Reuse ColumnName::new / CollectInto so splice accepts the same inputs as `new`
                // (ColumnName, &ColumnName, segment iterators, ...).
                stmts.push(quote_spanned! { expr.span() =>
                    #segments.extend(::delta_kernel::expressions::ColumnName::new(#expr));
                });
            }
        }
    }
    Ok(quote! {{
        let mut #segments = ::std::vec::Vec::<::std::string::String>::new();
        #(#stmts)*
        ::delta_kernel::expressions::ColumnName::new(#segments)
    }})
}
