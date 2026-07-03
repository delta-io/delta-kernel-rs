//! Parser and code generator for the `schema!` / `try_schema!` / `schema_ref!` function-like
//! macros. The grammar mirrors a Delta schema (the protocol `schemaString`) while letting the
//! caller interpolate arbitrary runtime values, in the spirit of `serde_json::json!`. See the
//! public macro docs in `delta_kernel::schema` for the full user-facing description.
//!
//! # Grammar
//!
//! ```text
//! body  := (entry ',')* entry?                 // 0+ comma-separated entries, optional trailing comma
//! entry := nullability name ':' type           // possibly nullable struct field
//!        | '(' EXPR ')'                        // interpolate one StructField
//!        | '..' '(' EXPR ')'                   // splice an `impl IntoIterator<Item = StructField>`
//! nullability := 'nullable' | 'not_null'
//! name  := STR_LITERAL
//!        | IDENT
//!        | '(' EXPR ')'                        // interpolate an `impl Into<String>`
//! type  := '[' nullability type ']'            // array with possibly-nullable elements
//!        | '{' body '}'                        // nested struct
//!        | '{' type '=>' nullability type '}'  // map with possibly-nullable values
//!        | '(' EXPR ')'                        // interpolate an `impl Into<DataType>`
//!        | IDENT                               // interpolate `DataType::<IDENT>`
//! ```
//!
//! # Design
//!
//! A recursive-descent parser that emits schema-building code directly as it walks the token
//! stream, with no intermediate AST. Each grammar production maps one-to-one onto a code fragment,
//! so each `emit_*` function parses its slice of the input and returns the `TokenStream` for the
//! corresponding expression; children are recursively emitted and spliced into their parent's
//! `quote!`, producing nested expressions. The one cross-cutting concern, duplicate-name
//! detection, is per-struct-body and folds into the body parser.
//!
//! `syn` serves purely as the tokenizer/cursor (`ParseStream`, `peek`, delimiter macros, and
//! `Expr`/`LitStr`/`Ident` sub-parsers). The entry point can be a simple closure because
//! `FnOnce(ParseStream) -> syn::Result<T>` satisfies `syn::parse::Parser`.

use std::collections::hash_map::Entry as MapEntry;
use std::collections::HashMap;

use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned, ToTokens};
use syn::parse::{ParseStream, Parser};
use syn::spanned::Spanned;
use syn::token::{Brace, Bracket, Paren};
use syn::{braced, bracketed, parenthesized, Expr, Ident, LitStr, Token};

mod kw {
    syn::custom_keyword!(nullable);
    syn::custom_keyword!(not_null);
}

/// Parses the macro body and emits the struct-building code, rejecting statically-detectable
/// duplicate field names.
/// - `fallible`: build nested structs with `try_new` and `?`-propagate, vs. `new_unchecked`.
/// - `wrap`: post-processes the resulting top-level `StructType` (e.g. `Arc::new`, error closure).
///
/// Duplicate-name errors accumulate in `errors` while parsing continues, and
/// are emitted only if parsing otherwise succeeds. A fatal parse error short-circuits via `?` and
/// is reported on its own (suppressing any duplicates seen to that point).
pub(crate) fn parse_schema(
    input: proc_macro::TokenStream,
    fallible: bool,
    wrap: impl FnOnce(TokenStream) -> TokenStream,
) -> proc_macro::TokenStream {
    // Emits a block of 1+ errors, in case parsing failed or duplicate fields were detected.
    let compile_errors_block = |errors: Vec<syn::Error>| {
        let invocations = errors.iter().map(syn::Error::to_compile_error);
        quote!({ #( #invocations )* })
    };
    try_parse_schema(input.into(), fallible)
        .map_or_else(compile_errors_block, wrap)
        .into()
}

/// Runs the parser over a [`proc_macro2::TokenStream`], returning the schema-building tokens or the
/// accumulated errors. Splitting this out from [`parse_schema`] separates the wrapping of the
/// `compile_error!` block from its rendering, and lets unit tests drive the parser directly.
fn try_parse_schema(input: TokenStream, fallible: bool) -> Result<TokenStream, Vec<syn::Error>> {
    let mut errors: Vec<syn::Error> = Vec::new();
    let parser = |input: ParseStream| emit_struct(input, fallible, &mut errors);
    match parser.parse2(input) {
        Ok(block) if errors.is_empty() => Ok(block),
        Ok(_) => Err(errors),
        Err(parse_error) => Err(vec![parse_error]),
    }
}

/// `body := (entry ',')* entry?`
///
/// Emits code to create a `StructType` instance, by building up a vec of `StructField`s and
/// recording any duplicate field names in `errors`. The `fallible` flag determines whether
/// `StructType::try_new` or `StructType::new_unchecked` is used.
fn emit_struct(
    input: ParseStream,
    fallible: bool,
    errors: &mut Vec<syn::Error>,
) -> syn::Result<TokenStream> {
    // Track and reject duplicate literal field names and duplicate identifiers
    let mut literals = HashMap::new();
    let mut idents = HashMap::new();
    let mut stmts = Vec::new();
    while !input.is_empty() {
        let entry = emit_field_entry(input, fallible, &mut literals, &mut idents, errors);
        stmts.push(entry?);
        if input.is_empty() {
            break;
        }
        input.parse::<Token![,]>()?;
    }
    let ctor = if fallible {
        quote!(try_new)
    } else {
        quote!(new_unchecked)
    };
    Ok(quote! {{
        let mut __fields = ::std::vec::Vec::new();
        #( #stmts )*
        delta_kernel::schema::StructType::#ctor(__fields)
    }})
}

/// `entry := nullability name ':' type | '(' EXPR ')' | '..' '(' EXPR ')'`
///
/// Emits one `__fields.push(...)` or `__fields.extend(...)` statement.
fn emit_field_entry(
    input: ParseStream,
    fallible: bool,
    literals: &mut HashMap<String, Span>,
    idents: &mut HashMap<String, Span>,
    errors: &mut Vec<syn::Error>,
) -> syn::Result<TokenStream> {
    // `..(xs)` -- splice an iterator of fields.
    if input.peek(Token![..]) {
        input.parse::<Token![..]>()?;
        let expr: Expr = parse_paren_to_end(input, "splice expression")?;
        return Ok(quote_spanned! { expr.span() =>
            __fields.extend(
                ::core::iter::IntoIterator::into_iter(#expr)
                    .map(delta_kernel::schema::ToSchemaField::to_schema_field)
            );
        });
    }
    // `(field)` -- interpolate one `StructField`. A named field always starts with a nullability
    // keyword, so a leading `(` is unambiguous.
    if input.peek(Paren) {
        let expr: Expr = parse_paren_to_end(input, "field expression")?;
        return Ok(quote_spanned! { expr.span() =>
            __fields.push(delta_kernel::schema::ToSchemaField::to_schema_field(#expr));
        });
    }
    // `nullability NAME : type`
    let nullable = parse_nullability(input)?;
    let name = emit_field_name(input, literals, idents, errors)?;
    input.parse::<Token![:]>()?;
    let dtype = emit_type(input, fallible, errors)?;
    Ok(quote! {
        __fields.push(delta_kernel::schema::StructField::new(#name, #dtype, #nullable));
    })
}

/// `name := STR_LITERAL | IDENT | '(' EXPR ')'`
///
/// Returns the name tokens (all three forms evaluate to `impl Into<String>`) and records the name
/// for duplicate detection. String literals are knowable by value (recorded case-insensitively, per
/// Delta's case-insensitive column rule); a bare ident is opaque but textually comparable; a
/// `(EXPR)` name is fully opaque and left to runtime validation.
fn emit_field_name(
    input: ParseStream,
    literals: &mut HashMap<String, Span>,
    idents: &mut HashMap<String, Span>,
    errors: &mut Vec<syn::Error>,
) -> syn::Result<TokenStream> {
    if input.peek(LitStr) {
        let lit: LitStr = input.parse()?;
        check_for_duplicates(literals, lit.value().to_lowercase(), lit.span(), errors);
        Ok(lit.to_token_stream())
    } else if input.peek(Paren) {
        let expr: Expr = parse_paren_to_end(input, "field name expression")?;
        Ok(expr.to_token_stream())
    } else {
        let ident: Ident = input.parse()?;
        check_for_duplicates(idents, ident.to_string(), ident.span(), errors);
        Ok(ident.to_token_stream())
    }
}

/// Emits a `DataType`-typed expression for the next `type` in the stream. `fallible` recursively
/// propagates the requesting macro's validation mode: when set, nested structs use `try_new` and
/// their results are `?`-propagated to the enclosing `DeltaResult` closure.
fn emit_type(
    input: ParseStream,
    fallible: bool,
    errors: &mut Vec<syn::Error>,
) -> syn::Result<TokenStream> {
    // `(EXPR)` -- anything that evaluates to `impl Into<DataType>`
    if input.peek(Paren) {
        let expr: Expr = parse_paren_to_end(input, "parenthesized type")?;
        return Ok(expr.to_token_stream());
    }
    // `[ nullability element ]` -- array.
    if input.peek(Bracket) {
        let content;
        bracketed!(content in input);
        let contains_null = parse_nullability(&content)?;
        let element = emit_type(&content, fallible, errors)?;
        ensure_empty(&content, "array element type")?;
        return Ok(quote! { delta_kernel::schema::ArrayType::new(#element, #contains_null) });
    }
    // `{ ... }` -- struct or map. A map is `KEY => VALUE`; the key is a single token tree, so a
    // `=>` in the second position unambiguously marks a map. Anything else (including empty) is a
    // struct body.
    if input.peek(Brace) {
        let content;
        braced!(content in input);
        if !content.peek2(Token![=>]) {
            let block = emit_struct(&content, fallible, errors)?;
            return Ok(if fallible { quote!(#block?) } else { block });
        }
        let key = emit_type(&content, fallible, errors)?;
        content.parse::<Token![=>]>()?;
        let value_contains_null = parse_nullability(&content)?;
        let value = emit_type(&content, fallible, errors)?;
        ensure_empty(&content, "map value type")?;
        return Ok(
            quote! { delta_kernel::schema::MapType::new(#key, #value, #value_contains_null) },
        );
    }
    // Bare, single-segment path/call -> `DataType::<PATH>` (e.g. `LONG`, `unshredded_variant()`).
    let expr: Expr = input.parse()?;
    validate_bare_datatype(&expr)?;
    Ok(quote_spanned! { expr.span() => delta_kernel::schema::DataType::#expr })
}

/// `nullability := 'nullable' | 'not_null'`, returning `contains_null`-style booleans.
fn parse_nullability(input: ParseStream) -> syn::Result<bool> {
    if input.peek(kw::nullable) {
        input.parse::<kw::nullable>()?;
        Ok(true)
    } else if input.peek(kw::not_null) {
        input.parse::<kw::not_null>()?;
        Ok(false)
    } else {
        Err(input.error("expected `nullable` or `not_null`"))
    }
}

/// Validates that a bare type is a single-segment path or call (e.g. `LONG` -> `DataType::LONG`),
/// rejecting qualified paths (e.g. `a::b`) and non-path expressions.
fn validate_bare_datatype(expr: &Expr) -> syn::Result<()> {
    let path = match expr {
        Expr::Path(path) if path.qself.is_none() => &path.path,
        Expr::Call(call) => match &*call.func {
            Expr::Path(path) if path.qself.is_none() => &path.path,
            other => return Err(syn::Error::new_spanned(other, "expected a data type")),
        },
        other => return Err(syn::Error::new_spanned(other, "expected a data type")),
    };
    // A turbofish (`foo::<T>`) stays within a single segment, so segment count alone distinguishes
    // it from a qualified path (`a::b`).
    if path.leading_colon.is_some() || path.segments.len() != 1 {
        return Err(syn::Error::new_spanned(path, "unexpected qualified path"));
    }
    Ok(())
}

/// Parses a parenthesized `( T )`, requiring `T` to be its sole remaining content.
fn parse_paren_to_end<T: syn::parse::Parse>(input: ParseStream, what: &str) -> syn::Result<T> {
    let content;
    parenthesized!(content in input);
    let value = content.parse()?;
    ensure_empty(&content, what)?;
    Ok(value)
}

/// Errors out (pointing at the trailing tokens) if `content` is not fully consumed.
fn ensure_empty(content: ParseStream, what: &str) -> syn::Result<()> {
    if content.is_empty() {
        Ok(())
    } else {
        Err(content.error(format!("unexpected tokens after {what}")))
    }
}

/// Records a name occurrence in the given namespace. When the same key reappears in this body,
/// pushes two errors -- one at the duplicate and one at the original occurrence -- which surface as
/// a `compile_error!` pointing at each.
fn check_for_duplicates(
    seen: &mut HashMap<String, Span>,
    key: String,
    span: Span,
    errors: &mut Vec<syn::Error>,
) {
    match seen.entry(key) {
        MapEntry::Occupied(first) => {
            errors.push(syn::Error::new(span, "duplicate field name"));
            errors.push(syn::Error::new(*first.get(), "first defined here"));
        }
        MapEntry::Vacant(slot) => {
            slot.insert(span);
        }
    }
}

#[cfg(test)]
mod tests {
    // These tests drive `try_parse_schema` directly (the same code path the proc macros run),
    // asserting only on the parse outcome and on the *caller-facing* diagnostics -- the compile
    // errors a user sees when they mistype an invocation. They deliberately avoid inspecting the
    // generated token stream, since its exact shape is an implementation detail.
    use rstest::rstest;

    use super::*;

    /// Malformed invocations that must be rejected with a specific, caller-facing diagnostic.
    #[rstest]
    #[case::missing_top_level_nullability(quote! { "a": INTEGER }, "nullable")]
    #[case::missing_array_element_nullability(quote! { nullable "a": [ STRING ] }, "nullable")]
    #[case::missing_map_value_nullability(quote! { nullable "a": { STRING => STRING } }, "nullable")]
    #[case::qualified_path_type(quote! { nullable "a": a::b }, "qualified path")]
    #[case::non_datatype_bare_type(quote! { nullable "a": 42 }, "data type")]
    #[case::trailing_tokens_in_parenthesized_type(quote! { nullable "a": (foo bar) }, "unexpected tokens")]
    #[case::trailing_tokens_in_array_element(quote! { nullable "a": [ nullable STRING extra ] }, "unexpected tokens")]
    #[case::trailing_tokens_in_map_value(quote! { nullable "a": { STRING => nullable STRING extra } }, "unexpected tokens")]
    fn rejects_with_diagnostic(#[case] input: TokenStream, #[case] needle: &str) {
        let errors = try_parse_schema(input, false).expect_err("expected rejection");
        let messages = Vec::from_iter(errors.iter().map(ToString::to_string));
        assert!(
            messages.iter().any(|m| m.contains(needle)),
            "no error contained {needle:?}; got: {messages:?}",
        );
    }

    /// Structurally broken invocations that `syn` rejects for us. We assert only that they fail,
    /// not on the wording, to avoid coupling the tests to `syn`'s error messages.
    #[rstest]
    #[case::missing_colon(quote! { nullable "a" INTEGER })]
    #[case::missing_comma_between_fields(quote! { not_null "a": INTEGER not_null "b": LONG })]
    fn rejects_structurally_invalid_input(#[case] input: TokenStream) {
        assert!(try_parse_schema(input, false).is_err());
    }

    /// Statically-detectable duplicate field names are rejected, with errors pointing at both the
    /// duplicate and the original occurrence. Literals collide case-insensitively (per Delta's
    /// column-name rule); bare identifiers collide by their text.
    #[rstest]
    #[case::case_insensitive_string_literals(quote! { nullable "id": INTEGER, nullable "ID": STRING })]
    #[case::repeated_identifiers(quote! { nullable NAME: INTEGER, not_null NAME: STRING })]
    fn rejects_duplicate_field_names(#[case] input: TokenStream) {
        let errors = try_parse_schema(input, false).expect_err("expected duplicate rejection");
        let messages = Vec::from_iter(errors.iter().map(ToString::to_string));
        assert!(
            messages.iter().any(|m| m.contains("duplicate field name")),
            "got: {messages:?}"
        );
        assert!(
            messages.iter().any(|m| m.contains("first defined here")),
            "got: {messages:?}"
        );
    }

    /// Well-formed invocations parse cleanly in both the infallible and fallible modes (the grammar
    /// is identical; only the generated constructor differs). The last two cases pin the boundary
    /// of static duplicate detection: it is per-struct-body, and interpolated `(EXPR)` names are
    /// opaque, so neither is flagged here (the fallible macro defers such checks to runtime).
    #[rstest]
    #[case::flat_fields(quote! {
        not_null "id": LONG,
        nullable "name": STRING,
        not_null "score": (DataType::DOUBLE),
    })]
    #[case::nested_struct_array_and_map(quote! {
        not_null "user": { nullable "city": STRING, nullable "zip": STRING },
        nullable "tags": [ not_null STRING ],
        not_null "props": { STRING => nullable STRING },
    })]
    #[case::interpolated_name_type_field_and_splice(quote! {
        not_null (format!("col_{}", 1)): (DataType::LONG),
        (StructField::nullable("z", DataType::STRING)),
        ..(::std::vec::Vec::<StructField>::new()),
    })]
    #[case::empty_body(quote! {})]
    #[case::same_name_in_sibling_nested_structs(quote! {
        not_null "a": { nullable "x": INTEGER },
        not_null "b": { nullable "x": INTEGER },
    })]
    #[case::interpolated_name_collision_deferred(quote! {
        nullable (n): INTEGER,
        nullable (n): STRING,
    })]
    fn accepts_well_formed_schema(
        #[case] input: TokenStream,
        #[values(false, true)] fallible: bool,
    ) {
        let result = try_parse_schema(input, fallible);
        assert!(
            result.is_ok(),
            "expected acceptance, got: {:?}",
            result
                .err()
                .map(|e| Vec::from_iter(e.iter().map(ToString::to_string))),
        );
    }
}
