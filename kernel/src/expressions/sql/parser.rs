//! Parser for a single CHECK-constraint comparison.
//!
//! Consumes the [`Token`] stream from [`super::token`] and produces a [`Comparison`] -- exactly one
//! `operand <op> operand`. Junctions (`AND`/`OR`/`NOT`), parentheses, and `IS [NOT] NULL` are out
//! of this grammar and surface as errors, which the caller treats as "not kernel-parsable".
//! Lowering ([`super::lower`]) resolves the operands against the table schema.

// WIP feature behind `check-constraints-in-dev`; some items have no caller until enforcement lands.
// TODO(#2896): remove this allow once check-constraint enforcement wires up a caller.
#![allow(dead_code)]

use std::iter::Peekable;
use std::vec;

use super::token::Token;
use crate::expressions::ColumnName;
use crate::{DeltaResult, Error};

/// An operand of a comparison: a column reference (its as-written path) or a literal (raw source
/// text, e.g. `42`, `'foo'`, `NULL`). Both are resolved/typed during lowering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Operand {
    Column(ColumnName),
    Literal(String),
}

/// A comparison operator. Kernel has no native `<=`/`>=`/`!=`; lowering maps these to the
/// corresponding `Predicate` constructors (`le`/`ge`/`ne`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CmpOp {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
    /// `<=>`, null-safe equality; lowering maps it to `not(distinct(..))`.
    NullSafeEq,
}

/// A single parsed comparison `left <op> right`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Comparison {
    pub(super) op: CmpOp,
    pub(super) left: Operand,
    pub(super) right: Operand,
}

/// Parse a token stream into a single [`Comparison`]. The grammar has no junctions, so any tokens
/// left over after one comparison are an error.
pub(super) fn parse(tokens: Vec<Token>) -> DeltaResult<Comparison> {
    let mut parser = Parser {
        tokens: tokens.into_iter().peekable(),
    };
    let comparison = parser.parse_comparison()?;
    if parser.advance().is_some() {
        return Err(Error::generic(
            "unexpected trailing input in CHECK constraint: only a single comparison is supported",
        ));
    }
    Ok(comparison)
}

struct Parser {
    tokens: Peekable<vec::IntoIter<Token>>,
}

impl Parser {
    fn peek(&mut self) -> Option<&Token> {
        self.tokens.peek()
    }

    fn advance(&mut self) -> Option<Token> {
        self.tokens.next()
    }

    // comparison := operand cmp operand
    fn parse_comparison(&mut self) -> DeltaResult<Comparison> {
        let left = self.parse_operand()?;
        let op = self.parse_cmp_op()?;
        let right = self.parse_operand()?;
        Ok(Comparison { op, left, right })
    }

    fn parse_cmp_op(&mut self) -> DeltaResult<CmpOp> {
        let op = match self.advance() {
            Some(Token::Lt) => CmpOp::Lt,
            Some(Token::Le) => CmpOp::Le,
            Some(Token::Gt) => CmpOp::Gt,
            Some(Token::Ge) => CmpOp::Ge,
            Some(Token::Eq) => CmpOp::Eq,
            Some(Token::Ne) => CmpOp::Ne,
            Some(Token::NullSafeEq) => CmpOp::NullSafeEq,
            other => return Err(expected("a comparison operator", other)),
        };
        Ok(op)
    }

    // operand := ('+' | '-')? Literal | Ident ( '.' Ident )*
    //
    // A leading sign applies only to a numeric literal (the tokenizer emits it as a separate
    // Plus/Minus operator): the sign is folded back into the literal's raw text so the existing
    // `parse_sql` types it, e.g. `-234` -> Literal("-234"). A sign before anything else (a column,
    // string/date literal, keyword) is not a single-comparison operand and errors -- unary minus on
    // a column and binary arithmetic are out of grammar.
    fn parse_operand(&mut self) -> DeltaResult<Operand> {
        match self.advance() {
            Some(sign @ (Token::Plus | Token::Minus)) => {
                let sign = if sign == Token::Minus { '-' } else { '+' };
                match self.advance() {
                    Some(Token::Literal(raw)) if starts_numeric(&raw) => {
                        Ok(Operand::Literal(format!("{sign}{raw}")))
                    }
                    other => Err(expected(
                        &format!("a numeric literal after '{sign}'"),
                        other,
                    )),
                }
            }
            Some(Token::Literal(raw)) => Ok(Operand::Literal(raw)),
            Some(Token::Ident(first)) => self.parse_column_path(first),
            other => Err(expected("a column or literal", other)),
        }
    }

    // Continue a dotted column path after its first `Ident`: `( '.' Ident )*`.
    fn parse_column_path(&mut self, first: String) -> DeltaResult<Operand> {
        let mut path = vec![first];
        while self.peek() == Some(&Token::Dot) {
            self.advance();
            match self.advance() {
                Some(Token::Ident(segment)) => path.push(segment),
                other => return Err(expected("an identifier after '.'", other)),
            }
        }
        Ok(Operand::Column(ColumnName::new(path)))
    }
}

/// Whether a literal's raw text is numeric -- begins with a digit or a leading dot (`.5`), unlike
/// string/binary/date/boolean/null literals.
fn starts_numeric(raw: &str) -> bool {
    raw.starts_with(|c: char| c.is_ascii_digit() || c == '.')
}

/// Build the `expected {what} in CHECK constraint, found {found:?}` error shared by the parser's
/// operand and operator sites.
fn expected(what: &str, found: Option<Token>) -> Error {
    Error::generic(format!(
        "expected {what} in CHECK constraint, found {found:?}"
    ))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::super::token::{Keyword, Token};
    use super::{parse, CmpOp, Comparison, Operand};
    use crate::expressions::ColumnName;

    fn ident(s: &str) -> Token {
        Token::Ident(s.to_string())
    }

    fn col(segments: &[&str]) -> Operand {
        Operand::Column(ColumnName::new(segments.iter().map(|s| s.to_string())))
    }

    /// Each operator token maps to its `CmpOp`, with the two operands preserved in order.
    #[rstest]
    #[case(Token::Lt, CmpOp::Lt)]
    #[case(Token::Le, CmpOp::Le)]
    #[case(Token::Gt, CmpOp::Gt)]
    #[case(Token::Ge, CmpOp::Ge)]
    #[case(Token::Eq, CmpOp::Eq)]
    #[case(Token::Ne, CmpOp::Ne)]
    #[case(Token::NullSafeEq, CmpOp::NullSafeEq)]
    fn parses_each_operator(#[case] tok: Token, #[case] expected: CmpOp) {
        let got = parse(vec![ident("a"), tok, Token::Literal("1".into())]).unwrap();
        assert_eq!(
            got,
            Comparison {
                op: expected,
                left: col(&["a"]),
                right: Operand::Literal("1".into()),
            }
        );
    }

    /// Operands may be column-column, column-literal, or literal-column, and a column may be a
    /// dotted path.
    #[rstest]
    #[case(vec![ident("a"), Token::Gt, ident("b")], col(&["a"]), col(&["b"]))]
    #[case(
        vec![Token::Literal("0".into()), Token::Lt, ident("a")],
        Operand::Literal("0".into()),
        col(&["a"])
    )]
    #[case(
        vec![ident("a"), Token::Dot, ident("b"), Token::Eq, Token::Literal("1".into())],
        col(&["a", "b"]),
        Operand::Literal("1".into())
    )]
    #[case(
        vec![ident("a"), Token::Gt, Token::Minus, Token::Literal("234".into())],
        col(&["a"]),
        Operand::Literal("-234".into())
    )]
    #[case(
        vec![Token::Plus, Token::Literal("5".into()), Token::Lt, ident("a")],
        Operand::Literal("+5".into()),
        col(&["a"])
    )]
    // A sign folds into a leading-dot decimal too (`.5`): `starts_numeric` accepts a leading `.`.
    #[case(
        vec![ident("a"), Token::Gt, Token::Minus, Token::Literal(".5".into())],
        col(&["a"]),
        Operand::Literal("-.5".into())
    )]
    fn parses_operand_shapes(
        #[case] tokens: Vec<Token>,
        #[case] left: Operand,
        #[case] right: Operand,
    ) {
        let got = parse(tokens).unwrap();
        assert_eq!(got.left, left);
        assert_eq!(got.right, right);
    }

    /// Anything outside a single `operand op operand` is rejected.
    #[rstest]
    #[case::trailing(vec![ident("a"), Token::Gt, Token::Literal("0".into()), ident("extra")])]
    #[case::missing_operator(vec![ident("a"), ident("b")])]
    #[case::dangling_dot(vec![ident("a"), Token::Dot, Token::Gt, Token::Literal("0".into())])]
    #[case::operator_first(vec![Token::Gt, Token::Literal("0".into())])]
    #[case::empty(vec![])]
    // A junction keyword has no place in a single comparison: the parser rejects it wherever it
    // appears (as an operand, or trailing after a complete comparison).
    #[case::keyword_operand(vec![ident("a"), Token::Gt, Token::Keyword(Keyword::And)])]
    #[case::keyword_junction(vec![
        ident("a"), Token::Gt, Token::Literal("0".into()),
        Token::Keyword(Keyword::And), ident("b"), Token::Lt, Token::Literal("9".into()),
    ])]
    #[case::sign_before_column(vec![Token::Minus, ident("a"), Token::Gt, Token::Literal("0".into())])]
    #[case::sign_before_string(vec![
        ident("a"), Token::Eq, Token::Minus, Token::Literal("'foo'".into()),
    ])]
    #[case::sign_then_eof(vec![ident("a"), Token::Gt, Token::Minus])]
    #[case::double_sign(vec![
        ident("a"), Token::Gt, Token::Minus, Token::Minus, Token::Literal("5".into()),
    ])]
    fn rejects_non_single_comparison(#[case] tokens: Vec<Token>) {
        assert!(parse(tokens).is_err());
    }
}
