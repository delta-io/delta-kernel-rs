//! Parser for a single CHECK-constraint comparison.
//!
//! Consumes the [`Token`] stream from [`super::token`] and produces a [`Comparison`] -- exactly one
//! `operand <op> operand`. Junctions (`AND`/`OR`/`NOT`), parentheses, and `IS [NOT] NULL` are out
//! of this grammar and surface as errors, which the caller treats as "not kernel-parsable".
//! Lowering ([`super::lower`]) resolves the operands against the table schema.

// WIP feature behind `check-constraints-in-dev`; some items have no caller until enforcement lands.
#![allow(dead_code)]

use super::token::Token;
use crate::{DeltaResult, Error};

/// An operand of a comparison: a column reference (raw path segments, as written) or a literal
/// (raw source text, e.g. `42`, `'foo'`, `NULL`). Both are resolved/typed during lowering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Operand {
    Column(Vec<String>),
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
    /// `<=>`, null-safe equality; lowering maps it to a not-distinct predicate.
    NullSafeEq,
}

/// A single parsed comparison `left <op> right`. Operands are resolved against the table schema
/// during lowering ([`super::lower`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Comparison {
    pub(super) op: CmpOp,
    pub(super) left: Operand,
    pub(super) right: Operand,
}

/// Parse a token stream into a single [`Comparison`]. The grammar has no junctions, so any tokens
/// left over after one comparison are an error.
pub(super) fn parse(tokens: Vec<Token>) -> DeltaResult<Comparison> {
    let mut parser = Parser { tokens, pos: 0 };
    let comparison = parser.parse_comparison()?;
    if parser.pos != parser.tokens.len() {
        return Err(Error::generic(
            "unexpected trailing input in CHECK constraint: only a single comparison is supported",
        ));
    }
    Ok(comparison)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    // comparison := operand cmp operand
    fn parse_comparison(&mut self) -> DeltaResult<Comparison> {
        let left = self.parse_operand()?;
        let op = self.cmp_op()?;
        let right = self.parse_operand()?;
        Ok(Comparison { op, left, right })
    }

    fn cmp_op(&mut self) -> DeltaResult<CmpOp> {
        let op = match self.advance() {
            Some(Token::Lt) => CmpOp::Lt,
            Some(Token::Le) => CmpOp::Le,
            Some(Token::Gt) => CmpOp::Gt,
            Some(Token::Ge) => CmpOp::Ge,
            Some(Token::Eq) => CmpOp::Eq,
            Some(Token::Ne) => CmpOp::Ne,
            Some(Token::NullSafeEq) => CmpOp::NullSafeEq,
            other => {
                return Err(Error::generic(format!(
                    "expected a comparison operator in CHECK constraint, found {other:?}"
                )))
            }
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
            Some(sign @ (Token::Plus | Token::Minus)) => match self.advance() {
                Some(Token::Literal(raw)) if starts_numeric(&raw) => {
                    let sign = if sign == Token::Minus { "-" } else { "+" };
                    Ok(Operand::Literal(format!("{sign}{raw}")))
                }
                other => Err(Error::generic(format!(
                    "expected a numeric literal after '{}' in CHECK constraint, found {other:?}",
                    if sign == Token::Minus { '-' } else { '+' }
                ))),
            },
            Some(Token::Literal(raw)) => Ok(Operand::Literal(raw)),
            Some(Token::Ident(first)) => {
                let mut path = vec![first];
                while self.peek() == Some(&Token::Dot) {
                    self.advance();
                    match self.advance() {
                        Some(Token::Ident(segment)) => path.push(segment),
                        other => {
                            return Err(Error::generic(format!(
                            "expected an identifier after '.' in CHECK constraint, found {other:?}"
                        )))
                        }
                    }
                }
                Ok(Operand::Column(path))
            }
            other => Err(Error::generic(format!(
                "expected a column or literal in CHECK constraint, found {other:?}"
            ))),
        }
    }
}

/// Whether a literal's raw text is a number (so a leading sign may fold into it). Numeric literals
/// begin with a digit or a leading-dot (`.5`); string/binary/date/boolean/null literals do not, so
/// a sign before them (`-'foo'`) is rejected.
fn starts_numeric(raw: &str) -> bool {
    raw.starts_with(|c: char| c.is_ascii_digit() || c == '.')
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::super::token::{Keyword, Token};
    use super::{parse, CmpOp, Comparison, Operand};

    fn ident(s: &str) -> Token {
        Token::Ident(s.to_string())
    }

    fn col(segments: &[&str]) -> Operand {
        Operand::Column(segments.iter().map(|s| s.to_string()).collect())
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
    // A leading Minus/Plus folds into a numeric literal's raw text (the tokenizer emits it as a
    // separate operator): `a > -234` -> right operand Literal("-234").
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
    fn parses_operand_shapes(
        #[case] tokens: Vec<Token>,
        #[case] left: Operand,
        #[case] right: Operand,
    ) {
        let got = parse(tokens).unwrap();
        assert_eq!(got.left, left);
        assert_eq!(got.right, right);
    }

    /// Anything outside a single `operand op operand` is rejected: trailing tokens (no junctions),
    /// a missing operator, a dangling dotted path, an operator with no left operand, and empty
    /// input.
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
    // A sign applies only to a numeric literal: unary minus on a column (`-a`) and a sign before a
    // non-numeric literal (`-'foo'`) are out of grammar.
    #[case::sign_before_column(vec![Token::Minus, ident("a"), Token::Gt, Token::Literal("0".into())])]
    #[case::sign_before_string(vec![
        ident("a"), Token::Eq, Token::Minus, Token::Literal("'foo'".into()),
    ])]
    fn rejects_non_single_comparison(#[case] tokens: Vec<Token>) {
        assert!(parse(tokens).is_err());
    }
}
