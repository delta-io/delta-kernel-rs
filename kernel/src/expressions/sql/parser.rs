//! Parser for a single SQL comparison.
//!
//! Consumes the [`Token`] stream from [`super::token`] and produces a [`Comparison`]: exactly one
//! `operand <op> operand`. For now the grammar covers only a single comparison; junctions
//! (`AND`/`OR`/`NOT`), parentheses, and `IS [NOT] NULL` are not yet supported and surface as
//! errors. A later stage resolves the operands against a schema.

// WIP feature behind `check-constraints-in-dev`; some items have no caller until discovery lands.
// TODO(#2896): remove this allow once check-constraint discovery wires up a caller.
#![allow(dead_code)]

use std::iter::Peekable;
use std::vec;

use super::token::Token;
use crate::expressions::ColumnName;
use crate::{DeltaResult, Error};

/// An operand of a comparison: a column reference (its as-written path), a literal (raw source
/// text, e.g. `42`, `'foo'`), or `NULL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Operand {
    Column(ColumnName),
    Literal(String),
    Null,
}

/// A comparison operator. Kernel has no native `<=`/`>=`/`!=` predicate op; lowering composes them
/// from the primitives it does have (`<`/`>`/`=`), e.g. `le` is `not(gt)`.
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
            "unexpected trailing input: only a single comparison is supported",
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

    // operand := ('+' | '-')? Number | Literal | Ident ( '.' Ident )*
    //
    // A leading sign applies only to a number (the tokenizer emits it as a separate Plus/Minus
    // operator): the sign is folded back into the number's raw text so the later lowering stage
    // types it, e.g. `-234` -> Literal("-234"). A sign before anything else (a column, string/date
    // literal, keyword) is not a single-comparison operand and errors: unary minus on a column and
    // binary arithmetic are out of grammar.
    fn parse_operand(&mut self) -> DeltaResult<Operand> {
        match self.advance() {
            Some(sign @ (Token::Plus | Token::Minus)) => {
                let sign = if sign == Token::Minus { '-' } else { '+' };
                match self.advance() {
                    Some(Token::Number(raw)) => Ok(Operand::Literal(format!("{sign}{raw}"))),
                    other => Err(expected(&format!("a number after '{sign}'"), other)),
                }
            }
            Some(Token::Number(raw)) | Some(Token::Literal(raw)) => Ok(Operand::Literal(raw)),
            Some(Token::Null) => Ok(Operand::Null),
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

/// Build the `expected {what}, found {found:?}` error shared by the parser's operand and operator
/// sites.
fn expected(what: &str, found: Option<Token>) -> Error {
    Error::generic(format!("expected {what}, found {found:?}"))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::super::token::{tokenize, Keyword, Token};
    use super::{parse, CmpOp, Comparison, Operand};
    use crate::expressions::ColumnName;

    fn ident(s: &str) -> Token {
        Token::Ident(s.to_string())
    }

    fn col(segments: &[&str]) -> Operand {
        Operand::Column(ColumnName::new(segments.iter().map(|s| s.to_string())))
    }

    fn lit(s: &str) -> Operand {
        Operand::Literal(s.to_string())
    }

    fn null() -> Operand {
        Operand::Null
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
        let got = parse(vec![ident("a"), tok, Token::Number("1".into())]).unwrap();
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
        vec![Token::Number("0".into()), Token::Lt, ident("a")],
        Operand::Literal("0".into()),
        col(&["a"])
    )]
    #[case(
        vec![ident("a"), Token::Dot, ident("b"), Token::Eq, Token::Number("1".into())],
        col(&["a", "b"]),
        Operand::Literal("1".into())
    )]
    // A three-segment path exercises loop re-entry in `parse_column_path` (two `.` iterations).
    #[case(
        vec![
            ident("a"), Token::Dot, ident("b"), Token::Dot, ident("c"),
            Token::Eq, Token::Number("1".into()),
        ],
        col(&["a", "b", "c"]),
        Operand::Literal("1".into())
    )]
    #[case(
        vec![ident("a"), Token::Gt, Token::Minus, Token::Number("234".into())],
        col(&["a"]),
        Operand::Literal("-234".into())
    )]
    #[case(
        vec![Token::Plus, Token::Number("5".into()), Token::Lt, ident("a")],
        Operand::Literal("+5".into()),
        col(&["a"])
    )]
    // A sign folds into a leading-dot decimal too (`.5`).
    #[case(
        vec![ident("a"), Token::Gt, Token::Minus, Token::Number(".5".into())],
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

    /// Run a raw constraint string through the real tokenizer and into the parser, the way a caller
    /// consumes it. The other tests hand-build `Token` vectors, so they cannot catch a mismatch
    /// between what the tokenizer emits and what the parser expects; these end-to-end cases do.
    /// `a > -.5` in particular exercises the tokenizer's leading-dot decimal plus the parser's
    /// sign reassembly (`Minus` + `Number(".5")` -> `Literal("-.5")`).
    #[rstest]
    #[case("a > -.5", CmpOp::Gt, col(&["a"]), lit("-.5"))]
    #[case("amount >= -234", CmpOp::Ge, col(&["amount"]), lit("-234"))]
    #[case("ratio == .25", CmpOp::Eq, col(&["ratio"]), lit(".25"))]
    #[case("a.b.c <= 100", CmpOp::Le, col(&["a", "b", "c"]), lit("100"))]
    #[case("x != y", CmpOp::Ne, col(&["x"]), col(&["y"]))]
    #[case("n <=> NULL", CmpOp::NullSafeEq, col(&["n"]), null())]
    #[case("+5 < a", CmpOp::Lt, lit("+5"), col(&["a"]))]
    #[case("status = 'active'", CmpOp::Eq, col(&["status"]), lit("'active'"))]
    fn tokenize_then_parse_yields_expected_comparison(
        #[case] sql: &str,
        #[case] op: CmpOp,
        #[case] left: Operand,
        #[case] right: Operand,
    ) {
        let got = parse(tokenize(sql).unwrap()).unwrap();
        assert_eq!(got, Comparison { op, left, right });
    }

    /// End-to-end rejections: inputs that fail the tokenizer (`(`) or parse to something outside a
    /// single comparison (junctions, `IS [NOT] NULL`, a bare boolean column) both surface as an
    /// error through the real pipeline.
    #[rstest]
    #[case::junction_and("a > 0 AND b < 9")]
    #[case::junction_or("a > 0 OR b > 0")]
    #[case::is_not_null("a IS NOT NULL")]
    #[case::parens("(amount > 0)")]
    #[case::bare_bool_column("is_active")]
    fn tokenize_then_parse_rejects_non_single_comparison(#[case] sql: &str) {
        assert!(tokenize(sql).and_then(parse).is_err());
    }

    /// Anything outside a single `operand op operand` is rejected.
    #[rstest]
    #[case::trailing(vec![ident("a"), Token::Gt, Token::Number("0".into()), ident("extra")])]
    #[case::missing_operator(vec![ident("a"), ident("b")])]
    #[case::dangling_dot(vec![ident("a"), Token::Dot, Token::Gt, Token::Number("0".into())])]
    #[case::operator_first(vec![Token::Gt, Token::Number("0".into())])]
    #[case::empty(vec![])]
    #[case::keyword_operand(vec![ident("a"), Token::Gt, Token::Keyword(Keyword::And)])]
    #[case::keyword_junction(vec![
        ident("a"), Token::Gt, Token::Number("0".into()),
        Token::Keyword(Keyword::And), ident("b"), Token::Lt, Token::Number("9".into()),
    ])]
    #[case::sign_before_column(vec![Token::Minus, ident("a"), Token::Gt, Token::Number("0".into())])]
    #[case::sign_before_string(vec![
        ident("a"), Token::Eq, Token::Minus, Token::Literal("'foo'".into()),
    ])]
    #[case::sign_then_eof(vec![ident("a"), Token::Gt, Token::Minus])]
    #[case::double_sign(vec![
        ident("a"), Token::Gt, Token::Minus, Token::Minus, Token::Number("5".into()),
    ])]
    fn rejects_non_single_comparison(#[case] tokens: Vec<Token>) {
        assert!(parse(tokens).is_err());
    }
}
