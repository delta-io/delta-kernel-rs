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
            other => {
                return Err(Error::generic(format!(
                    "expected a comparison operator in CHECK constraint, found {other:?}"
                )))
            }
        };
        Ok(op)
    }

    // operand := Literal | Ident ( '.' Ident )*
    fn parse_operand(&mut self) -> DeltaResult<Operand> {
        match self.advance() {
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
