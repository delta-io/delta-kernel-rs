//! A tokenizer for a small subset of SQL.
//!
//! Splits a SQL string into [`Token`]s; what the tokens mean is up to the caller. The token set is
//! intentionally minimal and grows with the grammar the parser supports. Literal tokens keep their
//! raw source text so a caller can decode them however it needs.
//!
//! Example: `` `col1` > 5 `` tokenizes to `[Ident("col1"), Gt, Literal("5")]`.

// WIP feature behind `check-constraints-in-dev`; some items have no caller until enforcement lands.
#![allow(dead_code)]

use std::iter::Peekable;
use std::str::Chars;

use crate::expressions::column_names::{is_simple_char, parse_escaped_field_name};
use crate::{DeltaResult, Error};

/// A peekable character stream over the source string.
type CharStream<'a> = Peekable<Chars<'a>>;

/// A lexical token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Token {
    Ident(String),
    /// A literal's raw, undecoded source text (quotes and any typed-literal keyword retained):
    /// numbers, quoted strings, `X'..'` binary, `TRUE`/`FALSE`/`NULL`, and typed
    /// `DATE`/`TIMESTAMP`/ `TIMESTAMP_LTZ`/`TIMESTAMP_NTZ` literals.
    Literal(String),
    Dot,
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    /// `<>` or `!=` (both mean not-equal).
    Ne,
    /// `<=>`, Spark's null-safe equality (`NULL <=> NULL` is true).
    NullSafeEq,
    /// `+` and `-`. Emitted as standalone operators (matching Spark, where a leading sign is unary
    /// minus/plus, not part of the number). A number is never lexed with a sign; the caller
    /// reassembles a signed literal from `Minus`/`Plus` followed by a numeric `Literal`.
    Plus,
    Minus,
    /// The `AND`/`OR`/`NOT`/`IS` keywords. Recognized here (not left as [`Token::Ident`]) so a
    /// backtick-quoted column literally named `` `AND` `` stays an `Ident`, distinct from the
    /// keyword. The current parser has no grammar for them and rejects them; they are tokenized so
    /// it can do so unambiguously.
    Keyword(Keyword),
}

/// A SQL keyword the tokenizer recognizes as a distinct token (see [`Token::Keyword`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Keyword {
    And,
    Or,
    Not,
    Is,
}

/// Tokenize `sql`, or return an error for any character/run outside the recognized token set (e.g.
/// arithmetic operators, parentheses, an unterminated string).
pub(super) fn tokenize(sql: &str) -> DeltaResult<Vec<Token>> {
    let mut chars = sql.chars().peekable();
    let mut tokens = Vec::new();
    while let Some(&c) = chars.peek() {
        match c {
            c if c.is_whitespace() => {
                chars.next();
            }
            // A `.` starts a number when a digit follows (a leading-dot decimal like `.5`);
            // otherwise it is a column-path separator.
            '.' => {
                if matches!(peek_second(&chars), Some(d) if d.is_ascii_digit()) {
                    tokens.push(Token::Literal(take_number(&mut chars)));
                } else {
                    chars.next();
                    tokens.push(Token::Dot);
                }
            }
            '=' => {
                chars.next();
                // Accept `==` as an alias for `=` (Spark SQL allows both); consume the optional
                // second `=`.
                chars.next_if_eq(&'=');
                tokens.push(Token::Eq);
            }
            '<' => {
                chars.next();
                // Maximal munch: `<=` may extend to `<=>` (null-safe equal), so after consuming
                // `=` check for a trailing `>` before settling on `Le`.
                let tok = if chars.next_if_eq(&'=').is_some() {
                    if chars.next_if_eq(&'>').is_some() {
                        Token::NullSafeEq
                    } else {
                        Token::Le
                    }
                } else if chars.next_if_eq(&'>').is_some() {
                    Token::Ne
                } else {
                    Token::Lt
                };
                tokens.push(tok);
            }
            '>' => {
                chars.next();
                let tok = if chars.next_if_eq(&'=').is_some() {
                    Token::Ge
                } else {
                    Token::Gt
                };
                tokens.push(tok);
            }
            // `!=` is not-equal; `!>` and `!<` are Spark aliases for `<=` and `>=` respectively
            // (Spark lexer: `LTE: '<=' | '!>'`, `GTE: '>=' | '!<'`).
            '!' => {
                chars.next();
                let tok = if chars.next_if_eq(&'=').is_some() {
                    Token::Ne
                } else if chars.next_if_eq(&'>').is_some() {
                    Token::Le
                } else if chars.next_if_eq(&'<').is_some() {
                    Token::Ge
                } else {
                    return Err(unexpected('!', sql));
                };
                tokens.push(tok);
            }
            '\'' => tokens.push(Token::Literal(take_quoted_string(&mut chars, sql)?)),
            // A backtick-quoted column field (`` `my col` ``, `` `a.b` ``). Reuse the crate's
            // column-name field parser so quoting/escaping matches `ColumnName` exactly; it expects
            // the opening backtick already consumed.
            '`' => {
                chars.next();
                tokens.push(Token::Ident(parse_escaped_field_name(&mut chars)?));
            }
            // `+`/`-` are standalone operators (Spark treats a leading sign as unary +/-, not part
            // of the number); the parser reassembles a signed literal from the sign + number.
            // TODO: recognize `(` / `)` as LParen / RParen tokens once the grammar supports
            // grouping.
            '+' => {
                chars.next();
                tokens.push(Token::Plus);
            }
            '-' => {
                chars.next();
                tokens.push(Token::Minus);
            }
            c if c.is_ascii_digit() => tokens.push(Token::Literal(take_number(&mut chars))),
            c if c.is_ascii_alphabetic() || c == '_' => {
                tokens.push(classify_word(&mut chars, sql)?)
            }
            _ => return Err(unexpected(c, sql)),
        }
    }
    Ok(tokens)
}

fn unexpected(c: char, sql: &str) -> Error {
    Error::generic(format!("unexpected character '{c}' in {sql}"))
}

/// Peek the second character of the stream (the one after the next), without consuming anything.
fn peek_second(chars: &CharStream<'_>) -> Option<char> {
    let mut lookahead = chars.clone();
    lookahead.next();
    lookahead.peek().copied()
}

/// Consume a `'...'` string literal, returning its raw text *including* the surrounding quotes and
/// any doubled `''` escapes.
///
/// Example: `'it''s'` returns `'it''s'` (verbatim). The caller must guarantee a leading `'`.
fn take_quoted_string(chars: &mut CharStream<'_>, sql: &str) -> DeltaResult<String> {
    chars.next();
    let mut out = String::from('\'');
    loop {
        match chars.next() {
            Some('\'') => {
                out.push('\'');
                // A doubled quote is an embedded single quote; keep scanning. Otherwise this was
                // the closing quote.
                match chars.next_if_eq(&'\'') {
                    Some(q) => out.push(q),
                    None => return Ok(out),
                }
            }
            Some(c) => out.push(c),
            None => {
                return Err(Error::generic(format!(
                    "unterminated string literal in {sql}"
                )))
            }
        }
    }
}

/// Consume an unsigned numeric literal: integer/fraction digits and an optional exponent. A leading
/// `+`/`-` is a separate operator token (see the `Plus`/`Minus` arms), not consumed here; the
/// exponent's own sign (`2e+1`) is part of the number.
///
/// Example: `2e+1` returns `2e+1`. Stops before a second operator, so `1+1` yields just `1`.
fn take_number(chars: &mut CharStream<'_>) -> String {
    let mut out = String::new();
    while let Some(c) = chars.next_if(|c| c.is_ascii_digit() || *c == '.') {
        out.push(c);
    }
    if let Some(e) = chars.next_if(|c| *c == 'e' || *c == 'E') {
        out.push(e);
        if let Some(sign) = chars.next_if(|c| *c == '+' || *c == '-') {
            out.push(sign);
        }
        while let Some(c) = chars.next_if(|c| c.is_ascii_digit()) {
            out.push(c);
        }
    }
    out
}

/// Scan a bare word and classify it: a keyword (`AND`/`OR`/`NOT`/`IS`), a boolean/null/typed/binary
/// literal, or an identifier. All classification is case-insensitive.
///
/// Keywords are recognized here rather than downstream so a backtick-quoted `` `AND` `` (parsed as
/// an `Ident` by the caller of this function) stays distinct from the bareword keyword `AND`.
fn classify_word(chars: &mut CharStream<'_>, sql: &str) -> DeltaResult<Token> {
    let mut word = String::new();
    while let Some(c) = chars.next_if(|c| is_simple_char(*c)) {
        word.push(c);
    }
    let token = match word.to_ascii_uppercase().as_str() {
        "AND" => Token::Keyword(Keyword::And),
        "OR" => Token::Keyword(Keyword::Or),
        "NOT" => Token::Keyword(Keyword::Not),
        "IS" => Token::Keyword(Keyword::Is),
        "NULL" | "TRUE" | "FALSE" => Token::Literal(word),
        // `X'deadbeef'` binary literal: only when a quote immediately follows (no whitespace),
        // otherwise `x` is a column named `x`.
        "X" if chars.peek() == Some(&'\'') => {
            let quoted = take_quoted_string(chars, sql)?;
            Token::Literal(format!("{word}{quoted}"))
        }
        // Typed literal: the keyword followed (whitespace allowed) by a quoted string. Without the
        // quote it is just a column named `date`/`timestamp`/etc.
        "DATE" | "TIMESTAMP" | "TIMESTAMP_LTZ" | "TIMESTAMP_NTZ" if quote_follows(chars) => {
            while chars.next_if(|c| c.is_whitespace()).is_some() {}
            let quoted = take_quoted_string(chars, sql)?;
            Token::Literal(format!("{word} {quoted}"))
        }
        _ => Token::Ident(word),
    };
    Ok(token)
}

/// Peek (without consuming) past any whitespace to see whether the next character is a `'`.
fn quote_follows(chars: &CharStream<'_>) -> bool {
    let mut lookahead = chars.clone();
    while matches!(lookahead.peek(), Some(c) if c.is_whitespace()) {
        lookahead.next();
    }
    lookahead.peek() == Some(&'\'')
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::{tokenize, Keyword, Token};

    fn ident(s: &str) -> Token {
        Token::Ident(s.to_string())
    }

    fn lit(s: &str) -> Token {
        Token::Literal(s.to_string())
    }

    /// `==` aliases `=`; `<>`/`!=` are `Ne`; `!>`/`!<` are Spark aliases for `<=`/`>=`.
    #[rstest]
    #[case("<", Token::Lt)]
    #[case("<=", Token::Le)]
    #[case(">", Token::Gt)]
    #[case(">=", Token::Ge)]
    #[case("=", Token::Eq)]
    #[case("==", Token::Eq)]
    #[case("!=", Token::Ne)]
    #[case("<>", Token::Ne)]
    #[case("<=>", Token::NullSafeEq)]
    #[case("!>", Token::Le)]
    #[case("!<", Token::Ge)]
    #[case("+", Token::Plus)]
    #[case("-", Token::Minus)]
    fn tokenizes_each_operator(#[case] op: &str, #[case] expected: Token) {
        assert_eq!(
            tokenize(&format!("a {op} 1")).unwrap(),
            [ident("a"), expected.clone(), lit("1")]
        );
        // Whitespace around the operator is optional.
        assert_eq!(
            tokenize(&format!("a{op}1")).unwrap(),
            [ident("a"), expected, lit("1")]
        );
    }

    /// A leading `+`/`-` is a standalone operator, not part of the number -- so a signed literal
    /// lexes as two tokens (the parser reassembles it), and whitespace between the sign and the
    /// digits (`- 5`) is irrelevant. The exponent's own sign stays inside the number.
    #[rstest]
    #[case("-5", &[Token::Minus, lit("5")])]
    #[case("- 5", &[Token::Minus, lit("5")])]
    #[case("+5", &[Token::Plus, lit("5")])]
    #[case("-.5", &[Token::Minus, lit(".5")])]
    #[case("-2e+1", &[Token::Minus, lit("2e+1")])]
    fn tokenizes_signed_number_as_sign_then_literal(#[case] sql: &str, #[case] expected: &[Token]) {
        assert_eq!(tokenize(sql).unwrap(), expected);
    }

    #[rstest]
    #[case("42", lit("42"))]
    #[case(".5", lit(".5"))]
    #[case("1e3", lit("1e3"))]
    #[case("2e+1", lit("2e+1"))] // the exponent sign is part of the number
    #[case("'foo'", lit("'foo'"))]
    #[case("'O''Brien'", lit("'O''Brien'"))] // doubled '' escape retained
    #[case("NULL", lit("NULL"))]
    #[case("TRUE", lit("TRUE"))]
    #[case("false", lit("false"))]
    #[case("DATE '1970-01-02'", lit("DATE '1970-01-02'"))]
    #[case("DATE'1970-01-02'", lit("DATE '1970-01-02'"))] // butted quote, space normalized in output
    #[case("X'01ff'", lit("X'01ff'"))]
    fn tokenizes_literal_as_single_raw_token(#[case] sql: &str, #[case] expected: Token) {
        assert_eq!(tokenize(sql).unwrap(), [expected]);
    }

    /// Every typed-literal keyword tokenizes to exactly one literal, so dropping one from the
    /// tokenizer's set (which would mis-lex it as an identifier) fails here.
    #[rstest]
    #[case("DATE '2024-01-01'")]
    #[case("TIMESTAMP '2024-01-01T00:00:00Z'")]
    #[case("TIMESTAMP_LTZ '2024-01-01T00:00:00Z'")]
    #[case("TIMESTAMP_NTZ '2024-01-01 00:00:00'")]
    fn every_typed_keyword_tokenizes_to_one_literal(#[case] sql: &str) {
        assert!(matches!(
            tokenize(sql).unwrap().as_slice(),
            [Token::Literal(_)]
        ));
    }

    /// `AND`/`OR`/`NOT`/`IS` are keyword tokens (case-insensitive); a backtick-quoted `` `AND` ``
    /// stays an `Ident`, distinct from the keyword.
    #[rstest]
    #[case("AND", Token::Keyword(Keyword::And))]
    #[case("or", Token::Keyword(Keyword::Or))]
    #[case("Not", Token::Keyword(Keyword::Not))]
    #[case("IS", Token::Keyword(Keyword::Is))]
    #[case("`AND`", ident("AND"))]
    fn tokenizes_keyword_vs_quoted_identifier(#[case] sql: &str, #[case] expected: Token) {
        assert_eq!(tokenize(sql).unwrap(), [expected]);
    }

    #[test]
    fn tokenizes_dotted_column_path() {
        assert_eq!(
            tokenize("a.b.c").unwrap(),
            [ident("a"), Token::Dot, ident("b"), Token::Dot, ident("c")]
        );
    }

    #[rstest]
    #[case("")]
    #[case("   ")]
    #[case("\t\n ")]
    fn tokenizes_empty_or_whitespace_only_input_to_no_tokens(#[case] sql: &str) {
        assert_eq!(tokenize(sql).unwrap(), []);
    }

    /// A malformed number is emitted verbatim as one raw literal token; the tokenizer does not
    /// validate numeric structure.
    #[rstest]
    #[case("1e", lit("1e"))]
    #[case("1.2.3", lit("1.2.3"))]
    #[case("1E3", lit("1E3"))]
    #[case("5e-3", lit("5e-3"))]
    fn tokenizes_malformed_number_as_single_raw_token(#[case] sql: &str, #[case] expected: Token) {
        assert_eq!(tokenize(sql).unwrap(), [expected]);
    }

    /// A backtick-quoted field is one `Ident` carrying its unescaped name (spaces, dots, and
    /// doubled-backtick escapes included), matching `ColumnName`.
    #[rstest]
    #[case("`my col`", ident("my col"))]
    #[case("`a.b`", ident("a.b"))]
    #[case("`a``b`", ident("a`b"))]
    fn tokenizes_backtick_quoted_field(#[case] sql: &str, #[case] expected: Token) {
        assert_eq!(tokenize(sql).unwrap(), [expected]);
    }

    #[test]
    fn tokenizes_backtick_field_in_comparison_and_path() {
        assert_eq!(
            tokenize("`my col` > 0").unwrap(),
            [ident("my col"), Token::Gt, lit("0")]
        );
        assert_eq!(
            tokenize("a.`b c`").unwrap(),
            [ident("a"), Token::Dot, ident("b c")]
        );
        assert!(tokenize("`unterminated").is_err());
    }

    #[rstest]
    #[case("amount", ident("amount"))]
    #[case("date", ident("date"))] // keyword not followed by a quote is a plain identifier
    #[case("x", ident("x"))]
    fn tokenizes_bareword_as_identifier(#[case] sql: &str, #[case] expected: Token) {
        assert_eq!(tokenize(sql).unwrap(), [expected]);
    }

    #[rstest]
    #[case("a ! b")] // lone `!`
    #[case("(a)")] // grouping not yet supported
    #[case("'unterminated")]
    fn rejects_ungrammatical_input(#[case] sql: &str) {
        assert!(tokenize(sql).is_err(), "expected {sql:?} to be rejected");
    }
}
