//! Path escaping for Delta actions, URL construction, and plan join keys.
//!
//! `uri_encode_path` retains Kernel's existing URL-safe encoding for writes. The plan-only DBR
//! canonicalizer preserves non-ASCII characters in relative path strings. Also detects whether a
//! location has a URI scheme, which decides whether it is absolute or relative.

use std::borrow::Cow;

use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use url::Url;

/// Characters encoded by [`uri_encode_path`]: `%`, space, URI-reserved ASCII and all controls.
/// Non-ASCII UTF-8 bytes are percent-encoded to keep their representation safe in a URL.
pub(crate) const HADOOP_URI_PATH_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// Percent-encodes a raw path component for URL construction, including non-ASCII UTF-8 bytes.
pub(crate) fn uri_encode_path(path: &str) -> Cow<'_, str> {
    utf8_percent_encode(path, HADOOP_URI_PATH_ENCODE_SET).into()
}

/// Encodes URI-reserved ASCII bytes while preserving non-ASCII characters as DBR does in paths.
/// Use this only for the new plan relations' relative-path join keys; URL serialization remains
/// the responsibility of `Url`.
#[cfg(any(feature = "declarative-plans", test))]
pub(crate) fn uri_encode_path_for_dbr(path: &str) -> Cow<'_, str> {
    if !path.bytes().any(needs_dbr_path_encoding) {
        return Cow::Borrowed(path);
    }
    let mut encoded = String::with_capacity(path.len());
    let mut copied_through = 0;
    for (index, character) in path.char_indices() {
        if !character.is_ascii() || !needs_dbr_path_encoding(character as u8) {
            continue;
        }
        encoded.push_str(&path[copied_through..index]);
        let byte = character as u8;
        encoded.push('%');
        encoded.push(hex_digit(byte >> 4));
        encoded.push(hex_digit(byte & 0x0f));
        copied_through = index + 1;
    }
    encoded.push_str(&path[copied_through..]);
    Cow::Owned(encoded)
}

#[cfg(any(feature = "declarative-plans", test))]
fn needs_dbr_path_encoding(byte: u8) -> bool {
    byte <= 0x20
        || byte == 0x7f
        || matches!(
            byte,
            b'"' | b'#'
                | b'%'
                | b'<'
                | b'>'
                | b'?'
                | b'['
                | b'\\'
                | b']'
                | b'^'
                | b'`'
                | b'{'
                | b'|'
                | b'}'
        )
}

#[cfg(any(feature = "declarative-plans", test))]
fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + nibble - 10) as char,
    }
}

/// Percent-decodes a URI path component back into its raw form. Returns `None` if the decoded
/// bytes are not valid UTF-8.
pub(crate) fn uri_decode_path(path: &str) -> Option<Cow<'_, str>> {
    percent_decode_str(path).decode_utf8().ok()
}

/// Returns the byte length of `location`'s URI scheme (excluding the terminating `:`), or `None`
/// if `location` does not begin with one, per [RFC 3986 section 3.1]:
/// `scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )`, terminated by `:`.
///
/// A location without a scheme is relative (per the Iceberg V4 path spec).
///
/// [RFC 3986 section 3.1]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
pub(crate) fn scheme_len(location: &str) -> Option<usize> {
    for (position, ch) in location.char_indices() {
        if ch == ':' {
            return (position > 0).then_some(position);
        }
        if !is_scheme_char(ch, position) {
            return None;
        }
    }
    None
}

/// Returns whether `ch` is allowed at `position` in a URI scheme, per [RFC 3986 section 3.1]:
/// the first character must be `ALPHA`; subsequent characters may also be `DIGIT`, `+`, `-`, or
/// `.`. Schemes are restricted to US-ASCII, so non-ASCII letters are rejected.
///
/// [RFC 3986 section 3.1]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
fn is_scheme_char(ch: char, position: usize) -> bool {
    if ch.is_ascii_alphabetic() {
        return true;
    }
    position > 0 && (ch.is_ascii_digit() || ch == '+' || ch == '-' || ch == '.')
}

/// Returns `url` as a string that ends in `/`, so a relative path can be appended to it with no
/// further separator.
pub(crate) fn with_trailing_slash(url: &Url) -> String {
    let mut base = url.as_str().to_string();
    if !base.ends_with('/') {
        base.push('/');
    }
    base
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[test]
    fn test_uri_encode_path_every_ascii_byte() {
        let must_encode: &[u8] = b" \"#%<>?[\\]^`{|}";
        for byte in 0x00..=0x7Fu8 {
            let input = String::from(byte as char);
            let result = uri_encode_path(&input);
            let is_control = byte <= 0x1F || byte == 0x7F;
            if must_encode.contains(&byte) || is_control {
                assert_eq!(result, format!("%{:02X}", byte), "byte 0x{byte:02X}");
            } else {
                assert_eq!(result, input, "byte 0x{byte:02X}");
            }
        }
    }

    /// Multi-char contexts for URI-only chars, pinning that surrounding unreserved chars
    /// pass through while the target gets encoded. Per-byte encoding decisions are covered
    /// exhaustively by `test_uri_encode_path_every_ascii_byte`; this guards against a
    /// refactor that only fails at character boundaries.
    #[rstest]
    #[case::backtick("a`b", "a%60b")]
    #[case::right_brace("a}b", "a%7Db")]
    #[case::double_quote("a\"b", "a%22b")]
    #[case::space("hello world", "hello%20world")]
    #[case::less_than("a<b", "a%3Cb")]
    #[case::greater_than("a>b", "a%3Eb")]
    #[case::pipe("a|b", "a%7Cb")]
    fn test_uri_encode_path_encodes_uri_only_set(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(uri_encode_path(input), expected);
    }

    /// Unreserved strings pass through byte-identical AND via the `Cow::Borrowed` fast
    /// path, so no allocation occurs on the common path input (dates, integers, short alnum
    /// strings, the Hive null-partition placeholder).
    #[rstest]
    #[case::empty("")]
    #[case::alnum("hello")]
    #[case::partition_path("p=abc/q=def/")]
    #[case::hive_default_partition("__HIVE_DEFAULT_PARTITION__")]
    #[case::alnum_unreserved("A-Za-z0-9_.~")]
    #[case::date("2024-01-15")]
    fn test_uri_encode_path_unreserved_borrows_and_passes_through(#[case] input: &str) {
        let result = uri_encode_path(input);
        assert_eq!(result, input);
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn dbr_path_encoding_preserves_non_ascii_and_escapes_reserved_ascii() {
        assert_eq!(uri_encode_path_for_dbr("München"), "München");
        assert_eq!(uri_encode_path_for_dbr("München/a b%"), "München/a%20b%25");
    }
}
