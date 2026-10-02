//! URI percent-encoding of raw path components, matching Hadoop's `Path.toUri().toString()`.
//!
//! Used both when encoding a filesystem path into `add.path` (Hive-style partition paths) and
//! when resolving a raw, table-relative AMT path against the table root.

use std::borrow::Cow;

use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

/// Characters encoded by [`uri_encode_path`]: `%`, space, and ASCII chars illegal in a
/// URI path, plus all ASCII controls via [`CONTROLS`]. Non-ASCII UTF-8 bytes are ALSO
/// percent-encoded by `utf8_percent_encode` (each byte of the UTF-8 sequence becomes
/// `%XX`), even though they are not in the [`AsciiSet`]. For example:
/// `uri_encode_path("München") == "M%C3%BCnchen"`, because `ü` is `0xC3 0xBC` in UTF-8.
///
/// TODO(#2423): Delta-Spark leaves non-ASCII bytes raw in `add.path` (e.g. `München`
/// stays as `München`). Our current behavior diverges — revisit once kernel matches
/// Delta-Spark's `add.path` encoding for non-ASCII.
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

/// URI percent-encodes a raw path component so it can be embedded in a URL path, matching
/// Hadoop `Path.toUri().toString()`. Unreserved characters (including `/`, `.`, and `:`) pass
/// through, so path separators and relative segments are preserved; the characters in
/// [`HADOOP_URI_PATH_ENCODE_SET`] and all non-ASCII bytes are percent-encoded. Returns
/// [`Cow::Borrowed`] when no encoding is needed (zero-allocation fast path).
pub(crate) fn uri_encode_path(path: &str) -> Cow<'_, str> {
    utf8_percent_encode(path, HADOOP_URI_PATH_ENCODE_SET).into()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Every ASCII byte: chars in `HADOOP_URI_PATH_ENCODE_SET` (space, `"`, `#`, `%`, `<`,
    /// `>`, `?`, `[`, `\`, `]`, `^`, backtick, `{`, `|`, `}`) plus all controls encode to
    /// `%XX`; everything else passes through unchanged.
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
}
