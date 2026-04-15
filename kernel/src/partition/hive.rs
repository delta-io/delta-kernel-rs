//! Hive-style partition path encoding.
//!
//! Percent-encodes partition column names and values for filesystem directory paths
//! (e.g., `region=US%2FEast/year=2024/`). Matches Hive's [`FileUtils.escapePathName`][hive]
//! and Spark's [`ExternalCatalogUtils.escapePathName`][spark].
//!
//! ```text
//! Step 2 (serialization):  Scalar::String("US/East")  -->  "US/East"     (partitionValues)
//! Step 3 (THIS MODULE):    "US/East"                  -->  "US%2FEast"   (directory name)
//! ```
//!
//! # Encoding rules
//!
//! Encodes: ASCII control chars (0x00-0x1F), DEL (0x7F),
//! `"` `#` `%` `'` `*` `/` `:` `=` `?` `\` `{` `[` `]` `^`.
//!
//! NOT encoded: space (0x20), non-ASCII (>= 0x80), `}`.
//!
//! On Windows, space (0x20), `<`, `>`, and `|` are additionally encoded to match
//! Spark's platform-specific behavior in `ExternalCatalogUtils.escapePathName`.
//!
//! See the encoding tables in the [`super`] module for comprehensive examples.
//!
//! [hive]: https://github.com/apache/hive/blob/trunk/common/src/java/org/apache/hadoop/hive/common/FileUtils.java
//! [spark]: https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/ExternalCatalogUtils.scala

use std::borrow::Cow;

const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

/// Placeholder for missing partition values in Hive-style directory paths.
///
/// Used when a partition column is null, empty string, or empty binary.
///
/// ```text
/// INSERT INTO t VALUES (1, NULL);   --> p=__HIVE_DEFAULT_PARTITION__/...
/// INSERT INTO t VALUES (2, '');     --> p=__HIVE_DEFAULT_PARTITION__/...
/// INSERT INTO t VALUES (3, 'US');   --> p=US/...
/// ```
pub(crate) const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";
const HIVE_DEFAULT_PARTITION_LEN: usize = HIVE_DEFAULT_PARTITION.len();

/// Percent-encodes a string for use in a Hive-style partition path segment.
///
/// Returns `Cow::Borrowed` when no encoding is needed (zero allocation fast path).
///
/// Examples: `"US"` passes through unchanged, `"Serbia/srb%"` becomes
/// `"Serbia%2Fsrb%25"`, `"a=b"` becomes `"a%3Db"`.
pub(crate) fn escape_partition_value(s: &str) -> Cow<'_, str> {
    let first = s.bytes().position(needs_escaping);
    let Some(first) = first else {
        return Cow::Borrowed(s);
    };

    // Each escaped byte expands from 1 byte to 3 (e.g., "/" -> "%2F"), adding 2 extra.
    // One pass over the string gives us the exact allocation size.
    let extra = s.bytes().filter(|b| needs_escaping(*b)).count() * 2;
    let mut out = String::with_capacity(s.len() + extra);
    out.push_str(&s[..first]);

    // All escapable bytes are ASCII (< 0x80). UTF-8 continuation bytes are >= 0x80, so
    // iterating by char correctly preserves multi-byte sequences. For example, in
    // "a/\u{00FC}" the slash is a single byte 0x2F (escaped), but \u{00FC} is two bytes
    // [0xC3, 0xBC] which must be emitted together. Byte-level iteration would split them.
    for c in s[first..].chars() {
        if c.is_ascii() && needs_escaping(c as u8) {
            let b = c as u8;
            out.push('%');
            out.push(HEX_UPPER[(b >> 4) as usize] as char);
            out.push(HEX_UPPER[(b & 0x0F) as usize] as char);
        } else {
            out.push(c);
        }
    }
    Cow::Owned(out)
}

/// Builds a Hive-style partition path prefix from column names and serialized values.
///
/// Returns `col1=val1/col2=val2/` with both names and values percent-encoded.
/// Missing values (`None` or empty string) use [`HIVE_DEFAULT_PARTITION`].
///
/// ```ignore
/// use crate::partition::hive::build_partition_path;
///
/// let path = build_partition_path(&[("country", Some("US")), ("year", Some("2025"))]);
/// assert_eq!(path, "country=US/year=2025/");
///
/// let null_path = build_partition_path(&[("col", None)]);
/// let empty_path = build_partition_path(&[("col", Some(""))]);
/// assert_eq!(null_path, empty_path);
/// assert_eq!(null_path, "col=__HIVE_DEFAULT_PARTITION__/");
/// ```
pub(crate) fn build_partition_path(columns: &[(&str, Option<&str>)]) -> String {
    // Lower-bound capacity: exact when no escaping needed (the common case for
    // partition names and most values like dates, integers, short strings).
    let cap: usize = columns
        .iter()
        .map(|(n, v)| n.len() + 1 + v.map_or(HIVE_DEFAULT_PARTITION_LEN, str::len) + 1)
        .sum();
    let mut path = String::with_capacity(cap);
    for (name, value) in columns {
        path.push_str(&escape_partition_value(name));
        path.push('=');
        match value {
            Some(v) if !v.is_empty() => path.push_str(&escape_partition_value(v)),
            _ => path.push_str(HIVE_DEFAULT_PARTITION),
        }
        path.push('/');
    }
    path
}

// ============================================================================
// Helpers
// ============================================================================

/// Returns true if the byte must be percent-encoded in a Hive partition path segment.
/// Matches the escape set from Hive's `FileUtils.escapePathName` and Spark's
/// `ExternalCatalogUtils.escapePathName`.
///
/// `}` (0x7D) is NOT escaped, matching Hive. Only `{` is in the set.
///
/// On Windows, additional characters are escaped. See the module-level encoding rules.
fn needs_escaping(b: u8) -> bool {
    if matches!(
        b,
        0x00..=0x1F
            | b'"'
            | b'#'
            | b'%'
            | b'\''
            | b'*'
            | b'/'
            | b':'
            | b'='
            | b'?'
            | b'\\'
            | 0x7F
            | b'{'
            | b'['
            | b']'
            | b'^'
    ) {
        return true;
    }
    #[cfg(target_os = "windows")]
    if matches!(b, b' ' | b'<' | b'>' | b'|') {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    /// Inverse of `escape_partition_value` for round-trip testing.
    /// Ported from Hive's `FileUtils.unescapePathName`.
    fn unescape_path_name(path: &str) -> String {
        let Some(first) = path.find('%') else {
            return path.to_owned();
        };
        let bytes = path.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        out.extend_from_slice(&bytes[..first]);
        let mut i = first;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                if let (Some(hi), Some(lo)) = (from_hex(bytes[i + 1]), from_hex(bytes[i + 2])) {
                    out.push(hi << 4 | lo);
                    i += 3;
                    continue;
                }
            }
            out.push(bytes[i]);
            i += 1;
        }
        String::from_utf8(out).expect("unescape produced invalid UTF-8")
    }

    fn from_hex(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'A'..=b'F' => Some(b - b'A' + 10),
            b'a'..=b'f' => Some(b - b'a' + 10),
            _ => None,
        }
    }

    // ============================================================================
    // escape_partition_value
    // ============================================================================

    /// Rows from the encoding table in `partition/mod.rs`.
    /// Rows 50/52 (non-UTF-8 binary like X'DEADBEEF', X'00FF') cannot be tested here
    /// because Rust's `&str` requires valid UTF-8, which prevents constructing those inputs.
    /// Rows 49/65/66 (empty/null) are tested via `build_partition_path` below.
    #[rstest]
    // Rows 47-48: NUL bytes. Delta-Spark fails at mkdirs, but our encoding succeeds.
    #[case("\x00", "%00")]
    #[case("before\x00after", "before%00after")]
    #[case("HELLO", "HELLO")] // 51
    #[case("/=%", "%2F%3D%25")] // 53
    #[case("a{b", "a%7Bb")] // 54
    #[case("a}b", "a}b")] // 55
    #[case("M\u{00FC}nchen", "M\u{00FC}nchen")] // 57
    #[case("\u{65E5}\u{672C}\u{8A9E}", "\u{65E5}\u{672C}\u{8A9E}")] // 58
    #[case("\u{1F3B5}\u{1F3B6}", "\u{1F3B5}\u{1F3B6}")] // 59
    #[case("a@b!c(d)", "a@b!c(d)")] // 61
    #[case("a&b+c$d;e,f", "a&b+c$d;e,f")] // 62
    #[case("Serbia/srb%", "Serbia%2Fsrb%25")] // 63
    #[case("100%25", "100%2525")] // 64
    fn test_escape_table_rows(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(escape_partition_value(input), expected);
    }

    /// Individual chars from the Hive escape set.
    #[rstest]
    #[case("\x00", "%00")]
    #[case("\x01", "%01")]
    #[case("\t", "%09")]
    #[case("\n", "%0A")]
    #[case("\r", "%0D")]
    #[case("\"", "%22")]
    #[case("#", "%23")]
    #[case("%", "%25")]
    #[case("'", "%27")]
    #[case("*", "%2A")]
    #[case("/", "%2F")]
    #[case(":", "%3A")]
    #[case("=", "%3D")]
    #[case("?", "%3F")]
    #[case("\\", "%5C")]
    #[case("[", "%5B")]
    #[case("]", "%5D")]
    #[case("^", "%5E")]
    #[case("{", "%7B")]
    #[case("\x7F", "%7F")]
    fn test_escape_individual_special_chars(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(escape_partition_value(input), expected);
    }

    /// Chars NOT in the escape set on any platform.
    #[rstest]
    #[case("", "")]
    #[case("US", "US")]
    #[case("2024-01-15", "2024-01-15")]
    #[case("}", "}")]
    #[case("~", "~")]
    #[case("@", "@")]
    #[case("!", "!")]
    #[case("(", "(")]
    #[case(")", ")")]
    #[case("&", "&")]
    #[case("+", "+")]
    #[case("$", "$")]
    #[case(";", ";")]
    #[case(",", ",")]
    fn test_escape_passthrough(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(escape_partition_value(input), expected);
    }

    /// Mixed strings: mid-string encoding, index-zero encoding, double-encoding.
    #[rstest]
    #[case("a/b", "a%2Fb")]
    #[case("a=b", "a%3Db")]
    #[case("100%", "100%25")]
    #[case("/abc", "%2Fabc")]
    #[case("\x01abc", "%01abc")]
    #[case("{}", "%7B}")]
    #[case("a{b}c", "a%7Bb}c")]
    #[case("%%", "%25%25")]
    #[case("a%b%c", "a%25b%25c")]
    #[case("%2F", "%252F")]
    #[case("%3D", "%253D")]
    #[case("region=us/east#1", "region%3Dus%2Feast%231")]
    fn test_escape_mixed(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(escape_partition_value(input), expected);
    }

    /// Non-ASCII preserved after escaped chars, including 2/3/4-byte UTF-8.
    #[rstest]
    #[case("a/\u{00FC}", "a%2F\u{00FC}")]
    #[case("M\u{00FC}nchen/Bayern", "M\u{00FC}nchen%2FBayern")]
    #[case("/\u{00FC}", "%2F\u{00FC}")]
    #[case("/\u{20AC}", "%2F\u{20AC}")]
    #[case("/\u{1D11E}", "%2F\u{1D11E}")]
    #[case(
        "\u{65E5}\u{672C}=\u{6771}\u{4EAC}",
        "\u{65E5}\u{672C}%3D\u{6771}\u{4EAC}"
    )]
    #[case("\u{1F3B5}/\u{1F3B6}", "\u{1F3B5}%2F\u{1F3B6}")]
    #[case("/\u{00E4}\u{00E4}", "%2F\u{00E4}\u{00E4}")]
    fn test_escape_unicode_after_special(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(escape_partition_value(input), expected);
    }

    /// Cow::Borrowed fast path when no encoding is needed.
    #[rstest]
    #[case("hello")]
    #[case("\u{00FC}ber")]
    fn test_escape_fast_path_returns_borrowed(#[case] input: &str) {
        assert!(matches!(escape_partition_value(input), Cow::Borrowed(_)));
    }

    // ============================================================================
    // Exhaustive / combinatorial
    // ============================================================================

    /// Every ASCII byte (0x00-0x7F) individually, verifying the exact Hive escape set.
    #[test]
    fn test_escape_every_ascii_byte() {
        #[allow(unused_mut)]
        let mut must_escape: Vec<u8> = vec![
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B,
            0x1C, 0x1D, 0x1E, 0x1F, b'"', b'#', b'%', b'\'', b'*', b'/', b':', b'=', b'?', b'\\',
            0x7F, b'{', b'[', b']', b'^',
        ];
        #[cfg(target_os = "windows")]
        must_escape.extend_from_slice(b" <>|");
        for byte in 0x00..=0x7Fu8 {
            let input = String::from(byte as char);
            let result = escape_partition_value(&input);
            if must_escape.contains(&byte) {
                assert_eq!(result, format!("%{:02X}", byte), "byte 0x{byte:02X}");
            } else {
                assert_eq!(result, input, "byte 0x{byte:02X}");
            }
        }
    }

    /// Every ASCII byte preceded by `/` (exercises slow path for all second bytes).
    #[test]
    fn test_escape_slash_followed_by_every_ascii() {
        for second in 0x01..=0x7Fu8 {
            let input = format!("/{}", second as char);
            let result = escape_partition_value(&input);
            assert!(result.starts_with("%2F"), "byte 0x{second:02X}: {result:?}");
            if needs_escaping(second) {
                assert_eq!(result, format!("%2F%{:02X}", second), "byte 0x{second:02X}");
            } else {
                assert_eq!(
                    result,
                    format!("%2F{}", second as char),
                    "byte 0x{second:02X}"
                );
            }
        }
    }

    /// Non-ASCII (u-umlaut) after every escaped char.
    #[test]
    fn test_escape_non_ascii_after_every_special() {
        for &byte in b"\"#%'*/:=?\\{[]^" {
            let input = format!("{}\u{00FC}", byte as char);
            let expected = format!("%{:02X}\u{00FC}", byte);
            assert_eq!(
                escape_partition_value(&input),
                expected,
                "byte 0x{byte:02X}"
            );
        }
    }

    // ============================================================================
    // Platform-sensitive chars (space, <, >, |)
    // ============================================================================

    /// On non-Windows, space/</>/| pass through unescaped (rows 56, 60, 67, 68).
    #[cfg(not(target_os = "windows"))]
    #[test]
    fn test_escape_platform_sensitive_chars_pass_through() {
        assert_eq!(escape_partition_value(" "), " ");
        assert_eq!(escape_partition_value("  "), "  ");
        assert_eq!(escape_partition_value("hello world"), "hello world");
        assert_eq!(escape_partition_value("<"), "<");
        assert_eq!(escape_partition_value(">"), ">");
        assert_eq!(escape_partition_value("|"), "|");
        assert_eq!(escape_partition_value("a<b>c|d"), "a<b>c|d");
        assert_eq!(
            escape_partition_value("2024-01-15 12:30:45"),
            "2024-01-15 12%3A30%3A45"
        );
    }

    /// On non-Windows, build_partition_path preserves spaces (rows 56, 67, 68).
    #[cfg(not(target_os = "windows"))]
    #[test]
    fn test_build_path_platform_sensitive_chars_pass_through() {
        assert_eq!(
            build_partition_path(&[("p", Some("hello world"))]),
            "p=hello world/"
        );
        assert_eq!(build_partition_path(&[("p", Some(" "))]), "p= /");
        assert_eq!(build_partition_path(&[("p", Some("  "))]), "p=  /");
    }

    /// On Windows, space/</>/| are escaped.
    #[cfg(target_os = "windows")]
    #[test]
    fn test_escape_windows_special_chars_are_escaped() {
        assert_eq!(escape_partition_value(" "), "%20");
        assert_eq!(escape_partition_value("  "), "%20%20");
        assert_eq!(escape_partition_value("hello world"), "hello%20world");
        assert_eq!(escape_partition_value("<"), "%3C");
        assert_eq!(escape_partition_value(">"), "%3E");
        assert_eq!(escape_partition_value("|"), "%7C");
        assert_eq!(escape_partition_value("a<b>c|d"), "a%3Cb%3Ec%7Cd");
        assert_eq!(
            escape_partition_value("2024-01-15 12:30:45"),
            "2024-01-15%2012%3A30%3A45"
        );
    }

    /// On Windows, build_partition_path escapes spaces.
    #[cfg(target_os = "windows")]
    #[test]
    fn test_build_path_windows_escapes_spaces() {
        assert_eq!(
            build_partition_path(&[("p", Some("hello world"))]),
            "p=hello%20world/"
        );
        assert_eq!(build_partition_path(&[("p", Some(" "))]), "p=%20/");
        assert_eq!(build_partition_path(&[("p", Some("  "))]), "p=%20%20/");
    }

    // ============================================================================
    // Round-trip: unescape(escape(s)) == s
    // ============================================================================

    #[test]
    fn test_round_trip_every_ascii_byte() {
        for byte in 0x01..=0x7Fu8 {
            let input = String::from(byte as char);
            let rt = unescape_path_name(&escape_partition_value(&input));
            assert_eq!(rt, input, "byte 0x{byte:02X}");
        }
    }

    #[test]
    fn test_round_trip_mixed_strings() {
        for input in [
            "hello",
            "",
            "a/b",
            "key=value",
            "100%",
            "region=us/east#1",
            "2024-01-15 12:30:45",
            "Serbia/srb%",
            "a\"b#c%d'e*f/g:h=i?j\\k",
            "path/with spaces/and=equals",
            "  spaces  ",
            "tab\there",
            "newline\nhere",
        ] {
            assert_eq!(
                unescape_path_name(&escape_partition_value(input)),
                input,
                "{input:?}"
            );
        }
    }

    #[test]
    fn test_round_trip_unicode() {
        for input in [
            "\u{00FC}ber",
            "\u{65E5}\u{672C}\u{8A9E}",
            "M\u{00FC}nchen/Bayern",
            "\u{65E5}\u{672C}=\u{6771}\u{4EAC}",
            "\u{1F3B5}/\u{1F3B6}",
            "a/\u{00FC}ber",
            "/\u{20AC}100",
            "caf\u{00E9}=\u{2615}",
        ] {
            assert_eq!(
                unescape_path_name(&escape_partition_value(input)),
                input,
                "{input:?}"
            );
        }
    }

    #[test]
    fn test_round_trip_two_char_combinations() {
        for first in 0x01..=0x7Fu8 {
            for second in 0x01..=0x7Fu8 {
                let input = format!("{}{}", first as char, second as char);
                let rt = unescape_path_name(&escape_partition_value(&input));
                assert_eq!(rt, input, "(0x{first:02X}, 0x{second:02X})");
            }
        }
    }

    /// Ported from Hive's `TestFileUtils.testPathEscapeChars`.
    #[test]
    fn test_round_trip_all_escaped_chars_combined() {
        let mut input = String::new();
        for byte in 0x00..=0x7Fu8 {
            if needs_escaping(byte) {
                input.push(byte as char);
            }
        }
        assert_eq!(unescape_path_name(&escape_partition_value(&input)), input);
    }

    // ============================================================================
    // build_partition_path
    // ============================================================================

    /// Rows from the encoding table in `partition/mod.rs`.
    #[rstest]
    #[case("p", Some(""), "p=__HIVE_DEFAULT_PARTITION__/")] // 49: empty binary
    #[case("p", Some("/=%"), "p=%2F%3D%25/")] // 53
    #[case("p", Some("a{b"), "p=a%7Bb/")] // 54
    #[case("p", Some("a}b"), "p=a}b/")] // 55
    #[case("p", Some("M\u{00FC}nchen"), "p=M\u{00FC}nchen/")] // 57
    #[case("p", Some("Serbia/srb%"), "p=Serbia%2Fsrb%25/")] // 63
    #[case("p", Some("100%25"), "p=100%2525/")] // 64
    #[case("p", None, "p=__HIVE_DEFAULT_PARTITION__/")] // 66: NULL
    fn test_build_path_table_rows(
        #[case] name: &str,
        #[case] value: Option<&str>,
        #[case] expected: &str,
    ) {
        assert_eq!(build_partition_path(&[(name, value)]), expected);
    }

    /// Additional single-column cases.
    #[rstest]
    #[case("country", Some("US"), "country=US/")]
    #[case("country", Some("US/A%B"), "country=US%2FA%25B/")]
    #[case("col=name", Some("value"), "col%3Dname=value/")]
    fn test_build_path_single_column(
        #[case] name: &str,
        #[case] value: Option<&str>,
        #[case] expected: &str,
    ) {
        assert_eq!(build_partition_path(&[(name, value)]), expected);
    }

    #[test]
    fn test_build_path_empty_columns_returns_empty() {
        assert_eq!(build_partition_path(&[]), "");
    }

    #[test]
    fn test_build_path_multiple_columns_preserves_order() {
        assert_eq!(
            build_partition_path(&[("country", Some("US")), ("year", Some("2025"))]),
            "country=US/year=2025/"
        );
    }

    #[test]
    fn test_build_path_mixed_null_and_values_preserves_order() {
        assert_eq!(
            build_partition_path(&[
                ("year", Some("2025")),
                ("region", None),
                ("country", Some("US")),
            ]),
            "year=2025/region=__HIVE_DEFAULT_PARTITION__/country=US/"
        );
    }
}
