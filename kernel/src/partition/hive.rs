//! Hive-style partition path encoding for Delta table writes.
//!
//! This module percent-encodes partition column names and values for use in **file system
//! directory paths** (e.g., `region=US%2FEast/year=2024/`). It implements the same encoding
//! as Hive's [`FileUtils.escapePathName`][hive] and Spark's
//! [`ExternalCatalogUtils.escapePathName`][spark].
//!
//! # When to use this module
//!
//! This module handles **file path encoding only**. It is completely independent from the
//! `partitionValues` map serialization in Add actions. The same partition value goes through
//! two separate transformations, and this module handles only the second one:
//!
//! ```text
//! Scalar::String("US/East")
//!         |
//!         |  (1) serialization: serialize for the Delta log
//!         v
//!   Some("US/East")           <- stored in AddFile.partitionValues as-is
//!         |
//!         |  (2) THIS MODULE: encode for the file system path
//!         v
//!   "US%2FEast"               <- used in directory name: region=US%2FEast/
//! ```
//!
//! Step (1) is handled internally by [`Transaction::partitioned_write_context`]. Step (2)
//! is what this module provides. The slash in `"US/East"` is a path separator on the file
//! system and must be encoded, but it appears raw in the Delta log JSON because
//! `partitionValues` is just a string map.
//!
//! # Encoding rules
//!
//! Encodes ASCII control characters (0x01-0x1F), DEL (0x7F), and the characters
//! `"` `#` `%` `'` `*` `/` `:` `=` `?` `\` `{` `[` `]` `^`. Everything else passes
//! through unchanged, including spaces (0x20) and non-ASCII characters.
//!
//! ```text
//! "hello"      -> "hello"         (no encoding, zero allocation)
//! "US/East"    -> "US%2FEast"     (slash encoded)
//! "a=b"        -> "a%3Db"         (equals encoded)
//! "100%"       -> "100%25"        (percent encoded)
//! "a b"        -> "a b"           (space NOT encoded)
//! "Muenchen"   -> "Muenchen"      (non-ASCII NOT encoded)
//! ```
//!
//! # Note
//!
//! These are **convenience utilities**. The Delta protocol does not require Hive-style
//! paths. [`DefaultEngine::write_parquet`] uses [`WriteContext::write_dir`] internally,
//! which calls these functions when column mapping is off. Custom engines may use them
//! directly or use flat paths.
//!
//! [`Transaction::partitioned_write_context`]: crate::transaction::Transaction::partitioned_write_context
//! [`DefaultEngine::write_parquet`]: crate::engine::default::DefaultEngine::write_parquet
//! [`WriteContext::write_dir`]: crate::transaction::WriteContext::write_dir
//!
//! [hive]: https://github.com/apache/hive/blob/trunk/common/src/java/org/apache/hadoop/hive/common/FileUtils.java
//! [spark]: https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/ExternalCatalogUtils.scala

use std::borrow::Cow;

/// The placeholder used for null partition values in Hive-style directory paths.
///
/// When a partition column value is null, Hive/Spark use this sentinel string as the
/// directory name (e.g., `country=__HIVE_DEFAULT_PARTITION__/`).
pub const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Returns true if the byte must be percent-encoded in a Hive partition path segment.
///
/// Escaped set (matches Hive/Spark):
///   - ASCII control characters 0x01-0x1F (0x00 is excluded, matching Spark)
///   - `"` `#` `%` `'` `*` `/` `:` `=` `?` `\` DEL(0x7F) `{` `[` `]` `^`
///
/// Note: `}` (0x7D) is NOT escaped, matching the Hive source. Only `{` is in the set.
fn needs_escaping(b: u8) -> bool {
    matches!(
        b,
        0x01..=0x1F
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
    )
}

/// Percent-encodes a string for use in a Hive-style partition path segment.
///
/// Encodes ASCII control characters (0x01-0x1F), DEL (0x7F), and the characters
/// `"` `#` `%` `'` `*` `/` `:` `=` `?` `\` `{` `[` `]` `^`. Everything else,
/// including NUL (0x00), spaces (0x20), and non-ASCII characters, passes through
/// unchanged. This matches the behavior of Hive's `FileUtils.escapePathName` and Spark's
/// `ExternalCatalogUtils.escapePathName`.
///
/// This is a convenience utility. The Delta protocol does not require Hive-style paths.
///
/// # Example
///
/// ```
/// use delta_kernel::partition::escape_partition_value;
///
/// assert_eq!(escape_partition_value("US"), "US");
/// assert_eq!(escape_partition_value("Serbia/srb%"), "Serbia%2Fsrb%25");
/// assert_eq!(escape_partition_value("a=b"), "a%3Db");
/// ```
pub fn escape_partition_value(s: &str) -> Cow<'_, str> {
    let first = s.bytes().position(needs_escaping);
    let Some(first) = first else {
        return Cow::Borrowed(s);
    };

    const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len() + 16); // room for a few percent-encoded chars
    out.push_str(&s[..first]);
    // The escape set is entirely ASCII (< 0x80). UTF-8 guarantees that non-ASCII characters
    // only contain bytes >= 0x80, so only single-byte ASCII characters can need escaping.
    // We iterate by char to correctly handle multi-byte UTF-8 sequences.
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
/// Returns a path like `col1=val1/col2=val2/` with both names and values encoded via
/// [`escape_partition_value`]. Null values (represented as `None`) use
/// [`HIVE_DEFAULT_PARTITION`] as the value.
///
/// The columns should be provided in the order they should appear in the path. Typically
/// this is the order from [`Transaction::logical_partition_columns`].
///
/// This is a convenience utility. The Delta protocol does not require Hive-style paths.
///
/// # Example
///
/// ```
/// use delta_kernel::partition::build_partition_path;
///
/// let path = build_partition_path(&[
///     ("country", Some("US")),
///     ("year", Some("2025")),
/// ]);
/// assert_eq!(path, "country=US/year=2025/");
///
/// let path_with_null = build_partition_path(&[
///     ("country", None),
/// ]);
/// assert_eq!(path_with_null, "country=__HIVE_DEFAULT_PARTITION__/");
/// ```
///
/// [`Transaction::logical_partition_columns`]: crate::transaction::Transaction::logical_partition_columns
pub fn build_partition_path(columns: &[(&str, Option<&str>)]) -> String {
    let mut path = String::new();
    for (name, value) in columns {
        path.push_str(&escape_partition_value(name));
        path.push('=');
        match value {
            Some(v) => path.push_str(&escape_partition_value(v)),
            None => path.push_str(HIVE_DEFAULT_PARTITION),
        }
        path.push('/');
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    // === escape_partition_value tests ===

    #[test]
    fn test_escape_partition_value_plain_ascii_passes_through_unchanged() {
        assert_eq!(escape_partition_value("hello"), "hello");
        assert_eq!(escape_partition_value("US"), "US");
        assert_eq!(escape_partition_value("2024-01-15"), "2024-01-15");
    }

    #[test]
    fn test_escape_partition_value_empty_string_passes_through() {
        assert_eq!(escape_partition_value(""), "");
    }

    #[test]
    fn test_escape_partition_value_slash_is_percent_encoded() {
        assert_eq!(escape_partition_value("a/b"), "a%2Fb");
    }

    #[test]
    fn test_escape_partition_value_equals_is_percent_encoded() {
        assert_eq!(escape_partition_value("a=b"), "a%3Db");
    }

    #[test]
    fn test_escape_partition_value_percent_is_percent_encoded() {
        assert_eq!(escape_partition_value("100%"), "100%25");
    }

    #[test]
    fn test_escape_partition_value_space_passes_through() {
        assert_eq!(escape_partition_value("a b"), "a b");
    }

    #[test]
    fn test_escape_partition_value_non_ascii_passes_through() {
        assert_eq!(escape_partition_value("\u{00FC}ber"), "\u{00FC}ber");
        assert_eq!(
            escape_partition_value("\u{65E5}\u{672C}\u{8A9E}"),
            "\u{65E5}\u{672C}\u{8A9E}"
        );
    }

    #[test]
    fn test_escape_partition_value_non_ascii_after_special_char_is_preserved() {
        assert_eq!(escape_partition_value("a/\u{00FC}"), "a%2F\u{00FC}");
        assert_eq!(
            escape_partition_value("M\u{00FC}nchen/Bayern"),
            "M\u{00FC}nchen%2FBayern"
        );
    }

    #[test]
    fn test_escape_partition_value_nul_byte_passes_through() {
        // 0x00 is NOT escaped, matching Spark's ExternalCatalogUtils which starts at \u0001.
        assert_eq!(escape_partition_value("\x00"), "\x00");
    }

    #[test]
    fn test_escape_partition_value_control_chars_are_percent_encoded() {
        assert_eq!(escape_partition_value("\x01"), "%01");
        assert_eq!(escape_partition_value("\n"), "%0A");
        assert_eq!(escape_partition_value("\r"), "%0D");
        assert_eq!(escape_partition_value("\t"), "%09");
    }

    #[test]
    fn test_escape_partition_value_all_special_chars_are_percent_encoded() {
        let cases = vec![
            ("\"", "%22"),
            ("#", "%23"),
            ("%", "%25"),
            ("'", "%27"),
            ("*", "%2A"),
            ("/", "%2F"),
            (":", "%3A"),
            ("=", "%3D"),
            ("?", "%3F"),
            ("\\", "%5C"),
            ("\x7F", "%7F"),
            ("{", "%7B"),
            ("[", "%5B"),
            ("]", "%5D"),
            ("^", "%5E"),
        ];
        for (input, expected) in cases {
            assert_eq!(escape_partition_value(input), expected, "input: {input:?}");
        }
    }

    #[test]
    fn test_escape_partition_value_closing_brace_passes_through() {
        assert_eq!(escape_partition_value("}"), "}");
        assert_eq!(escape_partition_value("a}b"), "a}b");
    }

    #[test]
    fn test_escape_partition_value_mixed_special_chars_are_encoded() {
        assert_eq!(escape_partition_value("Serbia/srb%"), "Serbia%2Fsrb%25");
    }

    #[test]
    fn test_escape_partition_value_colons_are_percent_encoded() {
        assert_eq!(
            escape_partition_value("2024-01-15 12:30:45"),
            "2024-01-15 12%3A30%3A45"
        );
    }

    #[test]
    fn test_escape_partition_value_fast_path_returns_borrowed() {
        let result = escape_partition_value("hello");
        assert!(
            matches!(result, Cow::Borrowed(_)),
            "expected Cow::Borrowed for plain ASCII"
        );
        let result = escape_partition_value("\u{00FC}ber");
        assert!(
            matches!(result, Cow::Borrowed(_)),
            "expected Cow::Borrowed for pure non-ASCII"
        );
    }

    // === build_partition_path tests ===

    #[test]
    fn test_build_partition_path_single_column_produces_hive_format() {
        let path = build_partition_path(&[("country", Some("US"))]);
        assert_eq!(path, "country=US/");
    }

    #[test]
    fn test_build_partition_path_multiple_columns_preserves_order() {
        let path = build_partition_path(&[("country", Some("US")), ("year", Some("2025"))]);
        assert_eq!(path, "country=US/year=2025/");
    }

    #[test]
    fn test_build_partition_path_null_value_uses_hive_default() {
        let path = build_partition_path(&[("country", None)]);
        assert_eq!(path, "country=__HIVE_DEFAULT_PARTITION__/");
    }

    #[test]
    fn test_build_partition_path_special_chars_are_encoded() {
        let path = build_partition_path(&[("country", Some("US/A%B"))]);
        assert_eq!(path, "country=US%2FA%25B/");
    }

    #[test]
    fn test_build_partition_path_empty_columns_returns_empty_string() {
        let path = build_partition_path(&[]);
        assert_eq!(path, "");
    }

    #[test]
    fn test_build_partition_path_column_name_is_also_escaped() {
        let path = build_partition_path(&[("col=name", Some("value"))]);
        assert_eq!(path, "col%3Dname=value/");
    }

    #[test]
    fn test_build_partition_path_mixed_null_and_values() {
        let path = build_partition_path(&[
            ("year", Some("2025")),
            ("region", None),
            ("country", Some("US")),
        ]);
        assert_eq!(
            path,
            "year=2025/region=__HIVE_DEFAULT_PARTITION__/country=US/"
        );
    }
}
