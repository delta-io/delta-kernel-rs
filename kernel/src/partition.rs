//! Partition-related utilities for Delta table writes.
//!
//! This module provides:
//!
//! - **Hive-style partition path encoding**: [`escape_partition_value`] and
//!   [`build_partition_path`] produce directory layouts like `col1=val1/col2=val2/` with
//!   special characters encoded using the same rules as:
//!   - Hive's [`FileUtils.escapePathName`][hive]
//!   - Spark's [`ExternalCatalogUtils.escapePathName`][spark]
//!
//! These are **convenience utilities**. The Delta protocol does not require any particular
//! file path format. The `partitionValues` map in the Add action is the source of truth for
//! partition column values, not the file path. Connectors may use flat paths like
//! `<table_root>/<uuid>.parquet` (which is what [`DefaultEngine::write_parquet`] does by
//! default) or Hive-style paths -- the choice is entirely up to the connector.
//!
//! These utilities are primarily useful for custom engine implementations that construct
//! file paths themselves (e.g., to write files into Hive-style partition directories).
//! Connectors using [`DefaultEngine::write_parquet`] write to flat paths and do not need
//! these utilities.
//!
//! ## Partitioned write utilities overview
//!
//! | Utility | Purpose | Who needs it |
//! |---------|---------|-------------|
//! | [`Transaction::partition_columns`] | Discover partition column names | All connectors |
//! | [`Scalar::serialize_partition_value`] | Serialize typed values for the `partitionValues` map | Custom engines only (DefaultEngine does this internally) |
//! | [`escape_partition_value`] | Hive-encode special chars in path segments | Custom engines wanting Hive-style paths |
//! | [`build_partition_path`] | Assemble `col=val/col=val/` path prefix | Custom engines wanting Hive-style paths |
//!
//! [`Transaction::partition_columns`]: crate::transaction::Transaction::partition_columns
//! [`Scalar::serialize_partition_value`]: crate::expressions::Scalar::serialize_partition_value
//!
//! [hive]: https://github.com/apache/hive/blob/trunk/common/src/java/org/apache/hadoop/hive/common/FileUtils.java
//! [spark]: https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/ExternalCatalogUtils.scala
//! [`DefaultEngine::write_parquet`]: crate::engine::default::DefaultEngine::write_parquet

use std::borrow::Cow;

const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

/// The placeholder used for null partition values in Hive-style directory paths.
///
/// When a partition column value is null, Hive/Spark use this sentinel string as the
/// directory name (e.g., `country=__HIVE_DEFAULT_PARTITION__/`).
pub const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Returns true if the byte must be percent-encoded in a Hive partition path segment.
///
/// Escaped set (matches Hive/Spark):
///   - ASCII control characters 0x00-0x1F
///   - `"` `#` `%` `'` `*` `/` `:` `=` `?` `\` DEL(0x7F) `{` `[` `]` `^`
fn needs_escaping(b: u8) -> bool {
    matches!(
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
    )
}

/// Percent-encodes a string for use in a Hive-style partition path segment.
///
/// Only the characters listed above are encoded. Everything else, including spaces (0x20) and
/// non-ASCII bytes (>= 0x80), passes through unchanged. This matches the behavior of Hive's
/// `FileUtils.escapePathName` and Spark's `ExternalCatalogUtils.escapePathName`.
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

    let bytes = s.as_bytes();
    let mut out = String::with_capacity(bytes.len() + 16);
    out.push_str(&s[..first]);
    for &b in &bytes[first..] {
        if needs_escaping(b) {
            out.push('%');
            out.push(HEX_UPPER[(b >> 4) as usize] as char);
            out.push(HEX_UPPER[(b & 0x0F) as usize] as char);
        } else {
            out.push(b as char);
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
/// this is the order from [`Transaction::partition_columns`] or
/// [`TableConfiguration::partition_columns`].
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
/// [`Transaction::partition_columns`]: crate::transaction::Transaction::partition_columns
/// [`TableConfiguration::partition_columns`]: crate::table_configuration::TableConfiguration::partition_columns
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
    fn test_plain_ascii_passes_through_unchanged() {
        assert_eq!(escape_partition_value("hello"), "hello");
        assert_eq!(escape_partition_value("US"), "US");
        assert_eq!(escape_partition_value("2024-01-15"), "2024-01-15");
    }

    #[test]
    fn test_empty_string_passes_through() {
        assert_eq!(escape_partition_value(""), "");
    }

    #[test]
    fn test_slash_encoded() {
        assert_eq!(escape_partition_value("a/b"), "a%2Fb");
    }

    #[test]
    fn test_equals_encoded() {
        assert_eq!(escape_partition_value("a=b"), "a%3Db");
    }

    #[test]
    fn test_percent_encoded() {
        assert_eq!(escape_partition_value("100%"), "100%25");
    }

    #[test]
    fn test_space_not_encoded() {
        assert_eq!(escape_partition_value("a b"), "a b");
    }

    #[test]
    fn test_non_ascii_not_encoded() {
        assert_eq!(escape_partition_value("uber"), "uber");
    }

    #[test]
    fn test_control_chars_encoded() {
        assert_eq!(escape_partition_value("\x01"), "%01");
        assert_eq!(escape_partition_value("\n"), "%0A");
    }

    #[test]
    fn test_all_special_chars_encoded() {
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
    fn test_mixed_value_with_special_chars() {
        assert_eq!(escape_partition_value("Serbia/srb%"), "Serbia%2Fsrb%25");
    }

    #[test]
    fn test_timestamp_value_colons_encoded() {
        assert_eq!(
            escape_partition_value("2024-01-15 12:30:45"),
            "2024-01-15 12%3A30%3A45"
        );
    }

    // === build_partition_path tests ===

    #[test]
    fn test_build_path_single_column() {
        let path = build_partition_path(&[("country", Some("US"))]);
        assert_eq!(path, "country=US/");
    }

    #[test]
    fn test_build_path_multiple_columns() {
        let path = build_partition_path(&[("country", Some("US")), ("year", Some("2025"))]);
        assert_eq!(path, "country=US/year=2025/");
    }

    #[test]
    fn test_build_path_null_value_uses_hive_default() {
        let path = build_partition_path(&[("country", None)]);
        assert_eq!(path, "country=__HIVE_DEFAULT_PARTITION__/");
    }

    #[test]
    fn test_build_path_special_chars_encoded() {
        let path = build_partition_path(&[("country", Some("US/A%B"))]);
        assert_eq!(path, "country=US%2FA%25B/");
    }

    #[test]
    fn test_build_path_empty_columns_returns_empty() {
        let path = build_partition_path(&[]);
        assert_eq!(path, "");
    }
}
