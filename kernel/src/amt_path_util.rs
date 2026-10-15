//! Resolution of AMT paths (relative vs absolute) against a table root, per the Iceberg V4
//! [relative paths specification].
//!
//! [relative paths specification]: https://iceberg.apache.org/spec/#paths-in-metadata

use url::{ParseError, Url};

use crate::path_encoding::uri_encode_path;
use crate::utils::require;
use crate::{KernelError, KernelResult};

/// Resolve an AMT `path` (as stored in the log or a manifest) into an absolute [`Url`].
///
/// If `path` has a URI scheme it is absolute; otherwise it is relative and joined onto
/// `table_root` with a single `/` separator, matching Iceberg V4's [relative paths specification].
/// In both cases `path` is a raw (unencoded) string, so its path component is percent-encoded
/// (preserving `/` separators) before parsing.
///
/// # Errors
///
/// Returns an error if `path` contains a `.` or `..` segment, or if the resolved location fails
/// to parse as a [`Url`].
///
/// [relative paths specification]: https://iceberg.apache.org/spec/#paths-in-metadata
pub(crate) fn resolve_amt_location(path: &str, table_root: &Url) -> KernelResult<Url> {
    match scheme_len(path) {
        Some(scheme_len) => {
            reject_dot_segments(path)?;
            let (prefix, raw_path) = split_authority(path, scheme_len);
            uri_with_encoded_path(prefix, raw_path).map_err(|e| {
                KernelError::generic(format!(
                    "Failed to parse absolute AMT location {path:?}: {e}"
                ))
            })
        }
        None => encode_and_join(path, table_root),
    }
}

/// Resolve a strictly table-relative `path` into an absolute [`Url`] under `table_root`.
///
/// Unlike [`resolve_amt_location`], an absolute `path` is rejected rather than used as-is: the
/// `path` must be non-empty, must not begin with `/`, and must not carry a URI scheme. This
/// enforces the invariant for callers (such as unencoded-relative deletion vectors) whose paths
/// are required to be table-relative, including those built without going through a validating
/// constructor.
///
/// # Errors
///
/// Returns an error if `path` is empty, begins with `/`, carries a URI scheme, or if the resolved
/// location fails to parse as a [`Url`].
pub(crate) fn resolve_table_relative(path: &str, table_root: &Url) -> KernelResult<Url> {
    validate_table_relative(path)?;
    encode_and_join(path, table_root)
}

/// Validates that `path` is strictly table-relative: non-empty, no leading `/`, and no URI scheme.
///
/// Shared by [`resolve_table_relative`] and by callers that validate a path at construction time,
/// before a table root is available to resolve against.
///
/// # Errors
///
/// Returns an error if `path` is empty, begins with `/`, or carries a URI scheme.
pub(crate) fn validate_table_relative(path: &str) -> KernelResult<()> {
    require!(
        !path.is_empty(),
        KernelError::generic("table-relative path must not be empty")
    );
    require!(
        !path.starts_with('/'),
        KernelError::generic(format!(
            "table-relative path must not begin with a leading '/': {path}"
        ))
    );
    require!(
        scheme_len(path).is_none(),
        KernelError::generic(format!(
            "table-relative path must not be an absolute URL: {path}"
        ))
    );
    Ok(())
}

/// Returns the byte length of `location`'s URI scheme (excluding the terminating `:`), or `None`
/// if `location` does not begin with one, per [RFC 3986 section 3.1]:
/// `scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )`, terminated by `:`.
///
/// A path without a scheme is relative (per the Iceberg V4 path spec).
///
/// [RFC 3986 section 3.1]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
fn scheme_len(location: &str) -> Option<usize> {
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

/// Resolves a relative `path` against `table_root`: percent-encodes `path` so reserved URI
/// characters survive a round trip through the object store, then concatenates it onto
/// `table_root` with a single `/` separator and parses the result.
///
/// # Errors
///
/// Returns an error if the resolved location fails to parse as a [`Url`].
fn encode_and_join(path: &str, table_root: &Url) -> KernelResult<Url> {
    reject_dot_segments(path)?;
    let mut base = table_root.as_str().to_string();
    if !base.ends_with('/') {
        base.push('/');
    }
    uri_with_encoded_path(&base, path).map_err(|e| {
        KernelError::generic(format!(
            "Failed to resolve relative AMT location {path:?} against table root {base}: {e}"
        ))
    })
}

/// Splits an absolute `location` whose scheme is `scheme_len` bytes long into its
/// `scheme:[//authority]` prefix and the raw path that follows. The authority, when present, ends
/// at the next `/`.
fn split_authority(location: &str, scheme_len: usize) -> (&str, &str) {
    let after_scheme = scheme_len + 1;
    let prefix_len = match location[after_scheme..].strip_prefix("//") {
        Some(rest) => after_scheme + 2 + rest.find('/').unwrap_or(rest.len()),
        None => after_scheme,
    };
    location.split_at(prefix_len)
}

/// Parses `prefix` followed by the percent-encoded `raw_path`, so reserved characters in the
/// raw path are not misparsed as URL syntax.
fn uri_with_encoded_path(prefix: &str, raw_path: &str) -> Result<Url, ParseError> {
    Url::parse(&format!("{prefix}{}", uri_encode_path(raw_path)))
}

/// Rejects a `path` containing a `.` or `..` segment: the Iceberg V4 path spec does not support
/// relative navigation, and URL parsing would otherwise silently collapse such segments.
fn reject_dot_segments(path: &str) -> KernelResult<()> {
    require!(
        !path
            .split('/')
            .any(|segment| segment == "." || segment == ".."),
        KernelError::generic(format!(
            "AMT path {path:?} contains a '.' or '..' segment, which is not supported"
        ))
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use test_utils::assert_result_error_with_message;

    use super::*;

    #[rstest]
    #[case::relative_path(
        "memory:///table/",
        "metadata/root.parquet",
        "memory:///table/metadata/root.parquet"
    )]
    #[case::absolute_path(
        "memory:///table/",
        "s3://bucket/table/metadata/root.parquet",
        "s3://bucket/table/metadata/root.parquet"
    )]
    #[case::table_root_without_trailing_slash_gets_one(
        "memory:///table",
        "metadata/root.parquet",
        "memory:///table/metadata/root.parquet"
    )]
    #[case::single_char_scheme_treated_as_absolute(
        "memory:///table/",
        "c:/foo/root.parquet",
        "c:/foo/root.parquet"
    )]
    // A colon inside a relative path segment is not a scheme delimiter (a `/` precedes it), so
    // the path stays relative.
    #[case::colon_in_relative_segment_stays_relative(
        "memory:///table/",
        "metadata/snap-123:456.parquet",
        "memory:///table/metadata/snap-123:456.parquet"
    )]
    // RFC 3986 requires the first scheme char to be ALPHA; a leading digit is not a scheme.
    #[case::leading_digit_scheme_treated_as_relative(
        "memory:///table/",
        "3com/root.parquet",
        "memory:///table/3com/root.parquet"
    )]
    // A non-ASCII leading letter (Greek alpha, U+03B1) is not a valid scheme char.
    #[case::non_ascii_scheme_treated_as_relative(
        "memory:///table/",
        "\u{03b1}scheme/root.parquet",
        "memory:///table/%CE%B1scheme/root.parquet"
    )]
    // A multi-char, non-alphanumeric scheme (`git+ssh`) is absolute and used as-is.
    #[case::compound_scheme_treated_as_absolute(
        "memory:///table/",
        "git+ssh://host/repo/root.parquet",
        "git+ssh://host/repo/root.parquet"
    )]
    // A raw space is percent-encoded, not left to break URL parsing.
    #[case::space_is_encoded(
        "memory:///table/",
        "metadata/leaf a.parquet",
        "memory:///table/metadata/leaf%20a.parquet"
    )]
    // A literal `%` is encoded to `%25` so it is not misread as a percent-escape.
    #[case::percent_is_encoded(
        "memory:///table/",
        "data/test%dv.bin",
        "memory:///table/data/test%25dv.bin"
    )]
    // A `#` is encoded, not interpreted as a URL fragment delimiter.
    #[case::hash_is_encoded("memory:///table/", "data/a#b.bin", "memory:///table/data/a%23b.bin")]
    // A `?` is encoded, not interpreted as a URL query delimiter.
    #[case::question_is_encoded(
        "memory:///table/",
        "data/a?b.bin",
        "memory:///table/data/a%3Fb.bin"
    )]
    // Non-ASCII bytes are percent-encoded per their UTF-8 encoding.
    #[case::non_ascii_is_encoded(
        "memory:///table/",
        "data/M\u{fc}nchen.bin",
        "memory:///table/data/M%C3%BCnchen.bin"
    )]
    // An already-encoded sequence is a literal `%` in a raw path, so it is encoded again.
    #[case::relative_percent_escape_is_literal(
        "memory:///table/",
        "data/a%20b.bin",
        "memory:///table/data/a%2520b.bin"
    )]
    // Dots inside a segment name are not relative navigation.
    #[case::dots_within_segment_name(
        "memory:///table/",
        "a/..b/.c.bin",
        "memory:///table/a/..b/.c.bin"
    )]
    // The path component of an absolute location is also raw and gets the same encoding.
    #[case::absolute_hash_is_encoded(
        "memory:///table/",
        "s3://bucket/data/a#b.parquet",
        "s3://bucket/data/a%23b.parquet"
    )]
    #[case::absolute_question_and_space_are_encoded(
        "memory:///table/",
        "s3://bucket/data/a b?c.parquet",
        "s3://bucket/data/a%20b%3Fc.parquet"
    )]
    #[case::absolute_percent_escape_is_literal(
        "memory:///table/",
        "s3://bucket/data/a%20b.parquet",
        "s3://bucket/data/a%2520b.parquet"
    )]
    fn test_resolve_amt_location(
        #[case] table_root: &str,
        #[case] path: &str,
        #[case] expected_location: &str,
    ) {
        let table_root = Url::parse(table_root).unwrap();
        let location = resolve_amt_location(path, &table_root).unwrap();
        assert_eq!(location.as_str(), expected_location);
    }

    #[rstest]
    #[case::relative_dot_dot("a/../b.bin")]
    #[case::relative_leading_dot("./a.bin")]
    #[case::relative_trailing_dot("a/.")]
    #[case::bare_dot_dot("..")]
    #[case::absolute_dot_dot("s3://bucket/x/../y.parquet")]
    fn test_resolve_amt_location_rejects_dot_segments(#[case] path: &str) {
        let table_root = Url::parse("memory:///table/").unwrap();
        assert_result_error_with_message(
            resolve_amt_location(path, &table_root),
            "contains a '.' or '..' segment",
        );
    }
}
