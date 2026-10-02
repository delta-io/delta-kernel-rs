//! Resolution of AMT paths (relative vs absolute) against a table root, per the Iceberg V4
//! [relative paths specification].
//!
//! [relative paths specification]: https://iceberg.apache.org/spec/#paths-in-metadata

use url::Url;

use crate::path_encoding::uri_encode_path;
use crate::utils::require;
use crate::{DeltaResult, KernelError};

/// Resolve an AMT `path` (as stored in the log or a manifest) into an absolute [`Url`].
///
/// A scheme-bearing `path` is absolute and parsed as-is. Otherwise `path` is treated as a raw,
/// relative string: it is percent-encoded (preserving `/` separators) and joined onto
/// `table_root` with a single `/` separator, matching Iceberg V4's [relative paths specification].
///
/// # Errors
///
/// Returns an error if the resolved location fails to parse as a [`Url`].
///
/// [relative paths specification]: https://iceberg.apache.org/spec/#paths-in-metadata
pub(crate) fn resolve_amt_location(path: &str, table_root: &Url) -> DeltaResult<Url> {
    if has_scheme(path) {
        // A URI scheme means the path is absolute and used as-is. Absolute AMT locations are
        // required to be well-formed URIs: a raw, decoded path carrying reserved characters
        // (`%`, `#`, `?`, space) is not re-encoded here, as encoding only its path component
        // would require authority-aware parsing we do not perform for absolute locations.
        Url::parse(path).map_err(|e| {
            KernelError::generic(format!(
                "Failed to parse absolute AMT location {path:?}: {e}"
            ))
        })
    } else {
        encode_and_join(path, table_root)
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
pub(crate) fn resolve_table_relative(path: &str, table_root: &Url) -> DeltaResult<Url> {
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
pub(crate) fn validate_table_relative(path: &str) -> DeltaResult<()> {
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
        !has_scheme(path),
        KernelError::generic(format!(
            "table-relative path must not be an absolute URL: {path}"
        ))
    );
    Ok(())
}

/// Returns whether `location` begins with a URI scheme, per [RFC 3986 section 3.1]:
/// `scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )`, terminated by `:`.
///
/// A path without a scheme is relative (per the Iceberg V4 path spec).
///
/// [RFC 3986 section 3.1]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
fn has_scheme(location: &str) -> bool {
    for (position, ch) in location.char_indices() {
        if ch == ':' {
            return position > 0;
        }
        if !is_scheme_char(ch, position) {
            return false;
        }
    }
    false
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
fn encode_and_join(path: &str, table_root: &Url) -> DeltaResult<Url> {
    let encoded = uri_encode_path(path);
    let mut base = table_root.as_str().to_string();
    if !base.ends_with('/') {
        base.push('/');
    }
    Url::parse(&format!("{base}{encoded}")).map_err(|e| {
        KernelError::generic(format!(
            "Failed to resolve relative AMT location {path:?} against table root {base}: {e}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

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
    // A `..` segment is preserved through encoding, then collapsed (not rejected) by URL path
    // normalization in `Url::parse`.
    #[case::dot_dot_is_normalized("memory:///table/", "a/../b.bin", "memory:///table/b.bin")]
    fn test_resolve_amt_location(
        #[case] table_root: &str,
        #[case] path: &str,
        #[case] expected_location: &str,
    ) {
        let table_root = Url::parse(table_root).unwrap();
        let location = resolve_amt_location(path, &table_root).unwrap();
        assert_eq!(location.as_str(), expected_location);
    }
}
