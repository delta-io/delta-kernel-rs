//! Resolution of AMT paths (relative vs absolute) against the table root.

use url::Url;

use crate::{DeltaResult, Error};

/// Resolve an AMT `path` (as stored in the log or a manifest) into an absolute [`Url`].
///
/// A `path` with a URI scheme is absolute and used as-is; otherwise it is relative and resolved
/// against `table_root` by concatenation with a single `/` separator, matching Iceberg V4's
/// [relative paths specification].
///
/// # Errors
///
/// Returns an error if the resolved location fails to parse as a [`Url`].
///
/// [relative paths specification]: https://iceberg.apache.org/spec/#paths-in-metadata
pub(crate) fn resolve_amt_location(path: &str, table_root: &Url) -> DeltaResult<Url> {
    if has_scheme(path) {
        // A URI scheme means the path is absolute and used as-is.
        Url::parse(path).map_err(|e| {
            Error::generic(format!(
                "Failed to parse absolute AMT location {path:?}: {e}"
            ))
        })
    } else {
        // Otherwise the path is relative and concatenated onto `table_root` with a single `/`.
        let mut base = table_root.as_str().to_string();
        if !base.ends_with('/') {
            base.push('/');
        }
        Url::parse(&format!("{base}{path}")).map_err(|e| {
            Error::generic(format!(
                "Failed to resolve relative AMT location {path:?} against table root {base}: {e}"
            ))
        })
    }
}

/// Returns whether `location` begins with a URI scheme, per [RFC 3986 section 3.1]:
/// `scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )`, terminated by `:`.
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
