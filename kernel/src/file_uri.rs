//! Resolution of the `uri` of a `file` value against the table root.

use url::Url;

use crate::path_encoding::scheme_len;
use crate::utils::require;
use crate::{KernelError, KernelResult};

/// Resolves the `uri` of a `file` value into the location of the bytes it references.
///
/// Kernel returns `file` values with their `uri` as stored. A connector that reads the referenced
/// bytes calls this first to find them. Resolution follows the `file` type protocol, which uses
/// Iceberg V4's [relative paths specification]:
///
/// - A `uri` with a URI scheme ([RFC 3986 section 3.1]) is absolute and is used as-is. Parsing it
///   into a [`Url`] normalizes any `.` or `..` segments.
/// - A `uri` without a scheme is relative. It is joined onto `table_root` with a single `/`,
///   without considering separators in the `uri`. Against `s3://bucket/table/`, `_files/a.png`
///   resolves to `s3://bucket/table/_files/a.png` and `/a.png` to `s3://bucket/table//a.png`.
///
/// The `uri` is an RFC 3986 URI-reference and is not percent-encoded again: `_files/a%20b.png`
/// names the object `_files/a b.png`.
///
/// The `uri` is only resolved, not checked: the bytes may not exist, and whether they are
/// accessible is up to the connector.
///
/// # Errors
///
/// Returns an error if a relative `uri` contains a `.` or `..` path segment, including the
/// percent-encoded forms `%2e` and `%2e%2e` and segments separated by `\`. The specification
/// does not support such navigation in relative paths, and URL parsing would otherwise resolve
/// it, possibly to a location outside the table root. Also returns an error if the resolved
/// location fails to parse as a [`Url`].
///
/// [relative paths specification]: https://iceberg.apache.org/spec/#paths-in-metadata
/// [RFC 3986 section 3.1]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
pub fn resolve_file_uri(table_root: &Url, uri: &str) -> KernelResult<Url> {
    if scheme_len(uri).is_some() {
        return Url::parse(uri).map_err(|e| {
            KernelError::generic(format!("Failed to parse absolute file uri {uri:?}: {e}"))
        });
    }
    require!(
        !has_dot_segment(uri),
        KernelError::generic(format!(
            "Relative file uri {uri:?} contains a '.' or '..' segment, which is not supported"
        ))
    );
    let mut base = table_root.as_str().to_string();
    if !base.ends_with('/') {
        base.push('/');
    }
    Url::parse(&format!("{base}{uri}")).map_err(|e| {
        KernelError::generic(format!(
            "Failed to resolve relative file uri {uri:?} against table root {base}: {e}"
        ))
    })
}

/// Returns whether the path of the relative reference `uri` (before any `?` or `#`) has a `.` or
/// `..` segment. Segments are split on `/` and also on `\`, which URL parsing treats as `/` for
/// some schemes, and `%2e` counts as `.`, as URL parsing does.
fn has_dot_segment(uri: &str) -> bool {
    let path = uri.split(['?', '#']).next().unwrap_or_default();
    path.split(['/', '\\']).any(|segment| {
        let segment = segment.replace("%2e", ".").replace("%2E", ".");
        segment == "." || segment == ".."
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::under_files("s3://bucket/table/", "_files/a.png", "s3://bucket/table/_files/a.png")]
    #[case::root_without_trailing_slash(
        "s3://bucket/table",
        "_files/a.png",
        "s3://bucket/table/_files/a.png"
    )]
    #[case::outside_files("s3://bucket/table/", "other/a.png", "s3://bucket/table/other/a.png")]
    #[case::local_root("file:///tmp/table/", "_files/a.png", "file:///tmp/table/_files/a.png")]
    // A relative `uri` is joined without considering its separators.
    #[case::leading_slash("s3://bucket/table/", "/a.png", "s3://bucket/table//a.png")]
    #[case::leading_double_slash(
        "s3://bucket/table/",
        "//host/a.png",
        "s3://bucket/table///host/a.png"
    )]
    // The `uri` is already percent-encoded and is not encoded again.
    #[case::percent_encoded(
        "s3://bucket/table/",
        "_files/a%20b.png",
        "s3://bucket/table/_files/a%20b.png"
    )]
    #[case::dots_inside_names(
        "s3://bucket/table/",
        "_files/a..b/.c.png",
        "s3://bucket/table/_files/a..b/.c.png"
    )]
    // A colon after the first `/` is not a scheme delimiter.
    #[case::colon_in_later_segment(
        "s3://bucket/table/",
        "_files/a:b.png",
        "s3://bucket/table/_files/a:b.png"
    )]
    // RFC 3986 requires a scheme to start with a letter, so this is a relative path.
    #[case::leading_digit_is_not_a_scheme(
        "s3://bucket/table/",
        "2024:01/a.png",
        "s3://bucket/table/2024:01/a.png"
    )]
    #[case::query(
        "s3://bucket/table/",
        "_files/a.png?v=1",
        "s3://bucket/table/_files/a.png?v=1"
    )]
    #[case::empty("s3://bucket/table/", "", "s3://bucket/table/")]
    #[case::absolute("s3://bucket/table/", "s3://other/x/a.png", "s3://other/x/a.png")]
    #[case::absolute_other_scheme(
        "s3://bucket/table/",
        "abfss://c@acct.dfs.core.windows.net/a.png",
        "abfss://c@acct.dfs.core.windows.net/a.png"
    )]
    #[case::absolute_under_table_root(
        "s3://bucket/table/",
        "s3://bucket/table/_files/a.png",
        "s3://bucket/table/_files/a.png"
    )]
    #[case::absolute_https(
        "s3://bucket/table/",
        "https://example.com/a.png",
        "https://example.com/a.png"
    )]
    // Dot segments are a relative-path rule only; parsing normalizes them in an absolute `uri`.
    #[case::absolute_with_dot_segment(
        "s3://bucket/table/",
        "s3://bucket/x/../a.png",
        "s3://bucket/a.png"
    )]
    fn test_resolve_file_uri(#[case] root: &str, #[case] uri: &str, #[case] expected: &str) {
        let root = Url::parse(root).unwrap();
        assert_eq!(resolve_file_uri(&root, uri).unwrap().as_str(), expected);
    }

    #[rstest]
    #[case::dot(".")]
    #[case::dot_dot("..")]
    #[case::leading_dot("./a.png")]
    #[case::parent("../other/a.png")]
    #[case::inner_parent("_files/../../a.png")]
    #[case::inner_dot("_files/./a.png")]
    #[case::trailing_parent("_files/..")]
    #[case::encoded_parent("_files/%2e%2e/a.png")]
    #[case::encoded_parent_upper("_files/%2E%2E/a.png")]
    #[case::mixed_encoded_parent("_files/.%2e/a.png")]
    #[case::encoded_dot("_files/%2e/a.png")]
    #[case::backslash_parent("_files\\..\\a.png")]
    #[case::leading_slash_parent("/../a.png")]
    #[case::parent_before_query("../a.png?x=1")]
    fn test_resolve_file_uri_rejects_dot_segments(#[case] uri: &str) {
        let root = Url::parse("s3://bucket/table/").unwrap();
        let err = resolve_file_uri(&root, uri).unwrap_err().to_string();
        assert!(err.contains("contains a '.' or '..' segment"), "{err}");
    }

    /// A `.` or `..` after `?` or `#` is not a path segment.
    #[rstest]
    #[case::query("_files/a.png?p=../x", "s3://bucket/table/_files/a.png?p=../x")]
    #[case::fragment("_files/a.png#..", "s3://bucket/table/_files/a.png#..")]
    fn test_resolve_file_uri_ignores_dots_after_path(#[case] uri: &str, #[case] expected: &str) {
        let root = Url::parse("s3://bucket/table/").unwrap();
        assert_eq!(resolve_file_uri(&root, uri).unwrap().as_str(), expected);
    }

    #[test]
    fn test_resolve_file_uri_rejects_unparseable_absolute_uri() {
        let root = Url::parse("s3://bucket/table/").unwrap();
        let err = resolve_file_uri(&root, "http://[bad/a.png")
            .unwrap_err()
            .to_string();
        assert!(err.contains("Failed to parse absolute file uri"), "{err}");
    }
}
