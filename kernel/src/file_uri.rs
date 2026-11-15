//! Resolution of the `uri` of a `file` value against the table root.

use url::Url;

use crate::path_encoding::{scheme_len, with_trailing_slash};
use crate::utils::require;
use crate::{KernelError, KernelResult};

/// Resolves the `uri` of a `file` value into the location of the bytes it references.
///
/// Kernel returns `file` values with their `uri` as stored. A connector that reads the referenced
/// bytes calls this first to find them. Resolution follows the `file` type protocol, which uses
/// the relative-path rule of Delta's Iceberg V4 metadata, itself following Iceberg V4's
/// [relative paths specification]:
///
/// - A `uri` with a URI scheme ([RFC 3986 section 3.1]) is absolute and is parsed as a [`Url`].
/// - A `uri` without a scheme is relative and is joined onto `table_root` with a single `/`.
///   Against `s3://bucket/table/`, `_files/a.png` resolves to `s3://bucket/table/_files/a.png`.
///
/// The `uri` is an RFC 3986 URI-reference and is not percent-encoded again: `_files/a%20b.png`
/// names the object `_files/a b.png`.
///
/// Kernel supports ordinary relative paths and absolute URLs. It rejects the forms that URL
/// parsing or an object store could interpret as a different path than the one written, so that
/// such a `uri` fails here instead of silently resolving to another location. Other unusual URI
/// forms, and storage-specific aliases of a location, are left to the connector.
///
/// Only the location is computed: kernel does not check that bytes exist there, and whether they
/// are accessible is up to the connector.
///
/// # Errors
///
/// Returns an error if:
/// - the `uri` contains a space or an ASCII control character, which URL parsing drops or trims;
/// - its path contains `\`, `%2F` or `%5C`, which URL parsing or an object store can treat as a
///   path separator;
/// - its path has a `.` or `..` segment, including `%2e` forms, which URL parsing or an object
///   store would navigate;
/// - an absolute `uri` fails to parse as a [`Url`];
/// - a relative `uri` has an empty path or starts with `/`, which the relative-path rule does not
///   allow, or `table_root` has a query or fragment, which would absorb the joined path.
///
/// [relative paths specification]: https://iceberg.apache.org/spec/#paths-in-metadata
/// [RFC 3986 section 3.1]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
pub fn resolve_file_uri(table_root: &Url, uri: &str) -> KernelResult<Url> {
    let unsupported =
        |reason: &str| KernelError::generic(format!("Unsupported file uri {uri:?}: {reason}"));
    match check_uri(uri).map_err(unsupported)? {
        CheckedUri::Absolute(url) => Ok(url),
        CheckedUri::Relative => {
            // The `uri` is appended to the root's text, so a query or fragment there would absorb
            // it.
            require!(
                table_root.query().is_none() && table_root.fragment().is_none(),
                unsupported(&format!(
                    "the table root {table_root} has a query or fragment"
                ))
            );
            let base = with_trailing_slash(table_root);
            Url::parse(&format!("{base}{uri}")).map_err(|e| unsupported(&e.to_string()))
        }
    }
}

/// A `uri` that [`check_uri`] accepts.
enum CheckedUri {
    Absolute(Url),
    Relative,
}

/// Checks `uri` against the forms [`resolve_file_uri`] supports, and parses it if it is absolute.
/// See [`resolve_file_uri`] for the rules. Its path is the part before any `?` or `#`.
///
/// AMT paths are checked differently (see `amt_path_util::reject_dot_segments`): they are raw
/// paths that kernel percent-encodes before parsing, so only a literal `.` or `..` segment
/// navigates there.
fn check_uri(uri: &str) -> Result<CheckedUri, &'static str> {
    if uri.chars().any(|c| c == ' ' || c.is_ascii_control()) {
        return Err("contains a space or an ASCII control character");
    }
    let path = uri.split(['?', '#']).next().unwrap_or_default();
    let lowercase = path.to_ascii_lowercase();
    if path.contains('\\') || lowercase.contains("%2f") || lowercase.contains("%5c") {
        return Err("its path contains '\\', '%2F' or '%5C', which can separate path segments");
    }
    if lowercase
        .split('/')
        .any(|segment| matches!(segment.replace("%2e", ".").as_str(), "." | ".."))
    {
        return Err("its path has a '.' or '..' segment");
    }
    if scheme_len(uri).is_some() {
        return Url::parse(uri)
            .map(CheckedUri::Absolute)
            .map_err(|_| "an absolute uri must parse as a URL");
    }
    if path.is_empty() {
        return Err("a relative uri must have a non-empty path");
    }
    if path.starts_with('/') {
        return Err("a relative uri must not start with '/'");
    }
    Ok(CheckedUri::Relative)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn root() -> Url {
        Url::parse("s3://bucket/table/").unwrap()
    }

    #[rstest]
    #[case::under_files("s3://bucket/table/", "_files/a.png", "s3://bucket/table/_files/a.png")]
    #[case::root_without_trailing_slash(
        "s3://bucket/table",
        "_files/a.png",
        "s3://bucket/table/_files/a.png"
    )]
    #[case::outside_files("s3://bucket/table/", "other/a.png", "s3://bucket/table/other/a.png")]
    #[case::local_root("file:///tmp/table/", "_files/a.png", "file:///tmp/table/_files/a.png")]
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
    // Only the path is checked, so dots and encoded separators after `?` or `#` are allowed.
    #[case::dots_in_query(
        "s3://bucket/table/",
        "_files/a.png?p=../x%2F",
        "s3://bucket/table/_files/a.png?p=../x%2F"
    )]
    #[case::dots_in_fragment(
        "s3://bucket/table/",
        "_files/a.png#..",
        "s3://bucket/table/_files/a.png#.."
    )]
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
    // An absolute `uri` doesn't use the table root, so a root with a query is fine.
    #[case::absolute_with_query_root(
        "s3://bucket/table/?token=x",
        "s3://other/a.png",
        "s3://other/a.png"
    )]
    fn test_resolve_file_uri(#[case] root: &str, #[case] uri: &str, #[case] expected: &str) {
        let root = Url::parse(root).unwrap();
        assert_eq!(resolve_file_uri(&root, uri).unwrap().as_str(), expected);
    }

    const CONTROL: &str = "space or an ASCII control character";
    const SEPARATOR: &str = "'%2F' or '%5C'";
    const DOT: &str = "'.' or '..' segment";

    #[rstest]
    // URL parsing drops tabs and newlines and trims spaces.
    #[case::tab_in_parent("_files/.\t./a.png", CONTROL)]
    #[case::newline_in_parent(".\n./a.png", CONTROL)]
    #[case::carriage_return("_files/.\r./a.png", CONTROL)]
    #[case::trailing_space("..  ", CONTROL)]
    #[case::inner_space("_files/a b.png", CONTROL)]
    #[case::nul("_files/a\0.png", CONTROL)]
    #[case::absolute_with_tab("s3://bucket/x/.\t./a.png", CONTROL)]
    // URL parsing or an object store can treat these as separators.
    #[case::backslash("_files\\a.png", SEPARATOR)]
    #[case::leading_backslash("\\a.png", SEPARATOR)]
    #[case::encoded_slash("_files/a%2Fb.png", SEPARATOR)]
    #[case::encoded_slash_lower("_files/a%2fb.png", SEPARATOR)]
    #[case::encoded_backslash("_files/a%5Cb.png", SEPARATOR)]
    #[case::encoded_slash_parent("_files/%2e%2e%2fsecret", SEPARATOR)]
    #[case::absolute_encoded_slash("s3://bucket/a%2Fb.png", SEPARATOR)]
    // Dot navigation, literal or `%2e`-encoded.
    #[case::dot(".", DOT)]
    #[case::dot_dot("..", DOT)]
    #[case::leading_dot("./a.png", DOT)]
    #[case::parent("../other/a.png", DOT)]
    #[case::inner_parent("_files/../../a.png", DOT)]
    #[case::inner_dot("_files/./a.png", DOT)]
    #[case::trailing_parent("_files/..", DOT)]
    #[case::encoded_parent("_files/%2e%2e/a.png", DOT)]
    #[case::encoded_parent_upper("_files/%2E%2E/a.png", DOT)]
    #[case::mixed_encoded_parent("_files/.%2e/a.png", DOT)]
    #[case::encoded_dot("_files/%2e/a.png", DOT)]
    #[case::parent_before_query("../a.png?x=1", DOT)]
    #[case::absolute_parent("s3://bucket/x/../a.png", DOT)]
    #[case::absolute_encoded_parent("s3://bucket/x/%2e%2e/a.png", DOT)]
    // The relative-path rule.
    #[case::leading_slash("/a.png", "must not start with '/'")]
    #[case::leading_double_slash("//host/a.png", "must not start with '/'")]
    #[case::empty("", "non-empty path")]
    #[case::only_query("?x", "non-empty path")]
    #[case::only_fragment("#x", "non-empty path")]
    #[case::unparseable_absolute("http://[bad/a.png", "must parse as a URL")]
    fn test_resolve_file_uri_rejects(#[case] uri: &str, #[case] reason: &str) {
        let err = resolve_file_uri(&root(), uri).unwrap_err().to_string();
        assert!(err.contains("Unsupported file uri"), "{err}");
        assert!(err.contains(reason), "{err}");
    }

    /// A relative `uri` cannot be joined onto a table root with a query or fragment, which would
    /// absorb it.
    #[rstest]
    #[case::query("s3://bucket/table/?token=x")]
    #[case::fragment("s3://bucket/table/#f")]
    fn test_resolve_file_uri_against_root_with_query_or_fragment(#[case] root: &str) {
        let root = Url::parse(root).unwrap();
        let err = resolve_file_uri(&root, "_files/a.png")
            .unwrap_err()
            .to_string();
        assert!(err.contains("query or fragment"), "{err}");
    }
}
