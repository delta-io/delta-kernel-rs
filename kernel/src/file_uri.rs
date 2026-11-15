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
/// - A `uri` without a scheme is relative and is joined onto `table_root` with a single `/`. For
///   example, against `s3://bucket/table/`, `_files/a.png` resolves to
///   `s3://bucket/table/_files/a.png`.
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
/// Returns [`KernelError::UnsupportedFileUri`] if:
/// - the `uri` contains a space or an ASCII control character, which URL parsing drops or trims, or
///   a `\`, which URL parsing can treat as `/`;
/// - its path contains `%2F` or `%5C`, which an object store can treat as a path separator. The
///   path is the part after the scheme and any `//` authority, up to the first `?` or `#`, so these
///   are allowed in the authority, query and fragment;
/// - its path has a `.` or `..` segment, including `%2e` forms, which URL parsing or an object
///   store would navigate;
/// - an absolute `uri` fails to parse as a [`Url`];
/// - a relative `uri` has an empty path or starts with `/`, which the relative-path rule does not
///   allow, or `table_root` has a query or fragment, which would absorb the joined path.
///
/// [relative paths specification]: https://iceberg.apache.org/spec/#paths-in-metadata
/// [RFC 3986 section 3.1]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
pub fn resolve_file_uri(table_root: &Url, uri: &str) -> KernelResult<Url> {
    match check_uri(uri)? {
        CheckedUri::Absolute(url) => Ok(url),
        CheckedUri::Relative => {
            // The `uri` is appended to the root's text, so a query or fragment there would absorb
            // it.
            require!(
                table_root.query().is_none() && table_root.fragment().is_none(),
                unsupported(
                    uri,
                    &format!("the table root {table_root} has a query or fragment")
                )
            );
            let base = with_trailing_slash(table_root);
            Url::parse(&format!("{base}{uri}")).map_err(|e| unsupported(uri, &e.to_string()))
        }
    }
}

/// Returns a [`KernelError::UnsupportedFileUri`] naming `uri` and `reason`.
fn unsupported(uri: &str, reason: &str) -> KernelError {
    KernelError::unsupported_file_uri(format!("{uri:?}: {reason}"))
}

/// A `uri` that [`check_uri`] accepts.
enum CheckedUri {
    Absolute(Url),
    Relative,
}

/// Checks `uri` against the forms [`resolve_file_uri`] supports, and parses it if it is absolute.
/// See [`resolve_file_uri`] for the rules.
///
/// AMT paths are checked differently (see `amt_path_util::reject_dot_segments`): they are raw
/// paths that kernel percent-encodes before parsing, so only a literal `.` or `..` segment
/// navigates there.
fn check_uri(uri: &str) -> KernelResult<CheckedUri> {
    let fail = |reason: &str| Err(unsupported(uri, reason));
    if uri.chars().any(|c| c == ' ' || c.is_ascii_control()) {
        return fail("contains a space or an ASCII control character");
    }
    if uri.contains('\\') {
        return fail("contains '\\', which can separate path segments");
    }
    let scheme_len = scheme_len(uri);
    let path = uri_path(uri, scheme_len).to_ascii_lowercase();
    if path.contains("%2f") || path.contains("%5c") {
        return fail("its path contains '%2F' or '%5C', which can separate path segments");
    }
    if path
        .split('/')
        .any(|segment| matches!(segment.replace("%2e", ".").as_str(), "." | ".."))
    {
        return fail("its path has a '.' or '..' segment");
    }
    if scheme_len.is_some() {
        return Url::parse(uri)
            .map(CheckedUri::Absolute)
            .map_err(|e| unsupported(uri, &format!("an absolute uri must parse as a URL: {e}")));
    }
    if path.is_empty() {
        return fail("a relative uri must have a non-empty path");
    }
    if path.starts_with('/') {
        return fail("a relative uri must not start with '/'");
    }
    Ok(CheckedUri::Relative)
}

/// Returns the path of `uri`, whose scheme is `scheme_len` bytes long if it has one: the part after
/// the scheme and any `//` authority, up to the first `?` or `#`. Per [RFC 3986 section 3.2], an
/// authority ends at the first `/`, `?` or `#`.
///
/// [RFC 3986 section 3.2]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.2
fn uri_path(uri: &str, scheme_len: Option<usize>) -> &str {
    let rest = match scheme_len {
        Some(len) => {
            let after_scheme = &uri[len + 1..];
            match after_scheme.strip_prefix("//") {
                Some(authority_and_path) => {
                    let end = authority_and_path
                        .find(['/', '?', '#'])
                        .unwrap_or(authority_and_path.len());
                    &authority_and_path[end..]
                }
                None => after_scheme,
            }
        }
        None => uri,
    };
    rest.split(['?', '#']).next().unwrap_or_default()
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
    const BACKSLASH: &str = "contains '\\'";
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
    #[case::backslash("_files\\a.png", BACKSLASH)]
    #[case::leading_backslash("\\a.png", BACKSLASH)]
    // URL parsing reads `\` as `/` for some schemes, so it would split the authority.
    #[case::backslash_in_authority("https://host\\a/b.png", BACKSLASH)]
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
        let err = resolve_file_uri(&root(), uri).unwrap_err();
        assert!(matches!(err, KernelError::UnsupportedFileUri(_)), "{err:?}");
        let err = err.to_string();
        assert!(err.contains("Unsupported file uri"), "{err}");
        assert!(err.contains(reason), "{err}");
    }

    /// Only the path is checked for encoded separators: the scheme and authority are not, so an
    /// encoded `/` or tab in a username is allowed, while a literal tab anywhere is rejected.
    #[test]
    fn test_resolve_file_uri_with_encoded_slash_in_username() {
        let uri = "https://tenant%2Fuser:pass@example.com/a.png";
        let parsed = Url::parse(uri).unwrap();
        assert_eq!(parsed.username(), "tenant%2Fuser");
        assert_eq!(parsed.path(), "/a.png");
        assert_eq!(resolve_file_uri(&root(), uri).unwrap(), parsed);
    }

    #[test]
    fn test_resolve_file_uri_with_encoded_tab_in_username() {
        let uri = "https://tenant%09user:pass@example.com/a.png";
        let parsed = Url::parse(uri).unwrap();
        assert_eq!(parsed.username(), "tenant%09user");
        assert_eq!(resolve_file_uri(&root(), uri).unwrap(), parsed);
    }

    #[test]
    fn test_resolve_file_uri_rejects_literal_tab_in_username() {
        let uri = "https://tenant\tuser:pass@example.com/a.png";
        assert_eq!(Url::parse(uri).unwrap().username(), "tenantuser");
        let err = resolve_file_uri(&root(), uri).unwrap_err().to_string();
        assert!(err.contains("ASCII control character"), "{err}");
    }

    #[rstest]
    #[case::relative("_files/a.png?x=%2F", None, "_files/a.png")]
    #[case::fragment("_files/a.png#%2F", None, "_files/a.png")]
    #[case::authority("s3://user%2F@bucket/x/a.png?q", Some(2), "/x/a.png")]
    #[case::no_path("s3://bucket", Some(2), "")]
    #[case::no_authority("mailto:a@b.c", Some(6), "a@b.c")]
    fn test_uri_path(#[case] uri: &str, #[case] scheme_len: Option<usize>, #[case] expected: &str) {
        assert_eq!(uri_path(uri, scheme_len), expected);
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
