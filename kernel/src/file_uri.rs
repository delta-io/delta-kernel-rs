//! Resolution of the `uri` of a `file` value against the table root, and the location where a
//! writer writes the files of new `file` references.

use percent_encoding::percent_decode_str;
use url::{Position, Url};
use uuid::Uuid;

use crate::path_encoding::{scheme_len, with_trailing_slash};
use crate::table_properties::FILE_TYPE_WRITE_LOCATION;
use crate::utils::require;
use crate::{KernelError, KernelResult};

/// The directory of the table root where writers write the files of new `file` references when
/// `delta.fileType.writeLocation` is not set.
const FILES_DIR: &str = "_files";

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

/// A unique location for a new file that a `file` value references, created by
/// [`BoundWriteContext::new_referenced_file_path`].
///
/// A connector writes the bytes to [`location`](Self::location) and stores [`uri`](Self::uri) in
/// the `uri` field of the `file` value.
///
/// [`BoundWriteContext::new_referenced_file_path`]:
///     crate::transaction::BoundWriteContext::new_referenced_file_path
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferencedFilePath {
    uri: String,
    location: Url,
}

impl ReferencedFilePath {
    /// The value to store in the `uri` field of the `file` value: relative to the table root (for
    /// example `_files/<uuid>.png`) when the write location is under `_files`, and absolute when
    /// the write location is outside the table root.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// The location to write the bytes to: [`uri`](Self::uri) resolved with [`resolve_file_uri`].
    pub fn location(&self) -> &Url {
        &self.location
    }
}

/// Creates a [`ReferencedFilePath`] with a new UUID name, optionally followed by `.<extension>`,
/// at the write location set by `write_location` (the `delta.fileType.writeLocation` value), or
/// under `_files` when it is not set.
///
/// Returns an error if `write_location` is invalid or unsupported (see
/// [`write_location_uri_prefix`]), or if `extension` is empty or has a character other than an
/// ASCII letter, digit, `-`, `_` or `.`.
pub(crate) fn new_referenced_file_path(
    table_root: &Url,
    write_location: Option<&str>,
    extension: Option<&str>,
) -> KernelResult<ReferencedFilePath> {
    let prefix = write_location_uri_prefix(table_root, write_location)?;
    let suffix = match extension {
        None => String::new(),
        Some(extension) => {
            require!(
                !extension.is_empty()
                    && extension
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')),
                KernelError::unsupported_referenced_file_path(format!(
                    "extension {extension:?} is unsupported: it must be non-empty and contain only \
                     ASCII letters, digits, '-', '_' and '.'"
                ))
            );
            format!(".{extension}")
        }
    };
    let uri = format!("{prefix}{}{suffix}", Uuid::new_v4());
    let location = resolve_file_uri(table_root, &uri)?;
    Ok(ReferencedFilePath { uri, location })
}

/// Returns the `uri` prefix, ending in `/`, under which a writer writes the files of new `file`
/// references: `_files/` when `write_location` is `None`, a relative `write_location` with a
/// trailing `/`, or the text of an absolute `write_location` parsed as a [`Url`] with a trailing
/// `/`.
///
/// Per the `file` type protocol, a valid value is either a table-root-relative path under `_files`
/// (`_files` itself, or `_files/<prefix>`) or an absolute URI outside the table root. A writer must
/// not write files at any other value, so this returns an error for one, and for a value that
/// [`resolve_file_uri`] does not support. Whether an absolute URI is outside the table root is
/// decided lexically; see [`is_within`].
fn write_location_uri_prefix(
    table_root: &Url,
    write_location: Option<&str>,
) -> KernelResult<String> {
    let Some(write_location) = write_location else {
        return Ok(format!("{FILES_DIR}/"));
    };
    let invalid = |reason: &str| {
        KernelError::unsupported_referenced_file_path(format!(
            "{FILE_TYPE_WRITE_LOCATION} {write_location:?} is invalid or unsupported: {reason}"
        ))
    };
    require!(
        !write_location.contains(['?', '#']),
        invalid("a write location cannot have a query or fragment")
    );
    let dir = write_location.strip_suffix('/').unwrap_or(write_location);
    let empty_segment = || invalid("a write location cannot have an empty path segment");
    match check_uri(write_location).map_err(|e| invalid(&e.to_string()))? {
        CheckedUri::Absolute(_) => {
            let url = Url::parse(&format!("{dir}/")).map_err(|e| invalid(&e.to_string()))?;
            require!(
                !url.cannot_be_a_base(),
                invalid("an absolute write location must be a hierarchical URI")
            );
            // A URL with neither a host nor a path, such as `s3://`, has an empty path, so the
            // file name would follow `//` and be read as the host.
            let segments = url
                .path()
                .strip_prefix('/')
                .ok_or_else(|| invalid("an absolute write location must have a host or a path"))?;
            let segments = segments.strip_suffix('/').unwrap_or(segments);
            require!(
                segments.is_empty() || !segments.split('/').any(str::is_empty),
                empty_segment()
            );
            require!(
                !is_within(&url, table_root),
                invalid("an absolute write location must be outside the table root")
            );
            Ok(url.into())
        }
        CheckedUri::Relative => {
            require!(
                dir == FILES_DIR || dir.starts_with(&format!("{FILES_DIR}/")),
                invalid("a relative write location must be under `_files`")
            );
            require!(!dir.split('/').any(str::is_empty), empty_segment());
            Ok(format!("{dir}/"))
        }
    }
}

/// Checks that a writer may write `uri`: it is a `uri` that [`resolve_file_uri`] supports, for any
/// table root without a query or fragment. The `file` type protocol requires a writer to write
/// each `uri` as an absolute URI or as a relative reference resolved per its URI resolution rules.
#[cfg(feature = "default-engine-base")]
pub(crate) fn validate_file_uri(uri: &str) -> KernelResult<()> {
    check_uri(uri).map(|_| ())
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

/// Returns whether `url` is `table_root` or under it.
///
/// The scheme and authority (`scheme://[userinfo@]host[:port]`) are compared as text, ignoring
/// case, and the paths segment by segment after percent-decoding them, as an object store does.
/// So `%74able` for `table` counts as the table root, as does `a/b` for a root spelled `a%2Fb`, but
/// a sibling that only shares a name prefix (`table2`) does not. The check is otherwise lexical: it
/// doesn't unify scheme aliases such as `s3` and `s3a`, a default port written out, or userinfo,
/// compares paths case-sensitively, and doesn't follow symlinks. A location spelled in one of those
/// ways counts as outside.
fn is_within(url: &Url, table_root: &Url) -> bool {
    let authority = |url: &Url| url[..Position::BeforePath].to_ascii_lowercase();
    // Paths are compared as decoded bytes, so an escape that is not valid UTF-8, such as `%FF`,
    // only matches itself.
    let segments = |url: &Url| -> Vec<Vec<u8>> {
        let path: Vec<u8> = percent_decode_str(url.path()).collect();
        path.split(|&byte| byte == b'/')
            .filter(|segment| !segment.is_empty())
            .map(<[u8]>::to_vec)
            .collect()
    };
    authority(url) == authority(table_root) && segments(url).starts_with(&segments(table_root))
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

    /// Writes accept exactly the `uri`s that resolution accepts.
    #[cfg(feature = "default-engine-base")]
    #[rstest]
    #[case("_files/a.png")]
    #[case("s3://other/a.png")]
    #[case("_files/a.png?p=../x")]
    #[case("")]
    #[case("/a.png")]
    #[case("\\a.png")]
    #[case("_files/../a.png")]
    #[case("_files/a%2Fb.png")]
    #[case("_files/.\t./a.png")]
    #[case("s3://bucket/x/../a.png")]
    #[case("http://[bad/a.png")]
    fn test_validate_file_uri_matches_resolve_file_uri(#[case] uri: &str) {
        assert_eq!(
            validate_file_uri(uri).is_ok(),
            resolve_file_uri(&root(), uri).is_ok(),
            "{uri:?}"
        );
    }

    #[rstest]
    #[case::default(None, "_files/")]
    #[case::files_dir(Some("_files"), "_files/")]
    #[case::files_dir_with_slash(Some("_files/"), "_files/")]
    #[case::files_prefix(Some("_files/p1"), "_files/p1/")]
    #[case::nested_files_prefix(Some("_files/a/b/"), "_files/a/b/")]
    #[case::absolute(Some("s3://other/files"), "s3://other/files/")]
    #[case::absolute_with_slash(Some("s3://other/files/"), "s3://other/files/")]
    #[case::absolute_bucket_root(Some("s3://other"), "s3://other/")]
    #[case::absolute_same_bucket(Some("s3://bucket/elsewhere"), "s3://bucket/elsewhere/")]
    // Only the table root's own path is inside it, not a sibling that shares its name as a prefix.
    #[case::absolute_sibling_with_shared_prefix(Some("s3://bucket/table2"), "s3://bucket/table2/")]
    // The check is lexical, so a scheme alias of the table root's scheme, userinfo, or an explicit
    // port makes the same location count as outside the table root.
    #[case::absolute_scheme_alias(Some("s3a://bucket/table/p1"), "s3a://bucket/table/p1/")]
    #[case::absolute_userinfo(Some("s3://user@bucket/table/p1"), "s3://user@bucket/table/p1/")]
    #[case::absolute_port(Some("s3://bucket:443/table/p1"), "s3://bucket:443/table/p1/")]
    fn test_write_location_uri_prefix(#[case] value: Option<&str>, #[case] expected: &str) {
        assert_eq!(write_location_uri_prefix(&root(), value).unwrap(), expected);
    }

    #[rstest]
    #[case::relative_outside_files("other/p1", "must be under `_files`")]
    #[case::relative_files_name_prefix("_filesx/p1", "must be under `_files`")]
    #[case::relative_files_wrong_case("_FILES/p1", "must be under `_files`")]
    #[case::relative_empty("", "non-empty path")]
    #[case::relative_leading_slash("/_files/p1", "must not start with '/'")]
    #[case::relative_parent("_files/../p1", DOT)]
    #[case::relative_encoded_slash("_files%2Fp1", SEPARATOR)]
    #[case::relative_empty_segment("_files//p1", "empty path segment")]
    #[case::relative_trailing_double_slash("_files//", "empty path segment")]
    #[case::relative_space("_files/a b", CONTROL)]
    #[case::relative_query("_files/p1?x", "query or fragment")]
    #[case::absolute_table_root("s3://bucket/table", "outside the table root")]
    #[case::absolute_table_root_with_slash("s3://bucket/table/", "outside the table root")]
    #[case::absolute_under_table_root("s3://bucket/table/_files/p1", "outside the table root")]
    // An encoded spelling of the table root, or a differently-cased scheme, is still the root.
    #[case::absolute_encoded_table_root("s3://bucket/%74able/p1", "outside the table root")]
    #[case::absolute_upper_case_scheme("S3://bucket/table/p1", "outside the table root")]
    // Encoded separators and dot navigation could lead back inside the table root.
    #[case::absolute_encoded_slash("s3://bucket/table%2F_files/p1", SEPARATOR)]
    #[case::absolute_encoded_backslash("s3://bucket/table%5C_files/p1", SEPARATOR)]
    #[case::absolute_parent("s3://bucket/x/../table/p1", DOT)]
    #[case::absolute_fragment("s3://other/files#x", "query or fragment")]
    #[case::absolute_not_hierarchical("mailto:files@example.com", "hierarchical URI")]
    #[case::absolute_no_host_or_path("s3://", "host or a path")]
    #[case::absolute_empty_segment("s3://other/files//p1", "empty path segment")]
    #[case::absolute_trailing_double_slash("s3://other/files//", "empty path segment")]
    fn test_write_location_uri_prefix_rejects(#[case] value: &str, #[case] reason: &str) {
        let err = write_location_uri_prefix(&root(), Some(value)).unwrap_err();
        assert!(
            matches!(err, KernelError::UnsupportedReferencedFilePath(_)),
            "{err:?}"
        );
        let err = err.to_string();
        assert!(err.contains("delta.fileType.writeLocation"), "{err}");
        assert!(err.contains(reason), "{err}");
    }

    #[rstest]
    #[case::invalid_utf8_differs("s3://bucket/%FF/", "s3://bucket/%FE/p1", false)]
    #[case::invalid_utf8_same("s3://bucket/%FF/", "s3://bucket/%FF/p1", true)]
    #[case::encoded_slash("s3://bucket/a%2Fb/", "s3://bucket/a/b/p1", true)]
    fn test_is_within(#[case] root: &str, #[case] url: &str, #[case] expected: bool) {
        let (root, url) = (Url::parse(root).unwrap(), Url::parse(url).unwrap());
        assert_eq!(is_within(&url, &root), expected);
    }

    // An object store decodes the whole path, so `a%2Fb` and `a/b` are the same prefix.
    #[test]
    fn test_write_location_under_root_with_encoded_slash() {
        let root = Url::parse("s3://bucket/a%2Fb/").unwrap();
        let err = write_location_uri_prefix(&root, Some("s3://bucket/a/b/files")).unwrap_err();
        assert!(err.to_string().contains("outside the table root"), "{err}");
    }

    #[rstest]
    #[case::default(None, None, "_files/", "s3://bucket/table/_files/", "")]
    #[case::extension(None, Some("png"), "_files/", "s3://bucket/table/_files/", ".png")]
    #[case::multi_part_extension(
        None,
        Some("tar.gz"),
        "_files/",
        "s3://bucket/table/_files/",
        ".tar.gz"
    )]
    #[case::files_prefix(
        Some("_files/p1"),
        None,
        "_files/p1/",
        "s3://bucket/table/_files/p1/",
        ""
    )]
    #[case::absolute(
        Some("s3://other/files"),
        Some("png"),
        "s3://other/files/",
        "s3://other/files/",
        ".png"
    )]
    fn test_new_referenced_file_path(
        #[case] write_location: Option<&str>,
        #[case] extension: Option<&str>,
        #[case] uri_prefix: &str,
        #[case] location_prefix: &str,
        #[case] suffix: &str,
    ) {
        let path = new_referenced_file_path(&root(), write_location, extension).unwrap();
        let name = path.uri().strip_prefix(uri_prefix).unwrap();
        let uuid = name.strip_suffix(suffix).unwrap();
        assert!(Uuid::parse_str(uuid).is_ok(), "{name}");
        assert_eq!(path.location().as_str(), format!("{location_prefix}{name}"));
        assert_eq!(
            resolve_file_uri(&root(), path.uri()).unwrap(),
            *path.location()
        );
    }

    #[test]
    fn test_new_referenced_file_path_is_unique() {
        let first = new_referenced_file_path(&root(), None, None).unwrap();
        let second = new_referenced_file_path(&root(), None, None).unwrap();
        assert_ne!(first.uri(), second.uri());
    }

    #[rstest]
    #[case::empty("")]
    #[case::slash("png/x")]
    #[case::space("p ng")]
    #[case::percent("p%2Fng")]
    fn test_new_referenced_file_path_rejects_unsupported_extension(#[case] extension: &str) {
        let err = new_referenced_file_path(&root(), None, Some(extension)).unwrap_err();
        assert!(
            matches!(err, KernelError::UnsupportedReferencedFilePath(_)),
            "{err:?}"
        );
        let err = err.to_string();
        assert!(err.contains("extension"), "{err}");
        assert!(err.contains("is unsupported"), "{err}");
    }

    #[test]
    fn test_new_referenced_file_path_rejects_invalid_write_location() {
        let err = new_referenced_file_path(&root(), Some("other"), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("must be under `_files`"), "{err}");
    }
}
