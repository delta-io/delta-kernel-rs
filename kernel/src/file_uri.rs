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
/// - A `uri` with a URI scheme ([RFC 3986 section 3.1]) is absolute and is parsed as a [`Url`],
///   which normalizes any `.` or `..` segments.
/// - A `uri` without a scheme is relative and is joined onto `table_root` with a single `/`.
///   Against `s3://bucket/table/`, `_files/a.png` resolves to `s3://bucket/table/_files/a.png`.
///
/// The `uri` is an RFC 3986 URI-reference and is not percent-encoded again: `_files/a%20b.png`
/// names the object `_files/a b.png`.
///
/// Only the location is computed: kernel does not check that bytes exist there, and whether they
/// are accessible is up to the connector.
///
/// # Errors
///
/// Returns an error if:
/// - the `uri` contains a space or an ASCII control character, which a URI cannot contain and which
///   URL parsing would otherwise drop or trim;
/// - a relative `uri` has an empty path, or its path starts with `/` or `\`, which the
///   relative-path rule does not allow;
/// - a relative `uri` has a `.` or `..` path segment once its path is percent-decoded and split on
///   `/` and `\`, as `_files/../a.png`, `_files/%2e%2e/a.png` and `_files/..%2fa.png` do. The
///   relative-path rule does not support such navigation, and URL parsing or the object store would
///   otherwise resolve it, possibly to a location outside the table root;
/// - the resolved location fails to parse as a [`Url`].
///
/// [relative paths specification]: https://iceberg.apache.org/spec/#paths-in-metadata
/// [RFC 3986 section 3.1]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
pub fn resolve_file_uri(table_root: &Url, uri: &str) -> KernelResult<Url> {
    reject_space_and_control_chars(uri)
        .map_err(|reason| KernelError::generic(format!("Invalid file uri {uri:?}: {reason}")))?;
    if scheme_len(uri).is_some() {
        return Url::parse(uri).map_err(|e| {
            KernelError::generic(format!("Failed to parse absolute file uri {uri:?}: {e}"))
        });
    }
    validate_relative_uri(uri).map_err(|reason| {
        KernelError::generic(format!("Invalid relative file uri {uri:?}: {reason}"))
    })?;
    let base = with_trailing_slash(table_root);
    Url::parse(&format!("{base}{uri}")).map_err(|e| {
        KernelError::generic(format!(
            "Failed to resolve relative file uri {uri:?} against table root {base}: {e}"
        ))
    })
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
/// Returns an error if `write_location` is invalid (see [`write_location_uri_prefix`]), or if
/// `extension` is empty or has a character other than an ASCII letter, digit, `-`, `_` or `.`.
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
                KernelError::generic(format!(
                    "Invalid referenced file extension {extension:?}: it must be non-empty and \
                     contain only ASCII letters, digits, '-', '_' and '.'"
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
/// references: `_files/` when `write_location` is `None`, and otherwise the `write_location` value
/// with a trailing `/`.
///
/// Per the `file` type protocol, a valid value is either a table-root-relative path under `_files`
/// (`_files` itself, or `_files/<prefix>`) or an absolute URI outside the table root. A writer must
/// not write files at any other value, so this returns an error for one. Whether an absolute URI is
/// outside the table root is decided lexically; see [`is_within`].
fn write_location_uri_prefix(
    table_root: &Url,
    write_location: Option<&str>,
) -> KernelResult<String> {
    let Some(value) = write_location else {
        return Ok(format!("{FILES_DIR}/"));
    };
    let invalid = |reason: &str| {
        KernelError::generic(format!(
            "Invalid {FILE_TYPE_WRITE_LOCATION} {value:?}: {reason}"
        ))
    };
    reject_space_and_control_chars(value).map_err(invalid)?;
    require!(
        !value.contains(['?', '#']),
        invalid("a write location cannot have a query or fragment")
    );
    let dir = value.strip_suffix('/').unwrap_or(value);
    if scheme_len(value).is_some() {
        // Parsing normalizes plain `.` and `..` segments, so the parsed location is what is
        // checked, and its text is the prefix of the `uri`s written there.
        let url = Url::parse(&format!("{dir}/")).map_err(|e| invalid(&e.to_string()))?;
        require!(
            !url.cannot_be_a_base(),
            invalid("an absolute write location must be a hierarchical URI")
        );
        // A decoded `.` or `..` segment would navigate once a store or `to_file_path` decodes it,
        // which could lead back inside the table root, so it is rejected before the comparison.
        require!(
            !decoded_segments(url.path())
                .iter()
                .any(|s| is_dot_segment(s)),
            invalid("an absolute write location must not contain a '.' or '..' segment")
        );
        require!(
            !is_within(&url, table_root),
            invalid("an absolute write location must be outside the table root")
        );
        Ok(url.into())
    } else {
        validate_relative_uri(value).map_err(invalid)?;
        require!(
            dir == FILES_DIR || dir.starts_with(&format!("{FILES_DIR}/")),
            invalid("a relative write location must be under `_files`")
        );
        require!(
            !dir.split('/').any(str::is_empty),
            invalid("a relative write location cannot have an empty path segment")
        );
        Ok(format!("{dir}/"))
    }
}

/// Checks that a writer may write `uri`: it is a `uri` that [`resolve_file_uri`] resolves, for any
/// table root. The `file` type protocol requires a writer to write each `uri` as an absolute URI or
/// as a relative reference resolved per its URI resolution rules.
pub(crate) fn validate_file_uri(uri: &str) -> Result<(), &'static str> {
    reject_space_and_control_chars(uri)?;
    if scheme_len(uri).is_some() {
        Url::parse(uri).map_err(|_| "an absolute uri must parse as a URL")?;
    } else {
        validate_relative_uri(uri)?;
    }
    Ok(())
}

/// Returns whether `url` is `table_root` or under it.
///
/// The paths are percent-decoded before they are split on `/` and `\`, as object stores and
/// `Url::to_file_path` decode them, and are then compared segment by segment. So an encoded
/// spelling of the table root (`%74able` for `table`, or `table%2F_files` for `table/_files`)
/// counts as inside it, but a sibling that only shares a name prefix (`table2`) does not.
///
/// The scheme and authority (`scheme://[userinfo@]host[:port]`) are compared as text, ignoring
/// case. The check is lexical: it doesn't unify scheme aliases such as `s3` and `s3a`, a default
/// port written out, or userinfo, so a location spelled with one of those counts as outside.
fn is_within(url: &Url, table_root: &Url) -> bool {
    let authority = |url: &Url| url[..Position::BeforePath].to_ascii_lowercase();
    let segments = |url: &Url| -> Vec<String> {
        let mut segments = decoded_segments(url.path());
        segments.retain(|segment| !segment.is_empty());
        segments
    };
    authority(url) == authority(table_root) && segments(url).starts_with(&segments(table_root))
}

/// Fails if `uri` contains a space or an ASCII control character. A URI cannot contain either
/// ([RFC 3986 section 2]), and URL parsing removes tabs and newlines and trims leading and
/// trailing spaces and controls, so the parsed location could differ from what was checked.
///
/// [RFC 3986 section 2]: https://datatracker.ietf.org/doc/html/rfc3986#section-2
fn reject_space_and_control_chars(uri: &str) -> Result<(), &'static str> {
    if uri.chars().any(|c| c == ' ' || c.is_ascii_control()) {
        return Err("contains a space or an ASCII control character");
    }
    Ok(())
}

/// Fails if the relative reference `uri` breaks the relative-path rule: its path (before any `?`
/// or `#`) is empty, starts with `/` or `\`, or has a `.` or `..` segment.
///
/// The `uri` is already percent-encoded, and URL parsing and object stores decode it, so segments
/// are checked after percent-decoding the path: `%2e` is a `.`, and `%2f` and `%5c` separate
/// segments. Segments are split on `/` and also on `\`, which URL parsing treats as `/` for some
/// schemes. AMT paths are checked differently (see `amt_path_util::reject_dot_segments`) because
/// they are raw paths that kernel percent-encodes before parsing, so only a literal `.` or `..`
/// segment navigates.
fn validate_relative_uri(uri: &str) -> Result<(), &'static str> {
    let path = uri.split(['?', '#']).next().unwrap_or_default();
    if path.is_empty() {
        return Err("a relative uri must have a non-empty path");
    }
    if path.starts_with(['/', '\\']) {
        return Err("a relative uri must not start with '/' or '\\'");
    }
    if decoded_segments(path).iter().any(|s| is_dot_segment(s)) {
        return Err("a relative uri must not contain a '.' or '..' segment");
    }
    Ok(())
}

/// Returns the segments of a URL `path` once it is percent-decoded and split on `/` and `\`, as
/// object stores and `Url::to_file_path` see them: `%2e` is a `.`, and `%2f` and `%5c` separate
/// segments. Empty segments are kept.
fn decoded_segments(path: &str) -> Vec<String> {
    percent_decode_str(path)
        .decode_utf8_lossy()
        .split(['/', '\\'])
        .map(str::to_owned)
        .collect()
}

fn is_dot_segment(segment: &str) -> bool {
    segment == "." || segment == ".."
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
    // An encoded `/` is accepted when it adds no `.` or `..` segment. Object stores decode it, so
    // this names `_files/a/b.png`, still under the table root.
    #[case::encoded_slash(
        "s3://bucket/table/",
        "_files/a%2Fb.png",
        "s3://bucket/table/_files/a%2Fb.png"
    )]
    #[case::query(
        "s3://bucket/table/",
        "_files/a.png?v=1",
        "s3://bucket/table/_files/a.png?v=1"
    )]
    // A `.` or `..` after `?` or `#` is not a path segment.
    #[case::dots_in_query(
        "s3://bucket/table/",
        "_files/a.png?p=../x",
        "s3://bucket/table/_files/a.png?p=../x"
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
    // The relative-path rule applies only to relative `uri`s; parsing normalizes an absolute one.
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
    #[case::dot(".", "'.' or '..' segment")]
    #[case::dot_dot("..", "'.' or '..' segment")]
    #[case::leading_dot("./a.png", "'.' or '..' segment")]
    #[case::parent("../other/a.png", "'.' or '..' segment")]
    #[case::inner_parent("_files/../../a.png", "'.' or '..' segment")]
    #[case::inner_dot("_files/./a.png", "'.' or '..' segment")]
    #[case::trailing_parent("_files/..", "'.' or '..' segment")]
    #[case::encoded_parent("_files/%2e%2e/a.png", "'.' or '..' segment")]
    #[case::encoded_parent_upper("_files/%2E%2E/a.png", "'.' or '..' segment")]
    #[case::mixed_encoded_parent("_files/.%2e/a.png", "'.' or '..' segment")]
    #[case::encoded_dot("_files/%2e/a.png", "'.' or '..' segment")]
    #[case::backslash_parent("_files\\..\\a.png", "'.' or '..' segment")]
    // Object stores decode `%2f` and `%5c`, which then separate a `..` segment.
    #[case::encoded_slash_parent("_files/%2e%2e%2f%2e%2e%2fsecret", "'.' or '..' segment")]
    #[case::encoded_slash_upper_parent("_files/..%2F..%2Fsecret", "'.' or '..' segment")]
    #[case::encoded_backslash_parent("_files/.%2e%5csecret", "'.' or '..' segment")]
    #[case::encoded_slash_after_parent(".%2e%2Fsecret", "'.' or '..' segment")]
    #[case::parent_before_query("../a.png?x=1", "'.' or '..' segment")]
    #[case::leading_slash("/a.png", "must not start with '/'")]
    #[case::leading_double_slash("//host/a.png", "must not start with '/'")]
    #[case::leading_backslash("\\a.png", "must not start with '/'")]
    #[case::empty("", "non-empty path")]
    #[case::only_query("?x", "non-empty path")]
    #[case::only_fragment("#x", "non-empty path")]
    // URL parsing drops tabs and newlines and trims spaces, which would turn these into `..`.
    #[case::tab_in_parent("_files/.\t./a.png", "space or an ASCII control character")]
    #[case::newline_in_parent(".\n./a.png", "space or an ASCII control character")]
    #[case::carriage_return("_files/.\r./a.png", "space or an ASCII control character")]
    #[case::tab_in_encoded_parent("%2\te%2\te/a.png", "space or an ASCII control character")]
    #[case::trailing_space("..  ", "space or an ASCII control character")]
    #[case::inner_space("_files/a b.png", "space or an ASCII control character")]
    #[case::nul("_files/a\0.png", "space or an ASCII control character")]
    #[case::absolute_with_tab("s3://bucket/x/.\t./a.png", "space or an ASCII control character")]
    fn test_resolve_file_uri_rejects(#[case] uri: &str, #[case] reason: &str) {
        let err = resolve_file_uri(&root(), uri).unwrap_err().to_string();
        assert!(err.contains(reason), "{err}");
    }

    #[test]
    fn test_resolve_file_uri_rejects_unparseable_absolute_uri() {
        let err = resolve_file_uri(&root(), "http://[bad/a.png")
            .unwrap_err()
            .to_string();
        assert!(err.contains("Failed to parse absolute file uri"), "{err}");
    }

    #[rstest]
    #[case::default(None, "_files/")]
    #[case::files_dir(Some("_files"), "_files/")]
    #[case::files_dir_with_slash(Some("_files/"), "_files/")]
    #[case::files_prefix(Some("_files/p1"), "_files/p1/")]
    #[case::nested_files_prefix(Some("_files/a/b/"), "_files/a/b/")]
    #[case::absolute(Some("s3://other/files"), "s3://other/files/")]
    #[case::absolute_with_slash(Some("s3://other/files/"), "s3://other/files/")]
    #[case::absolute_same_bucket(Some("s3://bucket/elsewhere"), "s3://bucket/elsewhere/")]
    // Only the table root's own path is inside it, not a sibling that shares its name as a prefix.
    #[case::absolute_sibling_with_shared_prefix(Some("s3://bucket/table2"), "s3://bucket/table2/")]
    // The check is lexical, so a scheme alias of the table root's scheme, userinfo, or an explicit
    // port makes the same location count as outside the table root.
    #[case::absolute_scheme_alias(Some("s3a://bucket/table/p1"), "s3a://bucket/table/p1/")]
    #[case::absolute_userinfo(Some("s3://user@bucket/table/p1"), "s3://user@bucket/table/p1/")]
    #[case::absolute_port(Some("s3://bucket:443/table/p1"), "s3://bucket:443/table/p1/")]
    // The prefix is the parsed location, which normalizes plain `.` and `..` segments.
    #[case::absolute_normalized(Some("s3://other/x/%2e/../files"), "s3://other/files/")]
    fn test_write_location_uri_prefix(#[case] value: Option<&str>, #[case] expected: &str) {
        assert_eq!(write_location_uri_prefix(&root(), value).unwrap(), expected);
    }

    #[rstest]
    #[case::relative_outside_files("other/p1", "must be under `_files`")]
    #[case::relative_files_name_prefix("_filesx/p1", "must be under `_files`")]
    #[case::relative_files_wrong_case("_FILES/p1", "must be under `_files`")]
    #[case::relative_empty("", "non-empty path")]
    #[case::relative_leading_slash("/_files/p1", "must not start with '/'")]
    #[case::relative_parent("_files/../p1", "'.' or '..' segment")]
    #[case::relative_empty_segment("_files//p1", "empty path segment")]
    #[case::relative_trailing_double_slash("_files//", "empty path segment")]
    #[case::relative_space("_files/a b", "space or an ASCII control character")]
    #[case::relative_query("_files/p1?x", "query or fragment")]
    #[case::absolute_table_root("s3://bucket/table", "outside the table root")]
    #[case::absolute_table_root_with_slash("s3://bucket/table/", "outside the table root")]
    #[case::absolute_under_table_root("s3://bucket/table/_files/p1", "outside the table root")]
    // An encoded spelling of the table root, or a differently-cased scheme, is still the root.
    #[case::absolute_encoded_table_root("s3://bucket/%74able/p1", "outside the table root")]
    #[case::absolute_upper_case_scheme("S3://bucket/table/p1", "outside the table root")]
    // An encoded `/` that joins the table root with its subdirectory is still inside the root.
    #[case::absolute_encoded_slash_upper("s3://bucket/table%2F_files/p1", "outside the table root")]
    #[case::absolute_encoded_slash_lower("s3://bucket/table%2f_files/p1", "outside the table root")]
    #[case::absolute_encoded_backslash("s3://bucket/table%5C_files/p1", "outside the table root")]
    // A `..` that appears only after decoding could lead back inside the table root.
    #[case::absolute_encoded_parent("s3://bucket/x%2F..%2Ftable/p1", "'.' or '..' segment")]
    #[case::absolute_encoded_backslash_parent(
        "s3://bucket/x%5C..%5Ctable/p1",
        "'.' or '..' segment"
    )]
    // Parsing collapses a plain `..`, so this is the table root's `p1`.
    #[case::absolute_parent_into_table_root("s3://bucket/x/../table/p1", "outside the table root")]
    #[case::absolute_fragment("s3://other/files#x", "query or fragment")]
    #[case::absolute_not_hierarchical("mailto:files@example.com", "hierarchical URI")]
    fn test_write_location_uri_prefix_rejects(#[case] value: &str, #[case] reason: &str) {
        let err = write_location_uri_prefix(&root(), Some(value))
            .unwrap_err()
            .to_string();
        assert!(err.contains("delta.fileType.writeLocation"), "{err}");
        assert!(err.contains(reason), "{err}");
    }

    /// On a local table, `Url::to_file_path` decodes an absolute write location, so an encoded `/`
    /// can name the table root, and an encoded `..` can lead back into it.
    #[rstest]
    #[case::encoded_slash("file:///tmp%2Ftable/x", "outside the table root")]
    #[case::encoded_parent("file:///tmp/outside/..%2Ftable/references", "'.' or '..' segment")]
    fn test_write_location_uri_prefix_rejects_encoded_local_table_root(
        #[case] value: &str,
        #[case] reason: &str,
    ) {
        let root = Url::parse("file:///tmp/table/").unwrap();
        let err = write_location_uri_prefix(&root, Some(value))
            .unwrap_err()
            .to_string();
        assert!(err.contains(reason), "{err}");
    }

    /// Writes accept exactly the `uri`s that resolution accepts.
    #[rstest]
    #[case("_files/a.png")]
    #[case("_files/a%2Fb.png")]
    #[case("s3://other/a.png")]
    #[case("s3://bucket/x/../a.png")]
    #[case("")]
    #[case("?x")]
    #[case("/a.png")]
    #[case("\\a.png")]
    #[case("_files/../a.png")]
    #[case("_files/..%2fa.png")]
    #[case("_files/.\t./a.png")]
    #[case("_files/a b.png")]
    #[case("http://[bad/a.png")]
    fn test_validate_file_uri_matches_resolve_file_uri(#[case] uri: &str) {
        assert_eq!(
            validate_file_uri(uri).is_ok(),
            resolve_file_uri(&root(), uri).is_ok(),
            "{uri:?}"
        );
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
    fn test_new_referenced_file_path_rejects_invalid_extension(#[case] extension: &str) {
        let err = new_referenced_file_path(&root(), None, Some(extension))
            .unwrap_err()
            .to_string();
        assert!(err.contains("Invalid referenced file extension"), "{err}");
    }

    #[test]
    fn test_new_referenced_file_path_rejects_invalid_write_location() {
        let err = new_referenced_file_path(&root(), Some("other"), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("must be under `_files`"), "{err}");
    }
}
