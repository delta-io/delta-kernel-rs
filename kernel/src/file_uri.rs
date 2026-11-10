//! Resolution of the `uri` of a `file` value against the table root, and the location where a
//! writer writes the files of new `file` references.

use url::Url;
use uuid::Uuid;

use crate::path_encoding::scheme_len;
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
/// - A `uri` with a URI scheme ([RFC 3986 section 3.1]) is absolute and is used as-is. Parsing it
///   into a [`Url`] normalizes any `.` or `..` segments.
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
/// - a relative `uri` has an empty path, or its path starts with `/`, which the relative-path rule
///   does not allow;
/// - a relative `uri` has a `.` or `..` path segment, including the percent-encoded forms `%2e` and
///   `%2e%2e` and segments separated by `\`. The relative-path rule does not support such
///   navigation, and URL parsing would otherwise resolve it, possibly to a location outside the
///   table root;
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
/// not write files at any other value, so this returns an error for one.
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
    let prefix = format!("{dir}/");
    if scheme_len(value).is_some() {
        let url = Url::parse(&prefix).map_err(|e| invalid(&e.to_string()))?;
        require!(
            !url.cannot_be_a_base(),
            invalid("an absolute write location must be a hierarchical URI")
        );
        let mut root = table_root.as_str().to_string();
        if !root.ends_with('/') {
            root.push('/');
        }
        require!(
            !url.as_str().starts_with(&root),
            invalid("an absolute write location must be outside the table root")
        );
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
    }
    Ok(prefix)
}

/// Checks that a writer may write `uri`: it contains no space or ASCII control character, and, if
/// relative, follows the relative-path rule (see [`resolve_file_uri`]). The `file` type protocol
/// requires a writer to write each `uri` as an absolute URI or as a relative reference resolved per
/// its URI resolution rules.
pub(crate) fn validate_file_uri(uri: &str) -> Result<(), &'static str> {
    reject_space_and_control_chars(uri)?;
    if scheme_len(uri).is_none() {
        validate_relative_uri(uri)?;
    }
    Ok(())
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
/// or `#`) is empty, starts with `/`, or has a `.` or `..` segment. Segments are split on `/` and
/// also on `\`, which URL parsing treats as `/` for some schemes, and `%2e` counts as `.`, as URL
/// parsing does.
fn validate_relative_uri(uri: &str) -> Result<(), &'static str> {
    let path = uri.split(['?', '#']).next().unwrap_or_default();
    if path.is_empty() {
        return Err("a relative uri must have a non-empty path");
    }
    if path.starts_with(['/', '\\']) {
        return Err("a relative uri must not start with '/'");
    }
    let has_dot_segment = path.split(['/', '\\']).any(|segment| {
        let segment = segment.replace("%2e", ".").replace("%2E", ".");
        segment == "." || segment == ".."
    });
    if has_dot_segment {
        return Err("a relative uri must not contain a '.' or '..' segment");
    }
    Ok(())
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
    #[case::relative_space("_files/a b", "space or an ASCII control character")]
    #[case::relative_query("_files/p1?x", "query or fragment")]
    #[case::absolute_table_root("s3://bucket/table", "outside the table root")]
    #[case::absolute_table_root_with_slash("s3://bucket/table/", "outside the table root")]
    #[case::absolute_under_table_root("s3://bucket/table/_files/p1", "outside the table root")]
    #[case::absolute_fragment("s3://other/files#x", "query or fragment")]
    #[case::absolute_not_hierarchical("mailto:files@example.com", "hierarchical URI")]
    fn test_write_location_uri_prefix_rejects(#[case] value: &str, #[case] reason: &str) {
        let err = write_location_uri_prefix(&root(), Some(value))
            .unwrap_err()
            .to_string();
        assert!(err.contains("delta.fileType.writeLocation"), "{err}");
        assert!(err.contains(reason), "{err}");
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
