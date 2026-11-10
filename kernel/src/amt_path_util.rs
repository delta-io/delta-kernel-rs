//! Resolution of AMT paths (relative vs absolute) against a table root, per the Iceberg V4
//! [relative paths specification].
//!
//! [relative paths specification]: https://iceberg.apache.org/spec/#paths-in-metadata

use url::Url;

use crate::expressions::{Expression, UnaryExpressionOp};
use crate::path_encoding::{uri_encode_path, with_trailing_slash};
use crate::uri_reference::{scheme_len, uri_decode_location, uri_encode_location};
use crate::utils::{require, to_file_size};
use crate::{FileMeta, KernelError, KernelResult};

/// Resolve an AMT `path` (as stored in the log or a manifest) into an absolute [`Url`].
///
/// If `path` has a URI scheme it is absolute; otherwise it is relative and joined onto
/// `table_root` with a single `/` separator, matching Iceberg V4's [relative paths specification].
/// In both cases `path` is a raw (unencoded) string, so its path component is percent-encoded
/// (preserving `/` separators) before parsing.
///
/// # Errors
///
/// Returns an error if `path` contains a `.` or `..` segment, if a relative `path` is empty or
/// begins with `/`, or if the resolved location fails to parse as a [`Url`].
///
/// [relative paths specification]: https://iceberg.apache.org/spec/#paths-in-metadata
pub(crate) fn resolve_amt_location(path: &str, table_root: &Url) -> KernelResult<Url> {
    match parse_absolute_amt_location(path)? {
        Some(url) => Ok(url),
        None => resolve_table_relative(path, table_root),
    }
}

/// Evaluates a STRING AMT data-file `location` to the `Add.path` it is read through: the path
/// component percent-encoded as in [`resolve_amt_location`], a scheme and authority kept as
/// written, and a relative location kept relative. Joining the value onto the table root gives
/// the [`Url`] [`resolve_amt_location`] returns.
pub(crate) fn add_path_expression(location: Expression) -> Expression {
    Expression::unary(UnaryExpressionOp::UriEncodePath, location)
}

/// Parses an absolute AMT `path` into a [`Url`] with its path component percent-encoded, as
/// [`resolve_amt_location`] does, or returns `None` if `path` is relative (has no URI scheme).
pub(crate) fn parse_absolute_amt_location(path: &str) -> KernelResult<Option<Url>> {
    if scheme_len(path).is_none() {
        return Ok(None);
    }
    reject_dot_segments(path)?;
    let url = Url::parse(&uri_encode_location(path)).map_err(|e| {
        KernelError::generic(format!(
            "Failed to parse absolute AMT location {path:?}: {e}"
        ))
    })?;
    Ok(Some(url))
}

/// Converts an absolute, percent-encoded `uri` (such as a Delta `p` deletion vector path) into the
/// raw form AMT locations are stored in: the inverse of [`resolve_amt_location`] for an absolute
/// path. The `scheme:[//authority]` prefix is kept verbatim and the path component is
/// percent-decoded.
pub(crate) fn raw_amt_location_from_uri(uri: &str) -> KernelResult<String> {
    let invalid =
        |reason: &str| KernelError::generic(format!("Invalid absolute URI {uri:?}: {reason}"));
    require!(scheme_len(uri).is_some(), invalid("no URI scheme"));
    let url = Url::parse(uri).map_err(|e| invalid(&e.to_string()))?;
    require!(
        url.query().is_none() && url.fragment().is_none(),
        invalid("a query or fragment cannot be stored as an AMT location")
    );
    let raw = uri_decode_location(uri).ok_or_else(|| invalid("path is not valid UTF-8"))?;
    Ok(raw.into_owned())
}

/// Builds the [`FileMeta`] of an AMT manifest file referenced by `path` (absolute, or relative to
/// `table_root`) with `size_in_bytes`. AMT references carry no modification time, so
/// `last_modified` is `i64::MAX`.
///
/// # Errors
/// Returns an error if `path` cannot be resolved (see [`resolve_amt_location`]), or if
/// `size_in_bytes` is negative (the error names `context` and `path`).
pub(crate) fn amt_manifest_filemeta(
    path: &str,
    size_in_bytes: i64,
    table_root: &Url,
    context: &str,
) -> KernelResult<FileMeta> {
    Ok(FileMeta {
        location: resolve_amt_location(path, table_root)?,
        last_modified: i64::MAX,
        size: to_file_size(size_in_bytes, format_args!("{context} '{path}'"))?,
    })
}

/// Checks that a raw AMT data-file `location` can become an `Add.path` that resolves where
/// [`resolve_amt_location`] does.
///
/// # Errors
///
/// Returns an error if `location` contains a `.` or `..` segment, which [`resolve_amt_location`]
/// rejects too, or if a relative `location` is empty or begins with `/` (see
/// [`validate_table_relative`]), which would resolve outside the table root.
pub(crate) fn validate_amt_data_file_location(location: &str) -> KernelResult<()> {
    reject_dot_segments(location)?;
    if scheme_len(location).is_none() {
        validate_table_relative(location)?;
    }
    Ok(())
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

/// Resolves a relative `path` against `table_root`: percent-encodes `path` so reserved URI
/// characters survive a round trip through the object store, then concatenates it onto
/// `table_root` with a single `/` separator and parses the result.
///
/// # Errors
///
/// Returns an error if the resolved location fails to parse as a [`Url`].
fn encode_and_join(path: &str, table_root: &Url) -> KernelResult<Url> {
    reject_dot_segments(path)?;
    let base = with_trailing_slash(table_root);
    Url::parse(&format!("{base}{}", uri_encode_path(path))).map_err(|e| {
        KernelError::generic(format!(
            "Failed to resolve relative AMT location {path:?} against table root {base}: {e}"
        ))
    })
}

/// Rejects an AMT location that is neither absolute nor table-relative (see
/// [`validate_table_relative`]) or that has a `.` or `..` segment (see [`reject_dot_segments`]).
pub(crate) fn validate_amt_location(location: &str) -> KernelResult<()> {
    if scheme_len(location).is_none() {
        validate_table_relative(location)?;
    }
    reject_dot_segments(location)
}

/// Rejects a `path` containing a `.` or `..` segment: the Iceberg V4 path spec does not support
/// relative navigation, and URL parsing would otherwise silently collapse such segments.
///
/// An AMT path is raw: kernel percent-encodes it before parsing, so `%` and `\` are data and only
/// a literal `.` or `..` segment navigates. A `file` value's `uri` is already percent-encoded, so
/// `file_uri::check_uri` also rejects `%2e` forms, `\`, and encoded separators.
pub(crate) fn reject_dot_segments(path: &str) -> KernelResult<()> {
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
    use crate::Engine as _;
    use crate::arrow::array::AsArray as _;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{Scalar, col};
    use crate::schema::{DataType, schema_ref};

    type Resolver = fn(&str, &Url) -> KernelResult<Url>;

    /// Evaluates `expression` over a one-row batch whose `location` column holds `location`.
    fn evaluate_on_location(location: &str, expression: Expression) -> KernelResult<String> {
        let handler = SyncEngine::new().evaluation_handler();
        let schema = schema_ref! { not_null "location": STRING };
        let batch = handler.create_many(schema.clone(), vec![vec![Scalar::from(location)]])?;
        let value = handler
            .new_expression_evaluator(schema, expression.into(), DataType::STRING)?
            .evaluate(batch.as_ref())?
            .try_into_record_batch()?;
        Ok(value.column(0).as_string::<i32>().value(0).to_string())
    }

    /// `expected_url` is where a scan reads the file: `add.path` joined onto the table root.
    #[rstest]
    #[case::plain_relative("data/a.parquet", "data/a.parquet", "memory:///table/data/a.parquet")]
    #[case::spaced_relative(
        "data/a b.parquet",
        "data/a%20b.parquet",
        "memory:///table/data/a%20b.parquet"
    )]
    #[case::percent_relative(
        "data/a%b.parquet",
        "data/a%25b.parquet",
        "memory:///table/data/a%25b.parquet"
    )]
    #[case::non_ascii_relative(
        "data/M\u{fc}nchen.parquet",
        "data/M%C3%BCnchen.parquet",
        "memory:///table/data/M%C3%BCnchen.parquet"
    )]
    #[case::query_and_fragment_relative(
        "data/a?b#c.parquet",
        "data/a%3Fb%23c.parquet",
        "memory:///table/data/a%3Fb%23c.parquet"
    )]
    #[case::plain_absolute(
        "s3://bucket/t/a.parquet",
        "s3://bucket/t/a.parquet",
        "s3://bucket/t/a.parquet"
    )]
    #[case::spaced_absolute(
        "s3://bucket/t/a b.parquet",
        "s3://bucket/t/a%20b.parquet",
        "s3://bucket/t/a%20b.parquet"
    )]
    #[case::spaced_absolute_without_authority(
        "file:/tmp/a b.parquet",
        "file:/tmp/a%20b.parquet",
        "file:///tmp/a%20b.parquet"
    )]
    fn test_add_path_expression(
        #[case] location: &str,
        #[case] expected: &str,
        #[case] expected_url: &str,
    ) {
        let path = evaluate_on_location(location, add_path_expression(col!("location"))).unwrap();
        assert_eq!(path, expected);
        let table_root = Url::parse("memory:///table/").unwrap();
        let url = table_root.join(&path).unwrap();
        assert_eq!(url.as_str(), expected_url);
        assert_eq!(url, resolve_amt_location(location, &table_root).unwrap());
    }

    #[rstest]
    #[case::parent_segment("../a.parquet", "contains a '.' or '..' segment")]
    #[case::current_segment("data/./a.parquet", "contains a '.' or '..' segment")]
    #[case::absolute_parent_segment("s3://bucket/t/../a.parquet", "contains a '.' or '..' segment")]
    #[case::empty("", "must not be empty")]
    #[case::leading_slash("/a.parquet", "must not begin with a leading '/'")]
    #[case::network_path("//host/a.parquet", "must not begin with a leading '/'")]
    fn validate_amt_data_file_location_rejects_invalid_locations(
        #[case] location: &str,
        #[case] message: &str,
    ) {
        assert_result_error_with_message(validate_amt_data_file_location(location), message);
    }

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
    #[case::encoded_space("s3://bucket/data/dv%20file.bin", "s3://bucket/data/dv file.bin")]
    #[case::unencoded("s3://bucket/data/dv.bin", "s3://bucket/data/dv.bin")]
    fn test_raw_amt_location_from_uri_round_trips(#[case] uri: &str, #[case] expected_raw: &str) {
        let raw = raw_amt_location_from_uri(uri).unwrap();
        assert_eq!(raw, expected_raw);
        let table_root = Url::parse("memory:///table/").unwrap();
        assert_eq!(
            resolve_amt_location(&raw, &table_root).unwrap().as_str(),
            uri
        );
    }

    #[rstest]
    #[case::relative("data/dv.bin", "no URI scheme")]
    #[case::not_a_url("s3://bucket:port/dv.bin", "Invalid absolute URI")]
    #[case::query("s3://bucket/dv.bin?x=1", "query or fragment")]
    #[case::fragment("s3://bucket/dv.bin#x", "query or fragment")]
    #[case::invalid_utf8("s3://bucket/%FF.bin", "not valid UTF-8")]
    fn test_raw_amt_location_from_uri_errors(#[case] uri: &str, #[case] message: &str) {
        assert_result_error_with_message(raw_amt_location_from_uri(uri), message);
    }

    #[rstest]
    #[case::relative_dot_dot("a/../b.bin", "contains a '.' or '..' segment")]
    #[case::relative_leading_dot("./a.bin", "contains a '.' or '..' segment")]
    #[case::relative_trailing_dot("a/.", "contains a '.' or '..' segment")]
    #[case::bare_dot_dot("..", "contains a '.' or '..' segment")]
    #[case::absolute_dot_dot("s3://bucket/x/../y.parquet", "contains a '.' or '..' segment")]
    #[case::empty("", "must not be empty")]
    #[case::leading_slash("/a.parquet", "must not begin with a leading '/'")]
    #[case::network_path("//host/a.parquet", "must not begin with a leading '/'")]
    fn test_resolve_amt_location_rejects_invalid_paths(#[case] path: &str, #[case] message: &str) {
        let table_root = Url::parse("memory:///table/").unwrap();
        assert_result_error_with_message(resolve_amt_location(path, &table_root), message);
    }

    #[rstest]
    #[case::invalid_port("s3://bucket:port/a.parquet")]
    #[case::space_in_authority("s3://my bucket/a.parquet")]
    fn test_resolve_amt_location_rejects_unparseable_absolute(#[case] path: &str) {
        let table_root = Url::parse("memory:///table/").unwrap();
        assert_result_error_with_message(
            resolve_amt_location(path, &table_root),
            "Failed to parse absolute AMT location",
        );
    }
}
