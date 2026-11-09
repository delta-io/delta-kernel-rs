//! RFC 3986 URI-reference syntax: whether a reference is absolute (has a scheme) or relative, and
//! percent-encoding or decoding its path while keeping any `scheme:[//authority]` prefix.

use std::borrow::Cow;

#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::path_encoding::uri_decode_path;
use crate::path_encoding::uri_encode_path;

/// Returns the byte length of `location`'s URI scheme (excluding the terminating `:`), or `None`
/// if `location` does not begin with one, per [RFC 3986 section 3.1]:
/// `scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )`, terminated by `:`.
///
/// [RFC 3986 section 3.1]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
pub(crate) fn scheme_len(location: &str) -> Option<usize> {
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

/// Percent-encodes the path of a raw URI reference with [`uri_encode_path`], keeping the
/// `scheme:[//authority]` prefix of an absolute reference verbatim. A relative reference is
/// encoded whole. Returns [`Cow::Borrowed`] when nothing needs encoding.
pub(crate) fn uri_encode_location(location: &str) -> Cow<'_, str> {
    let (prefix, path) = split_prefix(location);
    with_prefix(location, prefix, uri_encode_path(path))
}

/// The inverse of [`uri_encode_location`]: percent-decodes the path with [`uri_decode_path`],
/// keeping any `scheme:[//authority]` prefix verbatim. Returns `None` if the decoded path is not
/// valid UTF-8.
#[cfg(feature = "adaptive-metadata-in-dev")]
pub(crate) fn uri_decode_location(location: &str) -> Option<Cow<'_, str>> {
    let (prefix, path) = split_prefix(location);
    Some(with_prefix(location, prefix, uri_decode_path(path)?))
}

/// Splits `location` into its `scheme:[//authority]` prefix, empty for a relative reference, and
/// the path that follows. The authority, when present, ends at the next `/`.
fn split_prefix(location: &str) -> (&str, &str) {
    let Some(scheme_len) = scheme_len(location) else {
        return ("", location);
    };
    let after_scheme = scheme_len + 1;
    let prefix_len = match location[after_scheme..].strip_prefix("//") {
        Some(rest) => after_scheme + 2 + rest.find('/').unwrap_or(rest.len()),
        None => after_scheme,
    };
    location.split_at(prefix_len)
}

/// Rejoins `prefix` with a transformed `path`, borrowing `location` when the path is unchanged.
fn with_prefix<'a>(location: &'a str, prefix: &str, path: Cow<'_, str>) -> Cow<'a, str> {
    match path {
        Cow::Borrowed(_) => Cow::Borrowed(location),
        Cow::Owned(path) => Cow::Owned(format!("{prefix}{path}")),
    }
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
    #[case::single_char("s3://b/a", Some(2))]
    #[case::compound("git+ssh://host/repo", Some(7))]
    #[case::drive_letter("C:/data", Some(1))]
    #[case::empty("", None)]
    #[case::no_colon("abc", None)]
    #[case::leading_colon(":a", None)]
    #[case::leading_digit("3com:x", None)]
    #[case::colon_after_slash("data/a:b", None)]
    #[case::non_ascii_lead("\u{03b1}x:y", None)]
    #[case::non_ascii_mid("a\u{e9}:x", None)]
    fn test_scheme_len(#[case] location: &str, #[case] expected: Option<usize>) {
        assert_eq!(scheme_len(location), expected);
    }

    #[rstest]
    #[case::relative_unreserved("data/a.parquet", "data/a.parquet")]
    #[case::relative_space("data/a b.parquet", "data/a%20b.parquet")]
    #[case::relative_colon_kept("data/a:b.parquet", "data/a:b.parquet")]
    #[case::absolute_path_encoded("s3://bucket/a b#1.parquet", "s3://bucket/a%20b%231.parquet")]
    #[case::empty_authority("file:///tmp/a b.parquet", "file:///tmp/a%20b.parquet")]
    #[case::authority_kept_verbatim("s3://[::1]/t/[x]", "s3://[::1]/t/%5Bx%5D")]
    #[case::authority_only("s3://bucket", "s3://bucket")]
    #[case::no_authority("C:/data dir/a", "C:/data%20dir/a")]
    #[case::non_ascii("data/M\u{fc}nchen", "data/M%C3%BCnchen")]
    fn test_uri_encode_location_round_trips(#[case] raw: &str, #[case] encoded: &str) {
        let actual = uri_encode_location(raw);
        assert_eq!(matches!(actual, Cow::Borrowed(_)), raw == encoded);
        assert_eq!(actual, encoded);
        #[cfg(feature = "adaptive-metadata-in-dev")]
        assert_eq!(uri_decode_location(encoded).unwrap(), raw);
    }

    #[cfg(feature = "adaptive-metadata-in-dev")]
    #[test]
    fn test_uri_decode_location_rejects_invalid_utf8() {
        assert_eq!(uri_decode_location("s3://b/%FF"), None);
    }
}
