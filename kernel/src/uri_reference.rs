//! RFC 3986 URI-reference syntax for raw (unencoded) references: whether a reference is absolute
//! (has a scheme) or relative, and how to percent-encode it.

use std::borrow::Cow;

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
    let (prefix, path) = match scheme_len(location) {
        Some(scheme_len) => split_authority(location, scheme_len),
        None => ("", location),
    };
    match uri_encode_path(path) {
        Cow::Borrowed(_) => Cow::Borrowed(location),
        Cow::Owned(encoded) => Cow::Owned(format!("{prefix}{encoded}")),
    }
}

/// Splits an absolute `location` whose scheme is `scheme_len` bytes long into its
/// `scheme:[//authority]` prefix and the raw path that follows. The authority, when present, ends
/// at the next `/`.
pub(crate) fn split_authority(location: &str, scheme_len: usize) -> (&str, &str) {
    let after_scheme = scheme_len + 1;
    let prefix_len = match location[after_scheme..].strip_prefix("//") {
        Some(rest) => after_scheme + 2 + rest.find('/').unwrap_or(rest.len()),
        None => after_scheme,
    };
    location.split_at(prefix_len)
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
    #[case::authority_kept(
        "abfss://c@a.dfs.core.windows.net/t/[x]",
        "abfss://c@a.dfs.core.windows.net/t/%5Bx%5D"
    )]
    #[case::authority_only("s3://bucket", "s3://bucket")]
    #[case::no_authority("C:/data dir/a", "C:/data%20dir/a")]
    #[case::non_ascii("data/M\u{fc}nchen", "data/M%C3%BCnchen")]
    fn test_uri_encode_location(#[case] location: &str, #[case] expected: &str) {
        assert_eq!(uri_encode_location(location), expected);
    }

    #[test]
    fn test_uri_encode_location_borrows_when_unchanged() {
        assert!(matches!(
            uri_encode_location("s3://b/a.parquet"),
            Cow::Borrowed(_)
        ));
    }
}
