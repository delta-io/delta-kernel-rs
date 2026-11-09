//! RFC 3986 URI-reference syntax: whether a raw reference is absolute (has a scheme) or
//! relative.

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
}
