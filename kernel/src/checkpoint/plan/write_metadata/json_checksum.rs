use itertools::Itertools as _;
use md5::{Digest, Md5};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::Value;

/// Computes DBR's JSON checksum, ignoring only the top-level `checksum` field.
pub(super) fn json_checksum(value: &Value) -> String {
    let mut leaves = Vec::new();
    collect_leaves(value, String::new(), &mut leaves);
    // DBR sorts complete entries: a scalar array's index 10 precedes index 1 (`10=` < `1=`).
    leaves.sort_unstable();
    let canonical = leaves.into_iter().join(",");
    format!("{:x}", Md5::digest(canonical.as_bytes()))
}

fn collect_leaves(value: &Value, path: String, leaves: &mut Vec<String>) {
    let separator = if path.is_empty() { "" } else { "+" };
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if path.is_empty() && key == "checksum" {
                    continue;
                }
                collect_leaves(value, format!("{path}{separator}{}", quoted(key)), leaves);
            }
        }
        Value::Array(array) => {
            for (index, value) in array.iter().enumerate() {
                collect_leaves(value, format!("{path}{separator}{index}"), leaves);
            }
        }
        _ => {
            let value = match value {
                Value::String(string) => quoted(string),
                _ => value.to_string(),
            };
            leaves.push(if path.is_empty() {
                value
            } else {
                format!("{path}={value}")
            });
        }
    }
}

fn quoted(string: &str) -> String {
    const ENCODE: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~');
    format!("\"{}\"", utf8_percent_encode(string, ENCODE))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[test]
    fn protocol_checksum_vector() {
        assert_eq!(
            json_checksum(&json!({
                "k0": "'v 0'", "checksum": "adsaskfljadfkjadfkj",
                "k1": {"k2": 2, "k3": ["v3", [1, 2], {
                    "k4": "v4", "k5": ["v5", "v6", "v7"]
                }]}
            })),
            "6a92d155a59bf2eecbd4b4ec7fd1f875"
        );
    }

    #[rstest]
    #[case::empty_object(json!({}), "")]
    #[case::empty_array(json!([]), "")]
    #[case::empty_containers_and_root_checksum(
        json!({"checksum": {"ignored": [1]}, "a": [], "b": {}}), ""
    )]
    #[case::nested_checksum(
        json!({"checksum": null, "Checksum": 3, "a": {"checksum": 1}, "b": [{"checksum": 2}]}),
        r#""Checksum"=3,"a"+"checksum"=1,"b"+0+"checksum"=2"#
    )]
    #[case::literals_and_numbers(
        json!({"a": null, "b": true, "c": false, "d": 42, "e": -7, "f": 2.5}),
        r#""a"=null,"b"=true,"c"=false,"d"=42,"e"=-7,"f"=2.5"#
    )]
    #[case::unicode_and_reserved_key_order(
        json!({"a": "AZaz09-._~", "é": "雪", "~": "\\\"\n", " ": "+/%?&=#", "+": " ", "/": "\""}),
        concat!(r#""%20"="%2B%2F%25%3F%26%3D%23","%2B"="%20","%2F"="%22","#,
            r#""%C3%A9"="%E9%9B%AA","a"="AZaz09-._~","~"="%5C%22%0A""#)
    )]
    #[case::dbr_scalar_array_order(
        json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        "0=0,10=10,1=1,2=2,3=3,4=4,5=5,6=6,7=7,8=8,9=9"
    )]
    #[case::empty_keys_and_delimiters(
        json!({"": {"": ""}, "a+b": 1, "a": {"b": 2}}),
        r#"""+""="","a"+"b"=2,"a%2Bb"=1"#
    )]
    #[case::root_string(json!("é /"), r#""%C3%A9%20%2F""#)]
    #[case::root_null(json!(null), "null")]
    fn checksum_canonicalizes_leaves(#[case] value: Value, #[case] canonical: &str) {
        assert_eq!(
            json_checksum(&value),
            format!("{:x}", Md5::digest(canonical.as_bytes()))
        );
    }
}
