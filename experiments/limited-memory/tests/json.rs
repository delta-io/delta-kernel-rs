use delta_kernel_limited_memory_poc::{
    ActionBuffer, MemoryBudget, MemoryError, try_parse_json, try_to_json,
};
use serde::{Serialize, Serializer};
use serde_json::json;

#[test]
fn seeded_parser_round_trips_nested_values_escapes_and_numeric_boundaries() {
    for input in [
        r#"{"type":"struct","fields":[{"name":"quoted\"\u540d\ud83d\ude00","type":"long","nullable":true,"metadata":{"comment":"line\nend"}}]}"#,
        r#"[null,true,false,0,-1,18446744073709551615,-9223372036854775808,1.25,1e20,[],{}]"#,
        r#"{"duplicate":1,"duplicate":2}"#,
        r#""escaped\t\\\/\b\f\r""#,
    ] {
        let budget = MemoryBudget::new(1024 * 1024);
        let parsed = try_parse_json(&budget, input).unwrap();
        let output = try_to_json(&budget, &parsed).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output).unwrap(),
            serde_json::from_str::<serde_json::Value>(input).unwrap()
        );
        drop((parsed, output));
        assert_eq!(budget.usage().reserved, 0);
    }
}

#[test]
fn parser_budget_exhaustion_and_invalid_input_release_partial_trees() {
    let input = r#"{"schema":{"fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]},"txn":{"appId":"writer","version":3}}"#;
    let baseline = MemoryBudget::new(usize::MAX);
    drop(try_parse_json(&baseline, input).unwrap());
    let required = baseline.usage().peak;
    for cap in (0..required).step_by(13).chain([required - 1, required]) {
        let budget = MemoryBudget::new(cap);
        let parsed = try_parse_json(&budget, input);
        assert_eq!(parsed.is_ok(), cap >= required, "cap={cap}");
        drop(parsed);
        assert_eq!(budget.usage().reserved, 0);
        assert!(budget.usage().peak <= cap);
    }
    for input in [
        "{",
        "[1,]",
        r#"{"a":[1,2,}"#,
        r#""\ud800""#,
        "01",
        "{} false",
    ] {
        let budget = MemoryBudget::new(65536);
        assert!(matches!(
            try_parse_json(&budget, input),
            Err(MemoryError::InvalidJson)
        ));
        assert_eq!(budget.usage().reserved, 0);
    }
    let budget = MemoryBudget::new(0);
    assert!(try_parse_json(&budget, "{}").is_ok());
    assert!(try_parse_json(&budget, "[1]").is_err());
}

#[test]
fn invalid_json_and_serialization_errors_are_distinct_from_exhaustion() {
    struct Reject;
    impl Serialize for Reject {
        fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("source serialization rejected"))
        }
    }
    let budget = MemoryBudget::new(4096);
    let mut actions = ActionBuffer::new(&budget);
    actions
        .try_stage(&json!({"txn": {"appId": "ok", "version": 1}}))
        .unwrap();
    let used = budget.usage().reserved;
    assert_eq!(actions.try_stage(&Reject), Err(MemoryError::Serialization));
    assert_eq!(actions.len(), 1);
    assert_eq!(budget.usage().reserved, used);
    drop(actions);
    assert_eq!(budget.usage().reserved, 0);
}
