use std::collections::HashSet;

use delta_kernel::schema::{ArrayType, DataType, MapType, MetadataValue, StructField, StructType};
use delta_kernel_limited_memory_poc::{
    ActionBuffer, FileKeySet, LimitedBytes, LimitedSchema, LogTail, MemoryBudget, MemoryError,
    try_to_json,
};
use serde::Serialize;
use serde_json::json;

fn nested_schema() -> StructType {
    let mut field = StructField::nullable("escaped\"名", DataType::STRING);
    field.metadata.insert(
        "comment".into(),
        MetadataValue::String("payload".repeat(100)),
    );
    field.metadata.insert(
        "custom".into(),
        MetadataValue::Other(json!({"array": [1, true, null]})),
    );
    StructType::try_new([
        field,
        StructField::not_null(
            "nested",
            StructType::try_new([
                StructField::nullable("array", ArrayType::new(DataType::LONG, false)),
                StructField::nullable(
                    "map",
                    MapType::new(DataType::STRING, DataType::INTEGER, true),
                ),
            ])
            .unwrap(),
        ),
        StructField::nullable("amount", DataType::decimal(20, 3).unwrap()),
        StructField::nullable("variant", DataType::unshredded_variant()),
    ])
    .unwrap()
}

#[test]
fn nested_schema_and_projection_round_trip_through_the_real_kernel_parser() {
    let schema = nested_schema();
    let budget = MemoryBudget::new(1024 * 1024);
    let copied = LimitedSchema::try_from_kernel(&budget, &schema).unwrap();
    assert_eq!(copied.field_count(), 4);
    assert_eq!(copied.field_name(0), Some("escaped\"名"));
    assert_eq!(copied.field_name(100), None);
    let output = copied.try_to_json().unwrap();
    assert_eq!(
        serde_json::from_slice::<StructType>(&output).unwrap(),
        schema
    );
    let projection = copied.try_project(&[2, 1]).unwrap();
    let projected: StructType = serde_json::from_slice(&projection.try_to_json().unwrap()).unwrap();
    assert_eq!(
        projected
            .fields()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["amount", "nested"]
    );
    assert_eq!(projected.field("nested"), schema.field("nested"));
    assert!(matches!(
        copied.try_project(&[0, 0]),
        Err(MemoryError::InvalidProjection)
    ));
    assert!(matches!(
        copied.try_project(&[4]),
        Err(MemoryError::InvalidProjection)
    ));
    drop((copied, projection, output));
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn schema_exhaustion_releases_every_partially_constructed_node() {
    let source = nested_schema();
    let baseline = MemoryBudget::new(usize::MAX);
    drop(LimitedSchema::try_from_kernel(&baseline, &source).unwrap());
    let required = baseline.usage().peak;
    for cap in (0..required).step_by(17).chain([required - 1, required]) {
        let budget = MemoryBudget::new(cap);
        let copied = LimitedSchema::try_from_kernel(&budget, &source);
        assert_eq!(copied.is_ok(), cap >= required, "cap={cap}");
        drop(copied);
        assert_eq!(budget.usage().reserved, 0, "cap={cap}");
        assert!(budget.usage().peak <= cap);
    }
}

#[test]
fn schema_serialization_and_projection_failure_preserve_the_source() {
    let source = nested_schema();
    let baseline = MemoryBudget::new(usize::MAX);
    let copied = LimitedSchema::try_from_kernel(&baseline, &source).unwrap();
    let used = baseline.usage().reserved;
    let cap = baseline.usage().peak.max(used + 1);
    drop(copied);
    let budget = MemoryBudget::new(cap);
    let copied = LimitedSchema::try_from_kernel(&budget, &source).unwrap();
    assert!(copied.try_to_json().is_err());
    assert!(copied.try_project(&[0, 1, 2, 3]).is_err());
    assert_eq!(budget.usage().reserved, used);
    assert_eq!(copied.field_name(0), Some("escaped\"名"));
    drop(copied);
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn wide_schema_and_ten_thousand_tail_paths_share_the_cap() {
    let source = StructType::try_new((0..10_000).map(|i| {
        StructField::nullable(
            format!("col_{i}"),
            StructType::try_new([
                StructField::nullable("x", DataType::LONG),
                StructField::nullable("y", DataType::STRING),
            ])
            .unwrap(),
        )
    }))
    .unwrap();
    let budget = MemoryBudget::new(32 * 1024 * 1024);
    let copied = LimitedSchema::try_from_kernel(&budget, &source).unwrap();
    assert_eq!(copied.field_count(), 10_000);
    let mut tail = LogTail::new(&budget);
    for version in 1..=10_000 {
        tail.try_push(
            version,
            128,
            &format!("s3://bucket/table/_delta_log/{version:020}.json"),
        )
        .unwrap();
    }
    assert_eq!(tail.files().len(), 10_000);
    assert_eq!(tail.files()[9999].version, 10_000);
    assert!(budget.usage().peak <= budget.usage().limit);
    drop(copied);
    assert!(budget.usage().reserved > 0);
    drop(tail);
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn log_tail_rejection_preserves_prior_entries_and_releases_partial_paths() {
    let budget = MemoryBudget::new(4096);
    let mut tail = LogTail::new(&budget);
    tail.try_push(10, 4, "first").unwrap();
    let used = budget.usage().reserved;
    assert_eq!(
        tail.try_push(12, 4, "invalid"),
        Err(MemoryError::InvalidLogOrder)
    );
    assert_eq!(
        tail.try_push(10, 4, "duplicate"),
        Err(MemoryError::InvalidLogOrder)
    );
    assert!(tail.try_push(11, 4, &"x".repeat(4096)).is_err());
    assert_eq!(tail.files().len(), 1);
    assert_eq!(tail.files()[0].location.as_str(), "first");
    assert_eq!(budget.usage().reserved, used);
    drop(tail);
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn file_keys_match_hashset_and_duplicates_allocate_nothing() {
    let budget = MemoryBudget::new(2 * 1024 * 1024);
    let mut limited = FileKeySet::new(&budget);
    let mut expected = HashSet::new();
    for i in 0..5000 {
        let path = format!("file-{}", i % 500);
        let dv = (i % 3 == 0).then(|| format!("dv-{}", i % 7));
        assert_eq!(
            limited.try_insert(&path, dv.as_deref()).unwrap(),
            expected.insert(delta_kernel::log_replay::FileActionKey::new(
                path.clone(),
                dv.clone()
            ))
        );
        assert!(limited.contains(&path, dv.as_deref()));
        let before = budget.usage();
        assert!(!limited.try_insert(&path, dv.as_deref()).unwrap());
        assert_eq!(budget.usage(), before);
    }
    assert_eq!(limited.len(), expected.len());
    drop(limited);
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn failed_rehash_or_key_copy_preserves_existing_keys() {
    for cap in (1..3000).step_by(11) {
        let budget = MemoryBudget::new(cap);
        let mut keys = FileKeySet::new(&budget);
        let mut expected = 0;
        for i in 0..30 {
            let path = format!("file-{i}");
            let used = budget.usage().reserved;
            match keys.try_insert(&path, Some("dv")) {
                Ok(true) => expected += 1,
                Ok(false) => panic!("unique key was already present"),
                Err(_) => {
                    assert!(!keys.contains(&path, Some("dv")));
                    assert_eq!(budget.usage().reserved, used);
                    break;
                }
            }
        }
        assert_eq!(keys.len(), expected);
        for i in 0..expected {
            assert!(keys.contains(&format!("file-{i}"), Some("dv")));
        }
        drop(keys);
        assert_eq!(budget.usage().reserved, 0);
        assert!(budget.usage().peak <= cap);
    }
}

#[test]
fn staged_actions_and_commit_output_are_both_charged_and_match_kernel_json() {
    #[derive(Serialize)]
    struct Envelope<'a> {
        txn: &'a delta_kernel::actions::SetTransaction,
    }
    let txn = delta_kernel::actions::SetTransaction::new("writer".into(), 7, Some(42));
    let action = Envelope { txn: &txn };
    let budget = MemoryBudget::new(4096);
    let mut staged = ActionBuffer::new(&budget);
    staged.try_stage(&action).unwrap();
    assert_eq!(
        staged.actions().next().unwrap(),
        serde_json::to_vec(&action).unwrap()
    );
    let staged_used = budget.usage().reserved;
    let output = staged.try_encode_commit().unwrap();
    assert_eq!(budget.usage().reserved, staged_used + output.len());
    assert_eq!(output.last(), Some(&b'\n'));
    drop(staged);
    assert_eq!(budget.usage().reserved, output.len());
    drop(output);
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn failed_action_serialization_and_outer_staging_preserve_prior_actions() {
    let first = json!({"txn": {"appId": "writer", "version": 1}});
    for cap in (0..2048).step_by(7) {
        let budget = MemoryBudget::new(cap);
        let mut staged = ActionBuffer::new(&budget);
        let mut expected = 0;
        for _ in 0..8 {
            let used = budget.usage().reserved;
            if staged.try_stage(&first).is_err() {
                assert_eq!(budget.usage().reserved, used);
                break;
            }
            expected += 1;
        }
        assert_eq!(staged.len(), expected);
        assert!(
            staged
                .actions()
                .all(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).unwrap() == first)
        );
        drop(staged);
        assert_eq!(budget.usage().reserved, 0);
        assert!(budget.usage().peak <= cap);
    }
    let budget = MemoryBudget::new(0);
    assert!(matches!(
        try_to_json(&budget, &first),
        Err(MemoryError::AllocationFailed { limit: 0 })
    ));
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn one_budget_covers_overlapping_snapshot_scan_and_transaction_state() {
    let budget = MemoryBudget::new(64 * 1024);
    let schema = LimitedSchema::try_from_kernel(&budget, &nested_schema()).unwrap();
    let mut tail = LogTail::new(&budget);
    tail.try_push(1, 128, "_delta_log/00000000000000000001.json")
        .unwrap();
    let mut keys = FileKeySet::new(&budget);
    keys.try_insert("file.parquet", None).unwrap();
    let mut transaction = ActionBuffer::new(&budget);
    transaction
        .try_stage(&json!({"remove": {"path": "file.parquet", "dataChange": true}}))
        .unwrap();
    let used = budget.usage().reserved;
    let filler = LimitedBytes::try_copy(&budget, &vec![0; budget.usage().limit - used]).unwrap();
    assert!(schema.try_to_json().is_err());
    assert!(tail.try_push(2, 128, "next").is_err());
    assert!(keys.try_insert("next.parquet", None).is_err());
    assert!(
        transaction
            .try_stage(&json!({"txn": {"appId": "writer", "version": 1}}))
            .is_err()
    );
    assert_eq!(budget.usage().reserved, budget.usage().limit);
    drop(filler);
    transaction
        .try_stage(&json!({"txn": {"appId": "writer", "version": 1}}))
        .unwrap();
    assert_eq!(tail.files().len(), 1);
    assert_eq!(keys.len(), 1);
    assert_eq!(schema.field_count(), 4);
    drop((schema, tail, keys, transaction));
    assert_eq!(budget.usage().reserved, 0);
}
