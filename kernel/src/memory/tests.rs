use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rstest::rstest;

use super::*;
use crate::schema::{DataType, MetadataValue, StructField, StructType};
use crate::table_features::ColumnMappingMode;

#[test]
fn owning_iterator_drops_each_element_once_after_consumption_from_both_ends() {
    struct Item(Arc<AtomicUsize>);
    impl Drop for Item {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let budget = MemoryBudget::new(1024);
    let mut values = LimitedVec::new(&budget);
    for _ in 0..8 {
        values.try_push(Item(drops.clone())).unwrap();
    }
    let reserved = budget.usage().reserved;
    let mut iter = values.into_iter();
    drop(iter.next());
    drop(iter.next_back());
    assert_eq!(iter.len(), 6);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    assert_eq!(budget.usage().reserved, reserved);
    std::thread::spawn(move || drop(iter)).join().unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 8);
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn bounded_ordered_map_matches_indexmap_order_replacement_and_growth_failures() {
    let mut baseline = indexmap::IndexMap::new();
    let measured = MemoryBudget::new(usize::MAX);
    measured.with(|| {
        let mut actual = OrderedMap::new();
        for index in (0..300).map(|i| i * 17 % 151) {
            let key = format!("key-{index}");
            assert_eq!(
                actual.try_insert(key.clone(), index).unwrap(),
                baseline.insert(key, index)
            );
        }
        assert!(actual.iter().eq(baseline.iter()));
        assert!(actual.values().rev().eq(baseline.values().rev()));
        assert!(actual.keys().eq(baseline.keys()));
        for (index, key) in baseline.keys().enumerate() {
            assert_eq!(actual.get_index_of(key), Some(index));
            assert_eq!(actual.get_index(index), baseline.get_index(index));
        }
        let mut actual = actual.into_values();
        let mut expected = baseline.clone().into_values();
        assert_eq!(actual.next(), expected.next());
        assert_eq!(actual.next_back(), expected.next_back());
        assert!(actual.eq(expected));
    });
    let required = measured.usage().peak;
    assert_eq!(measured.usage().reserved, 0);
    for cap in (0..required).step_by(97).chain([required]) {
        let budget = MemoryBudget::new(cap);
        budget.with(|| {
            let mut actual = OrderedMap::new();
            for (key, value) in &baseline {
                let before: Vec<_> = actual.iter().map(|(k, v)| (k.clone(), *v)).collect();
                if actual.try_insert(key.clone(), *value).is_err() {
                    assert!(actual.iter().eq(before.iter().map(|(k, v)| (k, v))));
                    break;
                }
            }
        });
        assert!(budget.usage().peak <= cap);
        assert_eq!(budget.usage().reserved, 0);
    }
}

#[rstest]
#[case(ColumnMappingMode::None)]
#[case(ColumnMappingMode::Id)]
#[case(ColumnMappingMode::Name)]
fn bounded_physical_materialization_preserves_mapping_and_canonical_types(
    #[case] mode: ColumnMappingMode,
) {
    let annotate = |field: StructField, id: i64| {
        if mode == ColumnMappingMode::None {
            field
        } else {
            field.with_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(id)),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String(format!("physical_{id}")),
                ),
            ])
        }
    };
    let schema = StructType::try_new([
        annotate(
            StructField::nullable(
                "nested",
                StructType::try_new([
                    annotate(
                        StructField::nullable(
                            "array",
                            crate::schema::ArrayType::new(DataType::LONG, true),
                        ),
                        2,
                    ),
                    annotate(
                        StructField::nullable(
                            "map",
                            crate::schema::MapType::new(DataType::STRING, DataType::STRING, false),
                        ),
                        3,
                    ),
                ])
                .unwrap(),
            ),
            1,
        ),
        annotate(StructField::nullable("file", DataType::file_type()), 4),
        annotate(
            StructField::nullable("variant", DataType::unshredded_variant()),
            16,
        ),
    ])
    .unwrap();
    let expected = schema.make_physical(mode).unwrap();
    let budget = MemoryBudget::new(1024 * 1024);
    let actual = budget
        .with(|| schema.try_make_validated_physical(mode))
        .unwrap();
    assert_eq!(expected, actual);
    assert_eq!(
        serde_json::to_value(&expected).unwrap(),
        serde_json::to_value(&actual).unwrap()
    );
    assert!(budget.usage().reserved > 0);
    drop(actual);
    assert_eq!(budget.usage().reserved, 0);
    let required = budget.usage().peak;
    for cap in [0, 256, required - 1, required] {
        let budget = MemoryBudget::new(cap);
        let result = budget.with(|| schema.try_make_validated_physical(mode));
        assert_eq!(result.is_ok(), cap >= required, "mode={mode} cap={cap}");
        drop(result);
        assert_eq!(budget.usage().reserved, 0);
        assert!(budget.usage().peak <= cap);
    }
}

#[test]
fn boxed_payloads_preserve_alignment_and_zero_sized_iterator_drops() {
    #[repr(align(256))]
    struct Aligned([u8; 256]);
    let budget = MemoryBudget::new(256);
    let value = Owned::try_new_in(Aligned([7; 256]), budget.allocator()).unwrap();
    assert_eq!((&*value as *const Aligned) as usize % 256, 0);
    assert_eq!(value.0[0], 7);
    assert_eq!(budget.usage().reserved, 256);
    drop(value);
    assert_eq!(budget.usage().reserved, 0);

    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Empty;
    impl Drop for Empty {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }
    let zero = MemoryBudget::new(0);
    let mut values = LimitedVec::new(&zero);
    for _ in 0..7 {
        values.try_push(Empty).unwrap();
    }
    let mut iter = values.into_iter();
    drop(iter.next());
    drop(iter.next_back());
    drop(iter);
    assert_eq!(DROPS.load(Ordering::Relaxed), 7);
    assert_eq!(zero.usage().reserved, 0);
}

#[test]
fn shared_schema_text_and_boxed_nodes_release_on_the_last_owning_thread() {
    let budget = MemoryBudget::new(4096);
    let text = budget
        .with(|| SharedString::try_copy("schema payload"))
        .unwrap();
    let boxed = budget.with(|| Owned::try_new([0_u8; 256])).unwrap();
    let clone = text.clone();
    drop(text);
    assert_eq!(budget.usage().reserved, "schema payload".len() + 256);
    std::thread::spawn(move || drop((clone, boxed)))
        .join()
        .unwrap();
    assert_eq!(budget.usage().reserved, 0);
    let zero = MemoryBudget::new(0);
    assert!(zero.with(|| Owned::try_new(1)).is_err());
    assert!(zero.with(|| SharedString::try_copy("x")).is_err());
    assert_eq!(zero.usage().reserved, 0);
}
