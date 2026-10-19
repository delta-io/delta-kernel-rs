//! Connector-contract examples using a synthetic ASCII case-insensitive comparison, not ICU.

use std::cmp::Ordering;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch, StringArray};
use delta_kernel::expressions::{col, column_name, lit, Predicate};
use delta_kernel::schema::{schema_ref, DataType};
use rstest::rstest;
use serde_json::{json, Value};
use test_utils::{read_add_infos, read_scan, test_table_setup};

use super::{annotated_field, create_fixture, simple_batch, CollationStats, CollationTable};

const SYNTHETIC_ID: &str = "test.ASCII_CI.1";

#[derive(Clone, Copy, Debug)]
enum Operation {
    Equal,
    Greater,
}

impl Operation {
    fn matches(self, value: &str, literal: &str) -> bool {
        let ordering = value
            .to_ascii_lowercase()
            .cmp(&literal.to_ascii_lowercase());
        match self {
            Self::Equal => ordering == Ordering::Equal,
            Self::Greater => ordering == Ordering::Greater,
        }
    }

    fn may_match(self, stats: &Value, literal: &str) -> bool {
        let matching = &stats["statsWithCollation"][SYNTHETIC_ID];
        let Some(max) = matching["maxValues"]["name"].as_str() else {
            return true;
        };
        match self {
            Self::Equal => {
                let Some(min) = matching["minValues"]["name"].as_str() else {
                    return true;
                };
                min.to_ascii_lowercase() <= literal.to_ascii_lowercase()
                    && literal.to_ascii_lowercase() <= max.to_ascii_lowercase()
            }
            Self::Greater => self.matches(max, literal),
        }
    }
}

fn synthetic_fixture(stats_id: Option<&'static str>, operation: Operation) -> CollationTable {
    CollationTable {
        schema: schema_ref! {
            nullable "id": INTEGER,
            (annotated_field("name", DataType::STRING, "name", "test.ASCII_CI")),
        },
        files: [(1, "Alice"), (2, "Bob"), (3, "alice"), (4, "Zoe")]
            .into_iter()
            .map(|(id, name)| {
                (
                    simple_batch(vec![id], vec![name]),
                    stats_id.map(|identifier| {
                        let bound = if identifier == SYNTHETIC_ID {
                            name
                        } else {
                            match operation {
                                Operation::Equal => "zzz",
                                Operation::Greater => "aaa",
                            }
                        };
                        CollationStats {
                            identifier,
                            bounds: json!({
                                "minValues": { "name": bound },
                                "maxValues": { "name": bound },
                            }),
                        }
                    }),
                )
            })
            .collect(),
    }
}

fn rows(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        rows.extend(
            (0..batch.num_rows()).map(|row| (ids.value(row), names.value(row).to_string())),
        );
    }
    rows.sort();
    rows
}

#[test]
fn missing_required_minimum_retains_file() {
    let stats = json!({"statsWithCollation": {
        SYNTHETIC_ID: {"maxValues": {"name": "Alice"}}
    }});
    assert!(Operation::Equal.may_match(&stats, "Zoe"));
}

#[rstest]
#[case::matching(Some(SYNTHETIC_ID), true)]
#[case::missing(None, false)]
#[case::version_mismatch(Some("test.ASCII_CI.2"), false)]
#[case::provider_mismatch(Some("other.ASCII_CI.1"), false)]
#[case::name_mismatch(Some("test.ASCII_CS.1"), false)]
#[tokio::test]
async fn connector_uses_only_exact_operation_collation_stats(
    #[case] stats_id: Option<&'static str>,
    #[case] matching: bool,
    #[values(Operation::Equal, Operation::Greater)] operation: Operation,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    let fixture = synthetic_fixture(stats_id, operation);
    let snapshot = create_fixture(&path, engine.as_ref(), &fixture, &[]).await?;
    let adds = read_add_infos(&snapshot, engine.as_ref())?;
    let selected: Vec<_> = adds
        .iter()
        .filter_map(|add| {
            let stats = add.stats.as_ref().unwrap();
            operation
                .may_match(stats, "alice")
                .then(|| stats["minValues"]["id"].as_i64().unwrap() as i32)
        })
        .collect();
    assert_eq!(selected.len(), if matching { 2 } else { adds.len() });
    // The ID predicate encodes the connector's selected files for this one-row-per-file fixture.
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(Arc::new(Predicate::or_from(
            selected.iter().map(|id| col!("id").eq(lit(*id))),
        )))
        .build()?;
    let actual: Vec<_> = rows(&read_scan(&scan, engine.clone())?)
        .into_iter()
        .filter(|(_, name)| operation.matches(name, "alice"))
        .collect();
    let oracle: Vec<_> = rows(&read_scan(&snapshot.scan_builder().build()?, engine)?)
        .into_iter()
        .filter(|(_, name)| operation.matches(name, "alice"))
        .collect();
    assert_eq!(actual, oracle);
    Ok(())
}

#[derive(Clone, Debug)]
enum Filter {
    CaseInsensitiveEqual(&'static str),
    IdAtLeast(i32),
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
    Not(Box<Filter>),
}

impl Filter {
    fn matches(&self, id: i32, name: &str) -> bool {
        match self {
            Self::CaseInsensitiveEqual(literal) => Operation::Equal.matches(name, literal),
            Self::IdAtLeast(min) => id >= *min,
            Self::And(left, right) => left.matches(id, name) && right.matches(id, name),
            Self::Or(left, right) => left.matches(id, name) || right.matches(id, name),
            Self::Not(child) => !child.matches(id, name),
        }
    }

    fn conservative_predicate(&self, negated: bool) -> Predicate {
        match self {
            Self::CaseInsensitiveEqual(_) => Predicate::TRUE,
            Self::IdAtLeast(min) => {
                let predicate = col!("id").ge(lit(*min));
                if negated {
                    Predicate::not(predicate)
                } else {
                    predicate
                }
            }
            Self::Not(child) => child.conservative_predicate(!negated),
            Self::And(left, right) if !negated => Predicate::and(
                left.conservative_predicate(false),
                right.conservative_predicate(false),
            ),
            Self::Or(left, right) if negated => Predicate::and(
                left.conservative_predicate(true),
                right.conservative_predicate(true),
            ),
            Self::And(left, right) | Self::Or(left, right) => Predicate::or(
                left.conservative_predicate(negated),
                right.conservative_predicate(negated),
            ),
        }
    }
}

fn compound(and: bool) -> Filter {
    let left = Box::new(Filter::CaseInsensitiveEqual("alice"));
    let right = Box::new(Filter::IdAtLeast(3));
    if and {
        Filter::And(left, right)
    } else {
        Filter::Or(left, right)
    }
}

#[rstest]
#[case::comparison(Filter::CaseInsensitiveEqual("alice"), 4)]
#[case::and(compound(true), 2)]
#[case::or(compound(false), 4)]
#[case::not_comparison(Filter::Not(Box::new(Filter::CaseInsensitiveEqual("alice"))), 4)]
#[case::not_and(Filter::Not(Box::new(compound(true))), 4)]
#[case::not_or(Filter::Not(Box::new(compound(false))), 2)]
#[tokio::test]
async fn conservative_predicates_prune_files_without_losing_original_matches(
    #[case] original: Filter,
    #[case] expected_candidates: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    let fixture = synthetic_fixture(Some(SYNTHETIC_ID), Operation::Equal);
    let snapshot = create_fixture(&path, engine.as_ref(), &fixture, &[]).await?;
    let safe = original.conservative_predicate(false);
    assert!(!safe.references().contains(&column_name!("name")));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(Arc::new(safe))
        .build()?;
    let candidates = rows(&read_scan(&scan, engine.clone())?);
    assert_eq!(candidates.len(), expected_candidates);
    let actual: Vec<_> = candidates
        .iter()
        .filter(|(id, name)| original.matches(*id, name))
        .cloned()
        .collect();
    let oracle: Vec<_> = rows(&read_scan(&snapshot.scan_builder().build()?, engine)?)
        .into_iter()
        .filter(|(id, name)| original.matches(*id, name))
        .collect();
    assert_eq!(actual, oracle);
    Ok(())
}
