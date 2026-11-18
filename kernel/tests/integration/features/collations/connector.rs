//! Collation-aware file skipping and predicate pushdown using synthetic ASCII comparisons.
//!
//! Covers exact provider/name/version matching, missing-bound fallback and conservative AND/OR/NOT
//! pushdown followed by the original row filter, per the [collation RFC]'s reader requirements.
//! Skipping is exercised both in the connector, from returned statistics, and in Kernel, from a
//! collated comparison that the engine evaluates.
//!
//! [collation RFC]: https://github.com/delta-io/delta/blob/master/protocol_rfcs/collated-string-type.md

use std::cmp::Ordering;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch, StringArray};
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::engine::DelegatingEngine;
use delta_kernel::engine::arrow_expression::collation::{
    AsciiCaseInsensitive, CollationEvaluationHandler,
};
use delta_kernel::expressions::{Predicate, col, collated_comparison, column_name, lit};
use delta_kernel::scan::StatsOptions;
use delta_kernel::schema::{DataType, schema_ref};
use delta_kernel::{Engine, Snapshot};
use rstest::rstest;
use serde_json::{Value, json};
use test_utils::{
    assert_result_error_with_message, read_add_infos, read_scan, test_table_setup,
    test_table_setup_mt,
};

use super::{
    CollationStats, CollationTable, annotated_field, create_fixture, generated_table, simple_batch,
};
use crate::common::read_utils::collect_scan_paths;

const SYNTHETIC_ID: &str = "test.ASCII_CI.1";

/// Wraps `engine` so that it evaluates comparisons under `collation` ASCII case-insensitively.
fn with_collator(engine: Arc<dyn Engine>, collation: &'static str) -> Arc<dyn Engine> {
    let handler = CollationEvaluationHandler::new(Arc::new(AsciiCaseInsensitive(collation)));
    Arc::new(DelegatingEngine::new(engine).with_evaluation_handler(Arc::new(handler)))
}

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

/// Retains files when required bounds are absent (RFC: "Reader Requirements for Collations").
#[test]
fn missing_required_minimum_retains_file() {
    let stats = json!({"statsWithCollation": {
        SYNTHETIC_ID: {"maxValues": {"name": "Alice"}}
    }});
    assert!(Operation::Equal.may_match(&stats, "Zoe"));
}

/// Skips only with the operation's exact collation (RFC: "Reader Requirements for Collations").
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

    /// The filter as a Kernel predicate with the same structure, compared under [`SYNTHETIC_ID`].
    fn kernel_predicate(&self) -> Predicate {
        match self {
            Self::CaseInsensitiveEqual(literal) => {
                collated_comparison(SYNTHETIC_ID, col!("name").eq(lit(*literal)))
            }
            Self::IdAtLeast(min) => col!("id").ge(lit(*min)),
            Self::And(left, right) => {
                Predicate::and(left.kernel_predicate(), right.kernel_predicate())
            }
            Self::Or(left, right) => {
                Predicate::or(left.kernel_predicate(), right.kernel_predicate())
            }
            Self::Not(child) => Predicate::not(child.kernel_predicate()),
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

/// Preserves matches through AND/OR/NOT pushdown (RFC: "Reader Requirements for Collations").
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

/// Skips only with the predicate's exact collation (RFC: "Reader Requirements for Collations").
#[rstest]
#[case::commits("none", None, true, SYNTHETIC_ID)]
#[case::json_stats_v1_checkpoint("none", Some(CheckpointSpec::V1), false, SYNTHETIC_ID)]
#[case::name_mapped_v1_checkpoint("name", Some(CheckpointSpec::V1), true, SYNTHETIC_ID)]
#[case::id_mapped_v2_sidecars(
    "id",
    Some(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(1),
    })),
    true,
    SYNTHETIC_ID
)]
#[case::bounds_under_another_version("none", None, true, "test.ASCII_CI.2")]
#[case::checkpoint_bounds_under_another_version(
    "none",
    Some(CheckpointSpec::V1),
    true,
    "test.ASCII_CI.2"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collated_predicates_prune_files_without_losing_matches(
    #[case] mapping: &str,
    #[case] checkpoint: Option<CheckpointSpec>,
    #[case] struct_stats: bool,
    #[case] stats_id: &'static str,
    #[values(StatsOptions::json_only(), StatsOptions::all_struct())] stats: StatsOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let mut fixture = synthetic_fixture(Some(stats_id), Operation::Equal);
    // A file without collation bounds is kept by every collated comparison. It is the oldest
    // file, because a checkpoint samples the collations of its struct stats from the newest.
    fixture
        .files
        .insert(0, (simple_batch(vec![8], vec!["ALICE"]), None));
    // Byte order sorts "carol" last, so this file's binary bounds exclude "Dave".
    fixture.files.push((
        simple_batch(vec![5, 6, 7], vec!["Carol", "carol", "Dave"]),
        Some(CollationStats {
            identifier: stats_id,
            bounds: json!({
                "minValues": { "name": "Carol" },
                "maxValues": { "name": "Dave" },
            }),
        }),
    ));
    let (as_struct, as_json) = if struct_stats {
        ("true", "false")
    } else {
        ("false", "true")
    };
    let mut properties = vec![
        ("delta.columnMapping.mode", mapping),
        ("delta.checkpoint.writeStatsAsStruct", as_struct),
        ("delta.checkpoint.writeStatsAsJson", as_json),
    ];
    if matches!(checkpoint, Some(CheckpointSpec::V2(_))) {
        properties.push(("delta.feature.v2Checkpoint", "supported"));
    }
    let snapshot = create_fixture(&path, engine.as_ref(), &fixture, &properties).await?;
    if let Some(checkpoint) = &checkpoint {
        snapshot.checkpoint(engine.as_ref(), Some(checkpoint))?;
    }
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let engine = with_collator(engine, SYNTHETIC_ID);
    let all_rows = rows(&read_scan(
        &snapshot.clone().scan_builder().build()?,
        engine.clone(),
    )?);
    let candidate_rows = |predicate: Predicate| -> Result<_, Box<dyn std::error::Error>> {
        let scan = snapshot
            .clone()
            .scan_builder()
            .with_stats(stats.clone())
            .with_predicate(Arc::new(predicate))
            .build()?;
        Ok(rows(&read_scan(&scan, engine.clone())?))
    };
    let not = |filter| Filter::Not(Box::new(filter));
    for (original, kept_rows) in [
        (Filter::CaseInsensitiveEqual("alice"), 3),
        (Filter::CaseInsensitiveEqual("dave"), 4),
        (compound(true), 2),
        (compound(false), 7),
        (not(Filter::CaseInsensitiveEqual("alice")), 6),
        (not(compound(true)), 7),
        (not(compound(false)), 1),
    ] {
        let candidates = candidate_rows(original.kernel_predicate())?;
        // With bounds under another identifier, only the binary `id` comparison can skip.
        let expected_candidates = if stats_id == SYNTHETIC_ID {
            kept_rows
        } else {
            candidate_rows(original.conservative_predicate(false))?.len()
        };
        assert_eq!(candidates.len(), expected_candidates, "{original:?}");
        let matching = |rows: &[(i32, String)]| -> Vec<_> {
            rows.iter()
                .filter(|(id, name)| original.matches(*id, name))
                .cloned()
                .collect()
        };
        assert_eq!(matching(&candidates), matching(&all_rows), "{original:?}");
    }
    Ok(())
}

/// Only the first file matches `charlie` ignoring case, and its binary bounds, which end at
/// `Charlie`, would skip it.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collated_predicate_on_a_nested_column_prunes_files_by_its_nested_bounds(
    #[values("none", "name", "id")] mapping: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    const LCASE: &str = "spark.UTF8_LCASE.75.1";
    let (_temp, path, engine) = test_table_setup_mt()?;
    let fixture = generated_table(true, &["first", "last", "email"]);
    let properties = [("delta.columnMapping.mode", mapping)];
    let snapshot = create_fixture(&path, engine.as_ref(), &fixture, &properties).await?;
    let predicate = collated_comparison(LCASE, col!("name.first").eq(lit("charlie")));
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;
    let batches = read_scan(&scan, with_collator(engine, LCASE))?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collated_predicates_prune_sidecar_files_in_sequential_and_parallel_scan_metadata(
    #[values(false, true)] parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let fixture = synthetic_fixture(Some(SYNTHETIC_ID), Operation::Equal);
    let properties = [("delta.feature.v2Checkpoint", "supported")];
    let snapshot = create_fixture(&path, engine.as_ref(), &fixture, &properties).await?;
    let sidecars = CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(1),
    });
    snapshot.checkpoint(engine.as_ref(), Some(&sidecars))?;
    let scan = Snapshot::builder_for(&path)
        .build(engine.as_ref())?
        .scan_builder()
        .with_predicate(Arc::new(
            Filter::CaseInsensitiveEqual("alice").kernel_predicate(),
        ))
        .build()?;
    let kept = collect_scan_paths(&scan, with_collator(engine, SYNTHETIC_ID), parallel)?;
    assert_eq!(kept.len(), 2);
    Ok(())
}

#[rstest]
#[case::no_collator(None, "Unsupported opaque predicate")]
#[case::collator_rejects_the_collation(Some("test.ASCII_CI.2"), "Unknown collation")]
#[tokio::test]
async fn collated_predicate_fails_the_scan_when_the_engine_cannot_evaluate_it(
    #[case] collation: Option<&'static str>,
    #[case] message: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    let fixture = synthetic_fixture(Some(SYNTHETIC_ID), Operation::Equal);
    let snapshot = create_fixture(&path, engine.as_ref(), &fixture, &[]).await?;
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(
            Filter::CaseInsensitiveEqual("alice").kernel_predicate(),
        ))
        .build()?;
    let engine: Arc<dyn Engine> = match collation {
        Some(collation) => with_collator(engine, collation),
        None => engine,
    };
    assert_result_error_with_message(read_scan(&scan, engine), message);
    Ok(())
}
