//! Derives the AMT `content_stats` a scan's data skipping needs from its predicate.
//!
//! The scan rewrites its predicate into a data skipping predicate over Delta stats
//! (`stats_parsed.minValues.<col>`, `stats_parsed.maxValues.<col>`, `stats_parsed.nullCount.<col>`,
//! `stats_parsed.numRecords`). The stats that predicate references are exactly the stats skipping
//! needs, so they decide which `content_stats` leaves a manifest read projects.

use crate::KernelResult;
use crate::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT, STATS_PARSED};
use crate::content_tree::stats::{content_stats_read_field, get_field_id, stats_schema};
use crate::content_tree::{LOWER_BOUND, NAN_VALUE_COUNT, NULL_VALUE_COUNT, UPPER_BOUND};
use crate::expressions::{ColumnName, Predicate};
use crate::schema::{StructField, StructType};

/// Builds the `content_stats` column a manifest read needs for `skipping_predicate`, a data
/// skipping predicate over the physical names of `physical_schema` (see
/// `StateInfo::data_skipping_predicate`). `physical_schema` is the physical table schema, carrying
/// the Parquet field IDs that `content_stats` is keyed by.
///
/// Each Delta stat the predicate references maps to the AMT stat backing it: `minValues` to
/// `lower_bound`, `maxValues` to `upper_bound`, and `nullCount` to `null_value_count`. A bound also
/// requests `nan_value_count`, because AMT bounds exclude NaN values; only floating point leaves
/// have it. `numRecords` comes from the entry's `recordCount`, so it needs no stats.
///
/// Returns `None` when the predicate references no stats an AMT manifest records, e.g. when it
/// only prunes by partition values or record counts.
pub(crate) fn content_stats_read_field_for_predicate(
    physical_schema: &StructType,
    skipping_predicate: &Predicate,
) -> KernelResult<Option<StructField>> {
    let mut requested = Vec::new();
    for reference in skipping_predicate.references() {
        let Some((column, stats)) = referenced_stats(reference) else {
            continue;
        };
        let Some(field_id) = get_field_id(physical_schema.field_at(&column)?) else {
            continue;
        };
        requested.extend(stats.iter().map(|stat| (field_id, *stat)));
    }
    if requested.is_empty() {
        return Ok(None);
    }
    Ok(content_stats_read_field(
        &stats_schema(physical_schema)?,
        requested,
    ))
}

/// The physical column and AMT stats behind a skipping predicate reference such as
/// `stats_parsed.maxValues.a.b`, or `None` for references that need no `content_stats`.
fn referenced_stats(reference: &ColumnName) -> Option<(ColumnName, &'static [&'static str])> {
    let [root, category, column @ ..] = reference.path() else {
        return None;
    };
    if root != STATS_PARSED || column.is_empty() {
        return None;
    }
    let stats: &[&str] = match category.as_str() {
        MIN_VALUES => &[LOWER_BOUND, NAN_VALUE_COUNT],
        MAX_VALUES => &[UPPER_BOUND, NAN_VALUE_COUNT],
        NULL_COUNT => &[NULL_VALUE_COUNT],
        _ => return None,
    };
    Some((ColumnName::new(column.iter().cloned()), stats))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rstest::rstest;

    use super::*;
    use crate::content_tree::CONTENT_STATS_FIELD_NAME;
    use crate::content_tree::stats::field_with_id;
    use crate::expressions::{col, column_name, lit};
    use crate::scan::data_skipping::as_sql_data_skipping_predicate_with_stats_columns;
    use crate::schema::DataType;

    /// Leaves `x` (long, ID 1), `d` (double, ID 2), `n.y` (long, ID 4), and `p` (long, ID 5).
    fn physical_schema() -> StructType {
        let nested = StructType::new_unchecked([field_with_id("y", DataType::LONG, true, 4)]);
        StructType::new_unchecked([
            field_with_id("x", DataType::LONG, true, 1),
            field_with_id("d", DataType::DOUBLE, true, 2),
            field_with_id("n", nested.into(), true, 3),
            field_with_id("p", DataType::LONG, true, 5),
        ])
    }

    /// The read field for `pred`, rewritten with `x`, `d`, and `n.y` as stats columns and `p` as
    /// a partition column.
    fn read_field_for(pred: Predicate) -> Option<StructField> {
        let partition_columns = HashSet::from([column_name!("p")]);
        let stats_columns =
            HashSet::from([column_name!("x"), column_name!("d"), column_name!("n.y")]);
        let skipping = as_sql_data_skipping_predicate_with_stats_columns(
            &pred,
            &partition_columns,
            &stats_columns,
        )
        .expect("predicate should be eligible for data skipping");
        content_stats_read_field_for_predicate(&physical_schema(), &skipping).unwrap()
    }

    /// The stats of leaf `name` with the given `(stat, offset)` stats, at the IDs of the leaf's
    /// stats base.
    fn leaf(name: &str, field_id: i32, data_type: DataType, stats: &[(&str, i32)]) -> StructField {
        let base = 10_000 + 200 * field_id;
        let stats = stats.iter().map(|(stat, offset)| {
            let data_type = match *stat {
                LOWER_BOUND | UPPER_BOUND => data_type.clone(),
                _ => DataType::LONG,
            };
            field_with_id(stat, data_type, true, base + offset)
        });
        field_with_id(name, StructType::new_unchecked(stats).into(), true, base)
    }

    const LOWER: (&str, i32) = (LOWER_BOUND, 1);
    const UPPER: (&str, i32) = (UPPER_BOUND, 2);
    const NULLS: (&str, i32) = (NULL_VALUE_COUNT, 4);
    const NANS: (&str, i32) = (NAN_VALUE_COUNT, 5);

    // Null-intolerant comparisons are guarded by `col IS NOT NULL`, which reads the null count.
    #[rstest]
    #[case::greater_than(col!("x").gt(lit(5i64)), vec![leaf("x", 1, DataType::LONG, &[UPPER, NULLS])])]
    #[case::less_than(col!("x").lt(lit(5i64)), vec![leaf("x", 1, DataType::LONG, &[LOWER, NULLS])])]
    #[case::equal(
        col!("x").eq(lit(5i64)),
        vec![leaf("x", 1, DataType::LONG, &[LOWER, UPPER, NULLS])]
    )]
    #[case::is_null(col!("x").is_null(), vec![leaf("x", 1, DataType::LONG, &[NULLS])])]
    #[case::double_bound_reads_nan_count(
        col!("d").gt(lit(5.0)),
        vec![leaf("d", 2, DataType::DOUBLE, &[UPPER, NULLS, NANS])]
    )]
    #[case::nested(col!("n.y").lt(lit(5i64)), vec![leaf("n.y", 4, DataType::LONG, &[LOWER, NULLS])])]
    #[case::disjunction(
        Predicate::or(col!("x").gt(lit(5i64)), col!("n.y").is_null()),
        vec![
            leaf("x", 1, DataType::LONG, &[UPPER, NULLS]),
            leaf("n.y", 4, DataType::LONG, &[NULLS]),
        ]
    )]
    #[case::partition_conjunct_ignored(
        Predicate::and(col!("x").gt(lit(5i64)), col!("p").eq(lit(1i64))),
        vec![leaf("x", 1, DataType::LONG, &[UPPER, NULLS])]
    )]
    fn read_field_reads_only_the_stats_the_predicate_references(
        #[case] pred: Predicate,
        #[case] leaves: Vec<StructField>,
    ) {
        let read_field = read_field_for(pred).unwrap();
        let content_stats = StructType::new_unchecked(leaves);
        let expected = field_with_id(CONTENT_STATS_FIELD_NAME, content_stats.into(), true, 146);
        assert_eq!(read_field, expected);
    }

    #[test]
    fn read_field_is_none_for_partition_only_predicate() {
        assert_eq!(read_field_for(col!("p").eq(lit(1i64))), None);
    }

    #[test]
    fn read_field_is_none_without_stats_references() {
        let pred = col!("stats_parsed.numRecords").gt(lit(0i64));
        let read_field = content_stats_read_field_for_predicate(&physical_schema(), &pred).unwrap();
        assert_eq!(read_field, None);
    }

    #[test]
    fn read_field_skips_leaf_without_stats_field_id() {
        let schema = StructType::new_unchecked([field_with_id("x", DataType::LONG, true, -1)]);
        let pred = col!("stats_parsed.maxValues.x").gt(lit(5i64));
        let read_field = content_stats_read_field_for_predicate(&schema, &pred).unwrap();
        assert_eq!(read_field, None);
    }

    #[test]
    fn read_field_errors_on_reference_missing_from_schema() {
        let pred = col!("stats_parsed.maxValues.missing").gt(lit(5i64));
        assert!(content_stats_read_field_for_predicate(&physical_schema(), &pred).is_err());
    }
}
