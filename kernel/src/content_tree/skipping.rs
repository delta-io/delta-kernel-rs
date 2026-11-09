//! Skips AMT leaf manifests whose `content_stats` prove that no row matches a scan predicate.
//!
//! A root `DataManifest` entry's `content_stats` aggregate the stats of its leaf's data files, so a
//! leaf is judged like a single data file: its stats are mapped onto the Delta stats shape
//! (`minValues`/`maxValues`/`nullCount`) and evaluated by kernel's file-level
//! [`DataSkippingFilter`].

use std::collections::HashSet;
use std::sync::Arc;

use crate::Engine;
use crate::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT};
use crate::content_tree::stats::content_stats_leaf_name;
use crate::content_tree::{
    CONTENT_STATS_FIELD_NAME, LOWER_BOUND, NAN_VALUE_COUNT, NULL_VALUE_COUNT, UPPER_BOUND,
};
use crate::expressions::{ColumnName, Expression, Predicate, PredicateRef, lit, null_lit};
use crate::scan::data_skipping::DataSkippingFilter;
use crate::schema::{DataType, PrimitiveType, SchemaRef, StructType};

/// Builds a filter over root manifest batches (schema `input_schema`) that deselects the entries
/// whose `content_stats` prove that no row matches `predicate`, a predicate over physical column
/// names.
///
/// `stats_schema` is the physical Delta stats schema the scan reads and `content_stats` the
/// `content_stats` read schema projected from it. Each Delta stat is taken from the AMT stat
/// backing it: `minValues` from `lower_bound`, `maxValues` from `upper_bound`, and `nullCount` from
/// `null_value_count`. Any other stat, and any stat the entry does not record at the Delta stat's
/// type, is unknown, which keeps the entry. Returns `None` (keep every entry) when the predicate is
/// absent or ineligible for data skipping.
pub(crate) fn leaf_skipping_filter(
    engine: &dyn Engine,
    predicate: Option<PredicateRef>,
    stats_schema: &StructType,
    content_stats: &StructType,
    input_schema: SchemaRef,
) -> Option<DataSkippingFilter> {
    let stats_expr = delta_stats_expr(stats_schema, content_stats);
    let stats_columns = stats_columns(stats_schema);
    // AMT checkpoints are unpartitioned (#3320), so there are no partition values to extract, and
    // the filter's verdict is consulted only for `DataManifest` entries, so every row counts as an
    // add.
    DataSkippingFilter::new(
        engine,
        predicate,
        Some(&Arc::new(stats_schema.clone())),
        Arc::new(stats_expr),
        None,
        Arc::new(lit(true)),
        Arc::new(lit(true)),
        input_schema,
        &stats_columns,
        None,
    )
}

/// An expression producing a `stats_schema` value from a root manifest entry's `content_stats`.
fn delta_stats_expr(stats_schema: &StructType, content_stats: &StructType) -> Expression {
    let fields = stats_schema.fields().map(|field| {
        let stat = match field.name().as_str() {
            NULL_COUNT => NULL_VALUE_COUNT,
            MIN_VALUES => LOWER_BOUND,
            MAX_VALUES => UPPER_BOUND,
            _ => return null_lit(field.data_type().clone()),
        };
        match field.data_type() {
            DataType::Struct(category) => {
                category_expr(category, &mut Vec::new(), stat, content_stats)
            }
            other => null_lit(other.clone()),
        }
    });
    Expression::struct_from(fields)
}

/// An expression producing a value of `category` (a Delta stat category nested like the table,
/// at `path`), taking each leaf's value from its `stat` in `content_stats`.
fn category_expr(
    category: &StructType,
    path: &mut Vec<String>,
    stat: &str,
    content_stats: &StructType,
) -> Expression {
    let fields: Vec<_> = category
        .fields()
        .map(|field| {
            path.push(field.name().clone());
            let expr = match field.data_type() {
                DataType::Struct(nested) => category_expr(nested, path, stat, content_stats),
                leaf_type => leaf_stat_expr(path, stat, leaf_type, content_stats),
            };
            path.pop();
            expr
        })
        .collect();
    Expression::struct_from(fields)
}

/// The `stat` of the leaf at `path` as a `leaf_type` value, or a typed null when `content_stats`
/// does not record it at that type.
///
/// AMT bounds exclude NaN, while engines evaluate comparisons in a total order that places NaN
/// outside every bound (Arrow's `total_cmp` puts NaN above and negative NaN below every other
/// value). Either bound could then exclude a matching row, so a floating point bound is used only
/// when its `nan_value_count` proves the leaf holds no NaN.
fn leaf_stat_expr(
    path: &[String],
    stat: &str,
    leaf_type: &DataType,
    content_stats: &StructType,
) -> Expression {
    let name = content_stats_leaf_name(path);
    let unknown = || null_lit(leaf_type.clone());
    let Some(DataType::Struct(leaf_stats)) = content_stats.field(&name).map(|f| f.data_type())
    else {
        return unknown();
    };
    let recorded = |stat: &str, data_type: &DataType| {
        leaf_stats
            .field(stat)
            .is_some_and(|f| f.data_type() == data_type)
    };
    if !recorded(stat, leaf_type) {
        return unknown();
    }
    let column = |stat: &str| Expression::column([CONTENT_STATS_FIELD_NAME, name.as_str(), stat]);
    let is_floating = matches!(
        leaf_type,
        DataType::Primitive(PrimitiveType::Float | PrimitiveType::Double)
    );
    if stat == NULL_VALUE_COUNT || !is_floating {
        return column(stat);
    }
    if !recorded(NAN_VALUE_COUNT, &DataType::LONG) {
        return unknown();
    }
    let no_nans = Predicate::eq(column(NAN_VALUE_COUNT), lit(0i64));
    Expression::case_when([(no_nans, column(stat))], unknown())
}

/// The physical leaf paths that `stats_schema` carries stats for.
fn stats_columns(stats_schema: &StructType) -> HashSet<ColumnName> {
    fn collect(category: &StructType, path: &mut Vec<String>, out: &mut HashSet<ColumnName>) {
        for field in category.fields() {
            path.push(field.name().clone());
            match field.data_type() {
                DataType::Struct(nested) => collect(nested, path, out),
                _ => {
                    out.insert(ColumnName::new(path.iter()));
                }
            }
            path.pop();
        }
    }
    let mut columns = HashSet::new();
    for category in [NULL_COUNT, MIN_VALUES, MAX_VALUES] {
        if let Some(DataType::Struct(category)) =
            stats_schema.field(category).map(|f| f.data_type())
        {
            collect(category, &mut Vec::new(), &mut columns);
        }
    }
    columns
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::actions::NUM_RECORDS;
    use crate::content_tree::stats::{field_with_id, projected_stats_schema};
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{Scalar, StructData, col};
    use crate::schema::StructField;

    /// A physical table whose only column `c` (field ID 1) has type `data_type`.
    fn flat_table(data_type: DataType) -> StructType {
        StructType::new_unchecked([field_with_id("c", data_type, true, 1)])
    }

    /// A physical table whose only leaf `n.y` (field ID 4) is a LONG.
    fn nested_table() -> StructType {
        let n = StructType::new_unchecked([field_with_id("y", DataType::LONG, true, 4)]);
        StructType::new_unchecked([field_with_id("n", n.into(), true, 3)])
    }

    /// The physical Delta stats schema of a scan reading every stat of `columns`, a table schema
    /// whose leaves are the min/max types.
    fn delta_stats(columns: StructType) -> StructType {
        fn as_counts(columns: &StructType) -> StructType {
            StructType::new_unchecked(columns.fields().map(|f| match f.data_type() {
                DataType::Struct(nested) => StructField::nullable(f.name(), as_counts(nested)),
                _ => StructField::nullable(f.name(), DataType::LONG),
            }))
        }
        StructType::new_unchecked([
            StructField::nullable(NUM_RECORDS, DataType::LONG),
            StructField::nullable(NULL_COUNT, as_counts(&columns)),
            StructField::nullable(MIN_VALUES, columns.clone()),
            StructField::nullable(MAX_VALUES, columns),
        ])
    }

    fn c(data_type: DataType) -> StructType {
        StructType::new_unchecked([StructField::nullable("c", data_type)])
    }

    fn longs<const N: usize>(values: [i64; N]) -> Vec<Scalar> {
        values.into_iter().map(Scalar::Long).collect()
    }

    /// `[lower, upper]` double bounds over 10 non-null values, with `nans` NaNs.
    fn double_stats(nans: Option<i64>) -> Vec<Scalar> {
        let nans = nans.map_or(Scalar::Null(DataType::LONG), Scalar::Long);
        vec![1.0.into(), 3.0.into(), 10i64.into(), 0i64.into(), nans]
    }

    // Leaf stats values are in `content_stats` field order: lower_bound, upper_bound,
    // value_count, null_value_count, then nan_value_count for floating point leaves.
    #[rstest]
    #[case::bounds_prove_no_match(
        flat_table(DataType::LONG),
        c(DataType::LONG),
        col!("c").gt(lit(15i64)),
        longs([1, 9, 10, 0]),
        false
    )]
    #[case::bounds_may_match(
        flat_table(DataType::LONG),
        c(DataType::LONG),
        col!("c").gt(lit(5i64)),
        longs([1, 9, 10, 0]),
        true
    )]
    #[case::unknown_bounds_keep(
        flat_table(DataType::LONG),
        c(DataType::LONG),
        col!("c").gt(lit(15i64)),
        vec![Scalar::Null(DataType::LONG), Scalar::Null(DataType::LONG), 10i64.into(), 0i64.into()],
        true
    )]
    #[case::null_count_proves_no_nulls(
        flat_table(DataType::LONG),
        c(DataType::LONG),
        col!("c").is_null(),
        longs([1, 9, 10, 0]),
        false
    )]
    #[case::null_count_may_have_nulls(
        flat_table(DataType::LONG),
        c(DataType::LONG),
        col!("c").is_null(),
        longs([1, 9, 10, 2]),
        true
    )]
    #[case::nested_leaf(
        nested_table(),
        nested_table(),
        col!("n.y").gt(lit(15i64)),
        longs([1, 9, 10, 0]),
        false
    )]
    // A bound recorded at another type (e.g. before type widening) is unknown.
    #[case::bound_type_mismatch_keeps(
        flat_table(DataType::INTEGER),
        c(DataType::LONG),
        col!("c").gt(lit(15i64)),
        vec![1.into(), 9.into(), 10i64.into(), 0i64.into()],
        true
    )]
    #[case::double_without_nans(
        flat_table(DataType::DOUBLE),
        c(DataType::DOUBLE),
        col!("c").gt(lit(5.0)),
        double_stats(Some(0)),
        false
    )]
    // AMT bounds exclude NaN, which compares above 5.0, so a leaf that may hold NaN is kept.
    #[case::double_with_nans_keeps(
        flat_table(DataType::DOUBLE),
        c(DataType::DOUBLE),
        col!("c").gt(lit(5.0)),
        double_stats(Some(1)),
        true
    )]
    #[case::double_unknown_nans_keeps(
        flat_table(DataType::DOUBLE),
        c(DataType::DOUBLE),
        col!("c").gt(lit(5.0)),
        double_stats(None),
        true
    )]
    fn leaf_skipping_filter_keeps_entry_unless_stats_prove_no_match(
        #[case] table: StructType,
        #[case] columns: StructType,
        #[case] predicate: Predicate,
        #[case] leaf_values: Vec<Scalar>,
        #[case] expected_keep: bool,
    ) {
        let engine = SyncEngine::new();
        let stats_schema = delta_stats(columns);
        let content_stats = projected_stats_schema(&table, &stats_schema).unwrap();
        let input = Arc::new(StructType::new_unchecked([StructField::nullable(
            CONTENT_STATS_FIELD_NAME,
            content_stats.clone(),
        )]));
        let filter = leaf_skipping_filter(
            &engine,
            Some(Arc::new(predicate)),
            &stats_schema,
            &content_stats,
            input.clone(),
        )
        .expect("predicate should be eligible for data skipping");

        let leaf = content_stats.fields().next().unwrap();
        let DataType::Struct(leaf_type) = leaf.data_type() else {
            panic!("leaf stats must be a struct");
        };
        let leaf_stats =
            StructData::try_new(leaf_type.fields().cloned().collect(), leaf_values).unwrap();
        let entry_stats =
            StructData::try_new(vec![leaf.clone()], vec![Scalar::Struct(leaf_stats)]).unwrap();
        let batch = engine
            .evaluation_handler()
            .create_many(input, vec![vec![Scalar::Struct(entry_stats)]])
            .unwrap();
        assert_eq!(filter.apply(batch.as_ref()).unwrap(), [expected_keep]);
    }

    #[test]
    fn leaf_skipping_filter_is_none_without_predicate() {
        let table = flat_table(DataType::LONG);
        let stats_schema = delta_stats(c(DataType::LONG));
        let content_stats = projected_stats_schema(&table, &stats_schema).unwrap();
        let input = Arc::new(StructType::new_unchecked([StructField::nullable(
            CONTENT_STATS_FIELD_NAME,
            content_stats.clone(),
        )]));
        let filter = leaf_skipping_filter(
            &SyncEngine::new(),
            None,
            &stats_schema,
            &content_stats,
            input,
        );
        assert!(filter.is_none());
    }
}
