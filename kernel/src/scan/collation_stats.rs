//! Derives the collation bounds a scan reads from the collated comparisons of its predicate.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use super::PhysicalPredicate;
use crate::KernelResult;
use crate::actions::{COLLATION_STATS, MAX_VALUES, MIN_VALUES};
use crate::checkpoint::collation_stats::set_collation_stats;
use crate::expressions::collation::CollatedComparison;
use crate::expressions::{ColumnName, Expression, OpaquePredicate};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::table_configuration::TableConfiguration;
use crate::transforms::{ExpressionTransform, transform_output_type};

/// Adds to `read` the `statsWithCollation` min/max bounds of each non-partition STRING column
/// that `predicate` compares under a collation, one entry per collation.
pub(super) fn extend_read_schema(
    table: &TableConfiguration,
    read: Option<SchemaRef>,
    predicate: &PhysicalPredicate,
) -> KernelResult<Option<SchemaRef>> {
    let PhysicalPredicate::Some(predicate, _) = predicate else {
        return Ok(read);
    };
    let mut collated = CollatedColumns::default();
    collated.transform_pred(predicate);
    let data_schema = table.physical_data_schema_without_partition_columns();
    let mut bounds = Vec::with_capacity(collated.0.len());
    let mut identifiers = HashSet::with_capacity(collated.0.len());
    for (collation, columns) in collated.0 {
        let columns: Vec<ColumnName> = columns
            .into_iter()
            .filter(|column| {
                data_schema
                    .field_at(column)
                    .is_ok_and(|field| field.data_type() == &DataType::STRING)
            })
            .cloned()
            .collect();
        if columns.is_empty() {
            continue;
        }
        let stats = table
            .stats_schema_builder()
            .with_required_physical_columns(Some(&columns))
            .with_requested_physical_columns(Some(&columns))
            .build()?;
        // Some STRING columns, such as a file checksum, record no bounds.
        let (Some(min), Some(max)) = (stats.field(MIN_VALUES), stats.field(MAX_VALUES)) else {
            continue;
        };
        // Field names are unique ignoring case, so of the identifiers that differ only by case,
        // only the first has its bounds read.
        if identifiers.insert(collation.to_lowercase()) {
            let min_max = StructType::try_new([min.clone(), max.clone()])?;
            bounds.push(StructField::nullable(collation, min_max));
        }
    }
    if bounds.is_empty() {
        return Ok(read);
    }
    let collation_stats = StructField::nullable(COLLATION_STATS, StructType::try_new(bounds)?);
    let read = set_collation_stats(read.as_deref(), collation_stats)?;
    Ok(Some(Arc::new(read)))
}

/// Collects the physical columns that collated comparisons compare to a literal, by collation.
#[derive(Default)]
struct CollatedColumns<'a>(BTreeMap<&'a str, HashSet<&'a ColumnName>>);

impl<'a> ExpressionTransform<'a> for CollatedColumns<'a> {
    transform_output_type!(|'a, T| ());

    fn transform_pred_opaque(&mut self, predicate: &'a OpaquePredicate) {
        if let Some(collated) = predicate.op.any_ref().downcast_ref::<CollatedComparison>()
            && let Some(compared) = CollatedComparison::compared(&predicate.exprs)
            && let Expression::Column(column) = compared.value
        {
            self.0
                .entry(collated.collation())
                .or_default()
                .insert(column);
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::expressions::{Predicate, col, collated_comparison, lit};
    use crate::scan::StatsOptions;
    use crate::scan::state_info::tests::get_state_info_with_stats;
    use crate::schema::schema_ref;
    use crate::table_features::TableFeature;

    const COLLATION: &str = "test.ASCII_CI.1";

    fn read_schema(
        partitions: Vec<String>,
        predicate: Predicate,
        stats: StatsOptions,
        stats_columns: Option<&str>,
    ) -> Option<SchemaRef> {
        let schema = schema_ref! {
            nullable "id": LONG,
            nullable "name": STRING,
            nullable "nested": { nullable "child": STRING },
            (StructField::nullable("file", DataType::file_type())),
        };
        let properties = stats_columns
            .map(|columns| {
                (
                    "delta.dataSkippingStatsColumns".to_string(),
                    columns.to_string(),
                )
            })
            .into_iter()
            .collect();
        get_state_info_with_stats(
            schema,
            partitions,
            Some(Arc::new(predicate)),
            &[TableFeature::FileType],
            properties,
            vec![],
            stats,
        )
        .unwrap()
        .physical_stats_read_schema()
        .cloned()
    }

    fn equals(column: Expression, collation: &str) -> Predicate {
        collated_comparison(collation, column.eq(lit("bob")))
    }

    #[rstest]
    fn scan_reads_the_bounds_of_each_collated_comparison(
        #[values(StatsOptions::json_only(), StatsOptions::all_struct())] stats: StatsOptions,
        #[values(None, Some("id"))] stats_columns: Option<&str>,
    ) {
        let predicate = Predicate::and_from([
            equals(col!("name"), COLLATION),
            Predicate::not(equals(col!("nested.child"), COLLATION)),
            equals(col!("name"), "test.ASCII_CI.2"),
            col!("id").gt(lit(10i64)),
        ]);
        let read = read_schema(vec![], predicate, stats, stats_columns).unwrap();
        for bound in [MIN_VALUES, MAX_VALUES] {
            assert!(read.contains_col([bound, "id"]));
            assert!(read.contains_col([COLLATION_STATS, COLLATION, bound, "name"]));
            assert!(read.contains_col([COLLATION_STATS, COLLATION, bound, "nested", "child"]));
            assert!(read.contains_col([COLLATION_STATS, "test.ASCII_CI.2", bound, "name"]));
            assert!(!read.contains_col([COLLATION_STATS, "test.ASCII_CI.2", bound, "nested"]));
            assert!(!read.contains_col([COLLATION_STATS, COLLATION, bound, "id"]));
        }
    }

    #[rstest]
    #[case::non_string(vec![], equals(col!("id"), COLLATION))]
    #[case::partition(vec!["name".to_string()], equals(col!("name"), COLLATION))]
    #[case::not_a_column(vec![], equals(lit("bob"), COLLATION))]
    #[case::string_without_bounds(vec![], equals(col!("file.checksum"), COLLATION))]
    fn scan_reads_no_bounds_for_a_comparison_it_cannot_skip_with(
        #[case] partitions: Vec<String>,
        #[case] predicate: Predicate,
    ) {
        let read = read_schema(partitions, predicate, StatsOptions::all_struct(), None);
        assert!(read.is_none_or(|read| read.field(COLLATION_STATS).is_none()));
    }

    #[test]
    fn scan_reads_the_bounds_of_one_of_the_collations_that_differ_only_by_case() {
        // The first identifier in order has no bounds to read, so the second is the one kept.
        let predicate = Predicate::and_from([
            equals(col!("file.checksum"), "TEST.ASCII_CI.1"),
            equals(col!("name"), "TEST.ascii_ci.1"),
            equals(col!("name"), COLLATION),
        ]);
        let read = read_schema(vec![], predicate, StatsOptions::all_struct(), None).unwrap();
        assert!(read.contains_col([COLLATION_STATS, "TEST.ascii_ci.1", MIN_VALUES, "name"]));
        assert!(!read.contains_col([COLLATION_STATS, "TEST.ASCII_CI.1"]));
        assert!(!read.contains_col([COLLATION_STATS, COLLATION]));
    }
}
