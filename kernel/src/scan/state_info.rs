//! StateInfo handles the state that we use through log-replay in order to correctly construct all
//! the physical->logical transforms needed for each add file

use std::collections::HashSet;
use std::sync::Arc;

use tracing::{debug, enabled, warn, Level};

use crate::actions::NULL_COUNT;
use crate::expressions::ColumnName;
use crate::scan::field_classifiers::TransformFieldClassifier;
use crate::scan::transform_spec::{FieldTransformSpec, TransformSpec};
use crate::scan::{PhysicalPredicate, StatsOutputMode};
use crate::schema::{DataType, MetadataColumnSpec, SchemaRef, StructType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{get_any_level_column_physical_name, ColumnMappingMode};
use crate::{DeltaResult, Error, PredicateRef, StructField};

/// All the state needed to process a scan.
#[derive(Debug, Clone)]
pub(crate) struct StateInfo {
    /// The logical schema for this scan
    pub(crate) logical_schema: SchemaRef,
    /// The physical schema to read from parquet files
    pub(crate) physical_schema: SchemaRef,
    /// The physical predicate for data skipping
    pub(crate) physical_predicate: PhysicalPredicate,
    /// Transform specification for converting physical to logical data
    pub(crate) transform_spec: Option<Arc<TransformSpec>>,
    /// The column mapping mode for this scan
    pub(crate) column_mapping_mode: ColumnMappingMode,
    /// Physical stats schema for reading/parsing stats from checkpoint files.
    /// Used to construct checkpoint read schema with stats_parsed.
    pub(crate) physical_stats_schema: Option<SchemaRef>,
    /// Physical partition schema with native types for partition pruning via
    /// `partitionValues_parsed`. Fields use physical column names (for column mapping).
    /// Only present when the table has partition columns and a predicate is provided.
    pub(crate) physical_partition_schema: Option<SchemaRef>,
    /// Physical leaf paths which are expected to have stats collected.
    ///
    /// Differs from `physical_stats_schema` in that this is the per-table membership set
    /// (predicate-independent). `physical_stats_schema` is the per-scan projection shape
    /// (predicate-trimmed).
    ///
    /// Read-path mirror of `SharedWriteState.stats_columns`.
    pub(crate) physical_stats_columns: HashSet<ColumnName>,
}

/// Validating the metadata columns also extracts information needed to properly construct the full
/// `StateInfo`. We use this struct to group this information so it can be cleanly passed back from
/// `validate_metadata_columns`
#[derive(Default)]
struct MetadataInfo<'a> {
    /// What are the names of the requested metadata fields
    metadata_field_names: HashSet<&'a String>,
    /// The name of the column that's selecting row indexes if that's been requested or None if
    /// they are not requested. We remember this if it's been requested explicitly. this is so
    /// we can reference this column and not re-add it as a requested column if we're _also_
    /// requesting row-ids.
    selected_row_index_col_name: Option<&'a String>,
    /// the materializedRowIdColumnName extracted from the table config if row ids are requested,
    /// or None if they are not requested
    materialized_row_id_column_name: Option<&'a String>,
}

/// This validates that we have sensible metadata columns, and that the requested metadata is
/// supported by the table. Also computes and returns any extra info needed to build the transform
/// for the requested columns.
// Runs in O(supported_number_of_metadata_columns) time since each metadata
// column can appear at most once in the schema
fn validate_metadata_columns<'a>(
    logical_schema: &'a SchemaRef,
    table_configuration: &'a TableConfiguration,
) -> DeltaResult<MetadataInfo<'a>> {
    let mut metadata_info = MetadataInfo::default();
    let partition_columns = table_configuration.partition_columns();
    for metadata_column in logical_schema.metadata_columns() {
        // Ensure we don't have a metadata column with same name as a partition column
        if partition_columns.contains(metadata_column.name()) {
            return Err(Error::Schema(format!(
                "Metadata column names must not match partition columns: {}",
                metadata_column.name()
            )));
        }
        match metadata_column.get_metadata_column_spec() {
            Some(MetadataColumnSpec::RowIndex) => {
                metadata_info.selected_row_index_col_name = Some(metadata_column.name());
            }
            Some(MetadataColumnSpec::RowId) => {
                if table_configuration.table_properties().enable_row_tracking != Some(true) {
                    return Err(Error::unsupported("Row ids are not enabled on this table"));
                }
                let row_id_col = table_configuration
                    .metadata()
                    .configuration()
                    .get("delta.rowTracking.materializedRowIdColumnName")
                    .ok_or(Error::generic("No delta.rowTracking.materializedRowIdColumnName key found in metadata configuration"))?;
                metadata_info.materialized_row_id_column_name = Some(row_id_col);
            }
            Some(MetadataColumnSpec::RowCommitVersion) => {}
            Some(MetadataColumnSpec::FilePath) => {
                // FilePath metadata column is handled by the parquet reader
            }
            None => {}
        }
        metadata_info
            .metadata_field_names
            .insert(metadata_column.name());
    }
    Ok(metadata_info)
}

/// Build data-skipping schemas based on `StatsOutputMode` and `PhysicalPredicate`.
///
/// Returns `(physical_stats_schema, physical_partition_schema)`, where:
/// - `physical_stats_schema` contains data-column stats for `stats_parsed`.
/// - `physical_partition_schema` contains typed partition values for `partitionValues_parsed`.
///
/// All three arms route through `TableConfiguration::build_expected_stats_schemas`: the
/// `AllColumns` arm with no `requested_physical_columns` filter, and the two scoped arms
/// with the union of requested + predicate-referenced columns. That path applies the same
/// `BaseStatsTransform` -> `MinMaxStatsTransform` pipeline writers use, so the read-side
/// stats schema's shape matches the write-side exactly.
fn build_data_skipping_schemas(
    stats_output_mode: &StatsOutputMode,
    physical_predicate: &PhysicalPredicate,
    predicate_column_names_logical: &[ColumnName],
    table_configuration: &TableConfiguration,
    table_partition_schema: Option<SchemaRef>,
) -> DeltaResult<(Option<SchemaRef>, Option<SchemaRef>)> {
    // Filter partition schema to only predicate-referenced columns. The DataSkippingFilter
    // only needs partition columns that appear in the predicate, and the transform output
    // should not include unused partition columns.
    let predicate_partition_schema = match (&table_partition_schema, physical_predicate) {
        (Some(tps), PhysicalPredicate::Some(_, ref_schema)) => {
            // Partition values extracted from the string map via MapToStruct are always
            // nullable (map lookup can return null), so we force all partition fields nullable.
            ref_schema
                .with_fields_filtered_nonempty(|f| tps.field(f.name()).is_some())?
                .map(|partition_schema| {
                    let nullable_fields = partition_schema
                        .fields()
                        .map(|f| StructField::nullable(f.name(), f.data_type().clone()));
                    Arc::new(StructType::new_unchecked(nullable_fields))
                })
        }
        _ => None,
    };

    // `DataSkippingFilter` needs stats for every column its predicate references. Refs
    // without stats fold to NULL and pruning collapses to "keep every file", even when
    // the caller separately requested stats for some other set of columns via
    // `StatsOutputMode::Columns`. Union the two so the schema serves both. Unresolvable
    // refs (e.g. a predicate typo) are dropped here.
    let union_to_physical = |requested_logical: &[ColumnName]| -> Vec<ColumnName> {
        let mut union_logical: Vec<ColumnName> = requested_logical.to_vec();
        let existing: HashSet<&ColumnName> = requested_logical.iter().collect();
        for col in predicate_column_names_logical {
            if !existing.contains(col) {
                union_logical.push(col.clone());
            }
        }
        let logical_schema = table_configuration.logical_schema();
        let column_mapping_mode = table_configuration.column_mapping_mode();
        union_logical
            .iter()
            .filter_map(|col| {
                get_any_level_column_physical_name(&logical_schema, col, column_mapping_mode)
                    .inspect_err(|e| warn!("Failed to resolve physical name for column {col}: {e}"))
                    .ok()
            })
            .collect()
    };

    // A stats schema with only `numRecords` and `tightBounds` (the bookkeeping fields
    // `build_expected_stats_schemas` always emits) has nothing to prune by. Return `None`
    // in that case so the caller skips building a `DataSkippingFilter`. `nullCount` is the
    // per-column stats wrapper, so its presence is the signal that at least one data
    // column survived. The Delta protocol allows `minValues` / `maxValues` without
    // `nullCount`, but `build_expected_stats_schemas` always emits `nullCount` whenever it
    // emits min/max; this check relies on that implementation property.
    let with_data_cols = |stats_schema: SchemaRef| -> Option<SchemaRef> {
        stats_schema
            .field(NULL_COUNT)
            .is_some()
            .then_some(stats_schema)
    };

    let stats_schema = match (stats_output_mode, physical_predicate) {
        // Full table stats schema for stats_parsed.
        (StatsOutputMode::AllColumns, _) => with_data_cols(
            table_configuration
                .build_expected_stats_schemas(None, None)?
                .physical,
        ),
        // Explicit requested columns. Union in predicate refs so the stats schema covers
        // both sources.
        (StatsOutputMode::Columns(requested_columns), _) if !requested_columns.is_empty() => {
            let requested_physical = union_to_physical(requested_columns);
            with_data_cols(
                table_configuration
                    .build_expected_stats_schemas(None, Some(&requested_physical))?
                    .physical,
            )
        }
        // No explicit requested columns, but a predicate is present. Use just the predicate
        // refs so the stats schema is trimmed to what the rewritten predicate needs.
        (_, PhysicalPredicate::Some(_, _)) => {
            let predicate_refs_physical = union_to_physical(&[]);
            with_data_cols(
                table_configuration
                    .build_expected_stats_schemas(None, Some(&predicate_refs_physical))?
                    .physical,
            )
        }
        (_, _) => None,
    };
    Ok((stats_schema, predicate_partition_schema))
}

impl StateInfo {
    /// Create StateInfo with a custom field classifier for different scan types.
    /// Get the state needed to process a scan.
    ///
    /// `logical_schema` - The logical schema of the scan output, which includes partition columns
    /// `table_configuration` - The TableConfiguration for this table
    /// `predicate` - Optional predicate to filter data during the scan
    /// `stats_output_mode` - Controls how file statistics are handled during the scan
    /// `classifier` - The classifier to use for different scan types. Use `()` if not needed
    pub(crate) fn try_new<C: TransformFieldClassifier>(
        logical_schema: SchemaRef,
        table_configuration: &TableConfiguration,
        predicate: Option<PredicateRef>,
        stats_output_mode: StatsOutputMode,
        classifier: C,
    ) -> DeltaResult<Self> {
        let partition_columns = table_configuration.partition_columns();
        let column_mapping_mode = table_configuration.column_mapping_mode();
        let mut read_fields = Vec::with_capacity(logical_schema.num_fields());
        let mut transform_spec = Vec::with_capacity(logical_schema.num_fields());
        let mut last_physical_field: Option<String> = None;

        let metadata_info = validate_metadata_columns(&logical_schema, table_configuration)?;

        // Loop over all selected fields and build both the physical schema and transform spec
        for (index, logical_field) in logical_schema.fields().enumerate() {
            if let Some(spec) =
                classifier.classify_field(logical_field, index, &last_physical_field)
            {
                // Classifier has handled this field via a transformation, just push it and move on
                transform_spec.push(spec);
            } else if partition_columns.contains(logical_field.name()) {
                // push the transform for this partition column
                transform_spec.push(FieldTransformSpec::MetadataDerivedColumn {
                    field_index: index,
                    insert_after: last_physical_field.clone(),
                });
            } else {
                // Regular field field or a metadata column, figure out which and handle it
                match logical_field.get_metadata_column_spec() {
                    Some(MetadataColumnSpec::RowId) => {
                        let index_column_name = match metadata_info.selected_row_index_col_name {
                            Some(index_column_name) => index_column_name.to_string(),
                            None => {
                                // the index column isn't being explicitly requested, so add it to
                                // `read_fields` so the parquet_reader will generate it, and add a
                                // transform to drop it before returning logical data

                                // ensure we have a column name that isn't already in our schema
                                let index_column_name = (0..)
                                    .map(|i| format!("row_indexes_for_row_id_{i}"))
                                    .find(|name| logical_schema.field(name).is_none())
                                    .ok_or(Error::generic(
                                        "Couldn't generate row index column name",
                                    ))?;
                                read_fields.push(StructField::create_metadata_column(
                                    &index_column_name,
                                    MetadataColumnSpec::RowIndex,
                                ));
                                transform_spec.push(FieldTransformSpec::StaticDrop {
                                    field_name: index_column_name.clone(),
                                });
                                index_column_name
                            }
                        };
                        let Some(row_id_col_name) = metadata_info.materialized_row_id_column_name
                        else {
                            return Err(Error::internal_error(
                                "Should always return a materialized_row_id_column_name if selecting row ids"
                            ));
                        };

                        read_fields.push(StructField::nullable(row_id_col_name, DataType::LONG));
                        transform_spec.push(FieldTransformSpec::GenerateRowId {
                            field_name: row_id_col_name.to_string(),
                            row_index_field_name: index_column_name,
                        });
                    }
                    Some(MetadataColumnSpec::RowCommitVersion) => {
                        return Err(Error::unsupported("Row commit versions not supported"));
                    }
                    Some(MetadataColumnSpec::RowIndex)
                    | Some(MetadataColumnSpec::FilePath)
                    | None => {
                        // note that RowIndex and FilePath are handled in the parquet reader so we
                        // just add them as if they're normal physical
                        // columns
                        let physical_field = logical_field.make_physical(column_mapping_mode)?;
                        debug!("\n\n{logical_field:#?}\nAfter mapping: {physical_field:#?}\n\n");
                        let physical_name = physical_field.name.clone();

                        if !logical_field.is_metadata_column()
                            && metadata_info.metadata_field_names.contains(&physical_name)
                        {
                            return Err(Error::Schema(format!(
                                "Metadata column names must not match physical columns, but logical column '{}' has physical name '{}'",
                                logical_field.name(), physical_name,
                            )));
                        }
                        last_physical_field = Some(physical_name);
                        read_fields.push(physical_field);
                    }
                }
            }
        }

        let physical_schema = Arc::new(StructType::try_new(read_fields)?);

        // Logical column names referenced by the predicate. Fed into the stats schema
        // build below and into the dropped-refs observability log.
        let predicate_column_names: Vec<ColumnName> = predicate
            .as_ref()
            .map(|p| p.references().into_iter().cloned().collect())
            .unwrap_or_default();

        let physical_predicate = match predicate {
            Some(pred) => PhysicalPredicate::try_new(&pred, &logical_schema, column_mapping_mode)?,
            None => PhysicalPredicate::None,
        };

        // Stats-eligible column set. Partition columns are excluded; they flow through
        // `partitionValues_parsed` instead.
        let physical_stats_columns = table_configuration.physical_stats_columns_set(None);
        // Observability: predicate refs outside `physical_stats_columns` get folded to NULL
        // by the gate. Surface the dropped set so an engine operator can see what got folded.
        // The filter walk is bounded by predicate width but still does a physical-name
        // resolution per ref, so gate it on the log level to skip the work when DEBUG is off.
        if enabled!(Level::DEBUG) && matches!(physical_predicate, PhysicalPredicate::Some(_, _)) {
            let dropped: Vec<&ColumnName> = predicate_column_names
                .iter()
                .filter(|c| {
                    get_any_level_column_physical_name(&logical_schema, c, column_mapping_mode)
                        .ok()
                        .is_some_and(|physical| !physical_stats_columns.contains(&physical))
                })
                .collect();
            if !dropped.is_empty() {
                debug!(
                    "Checkpoint pushdown: predicate refs to non-stats columns folded to NULL: {:?}",
                    dropped
                );
            }
        }

        // Build partition schema with physical names for partition pruning in data skipping.
        // Only needed when we have a predicate and partition columns.
        // partition_columns stores logical names (per Delta protocol), so we zip the table's
        // logical and physical schemas (same field ordering, guaranteed by `make_physical`)
        // to match logical names and extract the corresponding physical fields without
        // per-field metadata lookups.
        let table_partition_schema = if !matches!(
            physical_predicate,
            PhysicalPredicate::None | PhysicalPredicate::StaticSkipAll
        ) && !partition_columns.is_empty()
        {
            let partition_fields: Vec<StructField> = table_configuration
                .logical_schema()
                .fields()
                .zip(table_configuration.physical_schema().fields())
                .filter(|(logical_f, _)| partition_columns.contains(logical_f.name()))
                .map(|(_, physical_f)| physical_f.clone())
                .collect();
            if partition_fields.is_empty() {
                None
            } else {
                Some(Arc::new(StructType::new_unchecked(partition_fields)))
            }
        } else {
            None
        };

        let (physical_stats_schema, physical_partition_schema) = build_data_skipping_schemas(
            &stats_output_mode,
            &physical_predicate,
            &predicate_column_names,
            table_configuration,
            table_partition_schema,
        )?;

        let transform_spec =
            if !transform_spec.is_empty() || column_mapping_mode != ColumnMappingMode::None {
                Some(Arc::new(transform_spec))
            } else {
                None
            };

        Ok(StateInfo {
            logical_schema,
            physical_schema,
            physical_predicate,
            transform_spec,
            column_mapping_mode,
            physical_stats_schema,
            physical_partition_schema,
            physical_stats_columns,
        })
    }

    /// Returns a conservative initial capacity for the dedup `HashSet` in
    /// [`ScanLogReplayProcessor`].
    ///
    /// The exact file count is not available at this point, so the hint is
    /// derived from whether stats are enabled: stats are only computed for
    /// non-trivial tables, so their presence is a reasonable proxy for table
    /// size. Using 4096 vs 512 as the two tiers eliminates the first 12-14
    /// hashbrown doubling events for medium/large tables while staying cheap
    /// for small ones.
    pub(crate) fn dedup_capacity_hint(&self) -> usize {
        if self.physical_stats_schema.is_some() {
            4096
        } else {
            512
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::actions::{Metadata, Protocol, MAX_VALUES, MIN_VALUES};
    use crate::expressions::{column_expr, column_name, Expression as Expr, Predicate as Pred};
    use crate::schema::{ColumnMetadataKey, MetadataValue};
    use crate::table_features::{FeatureType, TableFeature};
    use crate::utils::test_utils::assert_result_error_with_message;

    // get a state info with no predicate or extra metadata
    pub(crate) fn get_simple_state_info(
        schema: SchemaRef,
        partition_columns: Vec<String>,
    ) -> DeltaResult<StateInfo> {
        get_state_info(schema, partition_columns, None, &[], HashMap::new(), vec![])
    }

    /// When features are non-empty, uses protocol (3,7) with explicit feature lists.
    /// When features are empty, uses legacy protocol (2,5).
    pub(crate) fn get_state_info(
        schema: SchemaRef,
        partition_columns: Vec<String>,
        predicate: Option<PredicateRef>,
        features: &[TableFeature],
        metadata_configuration: HashMap<String, String>,
        metadata_cols: Vec<(&str, MetadataColumnSpec)>,
    ) -> DeltaResult<StateInfo> {
        get_state_info_with_stats(
            schema,
            partition_columns,
            predicate,
            features,
            metadata_configuration,
            metadata_cols,
            StatsOutputMode::default(),
        )
    }

    pub(crate) fn get_state_info_with_stats(
        schema: SchemaRef,
        partition_columns: Vec<String>,
        predicate: Option<PredicateRef>,
        features: &[TableFeature],
        metadata_configuration: HashMap<String, String>,
        metadata_cols: Vec<(&str, MetadataColumnSpec)>,
        stats_output_mode: StatsOutputMode,
    ) -> DeltaResult<StateInfo> {
        let metadata = Metadata::try_new(
            None,
            None,
            schema.clone(),
            partition_columns,
            10,
            metadata_configuration,
        )?;
        let protocol = if features.is_empty() {
            Protocol::try_new_legacy(2, 5)?
        } else {
            // This helper only handles known features. Unknown features would need
            // explicit placement on reader vs writer lists.
            assert!(
                features
                    .iter()
                    .all(|f| f.feature_type() != FeatureType::Unknown),
                "Test helper does not support unknown features"
            );
            let reader_features = features
                .iter()
                .filter(|f| f.feature_type() == FeatureType::ReaderWriter);
            Protocol::try_new_modern(reader_features, features)?
        };
        let table_configuration = TableConfiguration::try_new(
            metadata,
            protocol,
            Url::parse("s3://my-table").unwrap(),
            1,
        )?;

        let mut schema = schema;
        for (name, spec) in metadata_cols.into_iter() {
            schema = Arc::new(
                schema
                    .add_metadata_column(name, spec)
                    .expect("Couldn't add metadata col"),
            );
        }

        StateInfo::try_new(
            schema.clone(),
            &table_configuration,
            predicate,
            stats_output_mode,
            (),
        )
    }

    pub(crate) fn assert_transform_spec(
        transform_spec: &TransformSpec,
        requested_row_indexes: bool,
        expected_row_id_name: &str,
        expected_row_index_name: &str,
    ) {
        // if we requested row indexes, there's only one transform for the row id col, otherwise the
        // first transform drops the row index column, and the second one adds the row ids
        let expected_transform_count = if requested_row_indexes { 1 } else { 2 };
        let generate_offset = if requested_row_indexes { 0 } else { 1 };

        assert_eq!(transform_spec.len(), expected_transform_count);

        if !requested_row_indexes {
            // ensure we have a drop transform if we didn't request row indexes
            match &transform_spec[0] {
                FieldTransformSpec::StaticDrop { field_name } => {
                    assert_eq!(field_name, expected_row_index_name);
                }
                _ => panic!("Expected StaticDrop transform"),
            }
        }

        match &transform_spec[generate_offset] {
            FieldTransformSpec::GenerateRowId {
                field_name,
                row_index_field_name,
            } => {
                assert_eq!(field_name, expected_row_id_name);
                assert_eq!(row_index_field_name, expected_row_index_name);
            }
            _ => panic!("Expected GenerateRowId transform"),
        }
    }

    #[test]
    fn no_partition_columns() {
        // Test case: No partition columns, no column mapping
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
        ]));

        let state_info = get_simple_state_info(schema.clone(), vec![]).unwrap();

        // Should have no transform spec (no partitions, no column mapping)
        assert!(state_info.transform_spec.is_none());

        // Physical schema should match logical schema
        assert_eq!(state_info.logical_schema, schema);
        assert_eq!(state_info.physical_schema.fields().len(), 2);

        // No predicate
        assert_eq!(state_info.physical_predicate, PhysicalPredicate::None);
    }

    #[test]
    fn with_partition_columns() {
        // Test case: With partition columns
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("date", DataType::DATE), // Partition column
            StructField::nullable("value", DataType::LONG),
        ]));

        let state_info = get_simple_state_info(
            schema.clone(),
            vec!["date".to_string()], // date is a partition column
        )
        .unwrap();

        // Should have a transform spec for the partition column
        assert!(state_info.transform_spec.is_some());
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_eq!(transform_spec.len(), 1);

        // Check the transform spec for the partition column
        match &transform_spec[0] {
            FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after,
            } => {
                assert_eq!(*field_index, 1); // Index of "date" in logical schema
                assert_eq!(insert_after, &Some("id".to_string())); // After "id" which is physical
            }
            _ => panic!("Expected MetadataDerivedColumn transform"),
        }

        // Physical schema should not include partition column
        assert_eq!(state_info.logical_schema, schema);
        assert_eq!(state_info.physical_schema.fields().len(), 2); // Only id and value
    }

    #[test]
    fn multiple_partition_columns() {
        // Test case: Multiple partition columns interspersed with regular columns
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("col1", DataType::STRING),
            StructField::nullable("part1", DataType::STRING), // Partition
            StructField::nullable("col2", DataType::LONG),
            StructField::nullable("part2", DataType::INTEGER), // Partition
        ]));

        let state_info = get_simple_state_info(
            schema.clone(),
            vec!["part1".to_string(), "part2".to_string()],
        )
        .unwrap();

        // Should have transforms for both partition columns
        assert!(state_info.transform_spec.is_some());
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_eq!(transform_spec.len(), 2);

        // Check first partition column transform
        match &transform_spec[0] {
            FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after,
            } => {
                assert_eq!(*field_index, 1); // Index of "part1"
                assert_eq!(insert_after, &Some("col1".to_string()));
            }
            _ => panic!("Expected MetadataDerivedColumn transform"),
        }

        // Check second partition column transform
        match &transform_spec[1] {
            FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after,
            } => {
                assert_eq!(*field_index, 3); // Index of "part2"
                assert_eq!(insert_after, &Some("col2".to_string()));
            }
            _ => panic!("Expected MetadataDerivedColumn transform"),
        }

        // Physical schema should only have non-partition columns
        assert_eq!(state_info.physical_schema.fields().len(), 2); // col1 and col2
    }

    #[test]
    fn with_predicate() {
        // Test case: With a valid predicate
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
        ]));

        let predicate = Arc::new(column_expr!("value").gt(Expr::literal(10i64)));

        let state_info = get_state_info(
            schema.clone(),
            vec![], // no partition columns
            Some(predicate),
            &[],            // no table features
            HashMap::new(), // no extra metadata
            vec![],         // no metadata
        )
        .unwrap();

        // Should have a physical predicate
        match &state_info.physical_predicate {
            PhysicalPredicate::Some(_pred, schema) => {
                // Physical predicate exists
                assert_eq!(schema.fields().len(), 1); // Only "value" is referenced
            }
            _ => panic!("Expected PhysicalPredicate::Some"),
        }
    }

    #[test]
    fn partition_at_beginning() {
        // Test case: Partition column at the beginning
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("date", DataType::DATE), // Partition column
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
        ]));

        let state_info = get_simple_state_info(schema.clone(), vec!["date".to_string()]).unwrap();

        // Should have a transform spec for the partition column
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_eq!(transform_spec.len(), 1);

        match &transform_spec[0] {
            FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after,
            } => {
                assert_eq!(*field_index, 0); // Index of "date"
                assert_eq!(insert_after, &None); // No physical field before it, so prepend
            }
            _ => panic!("Expected MetadataDerivedColumn transform"),
        }
    }

    pub(crate) const ROW_TRACKING_FEATURES: &[TableFeature] =
        &[TableFeature::RowTracking, TableFeature::DomainMetadata];

    fn get_string_map(slice: &[(&str, &str)]) -> HashMap<String, String> {
        slice
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn request_row_ids() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )]));

        let state_info = get_state_info(
            schema.clone(),
            vec![],
            None,
            ROW_TRACKING_FEATURES,
            get_string_map(&[
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "some_row_id_col",
                ),
                (
                    "delta.rowTracking.materializedRowCommitVersionColumnName",
                    "some_row_commit_version_col",
                ),
            ]),
            vec![("row_id", MetadataColumnSpec::RowId)],
        )
        .unwrap();

        // Should have a transform spec for the row_id column
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_transform_spec(
            transform_spec,
            false, // we did not request row indexes
            "some_row_id_col",
            "row_indexes_for_row_id_0",
        );
    }

    #[test]
    fn request_row_ids_conflicting_row_index_col_name() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "row_indexes_for_row_id_0", /* this will conflict with the first generated name for
                                         * row indexes */
            DataType::STRING,
        )]));

        let state_info = get_state_info(
            schema.clone(),
            vec![],
            None,
            ROW_TRACKING_FEATURES,
            get_string_map(&[
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "some_row_id_col",
                ),
                (
                    "delta.rowTracking.materializedRowCommitVersionColumnName",
                    "some_row_commit_version_col",
                ),
            ]),
            vec![("row_id", MetadataColumnSpec::RowId)],
        )
        .unwrap();

        // Should have a transform spec for the row_id column
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_transform_spec(
            transform_spec,
            false, // we did not request row indexes
            "some_row_id_col",
            "row_indexes_for_row_id_1", // ensure we didn't conflict with the col in the schema
        );
    }

    #[test]
    fn request_row_ids_and_indexes() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )]));

        let state_info = get_state_info(
            schema.clone(),
            vec![],
            None,
            ROW_TRACKING_FEATURES,
            get_string_map(&[
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "some_row_id_col",
                ),
                (
                    "delta.rowTracking.materializedRowCommitVersionColumnName",
                    "some_row_commit_version_col",
                ),
            ]),
            vec![
                ("row_id", MetadataColumnSpec::RowId),
                ("row_index", MetadataColumnSpec::RowIndex),
            ],
        )
        .unwrap();

        // Should have a transform spec for the row_id column
        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_transform_spec(
            transform_spec,
            true, // we did request row indexes
            "some_row_id_col",
            "row_index",
        );
    }

    #[test]
    fn invalid_rowtracking_config() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )]));

        // Row IDs requested but row tracking not enabled → error
        let res = get_state_info(
            schema.clone(),
            vec![],
            None,
            &[], // no table features
            HashMap::new(),
            vec![("row_id", MetadataColumnSpec::RowId)],
        );
        assert_result_error_with_message(res, "Unsupported: Row ids are not enabled on this table");

        // Row tracking enabled but missing materializedRowIdColumnName → error
        let res = get_state_info(
            schema,
            vec![],
            None,
            ROW_TRACKING_FEATURES,
            get_string_map(&[("delta.enableRowTracking", "true")]),
            vec![("row_id", MetadataColumnSpec::RowId)],
        );
        assert_result_error_with_message(
            res,
            "Generic delta kernel error: No delta.rowTracking.materializedRowIdColumnName key found in metadata configuration",
        );
    }

    #[test]
    fn metadata_column_matches_partition_column() {
        let table_schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("part_col", DataType::STRING),
        ]));
        let metadata = Metadata::try_new(
            None,
            None,
            table_schema,
            vec!["part_col".to_string()],
            10,
            HashMap::new(),
        )
        .unwrap();
        let table_configuration = TableConfiguration::try_new(
            metadata,
            Protocol::try_new_legacy(2, 5).unwrap(),
            Url::parse("s3://my-table").unwrap(),
            1,
        )
        .unwrap();

        let read_schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )]));
        let read_schema = Arc::new(
            read_schema
                .add_metadata_column("part_col", MetadataColumnSpec::RowId)
                .expect("Couldn't add metadata col"),
        );
        let res = StateInfo::try_new(
            read_schema,
            &table_configuration,
            None,
            StatsOutputMode::default(),
            (),
        );
        assert_result_error_with_message(
            res,
            "Schema error: Metadata column names must not match partition columns: part_col",
        );
    }

    #[test]
    fn metadata_column_matches_read_field() {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "id",
            DataType::STRING,
        )
        .with_metadata(HashMap::<String, MetadataValue>::from([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                1.into(),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName
                    .as_ref()
                    .to_string(),
                "other".into(),
            ),
        ]))]));
        let res = get_state_info(
            schema.clone(),
            vec![],
            None,
            &[], // no table features
            get_string_map(&[("delta.columnMapping.mode", "name")]),
            vec![("other", MetadataColumnSpec::RowIndex)],
        );
        assert_result_error_with_message(
            res,
            "Schema error: Metadata column names must not match physical columns, but logical column 'id' has physical name 'other'"
        );
    }

    #[test]
    fn stats_columns_with_predicate() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
        ]));

        let predicate = Arc::new(column_expr!("value").gt(Expr::literal(10i64)));

        let state_info = get_state_info_with_stats(
            schema,
            vec![],
            Some(predicate),
            &[], // no table features
            HashMap::new(),
            vec![],
            StatsOutputMode::AllColumns,
        )
        .unwrap();

        // physical_stats_schema should be set (from expected_stats_schema)
        assert!(
            state_info.physical_stats_schema.is_some(),
            "physical_stats_schema should be Some when AllColumns is set"
        );
        // physical_predicate should still be active for data skipping
        assert!(
            matches!(state_info.physical_predicate, PhysicalPredicate::Some(..)),
            "physical_predicate should be PhysicalPredicate::Some for data skipping"
        );
    }

    #[test]
    fn stats_columns_with_predicate_merges_columns() {
        // When specific stats_columns are requested alongside a predicate, the stats
        // schema should include both the requested columns and predicate-referenced columns.
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
            StructField::nullable("extra", DataType::LONG),
        ]));

        let predicate = Arc::new(column_expr!("extra").gt(Expr::literal(5i64)));

        let state_info = get_state_info_with_stats(
            schema,
            vec![],
            Some(predicate),
            &[],
            HashMap::new(),
            vec![],
            StatsOutputMode::Columns(vec![column_name!("value")]),
        )
        .unwrap();

        let stats_schema = state_info
            .physical_stats_schema
            .expect("should have physical stats schema");

        let min_values = stats_schema
            .field(MIN_VALUES)
            .expect("should have minValues");
        if let DataType::Struct(inner) = min_values.data_type() {
            assert!(
                inner.field("value").is_some(),
                "minValues should have 'value' (requested)"
            );
            assert!(
                inner.field("extra").is_some(),
                "minValues should have 'extra' (from predicate)"
            );
            assert!(
                inner.field("id").is_none(),
                "minValues should not have 'id' (neither requested nor in predicate)"
            );
        } else {
            panic!("minValues should be a struct");
        }
    }

    #[test]
    fn non_empty_stats_columns_filters_schema() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING),
            StructField::nullable("value", DataType::LONG),
        ]));

        let state_info = get_state_info_with_stats(
            schema,
            vec![],
            None,
            &[], // no table features
            HashMap::new(),
            vec![],
            StatsOutputMode::Columns(vec![column_name!("value")]),
        )
        .unwrap();

        let stats_schema = state_info
            .physical_stats_schema
            .expect("should have physical stats schema");

        // Check that minValues/maxValues only contain 'value', not 'id'
        let min_values = stats_schema
            .field(MIN_VALUES)
            .expect("should have minValues");
        if let DataType::Struct(inner) = min_values.data_type() {
            assert!(
                inner.field("value").is_some(),
                "minValues should have 'value'"
            );
            assert!(
                inner.field("id").is_none(),
                "minValues should not have 'id'"
            );
        } else {
            panic!("minValues should be a struct");
        }
    }

    #[test]
    fn partition_schema_uses_physical_names_with_column_mapping() {
        // Verify that physical_partition_schema uses physical column names when column
        // mapping is enabled. The logical partition column "date" has physical name
        // "col-date-phys", and the schema should reflect the physical name.
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("id", DataType::STRING).with_metadata(HashMap::from([
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName
                        .as_ref()
                        .to_string(),
                    MetadataValue::String("col-id-phys".to_string()),
                ),
            ])),
            StructField::nullable("date", DataType::DATE).with_metadata(HashMap::from([
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                    MetadataValue::Number(2),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName
                        .as_ref()
                        .to_string(),
                    MetadataValue::String("col-date-phys".to_string()),
                ),
            ])),
            StructField::nullable("value", DataType::LONG).with_metadata(HashMap::from([
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                    MetadataValue::Number(3),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName
                        .as_ref()
                        .to_string(),
                    MetadataValue::String("col-value-phys".to_string()),
                ),
            ])),
        ]));

        let predicate = Arc::new(column_expr!("date").lt(Expr::literal(100i32)));

        let state_info = get_state_info(
            schema,
            vec!["date".to_string()],
            Some(predicate),
            &[TableFeature::ColumnMapping],
            get_string_map(&[("delta.columnMapping.mode", "name")]),
            vec![],
        )
        .unwrap();

        // physical_partition_schema should exist and use the physical column name
        let partition_schema = state_info
            .physical_partition_schema
            .as_ref()
            .expect("should have physical_partition_schema with predicate + partition columns");
        assert_eq!(partition_schema.num_fields(), 1);
        let field = partition_schema.fields().next().unwrap();
        assert_eq!(
            field.name(),
            "col-date-phys",
            "partition schema should use physical column name, not logical"
        );
        assert_eq!(field.data_type(), &DataType::DATE);
    }

    #[test]
    fn stats_columns_with_column_mapping_uses_physical_names() {
        let field_a: StructField = serde_json::from_value(serde_json::json!({
            "name": "col_a",
            "type": "long",
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": 1,
                "delta.columnMapping.physicalName": "phys_a"
            }
        }))
        .unwrap();

        let field_b: StructField = serde_json::from_value(serde_json::json!({
            "name": "col_b",
            "type": "long",
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": 2,
                "delta.columnMapping.physicalName": "phys_b"
            }
        }))
        .unwrap();

        let field_c: StructField = serde_json::from_value(serde_json::json!({
            "name": "col_c",
            "type": "long",
            "nullable": true,
            "metadata": {
                "delta.columnMapping.id": 3,
                "delta.columnMapping.physicalName": "phys_c"
            }
        }))
        .unwrap();

        let schema = Arc::new(StructType::new_unchecked(vec![field_a, field_b, field_c]));
        let mut props = HashMap::new();
        props.insert("delta.columnMapping.mode".to_string(), "name".to_string());

        // Request col_a via stats_columns (logical), and reference col_b via predicate (logical).
        // Both must be translated to physical names in the output stats schema.
        let predicate = Arc::new(column_expr!("col_b").gt(Expr::literal(5i64)));

        let state_info = get_state_info_with_stats(
            schema,
            vec![],
            Some(predicate),
            &[],
            props,
            vec![],
            StatsOutputMode::Columns(vec![column_name!("col_a")]),
        )
        .unwrap();

        let stats_schema = state_info
            .physical_stats_schema
            .expect("should have physical stats schema");

        let present = ["phys_a", "phys_b"];
        let absent = ["col_a", "col_b", "phys_c"];
        for stats_field in [MIN_VALUES, MAX_VALUES] {
            let DataType::Struct(inner) = stats_schema
                .field(stats_field)
                .unwrap_or_else(|| panic!("should have {stats_field}"))
                .data_type()
            else {
                panic!("{stats_field} should be a struct");
            };
            for name in present {
                assert!(
                    inner.field(name).is_some(),
                    "{stats_field} expected '{name}'"
                );
            }
            for name in absent {
                assert!(
                    inner.field(name).is_none(),
                    "{stats_field} unexpected '{name}'"
                );
            }
        }
    }

    // === physical_stats_columns trims the predicate-derived stats schema ===

    /// Flat schema with `n` long columns named `c0..c{n-1}`.
    fn flat_long_schema(n: usize) -> SchemaRef {
        Arc::new(StructType::new_unchecked(
            (0..n)
                .map(|i| StructField::nullable(format!("c{i}"), DataType::LONG))
                .collect::<Vec<_>>(),
        ))
    }

    /// `delta.dataSkippingNumIndexedCols=<n>` configuration map.
    fn num_indexed_cols_config(n: i32) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert(
            "delta.dataSkippingNumIndexedCols".to_string(),
            n.to_string(),
        );
        m
    }

    /// `delta.dataSkippingStatsColumns=<cols joined by ",">` configuration map.
    fn stats_columns_config(cols: &[&str]) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("delta.dataSkippingStatsColumns".to_string(), cols.join(","));
        m
    }

    /// Both `delta.dataSkippingStatsColumns` and `delta.dataSkippingNumIndexedCols` set
    /// simultaneously. Per the Delta protocol, the explicit list takes precedence over the
    /// cap; this helper exists for tests that exercise that precedence.
    fn both_configs(stats_cols: &[&str], num_indexed: i32) -> HashMap<String, String> {
        let mut m = stats_columns_config(stats_cols);
        m.insert(
            "delta.dataSkippingNumIndexedCols".to_string(),
            num_indexed.to_string(),
        );
        m
    }

    /// `delta.dataSkippingNumIndexedCols` caps the `physical_stats_columns` set to the
    /// first N leaves.
    #[test]
    fn stats_columns_honors_num_indexed_cols() {
        let schema = flat_long_schema(5);
        let state_info = get_state_info(
            schema,
            vec![],
            None, // no predicate; just check the cached set
            &[],
            num_indexed_cols_config(2),
            vec![],
        )
        .unwrap();
        let cols: HashSet<ColumnName> =
            ["c0", "c1"].iter().map(|s| ColumnName::new([*s])).collect();
        assert_eq!(state_info.physical_stats_columns, cols);
    }

    /// Predicate on a past-cap column: stats schema goes to `None` (no skipping), but
    /// the physical predicate is retained so engines can still apply it per-row.
    #[test]
    fn predicate_on_past_cap_column_drops_stats_schema() {
        let schema = flat_long_schema(5);
        let predicate = Arc::new(column_expr!("c4").gt(Expr::literal(10i64)));
        let state_info = get_state_info(
            schema,
            vec![],
            Some(predicate),
            &[],
            num_indexed_cols_config(2),
            vec![],
        )
        .unwrap();
        assert!(
            state_info.physical_stats_schema.is_none(),
            "Predicate on a past-cap column should produce no stats schema, got {:?}",
            state_info.physical_stats_schema
        );
        assert!(
            matches!(state_info.physical_predicate, PhysicalPredicate::Some(_, _)),
            "User predicate must be retained even when stats schema is dropped"
        );
    }

    /// Indexed AND past-cap: indexed leaf survives in the stats schema, past-cap leaf
    /// is dropped.
    #[test]
    fn predicate_on_mixed_indexed_and_past_cap_keeps_indexed_only() {
        let schema = flat_long_schema(5);
        let predicate = Arc::new(Pred::and(
            column_expr!("c0").gt(Expr::literal(10i64)),
            column_expr!("c4").gt(Expr::literal(10i64)),
        ));
        let state_info = get_state_info(
            schema,
            vec![],
            Some(predicate),
            &[],
            num_indexed_cols_config(2),
            vec![],
        )
        .unwrap();
        let stats_schema = state_info
            .physical_stats_schema
            .as_ref()
            .expect("should have stats schema (indexed arm survives)");
        for stats_field in [MIN_VALUES, MAX_VALUES] {
            let DataType::Struct(inner) = stats_schema
                .field(stats_field)
                .unwrap_or_else(|| panic!("should have {stats_field}"))
                .data_type()
            else {
                panic!("{stats_field} should be a struct");
            };
            assert!(
                inner.field("c0").is_some(),
                "{stats_field} should contain c0 (indexed)"
            );
            assert!(
                inner.field("c4").is_none(),
                "{stats_field} should NOT contain c4 (past cap)"
            );
        }
    }

    /// `numIndexedCols=2` against `{ a, b, s: { c, d } }` keeps `a, b` and drops the
    /// entire `s` struct, so a predicate on `s.c` produces no stats schema.
    #[test]
    fn predicate_on_nested_past_cap_leaf_drops_parent_struct() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable(
                "s",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::nullable("c", DataType::LONG),
                    StructField::nullable("d", DataType::LONG),
                ]))),
            ),
        ]));
        // Predicate only on the past-cap leaf -> stats schema goes empty -> None.
        let predicate = Arc::new(column_expr!("s.c").gt(Expr::literal(10i64)));
        let state_info = get_state_info(
            schema,
            vec![],
            Some(predicate),
            &[],
            num_indexed_cols_config(2),
            vec![],
        )
        .unwrap();
        assert!(state_info.physical_stats_schema.is_none());
        assert!(!state_info.physical_stats_columns.is_empty());
        assert!(!state_info
            .physical_stats_columns
            .contains(&ColumnName::new(["s", "c"])));
    }

    /// `delta.dataSkippingStatsColumns` selects exactly the listed leaves, regardless of
    /// their position relative to the (default) cap.
    #[rstest]
    #[case::single(&["c2"], &["c2"])]
    #[case::sparse_subset(&["c0", "c3"], &["c0", "c3"])]
    #[case::all_listed(&["c0", "c1", "c2", "c3", "c4"], &["c0", "c1", "c2", "c3", "c4"])]
    fn stats_columns_honors_explicit_stats_columns(
        #[case] listed: &[&str],
        #[case] expected: &[&str],
    ) {
        let schema = flat_long_schema(5);
        let state_info = get_state_info(
            schema,
            vec![],
            None,
            &[],
            stats_columns_config(listed),
            vec![],
        )
        .unwrap();
        let expected_cols: HashSet<ColumnName> =
            expected.iter().map(|s| ColumnName::new([*s])).collect();
        assert_eq!(state_info.physical_stats_columns, expected_cols);
    }

    /// `numIndexedCols=3` against `{ a, b, s: { c, d } }` keeps `a, b, s.c` and drops
    /// `s.d`. A predicate on both `s.c` and `s.d` keeps the `s` struct under
    /// `minValues` / `maxValues` with `c` only.
    #[test]
    fn predicate_on_nested_mixed_keeps_intersection_under_parent_struct() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
            StructField::nullable(
                "s",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::nullable("c", DataType::LONG),
                    StructField::nullable("d", DataType::LONG),
                ]))),
            ),
        ]));
        let predicate = Arc::new(Pred::and(
            column_expr!("s.c").gt(Expr::literal(10i64)),
            column_expr!("s.d").gt(Expr::literal(10i64)),
        ));
        let state_info = get_state_info(
            schema,
            vec![],
            Some(predicate),
            &[],
            num_indexed_cols_config(3),
            vec![],
        )
        .unwrap();
        let stats_schema = state_info
            .physical_stats_schema
            .as_ref()
            .expect("indexed arm survives");
        for stats_field in [MIN_VALUES, MAX_VALUES] {
            let DataType::Struct(outer) = stats_schema
                .field(stats_field)
                .unwrap_or_else(|| panic!("should have {stats_field}"))
                .data_type()
            else {
                panic!("{stats_field} should be a struct");
            };
            let DataType::Struct(inner) = outer
                .field("s")
                .unwrap_or_else(|| panic!("{stats_field} should have s"))
                .data_type()
            else {
                panic!("{stats_field}.s should be a struct");
            };
            assert!(
                inner.field("c").is_some(),
                "{stats_field}.s should keep c (indexed)"
            );
            assert!(
                inner.field("d").is_none(),
                "{stats_field}.s should drop d (past cap)"
            );
        }
    }

    /// `dataSkippingStatsColumns` with a parent struct path admits every leaf under that
    /// parent (the trie matches by prefix). `{ a, s: { c, d } }` with the property set to
    /// `"s"` should produce `{ s.c, s.d }` (and exclude `a`, which is not in the list).
    #[test]
    fn stats_columns_admits_all_children_of_nested_parent_in_explicit_list() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("a", DataType::LONG),
            StructField::nullable(
                "s",
                DataType::Struct(Box::new(StructType::new_unchecked(vec![
                    StructField::nullable("c", DataType::LONG),
                    StructField::nullable("d", DataType::LONG),
                ]))),
            ),
        ]));
        let state_info = get_state_info(
            schema,
            vec![],
            None,
            &[],
            stats_columns_config(&["s"]),
            vec![],
        )
        .unwrap();
        let expected: HashSet<ColumnName> =
            [ColumnName::new(["s", "c"]), ColumnName::new(["s", "d"])]
                .into_iter()
                .collect();
        assert_eq!(state_info.physical_stats_columns, expected);
    }

    /// `dataSkippingStatsColumns` ("A") takes precedence over `dataSkippingNumIndexedCols`
    /// ("B") whether A wants more columns than B allows or fewer.
    #[rstest]
    #[case::a_broader_than_b(&["c0", "c3", "c4"], 2, &["c0", "c3", "c4"])]
    #[case::a_narrower_than_b(&["c0"], 3, &["c0"])]
    fn stats_columns_explicit_list_overrides_num_indexed_cols(
        #[case] listed: &[&str],
        #[case] num_indexed: i32,
        #[case] expected: &[&str],
    ) {
        let schema = flat_long_schema(5);
        let state_info = get_state_info(
            schema,
            vec![],
            None,
            &[],
            both_configs(listed, num_indexed),
            vec![],
        )
        .unwrap();
        let expected_cols: HashSet<ColumnName> =
            expected.iter().map(|s| ColumnName::new([*s])).collect();
        assert_eq!(state_info.physical_stats_columns, expected_cols);
    }
}
