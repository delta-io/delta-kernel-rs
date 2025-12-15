use std::clone::Clone;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use super::data_skipping::DataSkippingFilter;
use super::state_info::StateInfo;
use super::{PhysicalPredicate, ScanMetadata};
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::get_log_add_schema;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{column_name, ColumnName, Expression, ExpressionRef, PredicateRef};
use crate::kernel_predicates::{DefaultKernelPredicateEvaluator, KernelPredicateEvaluator as _};
use crate::log_replay::{ActionsBatch, FileActionDeduplicator, FileActionKey, LogReplayProcessor};
use crate::scan::Scalar;
use crate::schema::ToSchema as _;
use crate::schema::{ColumnNamesAndTypes, DataType, MapType, StructField, StructType};
use crate::transforms::{get_transform_expr, parse_partition_values};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, ExpressionEvaluator};

/// [`ScanLogReplayProcessor`] performs log replay (processes actions) specifically for doing a table scan.
///
/// During a table scan, the processor reads batches of log actions (in reverse chronological order)
/// and performs the following steps:
///
/// - Data Skipping: Applies a predicate-based filter (via [`DataSkippingFilter`]) to quickly skip
///   files that are irrelevant for the query.
/// - Partition Pruning: Uses an optional partition filter (extracted from a physical predicate)
///   to exclude actions whose partition values do not meet the required criteria.
/// - Action Deduplication: Leverages the [`FileActionDeduplicator`] to ensure that for each unique file
///   (identified by its path and deletion vector unique ID), only the latest valid Add action is processed.
/// - Transformation: Applies a built-in transformation (`add_transform`) to convert selected Add actions
///   into [`ScanMetadata`], the intermediate format passed to the engine.
/// - Row Transform Passthrough: Any user-provided row-level transformation expressions (e.g. those derived
///   from projection or filters) are preserved and passed through to the engine, which applies them as part
///   of its scan execution logic.
///
/// As an implementation of [`LogReplayProcessor`], [`ScanLogReplayProcessor`] provides the
/// `process_actions_batch` method, which applies these steps to each batch of log actions and
/// produces a [`ScanMetadata`] result. This result includes the transformed batch, a selection
/// vector indicating which rows are valid, and any row-level transformation expressions that need
/// to be applied to the selected rows.
pub(crate) struct ScanLogReplayProcessor {
    partition_filter: Option<PredicateRef>,
    data_skipping_filter: Option<DataSkippingFilter>,
    add_transform: Arc<dyn ExpressionEvaluator>,
    state_info: Arc<StateInfo>,
    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to filter out files with Remove actions as
    /// well as duplicate entries in the log.
    seen_file_keys: HashSet<FileActionKey>,
}

impl ScanLogReplayProcessor {
    /// Create a new [`ScanLogReplayProcessor`] instance
    pub(crate) fn new(engine: &dyn Engine, state_info: Arc<StateInfo>) -> DeltaResult<Self> {
        // Extract the physical predicate from StateInfo's PhysicalPredicate enum.
        // The DataSkippingFilter and partition_filter components expect the predicate
        // in the format Option<(PredicateRef, SchemaRef)>, so we need to convert from
        // the enum representation to the tuple format.
        let physical_predicate = match &state_info.physical_predicate {
            PhysicalPredicate::Some(predicate, schema) => {
                // Valid predicate that can be used for data skipping and partition filtering
                Some((predicate.clone(), schema.clone()))
            }
            PhysicalPredicate::StaticSkipAll => {
                debug_assert!(false, "StaticSkipAll case should be handled at a higher level and not reach this code");
                None
            }
            PhysicalPredicate::None => {
                // No predicate provided
                None
            }
        };
        Ok(Self {
            partition_filter: physical_predicate.as_ref().map(|(e, _)| e.clone()),
            data_skipping_filter: DataSkippingFilter::new(engine, physical_predicate),
            add_transform: engine.evaluation_handler().new_expression_evaluator(
                get_log_add_schema().clone(),
                get_add_transform_expr(),
                SCAN_ROW_DATATYPE.clone(),
            )?,
            seen_file_keys: Default::default(),
            state_info,
        })
    }
}

/// A visitor that deduplicates a stream of add and remove actions into a stream of valid adds. Log
/// replay visits actions newest-first, so once we've seen a file action for a given (path, dvId)
/// pair, we should ignore all subsequent (older) actions for that same (path, dvId) pair. If the
/// first action for a given file is a remove, then that file does not show up in the result at all.
struct AddRemoveDedupVisitor<'seen> {
    deduplicator: FileActionDeduplicator<'seen>,
    selection_vector: Vec<bool>,
    state_info: Arc<StateInfo>,
    partition_filter: Option<PredicateRef>,
    row_transform_exprs: Vec<Option<ExpressionRef>>,
}

impl AddRemoveDedupVisitor<'_> {
    // These index positions correspond to the order of columns defined in
    // `selected_column_names_and_types()`
    const ADD_PATH_INDEX: usize = 0; // Position of "add.path" in getters
    const ADD_PARTITION_VALUES_INDEX: usize = 1; // Position of "add.partitionValues" in getters
    const ADD_DV_START_INDEX: usize = 2; // Start position of add deletion vector columns
    const BASE_ROW_ID_INDEX: usize = 5; // Position of add.baseRowId in getters
    const REMOVE_PATH_INDEX: usize = 6; // Position of "remove.path" in getters
    const REMOVE_DV_START_INDEX: usize = 7; // Start position of remove deletion vector columns

    fn new(
        seen: &mut HashSet<FileActionKey>,
        selection_vector: Vec<bool>,
        state_info: Arc<StateInfo>,
        partition_filter: Option<PredicateRef>,
        is_log_batch: bool,
    ) -> AddRemoveDedupVisitor<'_> {
        AddRemoveDedupVisitor {
            deduplicator: FileActionDeduplicator::new(
                seen,
                is_log_batch,
                Self::ADD_PATH_INDEX,
                Self::REMOVE_PATH_INDEX,
                Self::ADD_DV_START_INDEX,
                Self::REMOVE_DV_START_INDEX,
            ),
            selection_vector,
            state_info,
            partition_filter,
            row_transform_exprs: Vec::new(),
        }
    }

    fn is_file_partition_pruned(
        &self,
        partition_values: &HashMap<usize, (String, Scalar)>,
    ) -> bool {
        if partition_values.is_empty() {
            return false;
        }
        let Some(partition_filter) = &self.partition_filter else {
            return false;
        };
        let partition_values: HashMap<_, _> = partition_values
            .values()
            .map(|(k, v)| (ColumnName::new([k]), v.clone()))
            .collect();
        let evaluator = DefaultKernelPredicateEvaluator::from(partition_values);
        evaluator.eval_sql_where(partition_filter) == Some(false)
    }

    /// True if this row contains an Add action that should survive log replay. Skip it if the row
    /// is not an Add action, or the file has already been seen previously.
    fn is_valid_add<'a>(&mut self, i: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<bool> {
        // When processing file actions, we extract path and deletion vector information based on action type:
        // - For Add actions: path is at index 0, followed by DV fields at indexes 2-4
        // - For Remove actions (in log batches only): path is at index 5, followed by DV fields at indexes 6-8
        // The file extraction logic selects the appropriate indexes based on whether we found a valid path.
        // Remove getters are not included when visiting a non-log batch (checkpoint batch), so do
        // not try to extract remove actions in that case.
        let Some((file_key, is_add)) = self.deduplicator.extract_file_action(
            i,
            getters,
            !self.deduplicator.is_log_batch(), // skip_removes. true if this is a checkpoint batch
        )?
        else {
            return Ok(false);
        };

        // Apply partition pruning (to adds only) before deduplication, so that we don't waste memory
        // tracking pruned files. Removes don't get pruned and we'll still have to track them.
        //
        // WARNING: It's not safe to partition-prune removes (just like it's not safe to data skip
        // removes), because they are needed to suppress earlier incompatible adds we might
        // encounter if the table's schema was replaced after the most recent checkpoint.
        let partition_values = match &self.state_info.transform_spec {
            Some(transform) if is_add => {
                let partition_values =
                    getters[Self::ADD_PARTITION_VALUES_INDEX].get(i, "add.partitionValues")?;
                let partition_values = parse_partition_values(
                    &self.state_info.logical_schema,
                    transform,
                    &partition_values,
                    self.state_info.column_mapping_mode,
                )?;
                if self.is_file_partition_pruned(&partition_values) {
                    return Ok(false);
                }
                partition_values
            }
            _ => Default::default(),
        };

        // Check both adds and removes (skipping already-seen), but only transform and return adds
        if self.deduplicator.check_and_record_seen(file_key) || !is_add {
            return Ok(false);
        }
        let base_row_id: Option<i64> =
            getters[Self::BASE_ROW_ID_INDEX].get_opt(i, "add.baseRowId")?;
        let transform = self
            .state_info
            .transform_spec
            .as_ref()
            .map(|transform| {
                get_transform_expr(
                    transform,
                    partition_values,
                    &self.state_info.physical_schema,
                    base_row_id,
                )
            })
            .transpose()?;
        if transform.is_some() {
            // fill in any needed `None`s for previous rows
            self.row_transform_exprs.resize_with(i, Default::default);
            self.row_transform_exprs.push(transform);
        }
        Ok(true)
    }
}

impl RowVisitor for AddRemoveDedupVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // NOTE: The visitor assumes a schema with adds first and removes optionally afterward.
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            const LONG: DataType = DataType::LONG;
            let ss_map: DataType = MapType::new(STRING, STRING, true).into();
            let types_and_names = vec![
                (STRING, column_name!("add.path")),
                (ss_map, column_name!("add.partitionValues")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
                (LONG, column_name!("add.baseRowId")),
                (STRING, column_name!("remove.path")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        let (names, types) = NAMES_AND_TYPES.as_ref();
        if self.deduplicator.is_log_batch() {
            (names, types)
        } else {
            // All checkpoint actions are already reconciled and Remove actions in checkpoint files
            // only serve as tombstones for vacuum jobs. So we only need to examine the adds here.
            (&names[..6], &types[..6])
        }
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let is_log_batch = self.deduplicator.is_log_batch();
        let expected_getters = if is_log_batch { 10 } else { 6 };
        require!(
            getters.len() == expected_getters,
            Error::InternalError(format!(
                "Wrong number of AddRemoveDedupVisitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            if self.selection_vector[i] {
                self.selection_vector[i] = self.is_valid_add(i, getters)?;
            }
        }
        Ok(())
    }
}

pub(crate) static FILE_CONSTANT_VALUES_NAME: &str = "fileConstantValues";
pub(crate) static BASE_ROW_ID_NAME: &str = "baseRowId";
pub(crate) static DEFAULT_ROW_COMMIT_VERSION_NAME: &str = "defaultRowCommitVersion";
pub(crate) static TAGS_NAME: &str = "tags";

// NB: If you update this schema, ensure you update the comment describing it in the doc comment
// for `scan_row_schema` in scan/mod.rs! You'll also need to update ScanFileVisitor as the
// indexes will be off, and [`get_add_transform_expr`] below to match it.
pub(crate) static SCAN_ROW_SCHEMA: LazyLock<Arc<StructType>> = LazyLock::new(|| {
    // Note that fields projected out of a nullable struct must be nullable
    let partition_values = MapType::new(DataType::STRING, DataType::STRING, true);
    let file_constant_values = StructType::new_unchecked([
        StructField::nullable("partitionValues", partition_values),
        StructField::nullable(BASE_ROW_ID_NAME, DataType::LONG),
        StructField::nullable(DEFAULT_ROW_COMMIT_VERSION_NAME, DataType::LONG),
        StructField::nullable(
            "tags",
            MapType::new(
                DataType::STRING,
                DataType::STRING,
                /*valueContainsNull*/ true,
            ),
        ),
    ]);
    Arc::new(StructType::new_unchecked([
        StructField::nullable("path", DataType::STRING),
        StructField::nullable("size", DataType::LONG),
        StructField::nullable("modificationTime", DataType::LONG),
        StructField::nullable("stats", DataType::STRING),
        StructField::nullable("deletionVector", DeletionVectorDescriptor::to_schema()),
        StructField::nullable(FILE_CONSTANT_VALUES_NAME, file_constant_values),
    ]))
});

pub(crate) static SCAN_ROW_DATATYPE: LazyLock<DataType> =
    LazyLock::new(|| SCAN_ROW_SCHEMA.clone().into());

fn get_add_transform_expr() -> ExpressionRef {
    use crate::expressions::column_expr_ref;
    static EXPR: LazyLock<ExpressionRef> = LazyLock::new(|| {
        Arc::new(Expression::Struct(vec![
            column_expr_ref!("add.path"),
            column_expr_ref!("add.size"),
            column_expr_ref!("add.modificationTime"),
            column_expr_ref!("add.stats"),
            column_expr_ref!("add.deletionVector"),
            Arc::new(Expression::Struct(vec![
                column_expr_ref!("add.partitionValues"),
                column_expr_ref!("add.baseRowId"),
                column_expr_ref!("add.defaultRowCommitVersion"),
                column_expr_ref!("add.tags"),
            ])),
        ]))
    });
    EXPR.clone()
}

// TODO: remove once `scan_metadata_from` is pub.
#[allow(unused)]
pub(crate) fn get_scan_metadata_transform_expr() -> ExpressionRef {
    use crate::expressions::column_expr_ref;
    static EXPR: LazyLock<ExpressionRef> = LazyLock::new(|| {
        Arc::new(Expression::Struct(vec![Arc::new(Expression::Struct(
            vec![
                column_expr_ref!("path"),
                column_expr_ref!("fileConstantValues.partitionValues"),
                column_expr_ref!("size"),
                column_expr_ref!("modificationTime"),
                column_expr_ref!("stats"),
                column_expr_ref!("fileConstantValues.tags"),
                column_expr_ref!("deletionVector"),
                column_expr_ref!("fileConstantValues.baseRowId"),
                column_expr_ref!("fileConstantValues.defaultRowCommitVersion"),
            ],
        ))]))
    });
    EXPR.clone()
}

impl LogReplayProcessor for ScanLogReplayProcessor {
    type Output = ScanMetadata;

    fn process_actions_batch(&mut self, actions_batch: ActionsBatch) -> DeltaResult<Self::Output> {
        let ActionsBatch {
            actions,
            is_log_batch,
        } = actions_batch;
        // Build an initial selection vector for the batch which has had the data skipping filter
        // applied. The selection vector is further updated by the deduplication visitor to remove
        // rows that are not valid adds.
        let selection_vector = self.build_selection_vector(actions.as_ref())?;
        assert_eq!(selection_vector.len(), actions.len());

        let mut visitor = AddRemoveDedupVisitor::new(
            &mut self.seen_file_keys,
            selection_vector,
            self.state_info.clone(),
            self.partition_filter.clone(),
            is_log_batch,
        );
        visitor.visit_rows_of(actions.as_ref())?;

        // TODO: Teach expression eval to respect the selection vector we just computed so carefully!
        let result = self.add_transform.evaluate(actions.as_ref())?;
        ScanMetadata::try_new(
            result,
            visitor.selection_vector,
            visitor.row_transform_exprs,
        )
    }

    fn data_skipping_filter(&self) -> Option<&DataSkippingFilter> {
        self.data_skipping_filter.as_ref()
    }
}

/// Given an iterator of [`ActionsBatch`]s (batches of actions read from the log) and a predicate,
/// returns an iterator of [`ScanMetadata`]s (which includes the files to be scanned as
/// [`FilteredEngineData`] and transforms that must be applied to correctly read the data). Each row
/// that is selected in the returned `engine_data` _must_ be processed to complete the scan.
/// Non-selected rows _must_ be ignored.
///
/// Note: The iterator of [`ActionsBatch`]s ('action_iter' parameter) must be sorted by the order of
/// the actions in the log from most recent to least recent.
pub(crate) fn scan_action_iter(
    engine: &dyn Engine,
    action_iter: impl Iterator<Item = DeltaResult<ActionsBatch>>,
    state_info: Arc<StateInfo>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadata>>> {
    Ok(ScanLogReplayProcessor::new(engine, state_info)?.process_actions_iter(action_iter))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::actions::get_commit_schema;
    use crate::expressions::{BinaryExpressionOp, Scalar, VariadicExpressionOp};
    use crate::log_replay::ActionsBatch;
    use crate::scan::state::{DvInfo, Stats};
    use crate::scan::state_info::tests::{
        assert_transform_spec, get_simple_state_info, get_state_info,
    };
    use crate::scan::state_info::StateInfo;
    use crate::scan::test_utils::{
        add_batch_for_row_id, add_batch_simple, add_batch_with_partition_col,
        add_batch_with_remove, run_with_validate_callback,
    };
    use crate::scan::PhysicalPredicate;
    use crate::schema::MetadataColumnSpec;
    use crate::table_features::ColumnMappingMode;
    use crate::Expression as Expr;
    use crate::{
        engine::sync::SyncEngine,
        schema::{DataType, SchemaRef, StructField, StructType},
        ExpressionRef,
    };

    use super::scan_action_iter;

    // dv-info is more complex to validate, we validate that works in the test for visit_scan_files
    // in state.rs
    fn validate_simple(
        _: &mut (),
        path: &str,
        size: i64,
        stats: Option<Stats>,
        _: DvInfo,
        _: Option<ExpressionRef>,
        part_vals: HashMap<String, String>,
    ) {
        assert_eq!(
            path,
            "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
        );
        assert_eq!(size, 635);
        assert!(stats.is_some());
        assert_eq!(stats.as_ref().unwrap().num_records, 10);
        assert_eq!(part_vals.get("date"), Some(&"2017-12-10".to_string()));
        assert_eq!(part_vals.get("non-existent"), None);
    }

    #[test]
    fn test_scan_action_iter() {
        run_with_validate_callback(
            vec![add_batch_simple(get_commit_schema().clone())],
            None, // not testing schema
            None, // not testing transform
            &[true, false],
            (),
            validate_simple,
        );
    }

    #[test]
    fn test_scan_action_iter_with_remove() {
        run_with_validate_callback(
            vec![add_batch_with_remove(get_commit_schema().clone())],
            None, // not testing schema
            None, // not testing transform
            &[false, false, true, false],
            (),
            validate_simple,
        );
    }

    #[test]
    fn test_no_transforms() {
        let batch = vec![add_batch_simple(get_commit_schema().clone())];
        let logical_schema = Arc::new(StructType::new_unchecked(vec![]));
        let state_info = Arc::new(StateInfo {
            logical_schema: logical_schema.clone(),
            physical_schema: logical_schema.clone(),
            physical_predicate: PhysicalPredicate::None,
            transform_spec: None,
            column_mapping_mode: ColumnMappingMode::None,
        });
        let iter = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            state_info,
        )
        .unwrap();
        for res in iter {
            let scan_metadata = res.unwrap();
            assert!(
                scan_metadata.scan_file_transforms.is_empty(),
                "Should have no transforms"
            );
        }
    }

    #[test]
    fn test_simple_transform() {
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([
            StructField::new("value", DataType::INTEGER, true),
            StructField::new("date", DataType::DATE, true),
        ]));
        let partition_cols = vec!["date".to_string()];
        let state_info = get_simple_state_info(schema, partition_cols).unwrap();
        let batch = vec![add_batch_with_partition_col()];
        let iter = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            Arc::new(state_info),
        )
        .unwrap();

        fn validate_transform(transform: Option<&ExpressionRef>, expected_date_offset: i32) {
            assert!(transform.is_some());
            let Expr::Transform(transform) = transform.unwrap().as_ref() else {
                panic!("Transform should always be a Transform expr");
            };

            // With sparse transforms, we expect only one insertion for the partition column
            assert!(transform.prepended_fields.is_empty());
            let mut field_transforms = transform.field_transforms.iter();
            let (field_name, field_transform) = field_transforms.next().unwrap();
            assert_eq!(field_name, "value");
            assert!(!field_transform.is_replace);
            let [expr] = &field_transform.exprs[..] else {
                panic!("Expected a single insertion");
            };
            let Expr::Literal(Scalar::Date(date_offset)) = expr.as_ref() else {
                panic!("Expected a literal date");
            };
            assert_eq!(*date_offset, expected_date_offset);
            assert!(field_transforms.next().is_none());
        }

        for res in iter {
            let scan_metadata = res.unwrap();
            let transforms = scan_metadata.scan_file_transforms;
            // in this case we have a metadata action first and protocol 3rd, so we expect 4 items,
            // the first and 3rd being a `None`
            assert_eq!(transforms.len(), 4, "Should have 4 transforms");
            assert!(transforms[0].is_none(), "transform at [0] should be None");
            assert!(transforms[2].is_none(), "transform at [2] should be None");
            validate_transform(transforms[1].as_ref(), 17511);
            validate_transform(transforms[3].as_ref(), 17510);
        }
    }

    #[test]
    fn test_row_id_transform() {
        let schema: SchemaRef = Arc::new(StructType::new_unchecked([StructField::new(
            "value",
            DataType::INTEGER,
            true,
        )]));
        let state_info = get_state_info(
            schema.clone(),
            vec![],
            None,
            [
                ("delta.enableRowTracking", "true"),
                (
                    "delta.rowTracking.materializedRowIdColumnName",
                    "row_id_col",
                ),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            vec![("row_id", MetadataColumnSpec::RowId)],
        )
        .unwrap();

        let transform_spec = state_info.transform_spec.as_ref().unwrap();
        assert_transform_spec(
            transform_spec,
            false,
            "row_id_col",
            "row_indexes_for_row_id_0",
        );

        let batch = vec![add_batch_for_row_id(get_commit_schema().clone())];
        let iter = scan_action_iter(
            &SyncEngine::new(),
            batch
                .into_iter()
                .map(|batch| Ok(ActionsBatch::new(batch as _, true))),
            Arc::new(state_info),
        )
        .unwrap();

        for res in iter {
            let scan_metadata = res.unwrap();
            let transforms = scan_metadata.scan_file_transforms;
            assert_eq!(transforms.len(), 1, "Should have 1 transform");
            if let Some(Expr::Transform(transform_expr)) = transforms[0].as_ref().map(Arc::as_ref) {
                assert!(transform_expr.input_path.is_none());
                let row_id_transform = transform_expr
                    .field_transforms
                    .get("row_id_col")
                    .expect("Should have row_id_col transform");
                assert!(row_id_transform.is_replace);
                assert_eq!(row_id_transform.exprs.len(), 1);
                let expr = &row_id_transform.exprs[0];
                let expeceted_expr = Arc::new(Expr::variadic(
                    VariadicExpressionOp::Coalesce,
                    vec![
                        Expr::column(["row_id_col"]),
                        Expr::binary(
                            BinaryExpressionOp::Plus,
                            Expr::literal(42i64),
                            Expr::column(["row_indexes_for_row_id_0"]),
                        ),
                    ],
                ));
                assert_eq!(expr, &expeceted_expr);
            } else {
                panic!("Should have been a transform expression");
            }
        }
    }
}
