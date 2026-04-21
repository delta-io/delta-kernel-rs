use std::sync::{Arc, Mutex};

use itertools::Itertools;

use crate::action_reconciliation::ActionReconciliationIterator;
use crate::actions::{ADD_NAME, REMOVE_NAME, SIDECAR_NAME};
use crate::engine_data::filter_by_predicate;
use crate::expressions::{Expression, Predicate, Scalar, StructData, Transform};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{
    DeltaResult, Engine, EngineData, Error, EvaluationHandler, ExpressionEvaluator, FileMeta,
    PredicateEvaluator,
};

/// Field names of the `sidecar` action struct.
const SIDECAR_SCHEMA_PATH: &str = "path";
const SIDECAR_SCHEMA_SIZE_IN_BYTES: &str = "sizeInBytes";
const SIDECAR_SCHEMA_MODIFICATION_TIME: &str = "modificationTime";
const SIDECAR_SCHEMA_TAGS: &str = "tags";

/// Creates a single [`EngineData`] batch containing one row per sidecar file.
///
/// Each row has the `sidecar` field populated and all other fields set to null.
/// All rows are emitted in one batch via [`EvaluationHandler::create_many`].
///
/// Returns an empty `Vec` when there are no sidecars.
///
/// # Parameters
/// - `engine`: Implementation of [`Engine`] apis.
/// - `checkpoint_data_schema`: The checkpoint data schema.
/// - `sidecar_metas`: Pairs of (sidecar filename, FileMeta) for each sidecar file.
pub(super) fn create_sidecar_action_batches(
    engine: &dyn Engine,
    checkpoint_data_schema: &SchemaRef,
    sidecar_metas: &[(String, FileMeta)],
) -> DeltaResult<Vec<Box<dyn EngineData>>> {
    if sidecar_metas.is_empty() {
        return Ok(Vec::new());
    }

    // Derive the sidecar struct schema from the checkpoint data schema.
    let sidecar_field = checkpoint_data_schema
        .field(SIDECAR_NAME)
        .ok_or_else(|| Error::internal_error("checkpoint schema missing sidecar field"))?;
    let DataType::Struct(sidecar_struct) = sidecar_field.data_type() else {
        return Err(Error::internal_error(format!(
            "expected sidecar field to be struct, got {:?}",
            sidecar_field.data_type()
        )));
    };
    let sidecar_fields: Vec<StructField> = sidecar_struct.fields().cloned().collect();

    let checkpoint_data_fields: Vec<&StructField> = checkpoint_data_schema.fields().collect();
    let sidecar_col_idx = checkpoint_data_fields
        .iter()
        .position(|f| f.name() == SIDECAR_NAME)
        .ok_or_else(|| Error::internal_error("checkpoint schema missing sidecar field"))?;
    let null_template: Vec<Scalar> = checkpoint_data_fields
        .iter()
        .map(|f| Scalar::Null(f.data_type().clone()))
        .collect();

    // Build one row per sidecar.
    let rows: Vec<Vec<Scalar>> = sidecar_metas
        .iter()
        .map(|(filename, meta)| -> DeltaResult<Vec<Scalar>> {
            let size_in_bytes = i64::try_from(meta.size).map_err(|e| {
                Error::CheckpointWrite(format!(
                    "Failed to convert sidecar size {} to i64: {e}",
                    meta.size
                ))
            })?;

            // Sidecar struct values, ordered to match `sidecar_fields`.
            let values: Vec<Scalar> = sidecar_fields
                .iter()
                .map(|field| match field.name().as_str() {
                    // Per the protocol, implementations are encouraged to store only
                    // the file's name for the `path` field.
                    SIDECAR_SCHEMA_PATH => Ok(Scalar::from(filename.clone())),
                    SIDECAR_SCHEMA_SIZE_IN_BYTES => Ok(Scalar::from(size_in_bytes)),
                    SIDECAR_SCHEMA_MODIFICATION_TIME => Ok(Scalar::from(meta.last_modified)),
                    // `StructData::try_new` requires one value per schema field, so we
                    // emit `Scalar::Null` for `tags`. Sidecar tags are protocol details;
                    // we can expose them to connectors if there is a need in the future.
                    SIDECAR_SCHEMA_TAGS => Ok(Scalar::Null(field.data_type().clone())),
                    other => Err(Error::CheckpointWrite(format!(
                        "Unexpected sidecar field: {other}"
                    ))),
                })
                .try_collect()?;

            let mut row = null_template.clone();
            row[sidecar_col_idx] =
                Scalar::Struct(StructData::try_new(sidecar_fields.clone(), values)?);
            Ok(row)
        })
        .try_collect()?;

    let row_refs: Vec<&[Scalar]> = rows.iter().map(Vec::as_slice).collect();
    let batch = engine
        .evaluation_handler()
        .create_many(checkpoint_data_schema.clone(), &row_refs)?;
    Ok(vec![batch])
}

/// Transforms [`EngineData`] and filters out all-null rows.
struct NonNullRowsPicker {
    transform: Arc<dyn ExpressionEvaluator>,
    null_row_filter: Arc<dyn PredicateEvaluator>,
}

impl NonNullRowsPicker {
    fn pick(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        let transformed = self.transform.evaluate(batch)?;
        filter_by_predicate(self.null_row_filter.as_ref(), transformed)
    }
}

/// Splits checkpoint data into file-action batches (for sidecars) and non-file-action batches
/// (for the main checkpoint file).
///
/// # Example
///
/// Given a checkpoint data batch with columns `[add, remove, protocol, metadata, txn]`:
///
/// ```text
/// file_actions_picker   -> projects [add, remove], drops rows where both are null
/// non_file_actions_picker -> nulls out add/remove, drops rows where all remaining are null
/// ```
///
/// So a batch like:
///
/// ```text
/// | add    | remove | protocol | metadata | txn  |
/// |--------|--------|----------|----------|------|
/// | <add1> | null   | null     | null     | null |   // file action row
/// | null   | null   | <proto>  | null     | null |   // non-file action row
/// | null   | null   | null     | <meta>   | null |   // non-file action row
/// ```
///
/// is split into:
/// - file batch: `[add: <add1>, remove: null]`
/// - non-file batches:
///   - `[add: null, remove: null, protocol: <prot>, metadata: null, txn: null]`
///   - `[add: null, remove: null, protocol: null, metadata: <meta>, txn: null]`
pub(super) struct SidecarSplitter {
    checkpoint_data_iter: ActionReconciliationIterator,
    /// Projects to add/remove columns and filters out rows where both are null.
    file_actions_picker: NonNullRowsPicker,
    /// Nulls out add/remove columns and filters out rows where all remaining columns are null.
    non_file_actions_picker: NonNullRowsPicker,
    /// Accumulated non-file-action batches (protocol, metadata, txn, etc.).
    non_file_action_batches: Vec<Box<dyn EngineData>>,
    /// Set to `true` when the inner `checkpoint_data_iter` is exhausted.
    exhausted: bool,
}

impl SidecarSplitter {
    pub(super) fn new(
        checkpoint_data_iterator: ActionReconciliationIterator,
        eval_handler: &dyn EvaluationHandler,
        checkpoint_data_schema: SchemaRef,
    ) -> DeltaResult<Self> {
        // Derive sidecar output schema from the checkpoint data schema (add + remove only).
        let add_field = checkpoint_data_schema.field(ADD_NAME).ok_or_else(|| {
            Error::checkpoint_write(format!("Checkpoint data schema missing '{ADD_NAME}' field"))
        })?;
        let remove_field = checkpoint_data_schema.field(REMOVE_NAME).ok_or_else(|| {
            Error::checkpoint_write(format!(
                "Checkpoint data schema missing '{REMOVE_NAME}' field"
            ))
        })?;
        if !add_field.is_nullable() {
            return Err(Error::checkpoint_write(format!(
                "Checkpoint data schema '{ADD_NAME}' field must be nullable"
            )));
        }
        if !remove_field.is_nullable() {
            return Err(Error::checkpoint_write(format!(
                "Checkpoint data schema '{REMOVE_NAME}' field must be nullable"
            )));
        }
        let sidecar_output_schema: SchemaRef =
            StructType::try_new([add_field.clone(), remove_field.clone()])?.into();

        // Sidecar projector: select only add/remove columns.
        let file_action_projector = eval_handler.new_expression_evaluator(
            checkpoint_data_schema.clone(),
            Arc::new(Expression::struct_from([
                Expression::column([ADD_NAME]),
                Expression::column([REMOVE_NAME]),
            ])),
            sidecar_output_schema.clone().into(),
        )?;

        // Filters out rows where both add and remove are null.
        let file_actions_null_row_filter = eval_handler.new_predicate_evaluator(
            sidecar_output_schema,
            Arc::new(Predicate::or(
                Predicate::is_not_null(Expression::column([ADD_NAME])),
                Predicate::is_not_null(Expression::column([REMOVE_NAME])),
            )),
        )?;

        // Nulls out add/remove instead of dropping them so the data schema stays the
        // same as checkpoint_data_schema (add/remove columns are already nullable).
        let non_file_action_nullifier = eval_handler.new_expression_evaluator(
            checkpoint_data_schema.clone(),
            Arc::new(Expression::transform(
                Transform::new_top_level()
                    .with_replaced_field(
                        ADD_NAME,
                        Arc::new(Expression::literal(Scalar::Null(
                            add_field.data_type.clone(),
                        ))),
                    )
                    .with_replaced_field(
                        REMOVE_NAME,
                        Arc::new(Expression::literal(Scalar::Null(
                            remove_field.data_type.clone(),
                        ))),
                    ),
            )),
            checkpoint_data_schema.clone().into(),
        )?;

        // Filters out rows where all non-file-action columns are null.
        let non_file_actions_null_row_filter = {
            let predicate = Predicate::or_from(
                checkpoint_data_schema
                    .fields()
                    .filter(|f| f.name != ADD_NAME && f.name != REMOVE_NAME)
                    .map(|f| Predicate::is_not_null(Expression::column([f.name.as_str()]))),
            );
            eval_handler.new_predicate_evaluator(checkpoint_data_schema, Arc::new(predicate))?
        };

        Ok(Self {
            checkpoint_data_iter: checkpoint_data_iterator,
            file_actions_picker: NonNullRowsPicker {
                transform: file_action_projector,
                null_row_filter: file_actions_null_row_filter,
            },
            non_file_actions_picker: NonNullRowsPicker {
                transform: non_file_action_nullifier,
                null_row_filter: non_file_actions_null_row_filter,
            },
            non_file_action_batches: Vec::new(),
            exhausted: false,
        })
    }

    /// Creates a new `SidecarSplitter` wrapped in `Arc<Mutex<_>>` for
    /// shared mutable access. This is useful for passing [`SingleSidecarDataIterator`] to
    /// [`ParquetHandler::write_parquet_file`], which requires `Box<dyn Iterator + Send>`.
    pub(super) fn new_mut_shared(
        checkpoint_data_iterator: ActionReconciliationIterator,
        eval_handler: &dyn EvaluationHandler,
        checkpoint_data_schema: SchemaRef,
    ) -> DeltaResult<Arc<Mutex<Self>>> {
        Self::new(
            checkpoint_data_iterator,
            eval_handler,
            checkpoint_data_schema,
        )
        .map(|s| Arc::new(Mutex::new(s)))
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Consume the splitter and return the buffered non-file-action batches.
    pub(super) fn into_non_file_batches(self) -> Vec<Box<dyn EngineData>> {
        self.non_file_action_batches
    }

    /// Pull the next batch from the inner iterator, split it into file-action and non-file-action
    /// parts. Buffers the non-file-action part; returns the next non-empty file-action batch.
    /// Returns `None` and sets `exhausted` when the inner iterator is exhausted.
    fn next_file_actions_batch(&mut self) -> Option<DeltaResult<Box<dyn EngineData>>> {
        loop {
            let result = self.checkpoint_data_iter.next().or_else(|| {
                self.exhausted = true;
                None
            })?;
            let batch = match result.and_then(|f| f.apply_selection_vector()) {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            let non_file_actions_batch = match self.non_file_actions_picker.pick(batch.as_ref()) {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            if !non_file_actions_batch.is_empty() {
                self.non_file_action_batches.push(non_file_actions_batch);
            }
            match self.file_actions_picker.pick(batch.as_ref()) {
                Ok(file_actions_batch) if file_actions_batch.is_empty() => continue,
                other => return Some(other),
            }
        }
    }
}

/// Iterator that yields file-action batches for **one** sidecar file.
pub(super) struct SingleSidecarDataIterator {
    splitter: Arc<Mutex<SidecarSplitter>>,
    /// Soft cap on the number of file-action rows per sidecar. The actual count may exceed this
    /// because we operate on whole `EngineData` batches, which cannot be split.
    max_file_actions_hint: usize,
    yielded_row_count: usize,
}

impl SingleSidecarDataIterator {
    pub(super) fn new(
        splitter: Arc<Mutex<SidecarSplitter>>,
        max_file_actions_hint: usize,
    ) -> DeltaResult<Self> {
        if max_file_actions_hint == 0 {
            return Err(Error::checkpoint_write(
                "max_file_actions_hint must be greater than 0",
            ));
        }
        Ok(Self {
            splitter,
            max_file_actions_hint,
            yielded_row_count: 0,
        })
    }
}

impl Iterator for SingleSidecarDataIterator {
    type Item = DeltaResult<Box<dyn EngineData>>;

    /// Yields the next file-action batch for current sidecar.
    ///
    /// Returns `None` when either:
    /// - The row count for this chunk reaches `max_file_actions_hint`, or
    /// - The underlying data stream is exhausted.
    fn next(&mut self) -> Option<Self::Item> {
        if self.yielded_row_count >= self.max_file_actions_hint {
            return None;
        }
        let mut splitter = match self.splitter.lock() {
            Ok(guard) => guard,
            Err(e) => {
                return Some(Err(Error::internal_error(format!(
                    "sidecar splitter lock poisoned: {e}"
                ))))
            }
        };
        match splitter.next_file_actions_batch() {
            Some(Ok(file_batch)) => {
                self.yielded_row_count += file_batch.len();
                Some(Ok(file_batch))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests;
