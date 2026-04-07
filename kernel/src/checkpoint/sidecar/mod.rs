use std::sync::{Arc, Mutex};

use crate::action_reconciliation::ActionReconciliationIterator;
use crate::actions::{ADD_NAME, REMOVE_NAME};
use crate::engine_data::filter_by_predicate;
use crate::expressions::{Expression, Predicate, Scalar, Transform};
use crate::schema::{SchemaRef, StructType};
use crate::{
    DeltaResult, EngineData, Error, EvaluationHandler, ExpressionEvaluator, PredicateEvaluator,
};

/// Selects a subset of columns from a batch and filters out all-null rows.
struct ColumnPicker {
    projector: Arc<dyn ExpressionEvaluator>,
    null_row_filter: Arc<dyn PredicateEvaluator>,
}

impl ColumnPicker {
    fn pick(&self, batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        let projected = self.projector.evaluate(batch)?;
        filter_by_predicate(self.null_row_filter.as_ref(), projected)
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
/// | null   | null   | <proto>  | <meta>   | null |   // non-file action row
/// ```
///
/// is split into:
/// - file batch:     `[add: <add1>, remove: null]`
/// - non-file batch: `[add: null, remove: null, protocol: <proto>, metadata: <meta>, txn: null]`
pub(super) struct SidecarSplitter {
    checkpoint_data_iter: ActionReconciliationIterator,
    /// Projects to add/remove columns and filters out rows where both are null.
    file_actions_picker: ColumnPicker,
    /// Nulls out add/remove columns and filters out rows where all remaining columns are null.
    non_file_actions_picker: ColumnPicker,
    /// Accumulated non-file-action batches (protocol, metadata, txn, etc.).
    non_file_batches: Vec<Box<dyn EngineData>>,
    /// Set to `true` when the inner `checkpoint_data_iter` is exhausted.
    exhausted: bool,
}

impl SidecarSplitter {
    pub(super) fn new(
        checkpoint_data_iterator: ActionReconciliationIterator,
        eval_handler: &dyn EvaluationHandler,
        checkpoint_data_schema: SchemaRef,
    ) -> DeltaResult<Arc<Mutex<Self>>> {
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
        let file_action = eval_handler.new_expression_evaluator(
            checkpoint_data_schema.clone(),
            Arc::new(Expression::Struct(
                vec![
                    Arc::new(Expression::column([ADD_NAME])),
                    Arc::new(Expression::column([REMOVE_NAME])),
                ],
                None,
            )),
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

        // Main checkpoint projector: null out add/remove so file actions only appear in
        // the sidecar. Output schema is the same as the input since add/remove are
        // already nullable.
        let non_file_action = eval_handler.new_expression_evaluator(
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

        Ok(Arc::new(Mutex::new(Self {
            checkpoint_data_iter: checkpoint_data_iterator,
            file_actions_picker: ColumnPicker {
                projector: file_action,
                null_row_filter: file_actions_null_row_filter,
            },
            non_file_actions_picker: ColumnPicker {
                projector: non_file_action,
                null_row_filter: non_file_actions_null_row_filter,
            },
            non_file_batches: Vec::new(),
            exhausted: false,
        })))
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Drain the buffered non-file-action batches.
    pub(super) fn take_non_file_batches(&mut self) -> Vec<Box<dyn EngineData>> {
        std::mem::take(&mut self.non_file_batches)
    }

    /// Pull the next batch from the inner iterator, split it into file / non-file parts.
    /// Buffers the non-file part; returns the next non-empty file batch.
    /// Returns `None` and sets `exhausted` when the inner iterator is exhausted.
    fn next_batch(&mut self) -> Option<DeltaResult<Box<dyn EngineData>>> {
        loop {
            let result = self.checkpoint_data_iter.next().or_else(|| {
                self.exhausted = true;
                None
            })?;
            let batch = match result.and_then(|f| f.apply_selection_vector()) {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            let non_file_batch = match self.non_file_actions_picker.pick(batch.as_ref()) {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            if !non_file_batch.is_empty() {
                self.non_file_batches.push(non_file_batch);
            }
            match self.file_actions_picker.pick(batch.as_ref()) {
                Ok(file_batch) if file_batch.is_empty() => continue,
                other => return Some(other),
            }
        }
    }
}

/// Iterator that yields file-action batches for **one** sidecar file.
///
/// Returns `None` when either:
/// - The row count for this chunk reaches `max_file_actions_hint`, or
/// - The underlying data stream is exhausted.
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

    fn next(&mut self) -> Option<Self::Item> {
        if self.yielded_row_count >= self.max_file_actions_hint {
            return None; // hint reached -- start a new sidecar
        }
        let mut splitter = match self.splitter.lock() {
            Ok(guard) => guard,
            Err(e) => {
                return Some(Err(Error::generic(format!(
                    "sidecar splitter lock poisoned: {e}"
                ))))
            }
        };
        match splitter.next_batch() {
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
