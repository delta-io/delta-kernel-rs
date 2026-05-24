//! `CheckpointHintReader` -- reducer KDF that reads a `_last_checkpoint`
//! JSON scan and extracts the hint record.
//!
//! The file contains a single row. The reader captures it on the first
//! batch, returns [`KdfControl::Break`] to stop further input, and reduces
//! to `Option<CheckpointHintRecord>` -- `None` when the file was absent (zero
//! rows), `Some` when the hint was extracted.

use std::sync::LazyLock;

use delta_kernel_derive::ToSchema;
use serde::{Deserialize, Serialize};

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::plans::errors::DeltaError;
use crate::plans::kernel_reducers::{
    KdfControl, KernelReducer, KernelReducerKind, KernelReducerOutput,
};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, ToSchema};
use crate::{DeltaResult, EngineData};

/// `_last_checkpoint` JSON hint file record. Parsed by [`CheckpointHintReader`] from
/// the single-row hint file; deriving [`ToSchema`] lets the reader source the row
/// visitor's column schema from the struct definition.
///
/// Intentionally a subset of [`crate::last_checkpoint_hint::LastCheckpointHint`]: only
/// the fields the plans layer currently consumes (`version`, `size`, `parts`,
/// `sizeInBytes`, `numOfAddFiles`) are projected. `checkpointSchema`, `checksum`, and
/// `tags` are omitted; if the plans layer ever needs them (e.g. for footer skipping based
/// on the checkpoint schema), extend this record rather than reading them ad-hoc.
#[derive(ToSchema, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointHintRecord {
    /// Commit version the checkpoint covers.
    pub version: i64,
    /// Total logical size of the checkpoint (bytes), if known.
    pub size: Option<i64>,
    /// Number of parts for multipart checkpoints, if any. Stored as `i64` to keep the
    /// JSON deserialization aligned with [`LastCheckpointHint::parts`] (`Option<usize>`)
    /// on 64-bit targets.
    pub parts: Option<i64>,
    /// Total on-disk size of the checkpoint (bytes), if known.
    pub size_in_bytes: Option<i64>,
    /// Count of add-file actions in the checkpoint, if tracked.
    pub num_of_add_files: Option<i64>,
}

/// Reader state: holds the extracted record. `record.is_some()` doubles as the
/// "already processed" flag to short-circuit subsequent batches.
#[derive(Debug, Clone, Default)]
pub struct CheckpointHintReader {
    record: Option<CheckpointHintRecord>,
}

impl KernelReducer for CheckpointHintReader {
    fn kind(&self) -> KernelReducerKind {
        KernelReducerKind::CheckpointHint
    }

    fn finish(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        Box::new(*self)
    }

    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        if self.record.is_none() {
            self.visit_rows_of(batch)?;
        }
        Ok(if self.record.is_some() {
            KdfControl::Break
        } else {
            KdfControl::Continue
        })
    }
}

impl KernelReducerOutput for CheckpointHintReader {
    type Output = Option<CheckpointHintRecord>;

    fn into_output(self) -> Result<Self::Output, DeltaError> {
        Ok(self.record)
    }
}

impl RowVisitor for CheckpointHintReader {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| CheckpointHintRecord::to_schema().leaves(None));
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        if self.record.is_some() || row_count == 0 {
            return Ok(());
        }
        // The `_last_checkpoint` file is a single row per the protocol. Reject extra rows
        // loudly instead of silently consuming only row 0 -- the kernel may otherwise miss
        // a corrupt/multi-row hint file produced by a buggy writer or a different format.
        if row_count > 1 {
            return Err(crate::Error::generic(format!(
                "_last_checkpoint hint reader expected 1 row, got {row_count}"
            )));
        }
        // Column order matches the leaf traversal of `CheckpointHintRecord`.
        self.record = Some(CheckpointHintRecord {
            version: getters[0].get(0, "version")?,
            size: getters[1].get_opt(0, "size")?,
            parts: getters[2].get_opt(0, "parts")?,
            size_in_bytes: getters[3].get_opt(0, "sizeInBytes")?,
            num_of_add_files: getters[4].get_opt(0, "numOfAddFiles")?,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::engine::arrow_data::ArrowEngineData;

    #[test]
    fn empty_partition_produces_none() {
        let reader = CheckpointHintReader::default();
        let out = reader.into_output().unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn apply_reads_first_row_and_breaks() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("version", ArrowDataType::Int64, false),
            ArrowField::new("size", ArrowDataType::Int64, true),
            ArrowField::new("parts", ArrowDataType::Int64, true),
            ArrowField::new("sizeInBytes", ArrowDataType::Int64, true),
            ArrowField::new("numOfAddFiles", ArrowDataType::Int64, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(Int64Array::from(vec![Some(100)])),
            Arc::new(Int64Array::from(vec![None])),
            Arc::new(Int64Array::from(vec![Some(2048)])),
            Arc::new(Int64Array::from(vec![Some(7)])),
        ];
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        let engine_data = ArrowEngineData::new(batch);

        let mut reader = CheckpointHintReader::default();
        assert_eq!(reader.apply(&engine_data).unwrap(), KdfControl::Break);
        assert_eq!(reader.apply(&engine_data).unwrap(), KdfControl::Break);

        let out = reader.into_output().unwrap();
        let rec = out.expect("record");
        // Assert every projected field (full structural equality). Spot-checking only
        // `version` + `num_of_add_files` would silently miss a regression in `size`,
        // `parts`, or `size_in_bytes` decoding.
        assert_eq!(
            rec,
            CheckpointHintRecord {
                version: 42,
                size: Some(100),
                parts: None,
                size_in_bytes: Some(2048),
                num_of_add_files: Some(7),
            },
        );
    }
}
