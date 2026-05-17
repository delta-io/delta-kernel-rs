//! `CheckpointHintReader` — consumer KDF that reads a `_last_checkpoint`
//! JSON scan and extracts the hint record.
//!
//! The file contains a single row. The reader captures it on the first
//! batch, returns [`KdfControl::Break`] to stop further input, and reduces
//! to `Option<CheckpointHintRecord>` — `None` when the file was absent (zero
//! rows), `Some` when the hint was extracted.

use std::sync::LazyLock;

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::plans::errors::DeltaError;
use crate::plans::kdf::{
    take_single, ConsumerKdf, ConsumerKdfId, KdfControl, KdfOutput, KdfStateToken,
};
use crate::plans::record_schemas::CheckpointHintRecord;
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, ToSchema};
use crate::{DeltaResult, EngineData};

/// Reader state: holds the extracted record (or `None` if no row has been
/// seen yet) plus a flag to short-circuit subsequent batches.
#[derive(Debug, Clone, Default)]
pub struct CheckpointHintReader {
    record: Option<CheckpointHintRecord>,
    processed: bool,
}

impl CheckpointHintReader {
    pub fn new() -> Self {
        Self::default()
    }
}

impl_kdf!(CheckpointHintReader, ConsumerKdfId::CheckpointHint);

impl ConsumerKdf for CheckpointHintReader {
    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        if self.processed {
            return Ok(KdfControl::Break);
        }
        self.visit_rows_of(batch)?;
        Ok(if self.processed {
            KdfControl::Break
        } else {
            KdfControl::Continue
        })
    }
}

impl KdfOutput for CheckpointHintReader {
    type Output = Option<CheckpointHintRecord>;

    fn into_output(parts: Vec<Self>) -> Result<Self::Output, DeltaError> {
        // Global consumer: the executor produces exactly one partition.
        // Empty `parts` happens only on internal misuse — bubble up.
        let token = KdfStateToken::new(ConsumerKdfId::CheckpointHint);
        let mut single = take_single(parts, &token)?;
        Ok(single.record.take())
    }
}

impl RowVisitor for CheckpointHintReader {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| CheckpointHintRecord::to_schema().leaves(None));
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        if self.processed || row_count == 0 {
            return Ok(());
        }
        // The `_last_checkpoint` file is a single row; column order matches
        // the leaf traversal of `CheckpointHintRecord`.
        self.record = Some(CheckpointHintRecord {
            version: getters[0].get(0, "version")?,
            size: getters[1].get_opt(0, "size")?,
            parts: getters[2].get_opt(0, "parts")?,
            size_in_bytes: getters[3].get_opt(0, "sizeInBytes")?,
            num_of_add_files: getters[4].get_opt(0, "numOfAddFiles")?,
        });
        self.processed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::arrow::array::{Int32Array, Int64Array, RecordBatch};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use crate::engine::arrow_data::ArrowEngineData;

    #[test]
    fn empty_partition_produces_none() {
        let parts = vec![CheckpointHintReader::default()];
        let out = CheckpointHintReader::into_output(parts).unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn apply_reads_first_row_and_breaks() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("version", ArrowDataType::Int64, false),
            ArrowField::new("size", ArrowDataType::Int64, true),
            ArrowField::new("parts", ArrowDataType::Int32, true),
            ArrowField::new("sizeInBytes", ArrowDataType::Int64, true),
            ArrowField::new("numOfAddFiles", ArrowDataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Int64Array::from(vec![Some(100)])),
                Arc::new(Int32Array::from(vec![None])),
                Arc::new(Int64Array::from(vec![Some(2048)])),
                Arc::new(Int64Array::from(vec![Some(7)])),
            ],
        )
        .unwrap();
        let engine_data = ArrowEngineData::new(batch);

        let mut reader = CheckpointHintReader::default();
        assert_eq!(reader.apply(&engine_data).unwrap(), KdfControl::Break);
        assert_eq!(reader.apply(&engine_data).unwrap(), KdfControl::Break);

        let out = CheckpointHintReader::into_output(vec![reader]).unwrap();
        let rec = out.expect("record");
        assert_eq!(rec.version, 42);
        assert_eq!(rec.num_of_add_files, Some(7));
    }
}
