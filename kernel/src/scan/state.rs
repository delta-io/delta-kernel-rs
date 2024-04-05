//! This module encapsulates the state of a scan

use super::ColumnType;
use crate::schema::SchemaRef;

pub struct ScanState<'a> {
    pub(crate) column_types: Vec<ColumnType<'a>>,
    pub(crate) have_partition_cols: bool,
    pub(crate) logical_schema: SchemaRef,
    pub(crate) read_schema: SchemaRef,
}

impl<'a> ScanState<'a> {
    pub fn new(
        column_types: Vec<ColumnType<'a>>,
        have_partition_cols: bool,
        logical_schema: SchemaRef,
        read_schema: SchemaRef,
    ) -> Self {
        Self {
            column_types,
            have_partition_cols,
            logical_schema,
            read_schema,
        }
    }

    pub fn read_schema(&self) -> SchemaRef {
        self.read_schema.clone()
    }

    pub fn into_owned(self) -> ScanState<'static> {
        ScanState {
            column_types: self
                .column_types
                .into_iter()
                .map(|ct| ct.into_owned())
                .collect(),
            have_partition_cols: self.have_partition_cols,
            logical_schema: self.logical_schema.clone(),
            read_schema: self.read_schema.clone(),
        }
    }
}
