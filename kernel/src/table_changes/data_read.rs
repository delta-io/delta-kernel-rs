use std::collections::HashMap;
use std::iter::{empty, once};
use std::sync::Arc;

use itertools::{Either, Itertools};
use roaring::RoaringTreemap;
use url::Url;

use crate::actions::deletion_vector::{split_vector, treemap_to_bools};
use crate::expressions::{column_expr, ColumnName, Scalar};
use crate::scan::state::DvInfo;
use crate::scan::{parse_partition_value, ColumnType, ScanResult};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{DeltaResult, Engine, Error, Expression, FileMeta};

use super::scan::GlobalScanState;
use super::scan_file::{ScanFile, ScanFileType};
type ResolvedScanFile = (ScanFile, Option<Vec<bool>>);

pub(crate) struct ScanFileReader {
    global_scan_state: GlobalScanState,
    scan_file: ScanFile,
    selection_vector: Option<Vec<bool>>,
}

impl ScanFileReader {
    pub(crate) fn new(
        global_scan_state: GlobalScanState,
        scan_file: ScanFile,
        selection_vector: Option<Vec<bool>>,
    ) -> Self {
        Self {
            global_scan_state,
            scan_file,
            selection_vector,
        }
    }

    pub(crate) fn get_generated_columns(&self) -> DeltaResult<HashMap<String, Expression>> {
        // Both in-commit timestamps and file metadata are in milliseconds
        //
        // See:
        // [`FileMeta`]
        // [In-Commit Timestamps] : https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-in-commit-timestampsa
        let timestamp = Scalar::timestamp_from_millis(self.scan_file.timestamp)?;
        let commit_version: i64 = self
            .scan_file
            .commit_version
            .try_into()
            .map_err(Error::generic)?;
        let cols = ["_change_type", "_commit_version", "_commit_timestamp"];
        let expressions = match self.scan_file.tpe {
            ScanFileType::Cdc => [
                column_expr!("_change_type"),
                Expression::literal(commit_version),
                timestamp.into(),
            ],
            ScanFileType::Add => [
                "insert".into(),
                Expression::literal(commit_version),
                timestamp.into(),
            ],

            ScanFileType::Remove => [
                "delete".into(),
                Expression::literal(commit_version),
                timestamp.into(),
            ],
        };
        let generated_columns: HashMap<String, Expression> = cols
            .iter()
            .map(ToString::to_string)
            .zip(expressions)
            .collect();
        Ok(generated_columns)
    }
    fn get_expression(&self, scan_file: &ScanFile) -> DeltaResult<Expression> {
        let mut generated_columns = self.get_generated_columns()?;
        let all_fields = self
            .global_scan_state
            .all_fields
            .iter()
            .map(|field| match field {
                ColumnType::Partition(field_idx) => {
                    let field = self
                        .global_scan_state
                        .logical_schema
                        .fields
                        .get_index(*field_idx);
                    let Some((_, field)) = field else {
                        return Err(Error::generic(
                            "logical schema did not contain expected field, can't transform data",
                        ));
                    };
                    let name = field.physical_name(self.global_scan_state.column_mapping_mode)?;
                    let value_expression = parse_partition_value(
                        scan_file.partition_values.get(name),
                        field.data_type(),
                    )?;
                    Ok(value_expression.into())
                }
                ColumnType::Selected(field_name) =>
                // We take the expression from the map
                {
                    Ok(generated_columns
                        .remove(field_name)
                        .unwrap_or_else(|| ColumnName::new([field_name]).into()))
                }
            })
            .try_collect()?;
        Ok(Expression::Struct(all_fields))
    }
    fn read_schema(&self) -> SchemaRef {
        if self.scan_file.tpe == ScanFileType::Cdc {
            let fields = self
                .global_scan_state
                .read_schema
                .fields()
                .cloned()
                .chain(once(StructField::new(
                    "_change_type",
                    DataType::STRING,
                    false,
                )));
            StructType::new(fields).into()
        } else {
            self.global_scan_state.read_schema.clone()
        }
    }
    pub(crate) fn into_data(
        mut self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanResult>>> {
        let table_root = Url::parse(&self.global_scan_state.table_root)?;

        let expression = self.get_expression(&self.scan_file)?;
        let schema = self.read_schema();
        let evaluator = engine.get_expression_handler().get_evaluator(
            schema.clone(),
            expression,
            self.global_scan_state.logical_schema.into(),
        );

        let mut selection_vector = self.selection_vector.take();

        let location = table_root.join(&self.scan_file.path)?;
        let file = FileMeta {
            last_modified: 0,
            size: 0,
            location,
        };
        let read_result_iter = engine.get_parquet_handler().read_parquet_files(
            &[file],
            schema.clone(),
            None, // TODO: Add back the predicate
        )?;

        Ok(read_result_iter.map(move |batch| -> DeltaResult<_> {
            let batch = batch?;
            // to transform the physical data into the correct logical form
            let logical = evaluator.evaluate(batch.as_ref());
            let len = logical.as_ref().map_or(0, |res| res.len());
            // need to split the dv_mask. what's left in dv_mask covers this result, and rest
            // will cover the following results. we `take()` out of `selection_vector` to avoid
            // trying to return a captured variable. We're going to reassign `selection_vector`
            // to `rest` in a moment anyway
            let mut sv = selection_vector.take();
            let rest = split_vector(sv.as_mut(), len, None);
            let result = ScanResult {
                raw_data: logical,
                raw_mask: sv,
            };
            selection_vector = rest;
            Ok(result)
        }))
    }
}
pub(crate) fn resolve_scan_file_dv(
    engine: &dyn Engine,
    table_root: &Url,
    scan_file: ScanFile,
    remove_dv: Option<Arc<HashMap<String, DvInfo>>>,
) -> DeltaResult<impl Iterator<Item = ResolvedScanFile>> {
    let remove_dv = remove_dv.as_ref().and_then(|map| map.get(&scan_file.path));
    match (&scan_file.tpe, remove_dv) {
        (ScanFileType::Add, Some(rm_dv)) => {
            // Helper function to convert a treemap to (scan_file, selection_vector) pair.
            // This returns an empty iterator if nothing is selected.
            fn treemap_to_iter(
                treemap: RoaringTreemap,
                mut scan_file: ScanFile,
                tpe: ScanFileType,
            ) -> impl Iterator<Item = (ScanFile, Option<Vec<bool>>)> {
                if treemap.is_empty() {
                    Either::Left(empty())
                } else {
                    let added_dv = treemap_to_bools(treemap);
                    scan_file.tpe = tpe;
                    Either::Right(once((scan_file, Some(added_dv))))
                }
            }

            // Retrieve the deletion vector from the add action and remove action
            let add_dv = scan_file
                .dv_info
                .get_treemap(engine, table_root)?
                .unwrap_or(Default::default());
            let rm_dv = rm_dv
                .get_treemap(engine, table_root)?
                .unwrap_or(Default::default());

            let added = treemap_to_iter(&rm_dv - &add_dv, scan_file.clone(), ScanFileType::Add);
            let removed = treemap_to_iter(add_dv - rm_dv, scan_file, ScanFileType::Remove);

            Ok(Either::Right(added.chain(removed)))
        }
        (_, Some(_)) => Err(Error::generic(
            "Remove DV should only match to an add action!",
        )),
        (ScanFileType::Remove | ScanFileType::Add, None) => {
            let selection_vector = scan_file.dv_info.get_selection_vector(engine, table_root)?;
            Ok(Either::Left(once((scan_file, selection_vector))))
        }
        (ScanFileType::Cdc, None) => Ok(Either::Left(once((scan_file, None)))),
    }
}
