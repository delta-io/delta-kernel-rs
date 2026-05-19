use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use delta_kernel::arrow::array::{
    Array, ArrayRef, LargeListArray, ListArray, MapArray, RecordBatch, StructArray,
};
use delta_kernel::arrow::buffer::NullBuffer;
use delta_kernel::arrow::datatypes::{DataType, Field, SchemaRef};
use futures::StreamExt;

/// Wraps a child plan and per-batch enforces kernel's strict NOT NULL contract while
/// reshaping batches to declare `target_schema`'s exact field declarations. NOT NULL
/// violations on nested fields (parent non-null, child null) produce a hard error.
#[derive(Debug)]
pub struct NullabilityValidationExec {
    child: Arc<dyn ExecutionPlan>,
    target_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl NullabilityValidationExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, target_schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(target_schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            child,
            target_schema,
            properties,
        }
    }
}

impl DisplayAs for NullabilityValidationExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NullabilityValidationExec")
    }
}

impl ExecutionPlan for NullabilityValidationExec {
    fn name(&self) -> &str {
        "NullabilityValidationExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let [child] = children.try_into().map_err(|c: Vec<_>| {
            DataFusionError::Plan(format!(
                "NullabilityValidationExec requires exactly one child, got {}",
                c.len()
            ))
        })?;
        Ok(Arc::new(Self::new(child, self.target_schema.clone())))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.child.execute(partition, context)?;
        let target_schema = self.target_schema.clone();
        let mapped = input.map(move |batch| {
            let batch = batch?;
            let cols = target_schema
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(f, c)| validate_and_reshape(c, f.as_ref(), f.name()))
                .collect::<DfResult<Vec<_>>>()?;
            RecordBatch::try_new(target_schema.clone(), cols).map_err(DataFusionError::from)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.target_schema.clone(),
            mapped,
        )))
    }
}

/// Walk `array` against `target_field` in one pass: error on NOT NULL violations, rebuild
/// with the target's field declarations, preserve original null bitmaps. `path` is used
/// only for error messages (`"add.deletionVector.storageType"` etc).
fn validate_and_reshape(array: &ArrayRef, target_field: &Field, path: &str) -> DfResult<ArrayRef> {
    match target_field.data_type() {
        DataType::Struct(target_fields) => {
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| internal(format!("`{path}`: expected StructArray")))?;
            let parent_nulls = struct_arr.nulls();
            let new_columns = target_fields
                .iter()
                .zip(struct_arr.columns())
                .map(|(child_field, child_array)| {
                    if !child_field.is_nullable() {
                        check_not_null(child_array, parent_nulls, path, child_field.name())?;
                    }
                    let child_path = format!("{path}.{}", child_field.name());
                    validate_and_reshape(child_array, child_field, &child_path)
                })
                .collect::<DfResult<Vec<_>>>()?;
            Ok(Arc::new(StructArray::new(
                target_fields.clone(),
                new_columns,
                struct_arr.nulls().cloned(),
            )))
        }
        DataType::List(target_inner) => {
            let list_arr = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| internal(format!("`{path}`: expected ListArray")))?;
            let values =
                validate_and_reshape(list_arr.values(), target_inner, &format!("{path}[]"))?;
            Ok(Arc::new(ListArray::new(
                Arc::new(target_inner.as_ref().clone()),
                list_arr.offsets().clone(),
                values,
                list_arr.nulls().cloned(),
            )))
        }
        DataType::LargeList(target_inner) => {
            let list_arr = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| internal(format!("`{path}`: expected LargeListArray")))?;
            let values =
                validate_and_reshape(list_arr.values(), target_inner, &format!("{path}[]"))?;
            Ok(Arc::new(LargeListArray::new(
                Arc::new(target_inner.as_ref().clone()),
                list_arr.offsets().clone(),
                values,
                list_arr.nulls().cloned(),
            )))
        }
        DataType::Map(target_entries, sorted) => {
            let map_arr = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| internal(format!("`{path}`: expected MapArray")))?;
            let entries_ref: ArrayRef = Arc::new(map_arr.entries().clone());
            let new_entries =
                validate_and_reshape(&entries_ref, target_entries, &format!("{path}.entries"))?;
            let entries_struct = new_entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| internal(format!("`{path}`: map entries must be StructArray")))?;
            Ok(Arc::new(MapArray::new(
                Arc::new(target_entries.as_ref().clone()),
                map_arr.offsets().clone(),
                entries_struct.clone(),
                map_arr.nulls().cloned(),
                *sorted,
            )))
        }
        _ => Ok(array.clone()),
    }
}

/// Error if any row of `child` is null while the parent is present at that row. `parent_nulls`
/// is the parent struct's null bitmap (`None` = parent has no nulls, i.e. always present).
fn check_not_null(
    child: &ArrayRef,
    parent_nulls: Option<&NullBuffer>,
    parent_path: &str,
    child_name: &str,
) -> DfResult<()> {
    let Some(child_nulls) = child.nulls() else {
        return Ok(());
    };
    for idx in 0..child.len() {
        let parent_present = parent_nulls.map(|n| n.is_valid(idx)).unwrap_or(true);
        if parent_present && !child_nulls.is_valid(idx) {
            return Err(internal(format!(
                "nullability validation failed at row {idx}: `{parent_path}.{child_name}` is null while parent is present"
            )));
        }
    }
    Ok(())
}

fn internal(msg: String) -> DataFusionError {
    DataFusionError::Internal(msg)
}
