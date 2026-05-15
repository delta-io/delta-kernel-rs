use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::{
    Array, ArrayRef, ListArray, MapArray, NullBufferBuilder, RecordBatch, StructArray,
};
use delta_kernel::arrow::datatypes::{DataType, Field, Fields};
use futures::{Stream, StreamExt};

pub type ValidationRule = (String, String);

#[derive(Debug)]
pub struct NullabilityValidationExec {
    child: Arc<dyn ExecutionPlan>,
    validations: Vec<ValidationRule>,
    target_schema: delta_kernel::arrow::datatypes::SchemaRef,
    properties: Arc<PlanProperties>,
}

impl NullabilityValidationExec {
    pub fn new(
        child: Arc<dyn ExecutionPlan>,
        validations: Vec<ValidationRule>,
        target_schema: delta_kernel::arrow::datatypes::SchemaRef,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(target_schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            child,
            validations,
            target_schema,
            properties,
        }
    }
}

impl DisplayAs for NullabilityValidationExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NullabilityValidationExec(validations={})",
            self.validations.len()
        )
    }
}

impl ExecutionPlan for NullabilityValidationExec {
    fn name(&self) -> &str {
        "NullabilityValidationExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
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
        let child = super::expect_single_child(children, "NullabilityValidationExec")?;
        Ok(Arc::new(Self::new(
            child,
            self.validations.clone(),
            self.target_schema.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.child.execute(partition, context)?;
        Ok(Box::pin(NullabilityValidationStream {
            input,
            target_schema: self.target_schema.clone(),
            validations: self.validations.clone(),
        }))
    }
}

struct NullabilityValidationStream {
    input: SendableRecordBatchStream,
    target_schema: delta_kernel::arrow::datatypes::SchemaRef,
    validations: Vec<ValidationRule>,
}

impl Stream for NullabilityValidationStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if let Err(e) = validate_batch(&batch, &self.validations) {
                    return Poll::Ready(Some(Err(e)));
                }
                let cols = self
                    .target_schema
                    .fields()
                    .iter()
                    .zip(batch.columns())
                    .map(|(f, c)| cast_array_to_field(c, f.as_ref()))
                    .collect::<DfResult<Vec<_>>>()?;
                let out = RecordBatch::try_new(self.target_schema.clone(), cols)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
                Poll::Ready(Some(out))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for NullabilityValidationStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.target_schema.clone()
    }
}

fn cast_array_to_field(array: &ArrayRef, target_field: &Field) -> DfResult<ArrayRef> {
    match target_field.data_type() {
        DataType::Struct(target_fields) => cast_struct_array(array, target_fields),
        DataType::List(target_inner) => cast_list_array(array, target_inner.as_ref()),
        DataType::LargeList(target_inner) => cast_large_list_array(array, target_inner.as_ref()),
        DataType::Map(target_entries, _) => cast_map_array(array, target_entries.as_ref()),
        _ => Ok(array.clone()),
    }
}

fn cast_struct_array(array: &ArrayRef, target_fields: &Fields) -> DfResult<ArrayRef> {
    let struct_arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Internal("expected StructArray".into()))?;
    let new_columns = target_fields
        .iter()
        .zip(struct_arr.columns())
        .map(|(f, c)| cast_array_to_field(c, f.as_ref()))
        .collect::<DfResult<Vec<_>>>()?;
    let needs_parent_mask = target_fields
        .iter()
        .zip(new_columns.iter())
        .any(|(f, c)| !f.is_nullable() && c.null_count() > 0);

    let parent_nulls = if needs_parent_mask {
        let mut nulls = NullBufferBuilder::new(struct_arr.len());
        for row in 0..struct_arr.len() {
            let invalid_non_nullable_child = target_fields
                .iter()
                .zip(new_columns.iter())
                .any(|(f, c)| !f.is_nullable() && c.is_null(row));
            if invalid_non_nullable_child {
                nulls.append_null();
            } else {
                nulls.append_non_null();
            }
        }
        nulls.finish()
    } else {
        struct_arr.nulls().cloned()
    };

    Ok(Arc::new(StructArray::new(
        target_fields.clone(),
        new_columns,
        parent_nulls,
    )))
}

fn cast_list_array(array: &ArrayRef, target_element_field: &Field) -> DfResult<ArrayRef> {
    let list_arr = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Internal("expected ListArray".into()))?;
    let values = cast_array_to_field(list_arr.values(), target_element_field)?;
    Ok(Arc::new(ListArray::new(
        Arc::new(target_element_field.clone()),
        list_arr.offsets().clone(),
        values,
        list_arr.nulls().cloned(),
    )))
}

fn cast_large_list_array(array: &ArrayRef, target_element_field: &Field) -> DfResult<ArrayRef> {
    use delta_kernel::arrow::array::LargeListArray;
    let list_arr = array
        .as_any()
        .downcast_ref::<LargeListArray>()
        .ok_or_else(|| DataFusionError::Internal("expected LargeListArray".into()))?;
    let values = cast_array_to_field(list_arr.values(), target_element_field)?;
    Ok(Arc::new(LargeListArray::new(
        Arc::new(target_element_field.clone()),
        list_arr.offsets().clone(),
        values,
        list_arr.nulls().cloned(),
    )))
}

fn cast_map_array(array: &ArrayRef, target_entries_field: &Field) -> DfResult<ArrayRef> {
    let map_arr = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DataFusionError::Internal("expected MapArray".into()))?;
    let DataType::Struct(target_entry_fields) = target_entries_field.data_type() else {
        return Err(DataFusionError::Internal(
            "map entries target must be struct".into(),
        ));
    };
    let entries_ref: ArrayRef = Arc::new(map_arr.entries().clone());
    let new_entries = cast_struct_array(&entries_ref, target_entry_fields)?;
    let entries_struct = new_entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Internal("cast map entries not struct".into()))?;
    Ok(Arc::new(MapArray::new(
        Arc::new(target_entries_field.clone()),
        map_arr.offsets().clone(),
        entries_struct.clone(),
        map_arr.nulls().cloned(),
        false,
    )))
}

fn validate_batch(batch: &RecordBatch, validations: &[ValidationRule]) -> DfResult<()> {
    fn resolve_parent_struct<'a>(
        batch: &'a RecordBatch,
        parent_path: &str,
    ) -> DfResult<&'a StructArray> {
        let mut parts = parent_path.split('.');
        let root = parts
            .next()
            .ok_or_else(|| DataFusionError::Internal("empty parent path".into()))?;
        let root_col = batch
            .column_by_name(root)
            .ok_or_else(|| DataFusionError::Internal(format!("parent root `{root}` not found")))?;
        let mut current = root_col
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Internal(format!("parent root `{root}` not struct")))?;
        for seg in parts {
            let next_col = current
                .column_by_name(seg)
                .ok_or_else(|| DataFusionError::Internal(format!("segment `{seg}` not found")))?;
            current = next_col
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| DataFusionError::Internal(format!("segment `{seg}` not struct")))?;
        }
        Ok(current)
    }

    for (parent_name, child_name) in validations {
        let struct_arr = resolve_parent_struct(batch, parent_name)?;
        let child_col = struct_arr.column_by_name(child_name).ok_or_else(|| {
            DataFusionError::Internal(format!("child `{child_name}` not found in `{parent_name}`"))
        })?;
        let parent_nulls = struct_arr.nulls();
        let child_nulls = child_col.nulls();
        let Some(child_nulls) = child_nulls else {
            continue;
        };
        for idx in 0..struct_arr.len() {
            let parent_present = parent_nulls.map(|n| n.is_valid(idx)).unwrap_or(true);
            let child_is_null = !child_nulls.is_valid(idx);
            if parent_present && child_is_null {
                return Err(DataFusionError::Internal(format!(
                    "nullability validation failed at row {idx}: `{parent_name}.{child_name}` is null while parent is present"
                )));
            }
        }
    }
    Ok(())
}
