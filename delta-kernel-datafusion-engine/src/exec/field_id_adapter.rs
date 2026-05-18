//! [`PhysicalExprAdapterFactory`] that adapts a parquet scan's physical batches to the
//! kernel-declared logical schema by matching columns and nested struct/list/map children
//! by `PARQUET:field_id` metadata (with a name fallback).
//!
//! # Why this exists
//!
//! The fork's parquet opener delegates schema adaptation to whichever
//! [`PhysicalExprAdapterFactory`] the engine plugs into `FileScanConfig::expr_adapter_factory`
//! ([`opener.rs`] explicitly documents this contract). The default factory
//! ([`DefaultPhysicalExprAdapterFactory`]) does name-based matching, which is wrong for Delta
//! column-mapping tables where the physical parquet column names differ from the
//! kernel-declared logical names.
//!
//! Kernel exposes per-level identity for column-mapped tables via three different metadata
//! keys depending on mode:
//!
//! - `id` mode: `parquet.field.id` (→ Arrow `PARQUET:field_id`) on every nested field on
//!   both sides.
//! - `name` mode: `delta.columnMapping.physicalName` on logical fields, plain physical
//!   names on the parquet side; `PARQUET:field_id` is NOT stamped on logical inner fields.
//! - no column mapping: plain names line up on both sides.
//!
//! [`find_physical_match`] tries all three in order so a single adapter handles every Delta
//! column-mapping mode plus the no-CM baseline.
//!
//! # What it does
//!
//! For each [`Column`] reference in projection/predicate expressions:
//!
//! 1. Looks up the logical field by name in `logical_file_schema`.
//! 2. Finds the matching physical field via [`find_physical_match`].
//! 3. Rebuilds the column reference to point at the physical name.
//! 4. If the logical and physical [`DataType`]s differ (e.g. nested struct field names
//!    differ), wraps in a [`FieldIdReshapeColumnExpr`] that recursively rebuilds the
//!    array at evaluate time so its [`DataType`] matches the kernel-declared schema.
//!
//! This makes the parquet opener emit `RecordBatch`es whose schema (including all nested
//! struct field names) matches what the kernel declared, end-to-end. The only remaining
//! post-scan rewrite is [`super::NullabilityValidationExec`], which re-asserts strict NOT
//! NULL on parent struct nodes whose decoded children carry nulls (a case neither this
//! adapter nor the parquet decoder enforce on their own).

use std::any::Any;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use datafusion_common::error::DataFusionError;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Result as DfResult;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::expressions::{self, Column};
use datafusion_physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use delta_kernel::arrow::array::{
    Array, ArrayRef, LargeListArray, ListArray, MapArray, RecordBatch, StructArray,
};
use delta_kernel::arrow::compute::{can_cast_types, cast_with_options, CastOptions};
use delta_kernel::arrow::datatypes::{
    DataType, Field, FieldRef, Schema, SchemaRef,
};
use delta_kernel::schema::ColumnMetadataKey;

const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

/// Factory for [`FieldIdPhysicalExprAdapter`].
///
/// Hand an instance of this to a parquet
/// [`FileScanConfig::expr_adapter_factory`](datafusion_datasource::file_scan_config::FileScanConfig)
/// (e.g. via
/// [`ListingTableConfig::with_expr_adapter_factory`](datafusion::datasource::listing::ListingTableConfig))
/// so every file open during scan runs through field-id-aware schema adaptation.
#[derive(Debug, Clone, Default)]
pub(crate) struct FieldIdPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for FieldIdPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> DfResult<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(FieldIdPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
        }))
    }
}

/// Adapter that rewrites column references using `PARQUET:field_id`-aware matching.
#[derive(Debug)]
pub(crate) struct FieldIdPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
}

impl PhysicalExprAdapter for FieldIdPhysicalExprAdapter {
    fn rewrite(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DfResult<Arc<dyn PhysicalExpr>> {
        expr.transform(|e| {
            if let Some(column) = e.as_any().downcast_ref::<Column>() {
                return Ok(Transformed::yes(self.rewrite_column(column)?));
            }
            Ok(Transformed::no(e))
        })
        .data()
    }
}

impl FieldIdPhysicalExprAdapter {
    fn rewrite_column(&self, column: &Column) -> DfResult<Arc<dyn PhysicalExpr>> {
        let logical_field = match self
            .logical_file_schema
            .field_with_name(column.name())
        {
            Ok(field) => field,
            Err(_) => {
                // Column refers to something not in the logical schema (e.g. a partition
                // column kept only in the physical schema). Pass through unchanged.
                return Ok(Arc::new(column.clone()));
            }
        };

        // Match by field-id first, name second.
        let physical_idx = match find_physical_match(
            logical_field,
            self.physical_file_schema.fields(),
        ) {
            Some(idx) => idx,
            None => {
                // No physical counterpart. Forward-compat: kernel often reads newer logical
                // schemas against older files (e.g. `domainMetadata` against V1 checkpoints
                // that pre-date it). Match `DefaultPhysicalExprAdapter` semantics here:
                // - if the logical field is nullable, substitute a typed NULL literal;
                // - if it's non-nullable, surface a planning error (would be silent data
                //   corruption otherwise).
                if logical_field.is_nullable() {
                    let null_value =
                        ScalarValue::Null.cast_to(logical_field.data_type())?;
                    return Ok(expressions::lit(null_value));
                }
                return Err(DataFusionError::Plan(format!(
                    "FieldIdPhysicalExprAdapter: non-nullable logical column `{}` \
                     (PARQUET:field_id={:?}) has no physical match in file schema {:?}",
                    column.name(),
                    parse_parquet_field_id(logical_field),
                    self.physical_file_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>(),
                )));
            }
        };

        let physical_field = self.physical_file_schema.field(physical_idx);
        // Build a Column pointing at the physical name (reassign_expr_columns in the opener
        // later rebinds the index against the narrowed stream schema by name).
        let resolved_column = Column::new(physical_field.name(), physical_idx);

        if physical_field.data_type() == logical_field.data_type() {
            // Schemas agree — including nested struct field names. No reshape needed.
            return Ok(Arc::new(resolved_column));
        }

        // Validate the recursive shape matches by field-id at every level (or name fallback).
        // If matching fails, we surface a planning error instead of silently emitting wrong
        // data. The reshape will then be exercised by `evaluate` later.
        validate_reshape_compatible(physical_field, logical_field)?;

        Ok(Arc::new(FieldIdReshapeColumnExpr {
            inner: Arc::new(resolved_column),
            input_field: Arc::new(physical_field.clone()),
            target_field: Arc::new(logical_field.clone()),
        }))
    }
}

/// Recursively validates that a `physical_field` can be reshaped into `target_field` by
/// field-id-aware (with name fallback) child matching. Returns a planning error if any
/// nested level lacks a viable matching strategy.
fn validate_reshape_compatible(
    physical_field: &Field,
    target_field: &Field,
) -> DfResult<()> {
    match (physical_field.data_type(), target_field.data_type()) {
        (DataType::Struct(p), DataType::Struct(t)) => {
            // Every non-nullable target child must resolve to a physical child.
            for t_child in t.iter() {
                match find_physical_match(t_child.as_ref(), p) {
                    Some(idx) => {
                        let p_child = &p[idx];
                        validate_reshape_compatible(p_child.as_ref(), t_child.as_ref())?;
                    }
                    None if !t_child.is_nullable() => {
                        return Err(DataFusionError::Plan(format!(
                            "FieldIdPhysicalExprAdapter: target struct field `{}` is NOT NULL \
                             but has no physical counterpart by field-id or name in source struct",
                            t_child.name()
                        )));
                    }
                    None => {}
                }
            }
            Ok(())
        }
        (DataType::List(p), DataType::List(t))
        | (DataType::LargeList(p), DataType::LargeList(t)) => {
            validate_reshape_compatible(p.as_ref(), t.as_ref())
        }
        (DataType::Map(p, _), DataType::Map(t, _)) => {
            validate_reshape_compatible(p.as_ref(), t.as_ref())
        }
        (p, t) if p == t => Ok(()),
        // Delta's type-widening table feature lets a file's physical leaf type be a *narrower*
        // version of the logical target type (e.g. parquet INT16 against logical INT32 after a
        // BYTE -> INT widening). The kernel only declares the logical schema after validating
        // that the widening is Delta-spec-legal, so the runtime job here is just to cast each
        // leaf to the target type at evaluate time. We gate on `can_cast_types` so a true
        // structural mismatch still surfaces as a planning error instead of crashing at
        // evaluate.
        (p, t) if can_cast_types(p, t) => Ok(()),
        (p, t) => Err(DataFusionError::Plan(format!(
            "FieldIdPhysicalExprAdapter: leaf type mismatch between physical `{}` and target \
             `{}` for field `{}`; arrow refuses to cast between these types",
            p,
            t,
            target_field.name()
        ))),
    }
}

/// Match `target` against a slice of candidate physical fields. Resolution order, applied
/// per-level (top-level and every nested struct level):
///
/// 1. `PARQUET:field_id` equality. Used by Delta column-mapping mode `id` and any other path
///    where both sides stamp field IDs (the parquet decoder's native column matcher).
/// 2. `delta.columnMapping.physicalName`: when the target (logical) field carries this
///    metadata, treat its value as the physical name to look up in `candidates`. Used by
///    Delta column-mapping mode `name`, where the kernel does NOT stamp `parquet.field.id`
///    on logical fields.
/// 3. Case-sensitive name equality. Default for plain (non-column-mapped) tables and the
///    fallback when neither of the above resolves.
fn find_physical_match(target: &Field, candidates: &delta_kernel::arrow::datatypes::Fields) -> Option<usize> {
    if let Some(tid) = parse_parquet_field_id(target) {
        for (i, c) in candidates.iter().enumerate() {
            if parse_parquet_field_id(c) == Some(tid) {
                return Some(i);
            }
        }
    }
    if let Some(physical_name) = target
        .metadata()
        .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
    {
        for (i, c) in candidates.iter().enumerate() {
            if c.name() == physical_name {
                return Some(i);
            }
        }
    }
    for (i, c) in candidates.iter().enumerate() {
        if c.name() == target.name() {
            return Some(i);
        }
    }
    None
}

fn parse_parquet_field_id(field: &Field) -> Option<i64> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|s| s.parse::<i64>().ok())
}

/// A [`PhysicalExpr`] that wraps an inner column-producing expression and reshapes the
/// resulting array so its [`DataType`] matches a target [`Field`] declared by the kernel.
///
/// Reshaping is structural only:
/// - Struct: rebuild children positionally with target field declarations, matching nested
///   children by `PARQUET:field_id` (name fallback).
/// - List / LargeList: rebuild with the target element field; recurse into element.
/// - Map: rebuild entries struct with target entry-field declarations; recurse into entries.
/// - Primitive: pass through if data_types already agree, otherwise error (no type coercion).
///
/// The underlying array buffers are reused; we only re-stamp field declarations.
#[derive(Debug, Clone, Eq)]
pub(crate) struct FieldIdReshapeColumnExpr {
    inner: Arc<dyn PhysicalExpr>,
    input_field: FieldRef,
    target_field: FieldRef,
}

impl PartialEq for FieldIdReshapeColumnExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
            && self.input_field.eq(&other.input_field)
            && self.target_field.eq(&other.target_field)
    }
}

impl Hash for FieldIdReshapeColumnExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
        self.input_field.hash(state);
        self.target_field.hash(state);
    }
}

impl fmt::Display for FieldIdReshapeColumnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FIELD_ID_RESHAPE({} -> {})",
            self.inner,
            self.target_field.data_type()
        )
    }
}

impl PhysicalExpr for FieldIdReshapeColumnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> DfResult<DataType> {
        Ok(self.target_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DfResult<bool> {
        Ok(self.target_field.is_nullable())
    }

    fn return_field(&self, _input_schema: &Schema) -> DfResult<FieldRef> {
        Ok(Arc::clone(&self.target_field))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DfResult<ColumnarValue> {
        let value = self.inner.evaluate(batch)?;
        match value {
            ColumnarValue::Array(array) => {
                let reshaped = reshape_array(&array, self.target_field.as_ref())?;
                Ok(ColumnarValue::Array(reshaped))
            }
            ColumnarValue::Scalar(scalar) => {
                let as_array = scalar.to_array_of_size(1)?;
                let reshaped = reshape_array(&as_array, self.target_field.as_ref())?;
                let result = datafusion_common::ScalarValue::try_from_array(
                    reshaped.as_ref(),
                    0,
                )?;
                Ok(ColumnarValue::Scalar(result))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DfResult<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "FieldIdReshapeColumnExpr requires exactly one child".into(),
            ));
        }
        let child = children.pop().unwrap();
        Ok(Arc::new(Self {
            inner: child,
            input_field: Arc::clone(&self.input_field),
            target_field: Arc::clone(&self.target_field),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Recursively rebuild `array` to match `target_field`'s [`DataType`].
fn reshape_array(array: &ArrayRef, target_field: &Field) -> DfResult<ArrayRef> {
    match target_field.data_type() {
        DataType::Struct(target_fields) => {
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FieldIdReshape: expected StructArray for field `{}`, got {:?}",
                        target_field.name(),
                        array.data_type()
                    ))
                })?;
            let source_fields = struct_arr.fields();
            let mut new_columns: Vec<ArrayRef> =
                Vec::with_capacity(target_fields.len());
            for t_child in target_fields.iter() {
                match find_physical_match(t_child.as_ref(), source_fields) {
                    Some(idx) => {
                        let source_col = struct_arr.column(idx);
                        new_columns.push(reshape_array(source_col, t_child.as_ref())?);
                    }
                    None => {
                        if !t_child.is_nullable() {
                            return Err(DataFusionError::Internal(format!(
                                "FieldIdReshape: target struct field `{}` is NOT NULL but \
                                 missing from source struct (and no field-id or name match)",
                                t_child.name()
                            )));
                        }
                        new_columns.push(delta_kernel::arrow::array::new_null_array(
                            t_child.data_type(),
                            struct_arr.len(),
                        ));
                    }
                }
            }
            Ok(Arc::new(StructArray::new(
                target_fields.clone(),
                new_columns,
                struct_arr.nulls().cloned(),
            )))
        }
        DataType::List(target_element) => {
            let list_arr = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FieldIdReshape: expected ListArray for field `{}`, got {:?}",
                        target_field.name(),
                        array.data_type()
                    ))
                })?;
            let values = reshape_array(list_arr.values(), target_element.as_ref())?;
            Ok(Arc::new(ListArray::new(
                Arc::clone(target_element),
                list_arr.offsets().clone(),
                values,
                list_arr.nulls().cloned(),
            )))
        }
        DataType::LargeList(target_element) => {
            let list_arr = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FieldIdReshape: expected LargeListArray for field `{}`, got {:?}",
                        target_field.name(),
                        array.data_type()
                    ))
                })?;
            let values = reshape_array(list_arr.values(), target_element.as_ref())?;
            Ok(Arc::new(LargeListArray::new(
                Arc::clone(target_element),
                list_arr.offsets().clone(),
                values,
                list_arr.nulls().cloned(),
            )))
        }
        DataType::Map(target_entries, sorted) => {
            let map_arr = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "FieldIdReshape: expected MapArray for field `{}`, got {:?}",
                        target_field.name(),
                        array.data_type()
                    ))
                })?;
            // The Map's entries field is itself a non-nullable struct with two children
            // (key, value). We rebuild that struct using `reshape_array` so nested struct
            // renames inside the value (or key) propagate.
            let entries_ref: ArrayRef = Arc::new(map_arr.entries().clone());
            let reshaped_entries = reshape_array(&entries_ref, target_entries.as_ref())?;
            let entries_struct = reshaped_entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "FieldIdReshape: map entries reshape did not produce a StructArray"
                            .into(),
                    )
                })?;
            Ok(Arc::new(MapArray::new(
                Arc::clone(target_entries),
                map_arr.offsets().clone(),
                entries_struct.clone(),
                map_arr.nulls().cloned(),
                *sorted,
            )))
        }
        _ => {
            if array.data_type() == target_field.data_type() {
                Ok(Arc::clone(array))
            } else {
                // Delta type-widening path: physical leaf is a narrower type than the logical
                // target (validated in `validate_reshape_compatible` via `can_cast_types`).
                // Arrow's cast is the right primitive here -- it handles all numeric widenings
                // (BYTE -> SHORT -> INT -> LONG, FLOAT -> DOUBLE), decimal precision changes,
                // and other Delta-spec-legal widenings. Use safe casting so a value that
                // unexpectedly doesn't fit the target type surfaces as an error rather than
                // silently overflowing.
                cast_with_options(
                    array.as_ref(),
                    target_field.data_type(),
                    &CastOptions {
                        safe: false,
                        ..Default::default()
                    },
                )
                .map_err(|e| {
                    DataFusionError::Internal(format!(
                        "FieldIdReshape: failed to cast leaf for field `{}` from {:?} to {:?}: {e}",
                        target_field.name(),
                        array.data_type(),
                        target_field.data_type(),
                    ))
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use datafusion_physical_expr::expressions::Column;
    use delta_kernel::arrow::array::{
        Array, ArrayRef, Int32Array, Int64Array, StringArray, StructArray,
    };
    use delta_kernel::arrow::buffer::OffsetBuffer;
    use delta_kernel::arrow::datatypes::{DataType, Field, Fields, Schema};

    fn fid(field: Field, id: i64) -> Field {
        let mut md = HashMap::new();
        md.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
        field.with_metadata(md)
    }

    fn rewrite_column(
        logical_schema: SchemaRef,
        physical_schema: SchemaRef,
        column_name: &str,
    ) -> DfResult<Arc<dyn PhysicalExpr>> {
        // The engine builds projection expressions against the LOGICAL schema (that's what
        // it exposes through `TableProvider::schema()`). The adapter is responsible for
        // rebinding those references to the physical schema. So the input Column reference
        // here uses the logical name + logical index.
        let factory = FieldIdPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::clone(&logical_schema), physical_schema)?;
        let column =
            Arc::new(Column::new_with_schema(column_name, logical_schema.as_ref())?)
                as Arc<dyn PhysicalExpr>;
        adapter.rewrite(column)
    }

    fn evaluate_via_adapter(
        logical_schema: SchemaRef,
        physical_schema: SchemaRef,
        column_name: &str,
        batch: RecordBatch,
    ) -> DfResult<ArrayRef> {
        // We bind the column reference to the LOGICAL schema (engine-facing position +
        // logical name) and rely on the adapter to rebind it to the physical schema by
        // field-id. This matches the opener's flow: projection expressions are built
        // against the logical schema before being rewritten.
        let factory = FieldIdPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::clone(&logical_schema), physical_schema)?;
        let column = Arc::new(Column::new_with_schema(
            column_name,
            logical_schema.as_ref(),
        )?) as Arc<dyn PhysicalExpr>;
        let rewritten = adapter.rewrite(column)?;
        match rewritten.evaluate(&batch)? {
            ColumnarValue::Array(a) => Ok(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(batch.num_rows()),
        }
    }

    // Case 1: Flat rename. PARQUET:field_id links physical roots to logical roots; no
    // nested structs. Output column data_type already matches → pass through Column.
    #[test]
    fn flat_rename_passthrough() -> DfResult<()> {
        let logical = Arc::new(Schema::new(vec![
            fid(Field::new("logical_a", DataType::Int32, false), 1),
            fid(Field::new("logical_b", DataType::Utf8, true), 2),
        ]));
        let physical = Arc::new(Schema::new(vec![
            fid(Field::new("phys_a", DataType::Int32, false), 1),
            fid(Field::new("phys_b", DataType::Utf8, true), 2),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef,
            ],
        )?;

        let out_a = evaluate_via_adapter(
            Arc::clone(&logical),
            Arc::clone(&physical),
            "logical_a",
            batch.clone(),
        )?;
        assert_eq!(out_a.data_type(), &DataType::Int32);
        assert_eq!(out_a.len(), 3);

        let out_b = evaluate_via_adapter(
            Arc::clone(&logical),
            Arc::clone(&physical),
            "logical_b",
            batch,
        )?;
        assert_eq!(out_b.data_type(), &DataType::Utf8);
        Ok(())
    }

    // Case 2: Struct-of-primitive rename. Outer + inner field names differ; both linked by
    // field-id. The adapter must wrap in FieldIdReshapeColumnExpr so the output StructArray
    // has logical inner names.
    #[test]
    fn struct_of_primitive_rename() -> DfResult<()> {
        let logical_inner = Arc::new(fid(Field::new("logical_a", DataType::Int64, false), 2));
        let physical_inner = Arc::new(fid(Field::new("phys_a", DataType::Int64, false), 2));
        let logical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "logical_outer",
                DataType::Struct(Fields::from(vec![logical_inner.clone()])),
                true,
            ),
            1,
        )]));
        let physical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "phys_outer",
                DataType::Struct(Fields::from(vec![physical_inner.clone()])),
                true,
            ),
            1,
        )]));

        let inner_arr: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let phys_struct = StructArray::new(
            Fields::from(vec![physical_inner]),
            vec![inner_arr],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(phys_struct) as ArrayRef],
        )?;

        let out = evaluate_via_adapter(
            Arc::clone(&logical),
            Arc::clone(&physical),
            "logical_outer",
            batch,
        )?;
        // The output's DataType must match the LOGICAL field's DataType byte-for-byte (so
        // the opener's `RecordBatch::try_new(output_schema, arrays)` re-wrap will accept).
        assert_eq!(out.data_type(), logical.field(0).data_type());
        let out_struct = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(out_struct.fields()[0].name(), "logical_a");
        let inner = out_struct
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(inner.value(0), 10);
        assert_eq!(inner.value(2), 30);
        Ok(())
    }

    // Case 3: 3-level deep struct rename (cm_deeply_nested shape).
    #[test]
    fn three_level_deep_struct_rename() -> DfResult<()> {
        let l3_logical = Arc::new(fid(Field::new("logical_v", DataType::Utf8, true), 4));
        let l2_logical = Arc::new(fid(
            Field::new(
                "logical_l3",
                DataType::Struct(Fields::from(vec![l3_logical.clone()])),
                true,
            ),
            3,
        ));
        let l1_logical = Arc::new(fid(
            Field::new(
                "logical_l2",
                DataType::Struct(Fields::from(vec![l2_logical.clone()])),
                true,
            ),
            2,
        ));
        let logical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "logical_l1",
                DataType::Struct(Fields::from(vec![l1_logical.clone()])),
                true,
            ),
            1,
        )]));

        let l3_phys = Arc::new(fid(Field::new("phys_v", DataType::Utf8, true), 4));
        let l2_phys = Arc::new(fid(
            Field::new(
                "phys_l3",
                DataType::Struct(Fields::from(vec![l3_phys.clone()])),
                true,
            ),
            3,
        ));
        let l1_phys = Arc::new(fid(
            Field::new(
                "phys_l2",
                DataType::Struct(Fields::from(vec![l2_phys.clone()])),
                true,
            ),
            2,
        ));
        let physical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "phys_l1",
                DataType::Struct(Fields::from(vec![l1_phys.clone()])),
                true,
            ),
            1,
        )]));

        let v_arr: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let l3_arr =
            StructArray::new(Fields::from(vec![l3_phys]), vec![v_arr], None);
        let l2_arr = StructArray::new(
            Fields::from(vec![l2_phys]),
            vec![Arc::new(l3_arr) as ArrayRef],
            None,
        );
        let l1_arr = StructArray::new(
            Fields::from(vec![l1_phys]),
            vec![Arc::new(l2_arr) as ArrayRef],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(l1_arr) as ArrayRef],
        )?;

        let out = evaluate_via_adapter(
            Arc::clone(&logical),
            Arc::clone(&physical),
            "logical_l1",
            batch,
        )?;
        assert_eq!(out.data_type(), logical.field(0).data_type());

        // Walk three levels and assert all renamed names appear.
        let s1 = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(s1.fields()[0].name(), "logical_l2");
        let s2 = s1
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(s2.fields()[0].name(), "logical_l3");
        let s3 = s2
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(s3.fields()[0].name(), "logical_v");
        let v = s3
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(v.value(0), "a");
        assert_eq!(v.value(1), "b");
        Ok(())
    }

    // Case 4: List<Struct> rename. Element field is a struct whose inner names differ;
    // list offsets and validity must be preserved bit-for-bit.
    #[test]
    fn list_of_struct_rename() -> DfResult<()> {
        let logical_inner = Arc::new(fid(Field::new("logical_x", DataType::Int32, true), 3));
        let physical_inner = Arc::new(fid(Field::new("phys_x", DataType::Int32, true), 3));

        let logical_element = Arc::new(Field::new(
            "element",
            DataType::Struct(Fields::from(vec![logical_inner.clone()])),
            true,
        ));
        let physical_element = Arc::new(Field::new(
            "element",
            DataType::Struct(Fields::from(vec![physical_inner.clone()])),
            true,
        ));

        let logical = Arc::new(Schema::new(vec![fid(
            Field::new("logical_arr", DataType::List(Arc::clone(&logical_element)), true),
            1,
        )]));
        let physical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "phys_arr",
                DataType::List(Arc::clone(&physical_element)),
                true,
            ),
            1,
        )]));

        // List of 3 rows: [{x:1},{x:2}], [], [{x:3}].
        let inner_values: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]));
        let element_struct = StructArray::new(
            Fields::from(vec![physical_inner]),
            vec![inner_values],
            None,
        );
        let offsets = OffsetBuffer::<i32>::new(vec![0, 2, 2, 3].into());
        let phys_list = ListArray::new(
            physical_element,
            offsets,
            Arc::new(element_struct) as ArrayRef,
            None,
        );

        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(phys_list) as ArrayRef],
        )?;

        let out = evaluate_via_adapter(
            Arc::clone(&logical),
            Arc::clone(&physical),
            "logical_arr",
            batch,
        )?;
        assert_eq!(out.data_type(), logical.field(0).data_type());

        let out_list = out.as_any().downcast_ref::<ListArray>().unwrap();
        // Offsets must be preserved.
        let off = out_list.offsets();
        assert_eq!(off.as_ref(), &[0i32, 2, 2, 3]);
        // Element struct must use logical inner name.
        let elem_struct = out_list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(elem_struct.fields()[0].name(), "logical_x");
        let xs = elem_struct
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(xs.value(0), 1);
        assert_eq!(xs.value(1), 2);
        assert_eq!(xs.value(2), 3);
        Ok(())
    }

    // Case 5: Map<Utf8, Struct> rename. Covers the existing acceptance-fixture coverage
    // gap. Inner struct in `value` slot must be renamed; map offsets/validity preserved.
    #[test]
    fn map_value_struct_rename() -> DfResult<()> {
        let key_logical = Arc::new(fid(Field::new("key", DataType::Utf8, false), 2));
        let val_inner_logical =
            Arc::new(fid(Field::new("logical_v", DataType::Int64, true), 4));
        let val_logical = Arc::new(fid(
            Field::new(
                "value",
                DataType::Struct(Fields::from(vec![val_inner_logical.clone()])),
                true,
            ),
            3,
        ));
        let entries_logical = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                key_logical.clone(),
                val_logical.clone(),
            ])),
            false,
        ));

        let key_phys = Arc::new(fid(Field::new("key", DataType::Utf8, false), 2));
        let val_inner_phys = Arc::new(fid(Field::new("phys_v", DataType::Int64, true), 4));
        let val_phys = Arc::new(fid(
            Field::new(
                "value",
                DataType::Struct(Fields::from(vec![val_inner_phys.clone()])),
                true,
            ),
            3,
        ));
        let entries_phys = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![key_phys.clone(), val_phys.clone()])),
            false,
        ));

        let logical = Arc::new(Schema::new(vec![fid(
            Field::new("logical_m", DataType::Map(Arc::clone(&entries_logical), false), true),
            1,
        )]));
        let physical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "phys_m",
                DataType::Map(Arc::clone(&entries_phys), false),
                true,
            ),
            1,
        )]));

        // 2 maps: {"a"->{phys_v:10}, "b"->{phys_v:20}}, {"c"->{phys_v:30}}.
        let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let inner_vals: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(30)]));
        let val_struct = StructArray::new(
            Fields::from(vec![val_inner_phys]),
            vec![inner_vals],
            None,
        );
        let entries_struct = StructArray::new(
            Fields::from(vec![key_phys, val_phys]),
            vec![keys, Arc::new(val_struct) as ArrayRef],
            None,
        );
        let offsets = OffsetBuffer::<i32>::new(vec![0, 2, 3].into());
        let phys_map = MapArray::new(
            entries_phys,
            offsets,
            entries_struct,
            None,
            false,
        );

        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(phys_map) as ArrayRef],
        )?;

        let out = evaluate_via_adapter(
            Arc::clone(&logical),
            Arc::clone(&physical),
            "logical_m",
            batch,
        )?;
        assert_eq!(out.data_type(), logical.field(0).data_type());

        let out_map = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(out_map.offsets().as_ref(), &[0i32, 2, 3]);
        // Entries struct's value child must now use logical inner name.
        let entries = out_map.entries();
        let val_col = entries.column_by_name("value").unwrap();
        let val_struct = val_col
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(val_struct.fields()[0].name(), "logical_v");
        Ok(())
    }

    // Case 6: No field IDs anywhere → adapter passes the column through unchanged (the
    // names already match and the fallback name-match resolves trivially). This is the
    // "no column mapping" path.
    #[test]
    fn no_field_ids_passthrough() -> DfResult<()> {
        let logical = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let physical = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let rewritten =
            rewrite_column(Arc::clone(&logical), Arc::clone(&physical), "a")?;
        // Should not have wrapped in FieldIdReshapeColumnExpr.
        assert!(rewritten
            .as_any()
            .downcast_ref::<FieldIdReshapeColumnExpr>()
            .is_none());
        // Should be a Column.
        assert!(rewritten.as_any().downcast_ref::<Column>().is_some());
        Ok(())
    }

    // Case 7: Name-mode column mapping. Kernel stamps PARQUET:field_id in name mode too
    // (kernel/src/schema/mod.rs:508-524), so the same field-id matching applies. We
    // exercise it by using different physical / logical names with matching IDs.
    #[test]
    fn name_mode_uses_field_ids() -> DfResult<()> {
        let logical = Arc::new(Schema::new(vec![fid(
            Field::new("logical_rename", DataType::Int32, true),
            7,
        )]));
        let physical = Arc::new(Schema::new(vec![fid(
            Field::new("phys_rename", DataType::Int32, true),
            7,
        )]));
        let rewritten =
            rewrite_column(Arc::clone(&logical), Arc::clone(&physical), "logical_rename")?;
        // Data types match — should be a Column pointing at the physical name.
        let col = rewritten
            .as_any()
            .downcast_ref::<Column>()
            .expect("expected Column after adapter rewrite");
        assert_eq!(col.name(), "phys_rename");
        assert_eq!(col.index(), 0);
        Ok(())
    }

    // Case 8b: Forward-compat — logical schema has a nullable column that the physical
    // schema doesn't (e.g. `domainMetadata` against an older V1 checkpoint). The adapter
    // must substitute a typed NULL literal, matching DefaultPhysicalExprAdapter semantics.
    #[test]
    fn missing_nullable_logical_column_yields_null_literal() -> DfResult<()> {
        let logical = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b_missing", DataType::Utf8, true),
        ]));
        let physical = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let rewritten =
            rewrite_column(Arc::clone(&logical), Arc::clone(&physical), "b_missing")?;
        let literal = rewritten
            .as_any()
            .downcast_ref::<datafusion_physical_expr::expressions::Literal>()
            .expect("expected NULL literal for missing nullable column");
        assert!(literal.value().is_null());
        Ok(())
    }

    // Case 8c: Missing NON-nullable logical column must error — silently filling with
    // NULL would be data corruption.
    #[test]
    fn missing_non_nullable_logical_column_errors() {
        let logical = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b_missing", DataType::Utf8, false),
        ]));
        let physical = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let err = rewrite_column(logical, physical, "b_missing")
            .expect_err("missing non-nullable column should error");
        let s = err.to_string();
        assert!(
            s.contains("non-nullable logical column `b_missing`"),
            "unexpected error: {s}"
        );
    }

    // Case 8d: Delta column-mapping `name` mode -- logical inner fields carry
    // `delta.columnMapping.physicalName` instead of `PARQUET:field_id`. The adapter must
    // use the physicalName metadata to bridge logical name -> physical name at every
    // nested level.
    #[test]
    fn nested_struct_uses_column_mapping_physical_name() -> DfResult<()> {
        use datafusion_common::arrow::array::{Int64Array, StructArray};

        let physical_inner = Arc::new(Field::new("col-phys-inner", DataType::Int64, false));
        let physical_outer_dt = DataType::Struct(Fields::from(vec![physical_inner.clone()]));
        let physical_outer =
            Field::new("col-phys-outer", physical_outer_dt.clone(), true);

        let logical_inner = Field::new("logical_inner", DataType::Int64, false).with_metadata(
            HashMap::from([(
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref().to_string(),
                "col-phys-inner".to_string(),
            )]),
        );
        let logical_outer_dt =
            DataType::Struct(Fields::from(vec![Arc::new(logical_inner.clone())]));
        let logical_outer = Field::new("logical_outer", logical_outer_dt.clone(), true)
            .with_metadata(HashMap::from([(
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref().to_string(),
                "col-phys-outer".to_string(),
            )]));

        let physical = Arc::new(Schema::new(vec![physical_outer.clone()]));
        let logical = Arc::new(Schema::new(vec![logical_outer]));

        // Build a batch matching the physical schema: outer = { inner: 7 }.
        let inner_arr = Arc::new(Int64Array::from(vec![7]));
        let outer_arr = StructArray::new(
            Fields::from(vec![physical_inner]),
            vec![inner_arr as _],
            None,
        );
        let batch = RecordBatch::try_new(physical.clone(), vec![Arc::new(outer_arr) as _])?;

        // Rewrite the logical column reference (logical_outer) through the adapter.
        let result = evaluate_via_adapter(logical, physical, "logical_outer", batch)?;
        let outer = result
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("must produce StructArray after reshape");
        // Both top-level AND nested-inner field names must be reshaped to the logical names.
        assert_eq!(outer.fields().len(), 1);
        assert_eq!(outer.fields()[0].name(), "logical_inner");
        let inner = outer
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(inner.values(), &[7i64][..]);
        Ok(())
    }

    // Case 8: Partial fallback. Top-level fields linked by field-id; one inner struct
    // field has field-id, the sibling doesn't (kernel-side metadata anomaly). The
    // adapter must field-id-match where possible and name-match for the rest.
    #[test]
    fn partial_field_id_fallback() -> DfResult<()> {
        let logical_inner_a = Arc::new(fid(Field::new("inner_a", DataType::Int32, true), 2));
        let logical_inner_b =
            Arc::new(Field::new("inner_b", DataType::Utf8, true)); // no field-id
        let logical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "outer",
                DataType::Struct(Fields::from(vec![
                    logical_inner_a.clone(),
                    logical_inner_b.clone(),
                ])),
                true,
            ),
            1,
        )]));

        // Physical has inner_a renamed (matched by id) and inner_b with the same name as
        // logical (matched by name).
        let phys_inner_a =
            Arc::new(fid(Field::new("phys_inner_a", DataType::Int32, true), 2));
        let phys_inner_b = Arc::new(Field::new("inner_b", DataType::Utf8, true));
        let physical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "outer_phys",
                DataType::Struct(Fields::from(vec![
                    phys_inner_a.clone(),
                    phys_inner_b.clone(),
                ])),
                true,
            ),
            1,
        )]));

        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
        let phys_struct = StructArray::new(
            Fields::from(vec![phys_inner_a, phys_inner_b]),
            vec![a, b],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(phys_struct) as ArrayRef],
        )?;

        let out = evaluate_via_adapter(
            Arc::clone(&logical),
            Arc::clone(&physical),
            "outer",
            batch,
        )?;
        assert_eq!(out.data_type(), logical.field(0).data_type());
        let s = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(s.fields()[0].name(), "inner_a");
        assert_eq!(s.fields()[1].name(), "inner_b");
        let a_out = s
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(a_out.value(1), 2);
        let b_out = s
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(b_out.value(0), "x");
        Ok(())
    }
}
