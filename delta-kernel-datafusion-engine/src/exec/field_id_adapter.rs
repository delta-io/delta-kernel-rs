//! [`PhysicalExprAdapterFactory`] that adapts a parquet scan's physical batches to the
//! kernel-declared logical schema by matching columns and nested struct/list/map children
//! by `PARQUET:field_id` metadata (with a name fallback).
//!
//! For each [`Column`] reference in projection/predicate expressions:
//!
//! 1. Looks up the logical field by name in `logical_file_schema`.
//! 2. Finds the matching physical field via [`find_physical_match`].
//! 3. Rebuilds the column reference to point at the physical column index.
//! 4. If the logical and physical [`DataType`]s differ, wraps the column in a two-stage chain:
//!    - inner [`RenameNestedFieldsByIdExpr`] -- a pure metadata rename via kernel's
//!      [`apply_schema_to`] driven by a field-id-aware walk of the parquet field against the
//!      logical field.
//!    - outer [`CastColumnExpr`] -- drops parquet-only extras, fills missing-but-nullable targets
//!      with NULL, errors on missing-and-non-nullable, applies type-widening casts.
//!
//! [`apply_schema_to`]: delta_kernel::engine::arrow_expression::apply_schema::apply_schema_to

use std::any::Any;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use datafusion_common::error::DataFusionError;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result as DfResult, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::expressions::{self, CastColumnExpr, Column};
use datafusion_physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use delta_kernel::engine::arrow_conversion::TryFromArrow;
use delta_kernel::engine::arrow_expression::apply_schema::apply_schema_to;
use delta_kernel::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use delta_kernel::schema::{DataType as KernelDataType, StructField};

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
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DfResult<Arc<dyn PhysicalExpr>> {
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
        let logical_field = match self.logical_file_schema.field_with_name(column.name()) {
            Ok(field) => field,
            Err(_) => {
                // Column refers to something not in the logical schema (e.g. a partition
                // column kept only in the physical schema). Pass through unchanged.
                return Ok(Arc::new(column.clone()));
            }
        };

        // Virtual columns (parquet `RowNumber` etc.) are injected by the decoder via
        // `ArrowReaderOptions::with_virtual_columns` and never appear in the on-disk schema.
        // Pass through; the opener's `reassign_expr_columns` rebinds by name post-injection.
        if is_virtual_column(logical_field) {
            return Ok(Arc::new(column.clone()));
        }

        let physical_idx =
            match find_physical_match(logical_field, self.physical_file_schema.fields()) {
                Some(idx) => idx,
                None => {
                    // Forward-compat (e.g. `domainMetadata` against pre-DM V1 checkpoints):
                    // nullable -> typed NULL literal; non-nullable -> planning error.
                    if logical_field.is_nullable() {
                        let null_value = ScalarValue::Null.cast_to(logical_field.data_type())?;
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
        // Reference the physical name; the opener's `reassign_expr_columns` rebinds the
        // index against the narrowed stream schema by name.
        let resolved_column = Column::new(physical_field.name(), physical_idx);

        if physical_field.data_type() == logical_field.data_type() {
            return Ok(Arc::new(resolved_column));
        }

        // Types differ. Build a per-column rename chain:
        //   Column -> RenameNestedFieldsByIdExpr (kernel) -> CastColumnExpr (datafusion)
        // The renamed-physical field mirrors the parquet shape with logical names stamped
        // on matched children, so the outer cast's name-based matching just works.
        let renamed_physical_field =
            Arc::new(build_renamed_physical_field(physical_field, logical_field));
        let renamed_kernel_type = StructField::try_from_arrow(renamed_physical_field.as_ref())
            .map_err(|e| {
                DataFusionError::Plan(format!(
                    "FieldIdPhysicalExprAdapter: failed to build kernel rename schema \
                         for column `{}`: {e}",
                    column.name(),
                ))
            })?
            .data_type()
            .clone();
        let rename_expr = Arc::new(RenameNestedFieldsByIdExpr {
            inner: Arc::new(resolved_column),
            renamed_field: Arc::clone(&renamed_physical_field),
            rename_target: Arc::new(renamed_kernel_type),
        });
        Ok(Arc::new(CastColumnExpr::new(
            rename_expr,
            renamed_physical_field,
            Arc::new(logical_field.clone()),
            None,
        )))
    }
}

/// Match `target` against `candidates` by `PARQUET:field_id` equality (preferred) and
/// case-sensitive name equality (fallback). Kernel's `make_physical(Name)` stamps field
/// ids in column-mapping mode, so the name fallback only fires for plain tables.
fn find_physical_match(target: &Field, candidates: &Fields) -> Option<usize> {
    if let Some(tid) = parse_parquet_field_id(target) {
        for (i, c) in candidates.iter().enumerate() {
            if parse_parquet_field_id(c) == Some(tid) {
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

/// A "virtual" parquet column carries an Arrow extension type prefixed with
/// `parquet.virtual.` (e.g. `RowNumber` -> `parquet.virtual.row_number`); these are
/// injected by the decoder and never appear in the on-disk parquet schema. Prefix matching
/// covers all virtual types without enumerating each one.
fn is_virtual_column(field: &Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .is_some_and(|name| name.starts_with("parquet.virtual."))
}

/// [`PhysicalExpr`] that renames the inner column's nested children to match a precomputed
/// kernel target type. The structural transform (drop extras, null-fill missing, type-widen)
/// happens in the outer [`CastColumnExpr`] wrapping this expression.
#[derive(Debug, Clone)]
pub(crate) struct RenameNestedFieldsByIdExpr {
    inner: Arc<dyn PhysicalExpr>,
    /// Arrow field mirroring the parquet shape with logical names stamped on matched
    /// children. Reported as `data_type()` / `return_field()` for downstream consistency.
    renamed_field: FieldRef,
    /// Kernel form of `renamed_field.data_type()`, cached for per-batch evaluate.
    rename_target: Arc<KernelDataType>,
}

// `rename_target` is a pure function of `renamed_field`, so it's excluded from eq/hash.
impl PartialEq for RenameNestedFieldsByIdExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner) && self.renamed_field == other.renamed_field
    }
}

impl Eq for RenameNestedFieldsByIdExpr {}

impl Hash for RenameNestedFieldsByIdExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
        self.renamed_field.hash(state);
    }
}

impl fmt::Display for RenameNestedFieldsByIdExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FIELD_ID_RENAME({} -> {})",
            self.inner,
            self.renamed_field.data_type()
        )
    }
}

impl PhysicalExpr for RenameNestedFieldsByIdExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> DfResult<DataType> {
        Ok(self.renamed_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DfResult<bool> {
        Ok(self.renamed_field.is_nullable())
    }

    fn return_field(&self, _input_schema: &Schema) -> DfResult<FieldRef> {
        Ok(Arc::clone(&self.renamed_field))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DfResult<ColumnarValue> {
        let rename = |array: &delta_kernel::arrow::array::ArrayRef| {
            apply_schema_to(array, &self.rename_target).map_err(|e| {
                DataFusionError::Internal(format!(
                    "FieldIdRename: kernel apply_schema_to failed for `{}`: {e}",
                    self.renamed_field.name(),
                ))
            })
        };
        match self.inner.evaluate(batch)? {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(rename(&array)?)),
            ColumnarValue::Scalar(scalar) => {
                let renamed = rename(&scalar.to_array_of_size(1)?)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    renamed.as_ref(),
                    0,
                )?))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DfResult<Arc<dyn PhysicalExpr>> {
        let [child] = children.try_into().map_err(|c: Vec<_>| {
            DataFusionError::Internal(format!(
                "RenameNestedFieldsByIdExpr requires exactly one child, got {}",
                c.len()
            ))
        })?;
        Ok(Arc::new(Self {
            inner: child,
            renamed_field: Arc::clone(&self.renamed_field),
            rename_target: Arc::clone(&self.rename_target),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Build an Arrow [`Field`] with the *structure* of `physical` but the *names* and
/// *nullability* of `logical` wherever a field-id (or name) match exists. Recurses into
/// structs, lists, and map entries. The result mirrors parquet 1:1 so `apply_schema_to`
/// can rename in lockstep without rebuilding arrays. Logical nullability is stamped on
/// matched children to satisfy `CastColumnExpr::validate_struct_compatibility`. Parquet-only
/// children pass through unchanged; the outer cast drops them.
fn build_renamed_physical_field(physical: &Field, logical: &Field) -> Field {
    let rename_child =
        |p: &FieldRef, l: &FieldRef| Arc::new(build_renamed_physical_field(p.as_ref(), l.as_ref()));
    let renamed_type = match (physical.data_type(), logical.data_type()) {
        (DataType::Struct(phys_children), DataType::Struct(log_children)) => {
            let renamed: Vec<FieldRef> = phys_children
                .iter()
                .map(|pc| match find_physical_match(pc.as_ref(), log_children) {
                    Some(li) => {
                        Arc::new(build_renamed_physical_field(pc.as_ref(), &log_children[li]))
                    }
                    None => Arc::clone(pc),
                })
                .collect();
            DataType::Struct(renamed.into())
        }
        (DataType::List(p), DataType::List(l)) => DataType::List(rename_child(p, l)),
        (DataType::LargeList(p), DataType::LargeList(l)) => DataType::LargeList(rename_child(p, l)),
        (DataType::Map(p, sorted), DataType::Map(l, _)) => {
            DataType::Map(rename_child(p, l), *sorted)
        }
        _ => physical.data_type().clone(),
    };
    Field::new(logical.name(), renamed_type, logical.is_nullable())
        .with_metadata(physical.metadata().clone())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion_physical_expr::expressions::Column;
    use delta_kernel::arrow::array::{
        Array, ArrayRef, Int32Array, Int64Array, StringArray, StructArray,
    };
    use delta_kernel::arrow::datatypes::Schema;

    use super::*;

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
        let column = Arc::new(Column::new_with_schema(
            column_name,
            logical_schema.as_ref(),
        )?) as Arc<dyn PhysicalExpr>;
        adapter.rewrite(column)
    }

    fn evaluate_via_adapter(
        logical_schema: SchemaRef,
        physical_schema: SchemaRef,
        column_name: &str,
        batch: RecordBatch,
    ) -> DfResult<ArrayRef> {
        let rewritten = rewrite_column(logical_schema, physical_schema, column_name)?;
        match rewritten.evaluate(&batch)? {
            ColumnarValue::Array(a) => Ok(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(batch.num_rows()),
        }
    }

    // Flat rename. PARQUET:field_id links physical roots to logical roots; no nested
    // structs. Output column data_type already matches → pass through Column (no
    // reshape needed).
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

    // Nested missing-nullable-field schema evolution. The logical struct has one more
    // (nullable) inner field than the physical parquet does — the outer `CastColumnExpr`
    // inserts a NULL column for the missing field. Production scenario; exercises the
    // rename + cast pipeline. Deep recursion (lists, maps, deeper structs) is exercised
    // end-to-end by the engine's integration tests.
    #[test]
    fn nested_missing_nullable_field_filled_with_nulls() -> DfResult<()> {
        // Logical (kernel's physical_schema for an evolved table): struct has `a` and `b`.
        // Physical (parquet, written before `b` was added): struct has only `a`.
        // Names match between logical and physical at every level (per `make_physical`).
        let logical_a = Arc::new(fid(Field::new("a", DataType::Int64, false), 2));
        let logical_b = Arc::new(fid(Field::new("b", DataType::Utf8, true), 3));
        let physical_a = Arc::new(fid(Field::new("a", DataType::Int64, false), 2));

        let logical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "outer",
                DataType::Struct(Fields::from(vec![logical_a.clone(), logical_b])),
                true,
            ),
            1,
        )]));
        let physical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "outer",
                DataType::Struct(Fields::from(vec![physical_a.clone()])),
                true,
            ),
            1,
        )]));

        let a_values: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let phys_struct = StructArray::new(Fields::from(vec![physical_a]), vec![a_values], None);
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(phys_struct) as ArrayRef],
        )?;

        let out =
            evaluate_via_adapter(Arc::clone(&logical), Arc::clone(&physical), "outer", batch)?;
        // Output's DataType must match the LOGICAL field's DataType byte-for-byte (so the
        // opener's `RecordBatch::try_new(output_schema, arrays)` re-wrap will accept).
        assert_eq!(out.data_type(), logical.field(0).data_type());
        let out_struct = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(out_struct.fields().len(), 2);
        let a_out = out_struct
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a_out.value(0), 10);
        assert_eq!(a_out.value(2), 30);
        // `b` was missing in parquet → adapter inserts an all-null column.
        let b_out = out_struct.column(1);
        assert_eq!(b_out.len(), 3);
        assert_eq!(b_out.null_count(), 3);
        Ok(())
    }

    // No field IDs anywhere → adapter resolves by name fallback and passes through
    // unchanged. This is the "no column mapping" baseline.
    #[test]
    fn no_field_ids_passthrough() -> DfResult<()> {
        let logical = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let physical = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let rewritten = rewrite_column(Arc::clone(&logical), Arc::clone(&physical), "a")?;
        // Should not have wrapped — physical and logical types already agree.
        assert!(rewritten
            .as_any()
            .downcast_ref::<CastColumnExpr>()
            .is_none());
        assert!(rewritten
            .as_any()
            .downcast_ref::<RenameNestedFieldsByIdExpr>()
            .is_none());
        // Should be a plain Column.
        assert!(rewritten.as_any().downcast_ref::<Column>().is_some());
        Ok(())
    }

    // Name-mode column mapping. Kernel stamps PARQUET:field_id in name mode too (via
    // `StructField::make_physical`), so the same field-id matching applies. We exercise it
    // by using different physical / logical names with matching IDs.
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
        let rewritten = rewrite_column(
            Arc::clone(&logical),
            Arc::clone(&physical),
            "logical_rename",
        )?;
        // Data types match — should be a Column pointing at the physical name.
        let col = rewritten
            .as_any()
            .downcast_ref::<Column>()
            .expect("expected Column after adapter rewrite");
        assert_eq!(col.name(), "phys_rename");
        assert_eq!(col.index(), 0);
        Ok(())
    }

    // Forward-compat: logical schema has a nullable column that the physical schema
    // doesn't (e.g. `domainMetadata` against an older V1 checkpoint). The adapter must
    // substitute a typed NULL literal, matching DefaultPhysicalExprAdapter semantics.
    #[test]
    fn missing_nullable_logical_column_yields_null_literal() -> DfResult<()> {
        let logical = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b_missing", DataType::Utf8, true),
        ]));
        let physical = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let rewritten = rewrite_column(Arc::clone(&logical), Arc::clone(&physical), "b_missing")?;
        let literal = rewritten
            .as_any()
            .downcast_ref::<datafusion_physical_expr::expressions::Literal>()
            .expect("expected NULL literal for missing nullable column");
        assert!(literal.value().is_null());
        Ok(())
    }

    // Missing NON-nullable logical column must error — silently filling with NULL would
    // be data corruption.
    #[test]
    fn missing_non_nullable_logical_column_errors() {
        let logical = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b_missing", DataType::Utf8, false),
        ]));
        let physical = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let err = rewrite_column(logical, physical, "b_missing")
            .expect_err("missing non-nullable column should error");
        let s = err.to_string();
        assert!(
            s.contains("non-nullable logical column `b_missing`"),
            "unexpected error: {s}"
        );
    }
}
