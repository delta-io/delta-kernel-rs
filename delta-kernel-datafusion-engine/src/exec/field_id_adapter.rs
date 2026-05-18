//! [`PhysicalExprAdapterFactory`] that adapts a parquet scan's physical batches to the
//! kernel-declared logical schema by matching columns and nested struct/list/map children
//! by `PARQUET:field_id` metadata (with a name fallback).
//!
//! # Why this exists
//!
//! The fork's parquet opener delegates schema adaptation to whichever
//! [`PhysicalExprAdapterFactory`] the engine plugs into `FileScanConfig::expr_adapter_factory`.
//! The default factory ([`DefaultPhysicalExprAdapterFactory`]) does name-based matching at every
//! nesting level. That's wrong for Delta column-mapping tables where nested parquet struct
//! children carry different names than the kernel-declared logical schema even though their
//! `PARQUET:field_id` metadata agrees.
//!
//! # What it does
//!
//! For each [`Column`] reference in projection/predicate expressions:
//!
//! 1. Looks up the logical field by name in `logical_file_schema`.
//! 2. Finds the matching physical field via [`find_physical_match`].
//! 3. Rebuilds the column reference to point at the physical column index.
//! 4. If the logical and physical [`DataType`]s differ, wraps the column in a two-stage
//!    expression chain:
//!    - inner [`RenameNestedFieldsByIdExpr`] — at evaluate time, calls kernel's
//!      [`apply_schema_to`] to rename nested struct / list element / map entries fields
//!      to their logical names, using a precomputed kernel [`KernelDataType`] built from
//!      a field-id-aware walk of the parquet field against the logical field. This is a
//!      pure rename (no buffer copies, no reorder, no projection, no cast).
//!    - outer [`CastColumnExpr`] (provided by DataFusion) — handles everything the
//!      rename doesn't: drops parquet-only extras, fills missing-but-nullable target
//!      fields with NULL, errors on missing-and-non-nullable targets, and applies Delta
//!      type-widening casts at the leaves. After the inner rename, all matched
//!      children carry logical names so [`cast_column`]'s name-based matching just works.
//!
//! The downstream [`super::NullabilityValidationExec`] still re-asserts strict NOT NULL on
//! parent struct nodes whose decoded children carry nulls — a case neither this adapter nor
//! the parquet decoder enforce on their own.
//!
//! [`apply_schema_to`]: delta_kernel::engine::arrow_expression::apply_schema::apply_schema_to
//! [`cast_column`]: datafusion_common::nested_struct::cast_column
//! [`KernelDataType`]: delta_kernel::schema::DataType

use std::any::Any;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use datafusion_common::error::DataFusionError;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Result as DfResult;
use datafusion_common::ScalarValue;
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

        // Virtual columns (parquet `RowNumber` etc.) are materialized by the parquet
        // decoder via [`ArrowReaderOptions::with_virtual_columns`]; they never appear in
        // the on-disk physical schema, so field-id / name matching against the physical
        // schema would always miss. Pass the column reference through and let the
        // downstream column-rebinding (`reassign_expr_columns` in the opener) resolve it
        // against the post-virtual-injection stream schema by name.
        if is_virtual_column(logical_field) {
            return Ok(Arc::new(column.clone()));
        }

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
        // Build a Column pointing at the physical name (`reassign_expr_columns` in the
        // opener later rebinds the index against the narrowed stream schema by name).
        let resolved_column = Column::new(physical_field.name(), physical_idx);

        if physical_field.data_type() == logical_field.data_type() {
            // Schemas already agree at every level — no reshape needed.
            return Ok(Arc::new(resolved_column));
        }

        // Types differ. Build a per-column rename chain:
        //   Column → RenameNestedFieldsByIdExpr (kernel) → CastColumnExpr (datafusion)
        //
        // The renamed-physical field describes the *structure* of the parquet column with
        // logical names stamped onto every nested child that has a field-id (or name) match.
        // It has the same shape as the parquet column, so kernel's `apply_schema_to` can
        // walk both in lockstep and rename without copying buffers. The outer
        // `CastColumnExpr` then sees a struct whose children share names with the target
        // logical field, so its name-based `cast_column` machinery handles the remaining
        // project + null-fill + cast.
        let renamed_physical_field = Arc::new(build_renamed_physical_field(
            physical_field,
            logical_field,
        ));
        let renamed_kernel_type =
            StructField::try_from_arrow(renamed_physical_field.as_ref())
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

/// Match `target` against a slice of candidate physical fields by `PARQUET:field_id`
/// equality (preferred) and case-sensitive name equality (fallback).
///
/// The `delta.columnMapping.physicalName` case kernel handles for `Name` mode is implicit
/// here: kernel's `make_physical(Name)` stamps `parquet.field.id` on every field
/// ([`StructField::logical_to_physical_metadata`](../../../kernel/src/schema/mod.rs)), so by
/// the time the adapter sees `target` it always has a field id when column mapping is
/// active. The name fallback covers plain (non-column-mapped) tables.
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

/// A field is a "virtual" parquet column when it carries an Arrow extension type prefixed
/// with `parquet.virtual.` (e.g. [`parquet::arrow::RowNumber`] → `parquet.virtual.row_number`).
/// Such columns are materialized at decode time by arrow-rs via
/// [`ArrowReaderOptions::with_virtual_columns`] — they never appear in the on-disk parquet
/// schema, so field-id / name matching against the physical schema would always miss.
///
/// We match on the prefix rather than enumerating each typed extension so we don't need to
/// list `RowNumber`, `RowGroupId`, future virtual types, etc. one by one.
fn is_virtual_column(field: &Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .is_some_and(|name| name.starts_with("parquet.virtual."))
}

/// A [`PhysicalExpr`] that wraps an inner column-producing expression and renames its nested
/// struct / list-element / map-entries fields to match a precomputed kernel target type.
///
/// At plan time, [`build_renamed_physical_field`] walks the parquet field against the logical
/// field by `PARQUET:field_id` (with a name fallback) and produces an Arrow [`Field`] that
/// mirrors the parquet structure exactly but with logical names stamped on every matched
/// child. That Arrow field is converted to a kernel [`KernelDataType`] handed to
/// [`apply_schema_to`], which does a pure metadata rename in lockstep (no buffer copies, no
/// reorder, no projection).
///
/// The structural transform — dropping parquet-only extras, filling missing-but-nullable
/// targets, and applying type-widening casts — happens in the outer [`CastColumnExpr`] that
/// wraps this expression in [`FieldIdPhysicalExprAdapter::rewrite_column`].
#[derive(Debug, Clone)]
pub(crate) struct RenameNestedFieldsByIdExpr {
    inner: Arc<dyn PhysicalExpr>,
    /// Arrow field whose shape mirrors the parquet column but whose nested children carry
    /// logical names where a field-id match was found. Reported as `data_type()` /
    /// `return_field()` so the outer `CastColumnExpr` and downstream simplifiers see a
    /// consistent schema.
    renamed_field: FieldRef,
    /// Kernel form of `renamed_field.data_type()`. Cached here so per-batch evaluate is a
    /// single call into `apply_schema_to`.
    rename_target: Arc<KernelDataType>,
}

// `Arc<dyn PhysicalExpr>` doesn't auto-derive `PartialEq`/`Hash` for trait objects, but
// `PhysicalExpr` itself implements value equality and hashing via `dyn_eq`/`dyn_hash`.
// `rename_target` is a pure function of `renamed_field`, so it's excluded.
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
        let value = self.inner.evaluate(batch)?;
        match value {
            ColumnarValue::Array(array) => {
                let renamed = apply_schema_to(&array, &self.rename_target)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "FieldIdRename: kernel apply_schema_to failed for `{}`: {e}",
                            self.renamed_field.name(),
                        ))
                    })?;
                Ok(ColumnarValue::Array(renamed))
            }
            ColumnarValue::Scalar(scalar) => {
                let as_array = scalar.to_array_of_size(1)?;
                let renamed = apply_schema_to(&as_array, &self.rename_target)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "FieldIdRename: kernel apply_schema_to failed for `{}`: {e}",
                            self.renamed_field.name(),
                        ))
                    })?;
                let result = ScalarValue::try_from_array(renamed.as_ref(), 0)?;
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
                "RenameNestedFieldsByIdExpr requires exactly one child".into(),
            ));
        }
        let child = children.pop().unwrap();
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

/// Build an Arrow [`Field`] that has the *structure* of `physical` but adopts the *names*
/// and *nullability* of `logical` wherever a field-id (or name) match exists. Recurses
/// into structs, list elements, and map entries.
///
/// The result mirrors the parquet column 1:1 (same nesting, same child count, same data
/// types) so that kernel's [`apply_schema_to`] — which walks input and target in lockstep
/// — can rename without rebuilding any arrays. We also stamp **logical nullability** on
/// matched children so the outer [`CastColumnExpr`]'s `validate_struct_compatibility` step
/// doesn't reject a `nullable: true → false` tightening that parquet writers commonly
/// produce (the actual `nulls in non-nullable field` check is enforced downstream by
/// [`super::NullabilityValidationExec`]).
///
/// Parquet-only children are kept under their physical names and physical nullability;
/// the outer [`CastColumnExpr`] drops them.
fn build_renamed_physical_field(physical: &Field, logical: &Field) -> Field {
    let renamed_type = match (physical.data_type(), logical.data_type()) {
        (DataType::Struct(phys_children), DataType::Struct(log_children)) => {
            let renamed: Vec<FieldRef> = phys_children
                .iter()
                .map(|pc| {
                    let pc_ref = pc.as_ref();
                    match find_physical_match(pc_ref, log_children) {
                        Some(li) => {
                            Arc::new(build_renamed_physical_field(pc_ref, &log_children[li]))
                        }
                        None => Arc::clone(pc),
                    }
                })
                .collect();
            DataType::Struct(renamed.into())
        }
        (DataType::List(phys_elem), DataType::List(log_elem)) => DataType::List(Arc::new(
            build_renamed_physical_field(phys_elem.as_ref(), log_elem.as_ref()),
        )),
        (DataType::LargeList(phys_elem), DataType::LargeList(log_elem)) => DataType::LargeList(
            Arc::new(build_renamed_physical_field(phys_elem.as_ref(), log_elem.as_ref())),
        ),
        (DataType::Map(phys_entries, sorted), DataType::Map(log_entries, _)) => DataType::Map(
            Arc::new(build_renamed_physical_field(phys_entries.as_ref(), log_entries.as_ref())),
            *sorted,
        ),
        _ => physical.data_type().clone(),
    };
    Field::new(logical.name(), renamed_type, logical.is_nullable())
        .with_metadata(physical.metadata().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{Operator, ScalarUDF};
    use datafusion_functions::core::getfield::GetFieldFunc;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, IsNullExpr};
    use datafusion_physical_expr::ScalarFunctionExpr;
    use delta_kernel::arrow::array::{
        Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray, StructArray,
    };
    use delta_kernel::arrow::datatypes::Schema;

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
        let phys_struct = StructArray::new(
            Fields::from(vec![physical_a]),
            vec![a_values],
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

    // Forward-looking pushdown-readiness test. Simulates what DataFusion's parquet row
    // filter would do *if* it pushed down a struct-field predicate (today it defers
    // struct predicates to post-decode — see
    // `datafusion-fork/datafusion/datasource-parquet/src/row_filter.rs:60-65`). We build
    // the adapter-rewritten expression chain ourselves and evaluate it directly against
    // a *physical-named* `RecordBatch`, mirroring what the row filter hands a compiled
    // `DatafusionArrowPredicate`:
    //
    //     get_field(
    //         CastColumnExpr(
    //             RenameNestedFieldsByIdExpr(Column(phys_outer))
    //         ),
    //         "logical_inner"
    //     ) > 10
    //
    // wrapped as `pred OR pred IS NULL` to match the engine's NULL-preserving filter
    // semantics (`compile/logical/scan.rs:170`). Locks in checkpoint C1 from the
    // pushdown-readiness analysis.
    //
    // Out of scope (not blocking correctness):
    //   L1 — `PruningPredicate` can't see through `CastColumnExpr` /
    //        `RenameNestedFieldsByIdExpr` to derive stats bounds, so row-group / page
    //        pruning silently no-ops for column-mapped nested predicates (safe).
    //   L2 — `get_field` on a struct forces materializing the whole physical struct
    //        (no leaf-only `ProjectionMask` precision). Perf only.
    //   L3 — DataFusion's row filter currently defers struct predicates to post-decode.
    //        When that lands upstream, this is the test that confirms our pipeline is
    //        already correct under row-filter evaluation.
    #[test]
    fn simulated_row_filter_predicate_on_renamed_nested_field() -> DfResult<()> {
        // Physical: outer struct with `phys_inner` field-id=2; outer field-id=1.
        let phys_inner = Arc::new(fid(Field::new("phys_inner", DataType::Int64, false), 2));
        let physical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "phys_outer",
                DataType::Struct(Fields::from(vec![Arc::clone(&phys_inner)])),
                true,
            ),
            1,
        )]));

        // Logical: same field-ids, different (logical) names at every level.
        let logical_inner =
            Arc::new(fid(Field::new("logical_inner", DataType::Int64, false), 2));
        let logical = Arc::new(Schema::new(vec![fid(
            Field::new(
                "logical_outer",
                DataType::Struct(Fields::from(vec![Arc::clone(&logical_inner)])),
                true,
            ),
            1,
        )]));

        // 3 rows: inner values 7, 11, 13. Predicate `inner > 10` should select rows
        // 1 and 2 only.
        let inner_values: ArrayRef = Arc::new(Int64Array::from(vec![7, 11, 13]));
        let phys_struct =
            StructArray::new(Fields::from(vec![phys_inner]), vec![inner_values], None);
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(phys_struct) as ArrayRef],
        )?;

        // Adapter rewrites `Column("logical_outer")` -> rename + cast chain. The
        // resolved column references the physical name (`phys_outer`) but the chain
        // produces a struct typed as the logical field, including the inner rename.
        let factory = FieldIdPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::clone(&logical), Arc::clone(&physical))?;
        let logical_outer_col = Arc::new(Column::new_with_schema(
            "logical_outer",
            logical.as_ref(),
        )?) as Arc<dyn PhysicalExpr>;
        let rewritten = adapter.rewrite(logical_outer_col)?;

        // Sanity: chain produces logical inner-field name on the materialized struct.
        // (Locks in the half of C1 that `nested_missing_nullable_field_filled_with_nulls`
        // doesn't cover — that test exercises the missing-field shape, this one
        // exercises the field-id-based rename shape.)
        let chain_value = rewritten.evaluate(&batch)?;
        let chain_arr = match chain_value {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(batch.num_rows())?,
        };
        let chain_struct = chain_arr
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("rename chain output must be a StructArray");
        assert_eq!(chain_struct.fields().len(), 1);
        assert_eq!(
            chain_struct.fields()[0].name(),
            "logical_inner",
            "RenameNestedFieldsByIdExpr must surface logical inner-field name"
        );

        // Compose the row-filter-shaped predicate manually:
        //   get_field(rewritten, "logical_inner") > Int64(10)
        let get_field_udf = Arc::new(ScalarUDF::new_from_impl(GetFieldFunc::new()));
        let get_inner = Arc::new(ScalarFunctionExpr::new(
            "get_field",
            get_field_udf,
            vec![
                rewritten,
                expressions::lit(ScalarValue::Utf8(Some("logical_inner".into()))),
            ],
            Arc::new(Field::new("logical_inner", DataType::Int64, false)),
            Arc::new(ConfigOptions::default()),
        )) as Arc<dyn PhysicalExpr>;
        let gt_ten = Arc::new(BinaryExpr::new(
            Arc::clone(&get_inner),
            Operator::Gt,
            expressions::lit(ScalarValue::Int64(Some(10))),
        )) as Arc<dyn PhysicalExpr>;

        // Engine semantics: `pred OR pred IS NULL` keeps rows whose predicate is NULL,
        // matching kernel scan-skipping. See `compile/logical/scan.rs:170`.
        let is_null =
            Arc::new(IsNullExpr::new(Arc::clone(&gt_ten))) as Arc<dyn PhysicalExpr>;
        let null_preserving =
            Arc::new(BinaryExpr::new(gt_ten, Operator::Or, is_null)) as Arc<dyn PhysicalExpr>;

        let mask_value = null_preserving.evaluate(&batch)?;
        let mask_arr = match mask_value {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(batch.num_rows())?,
        };
        let mask = mask_arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("predicate must evaluate to BooleanArray");
        let actual: Vec<Option<bool>> = (0..mask.len())
            .map(|i| (!mask.is_null(i)).then(|| mask.value(i)))
            .collect();
        assert_eq!(
            actual,
            vec![Some(false), Some(true), Some(true)],
            "inner > 10 must match rows where inner = 11 and 13"
        );
        Ok(())
    }

    // No field IDs anywhere → adapter resolves by name fallback and passes through
    // unchanged. This is the "no column mapping" baseline.
    #[test]
    fn no_field_ids_passthrough() -> DfResult<()> {
        let logical = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let physical = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let rewritten =
            rewrite_column(Arc::clone(&logical), Arc::clone(&physical), "a")?;
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

    // Forward-compat: logical schema has a nullable column that the physical schema
    // doesn't (e.g. `domainMetadata` against an older V1 checkpoint). The adapter must
    // substitute a typed NULL literal, matching DefaultPhysicalExprAdapter semantics.
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

    // Missing NON-nullable logical column must error — silently filling with NULL would
    // be data corruption.
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

}
