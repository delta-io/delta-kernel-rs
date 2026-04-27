//! Spatial ST_Intersects predicate implemented as an [`ArrowOpaquePredicateOp`].
//!
//! Enables row-level filtering of WKB geometry columns using spatial intersection. Construct a
//! predicate via [`st_intersects_predicate`] and pass it as the scan's predicate.
//!
//! Data skipping is lowered to a kernel-native AND of four 2D axis-aligned bounding-box (AABB)
//! inequalities over WKB-encoded Point stats. [`StXOp`] and [`StYOp`] are internal opaque
//! expression ops that extract the `x` / `y` coordinate from a WKB Point array at skipping time.
//! Partition pruning (direct evaluation against parquet footer scalars) is not yet implemented.

use std::sync::Arc;

use geo::BoundingRect as _;
use geo_traits::to_geo::ToGeoGeometry as _;
use geoarrow_array::array::WkbArray;
use geoarrow_schema::{Crs, Metadata, WkbType};

use crate::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Builder, RecordBatch,
};
use crate::arrow::compute::kernels::boolean::not as arrow_not;
use crate::engine::arrow_expression::evaluate_expression::evaluate_expression;
use crate::engine::arrow_expression::opaque::{
    ArrowOpaqueExpression, ArrowOpaqueExpressionOp, ArrowOpaquePredicate, ArrowOpaquePredicateOp,
};
use crate::error::{DeltaResult, Error};
use crate::expressions::{Expression, Predicate, Scalar, ScalarExpressionEvaluator};
use crate::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use crate::schema::{ColumnName, DataType, GeometryType};

/// Opaque predicate op implementing the ST_Intersects spatial predicate.
///
/// Evaluates row-by-row whether a WKB geometry column intersects a fixed WKB query geometry.
/// Expects exactly two args: a column expression evaluating to a binary (WKB) array, and a
/// binary WKB literal representing the query geometry.
#[derive(Debug, PartialEq)]
pub struct StIntersectsOp;

/// Creates an ST_Intersects predicate that filters rows where the named WKB geometry column
/// intersects the query geometry provided as raw ISO WKB bytes.
///
/// The query literal is tagged with the default [`GeometryType`] (`OGC:CRS84`); callers that
/// know the SRID of their query geometry should use [`st_intersects_predicate_with_crs`]
/// instead. Under the hood, every kernel-produced geo literal rides as a
/// [`Scalar::Geometry`] so its CRS is preserved through evaluation.
///
/// # Parameters
///
/// - `col`: the name of the geometry column in the table (must store geometries as WKB binary).
/// - `wkb`: the query geometry encoded as ISO WKB bytes.
pub fn st_intersects_predicate(col: impl Into<ColumnName>, wkb: Vec<u8>) -> Predicate {
    st_intersects_predicate_with_crs(col, GeometryType::default(), wkb)
}

/// Creates an ST_Intersects predicate whose literal carries its declared CRS via
/// [`Scalar::Geometry`]. This is the recommended form when the caller knows the SRID of the
/// query geometry and wants it to round-trip through the predicate.
///
/// # Parameters
///
/// - `col`: the name of the geometry column.
/// - `ty`: the [`GeometryType`] describing the CRS of the query geometry.
/// - `wkb`: the query geometry encoded as ISO WKB bytes.
pub fn st_intersects_predicate_with_crs(
    col: impl Into<ColumnName>,
    ty: GeometryType,
    wkb: Vec<u8>,
) -> Predicate {
    Predicate::arrow_opaque(
        StIntersectsOp,
        [
            Expression::column(col.into()),
            Expression::literal(Scalar::geometry(ty, wkb)),
        ],
    )
}

/// Parse a raw ISO WKB byte slice into a `geo::Geometry`.
fn parse_wkb(bytes: &[u8]) -> DeltaResult<geo::Geometry<f64>> {
    let wkb = wkb::reader::read_wkb(bytes)
        .map_err(|e| Error::invalid_expression(format!("invalid WKB geometry: {e}")))?;
    wkb.try_to_geometry()
        .ok_or_else(|| Error::invalid_expression("WKB geometry is an empty point"))
}

/// 2D axis-aligned bounding box of a single geometry expression, expressed as four
/// [`Expression`]s that all produce `f64`.
///
/// Used to lower `ST_Intersects(a, b)` into the kernel-native AABB overlap predicate
/// `AND(a.xmax >= b.xmin, a.xmin <= b.xmax, a.ymax >= b.ymin, a.ymin <= b.ymax)`.
struct Bbox {
    xmin: Expression,
    ymin: Expression,
    xmax: Expression,
    ymax: Expression,
}

/// Attempts to lower an ST_Intersects operand to its AABB corners.
///
/// Two operand shapes are supported:
/// - `Expression::Literal(Scalar::Geometry(g))`: parses the WKB bytes to a `geo::Geometry`,
///   asks for its `bounding_rect()`, and emits four `f64` literal expressions. Geography is
///   explicitly rejected here because Geography is a separate `Scalar` variant; this branch
///   is guarded by the `Scalar::Geometry` pattern.
/// - `Expression::Column(col)`: fetches the min- and max-corner stat expressions from the
///   [`IndirectDataSkippingPredicateEvaluator`] and wraps each with the internal [`StXOp`] /
///   [`StYOp`] opaque coord extractor, so at stats-batch eval time the four scalar corner
///   values are recovered from the stored WKB-encoded Points.
///
/// Returns `None` for any other operand shape (including degenerate geometries whose
/// bounding box is undefined, e.g. empty collections).
fn operand_bbox(
    operand: &Expression,
    evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
) -> Option<Bbox> {
    match operand {
        Expression::Literal(Scalar::Geometry(g)) => {
            let geom = parse_wkb(g.bytes()).ok()?;
            let rect = geom.bounding_rect()?;
            let (min, max) = (rect.min(), rect.max());
            Some(Bbox {
                xmin: Expression::literal(min.x),
                ymin: Expression::literal(min.y),
                xmax: Expression::literal(max.x),
                ymax: Expression::literal(max.y),
            })
        }
        Expression::Column(col) => {
            // min/max are themselves WKB-encoded Point stats; StXOp/StYOp will extract the
            // coordinate from each at evaluation time.
            let min_expr = evaluator.get_min_stat(col, &DataType::BINARY)?;
            let max_expr = evaluator.get_max_stat(col, &DataType::BINARY)?;
            Some(Bbox {
                xmin: Expression::arrow_opaque(StXOp, [min_expr.clone()]),
                ymin: Expression::arrow_opaque(StYOp, [min_expr]),
                xmax: Expression::arrow_opaque(StXOp, [max_expr.clone()]),
                ymax: Expression::arrow_opaque(StYOp, [max_expr]),
            })
        }
        _ => None,
    }
}

impl ArrowOpaquePredicateOp for StIntersectsOp {
    fn name(&self) -> &str {
        "st_intersects"
    }

    /// Evaluates the spatial intersection vectorised over an Arrow [`RecordBatch`].
    ///
    /// Wraps the column and query literal as CRS-tagged [`WkbArray`]s and delegates the
    /// actual intersection test to [`geoarrow_expr_geo::intersects`], which handles tri-valued
    /// SQL null semantics internally (null on either side produces null out).
    ///
    /// # Parameters
    ///
    /// - `args`: exactly two expressions -- the geometry column and a geometry literal query.
    /// - `batch`: the Arrow data to filter.
    /// - `inverted`: if `true`, returns rows that do NOT intersect (ST_Disjoint semantics).
    ///
    /// # Errors
    ///
    /// Returns an error when `args` is not length 2, when the column side is not an
    /// `Expression::Column`, when the literal side is not a [`Scalar::Geometry`] (legacy
    /// `Scalar::Binary` geo literals are rejected; use [`st_intersects_predicate`] to construct
    /// a CRS-tagged literal from raw WKB bytes), or when the underlying WKB-array wrap / spatial
    /// predicate evaluation fails.
    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> DeltaResult<BooleanArray> {
        let [col_expr, lit_expr] = args else {
            return Err(Error::invalid_expression(format!(
                "st_intersects requires exactly 2 args, got {}",
                args.len()
            )));
        };

        // Column-side WkbType: pulled from the batch schema's extension metadata. We
        // `unwrap_or_default()` -- geoarrow does the same fallback inside TryFrom<(Array, Field)>,
        // and we are not enforcing CRS match here (separate follow-up).
        let col_name = match col_expr {
            Expression::Column(name) => name,
            _ => {
                return Err(Error::invalid_expression(
                    "st_intersects: first argument must be a column reference",
                ))
            }
        };
        let col_leaf = col_name.path().last().ok_or_else(|| {
            Error::invalid_expression("st_intersects: column reference has no path segments")
        })?;
        let schema = batch.schema();
        let col_field = schema.field_with_name(col_leaf).map_err(|e| {
            Error::invalid_expression(format!(
                "st_intersects: column '{col_leaf}' not found in batch schema: {e}"
            ))
        })?;
        let col_wkb_type = col_field
            .try_extension_type::<WkbType>()
            .unwrap_or_default();

        // Literal-side WkbType: pulled from the Scalar::Geometry's declared GeometryType.
        // Scalar::Binary literals are rejected -- every query geometry must travel with a
        // typed GeometryType so its CRS is known. Kernel-side constructors already produce
        // Scalar::Geometry (see `st_intersects_predicate` / `st_intersects_predicate_with_crs`);
        // this branch only fires when an external caller bypasses those constructors.
        let lit_wkb_type = match lit_expr {
            Expression::Literal(Scalar::Geometry(g)) => WkbType::new(Arc::new(Metadata::new(
                Crs::from_srid(g.ty().srid().to_string()),
                None,
            ))),
            _ => {
                return Err(Error::invalid_expression(
                    "st_intersects: second argument must be a Scalar::Geometry literal",
                ))
            }
        };

        // Evaluate both args to Arrow arrays. The Scalar::Geometry literal broadcasts to
        // batch.num_rows() copies as a BinaryArray via Scalar::to_array.
        let col_array = evaluate_expression(col_expr, batch, None)?;
        let lit_array = evaluate_expression(lit_expr, batch, None)?;

        // Wrap as CRS-tagged WkbArrays. TryFrom<(&dyn Array, WkbType)> handles both Binary and
        // LargeBinary inputs and does the downcast internally (geoarrow-array wkb.rs:176-191).
        let col_wkb = WkbArray::try_from((col_array.as_ref(), col_wkb_type))
            .map_err(|e| Error::invalid_expression(format!("st_intersects: column: {e}")))?;
        let lit_wkb = WkbArray::try_from((lit_array.as_ref(), lit_wkb_type))
            .map_err(|e| Error::invalid_expression(format!("st_intersects: literal: {e}")))?;

        // Delegate to geoarrow-expr-geo. Tri-valued null semantics are handled by the library.
        let result = geoarrow_expr_geo::intersects(&col_wkb, &lit_wkb).map_err(|e| {
            Error::invalid_expression(format!("st_intersects spatial evaluation failed: {e}"))
        })?;

        // NOT ST_Intersects (ST_Disjoint). `arrow_not` preserves nulls, matching SQL semantics.
        if inverted {
            Ok(arrow_not(&result)?)
        } else {
            Ok(result)
        }
    }

    /// Partition pruning is not implemented for spatial predicates; returns `Ok(None)`.
    fn eval_pred_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _eval_pred: &DirectPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        Ok(None)
    }

    /// Direct (parquet footer / scalar) stats-based skipping is not yet implemented.
    // TODO(followup): end-to-end eval over footer stats once we have scalar coord extractors.
    fn eval_as_data_skipping_predicate(
        &self,
        _predicate_evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<bool> {
        None
    }

    /// Lowers `ST_Intersects(a, b)` into the kernel-native 2D bounding-box overlap predicate
    ///
    /// ```text
    /// AND(a.xmax >= b.xmin, a.xmin <= b.xmax,
    ///     a.ymax >= b.ymin, a.ymin <= b.ymax)
    /// ```
    ///
    /// so file stats can be evaluated against it during log replay. Returns `None` (the predicate
    /// is dropped from skipping, which is always safe) in these cases:
    /// - `inverted == true`: NOT ST_Intersects cannot be pruned by bbox -- a bounding box that
    ///   covers the query does not prove the file contains a geometry that fails to intersect it.
    /// - Both operands are literals: degenerate; the caller can fold this statically, and it
    ///   would not reference any column stats anyway.
    /// - Either operand is neither `Expression::Column` nor `Expression::Literal(Scalar::Geometry)`.
    /// - A literal's WKB fails to parse or has no bounding rect (e.g. empty geometry).
    /// - Required stats are missing from the [`IndirectDataSkippingPredicateEvaluator`].
    fn as_data_skipping_predicate(
        &self,
        predicate_evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate> {
        if inverted {
            return None;
        }
        let [a_expr, b_expr] = exprs else { return None };

        // Both-literal: no column stats involved; skipping buys nothing here.
        if matches!(a_expr, Expression::Literal(_)) && matches!(b_expr, Expression::Literal(_)) {
            return None;
        }

        let a = operand_bbox(a_expr, predicate_evaluator)?;
        let b = operand_bbox(b_expr, predicate_evaluator)?;

        Some(Predicate::and_from([
            Predicate::ge(a.xmax, b.xmin),
            Predicate::le(a.xmin, b.xmax),
            Predicate::ge(a.ymax, b.ymin),
            Predicate::le(a.ymin, b.ymax),
        ]))
    }
}

/// Extracts the `x` coordinate from a WKB-encoded Point BinaryArray, producing a Float64 array.
///
/// Used by [`StIntersectsOp::as_data_skipping_predicate`] to lift min/max Point stats columns
/// to per-corner scalar coordinate expressions for AABB overlap skipping. Conservative on all
/// data errors: null rows, non-Point WKB, and malformed WKB all produce `null` in the output
/// so the surrounding stats-based predicate returns NULL (= keep the file).
///
/// Shape mismatches (wrong arg count, non-Binary input array) remain `Err` -- those are
/// planner bugs, not row-level data issues.
#[derive(Debug, PartialEq)]
pub struct StXOp;

/// Extracts the `y` coordinate from a WKB-encoded Point BinaryArray, producing a Float64 array.
///
/// See [`StXOp`] for the full contract; this op differs only in which coordinate it emits.
#[derive(Debug, PartialEq)]
pub struct StYOp;

/// Which coordinate to extract from a WKB Point.
#[derive(Clone, Copy)]
enum Coord {
    X,
    Y,
}

/// Shared implementation for [`StXOp`] / [`StYOp`]: walks a BinaryArray of WKB bytes and emits
/// a Float64Array with the requested coordinate (or null for any row that is not a valid Point).
fn eval_point_coord(
    op_name: &str,
    args: &[Expression],
    batch: &RecordBatch,
    coord: Coord,
) -> DeltaResult<ArrayRef> {
    let [arg] = args else {
        return Err(Error::invalid_expression(format!(
            "{op_name} requires exactly 1 arg, got {}",
            args.len()
        )));
    };

    let arr = evaluate_expression(arg, batch, None)?;
    let bin = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
        Error::invalid_expression(format!("{op_name}: expected a binary input array"))
    })?;

    let mut builder = Float64Builder::with_capacity(bin.len());
    for i in 0..bin.len() {
        if bin.is_null(i) {
            builder.append_null();
            continue;
        }
        // Malformed WKB or non-Point geometries conservatively yield null. Bubbling up an
        // Err here would fail the whole scan for a single bad stats row; data-skipping must
        // degrade gracefully.
        match parse_wkb(bin.value(i)) {
            Ok(geo::Geometry::Point(p)) => {
                let v = match coord {
                    Coord::X => p.x(),
                    Coord::Y => p.y(),
                };
                builder.append_value(v);
            }
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

impl ArrowOpaqueExpressionOp for StXOp {
    fn name(&self) -> &str {
        "st_x"
    }

    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        eval_point_coord(self.name(), args, batch, Coord::X)
    }

    fn eval_expr_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        Err(Error::unsupported(
            "st_x: scalar expression evaluation is not supported",
        ))
    }
}

impl ArrowOpaqueExpressionOp for StYOp {
    fn name(&self) -> &str {
        "st_y"
    }

    fn eval_expr(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        _result_type: Option<&DataType>,
    ) -> DeltaResult<ArrayRef> {
        eval_point_coord(self.name(), args, batch, Coord::Y)
    }

    fn eval_expr_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _exprs: &[Expression],
    ) -> DeltaResult<Scalar> {
        Err(Error::unsupported(
            "st_y: scalar expression evaluation is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{StIntersectsOp, StXOp, StYOp};
    use crate::arrow::array::{Array as _, BinaryArray, BooleanArray, Float64Array, RecordBatch};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use crate::engine::arrow_expression::evaluate_expression::{
        evaluate_expression, evaluate_predicate,
    };
    use crate::engine::arrow_expression::opaque::{
        ArrowOpaqueExpression, ArrowOpaqueExpressionOp, ArrowOpaquePredicate,
        ArrowOpaquePredicateOp,
    };
    use crate::expressions::{BinaryPredicateOp, Expression, Predicate, Scalar};
    use crate::kernel_predicates::DataSkippingPredicateEvaluator;
    use crate::scan::data_skipping::as_data_skipping_predicate;
    use crate::schema::{ColumnName, DataType, GeometryType};

    /// Encode a 2D point as ISO WKB (little-endian).
    fn point_wkb(x: f64, y: f64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(21);
        buf.push(0x01); // little-endian
        buf.extend_from_slice(&1u32.to_le_bytes()); // WKB type: Point = 1
        buf.extend_from_slice(&x.to_le_bytes());
        buf.extend_from_slice(&y.to_le_bytes());
        buf
    }

    /// Encode a closed polygon ring as ISO WKB (little-endian). `rings` is a slice of rings;
    /// each ring is a slice of (x, y) pairs. The caller is responsible for closing each ring
    /// (first == last coordinate).
    fn polygon_wkb(rings: &[&[(f64, f64)]]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x01); // little-endian
        buf.extend_from_slice(&3u32.to_le_bytes()); // WKB type: Polygon = 3
        buf.extend_from_slice(&(rings.len() as u32).to_le_bytes());
        for ring in rings {
            buf.extend_from_slice(&(ring.len() as u32).to_le_bytes());
            for &(x, y) in *ring {
                buf.extend_from_slice(&x.to_le_bytes());
                buf.extend_from_slice(&y.to_le_bytes());
            }
        }
        buf
    }

    /// Build a RecordBatch with a single `geom` column of type Binary, containing WKB geometries.
    fn make_geom_batch(wkb_values: Vec<Option<Vec<u8>>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "geom",
            ArrowDataType::Binary,
            true,
        )]));
        let array: BinaryArray = wkb_values
            .iter()
            .map(|v| v.as_deref())
            .collect::<Vec<_>>()
            .into_iter()
            .collect();
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_st_intersects_basic() {
        // Query geometry: unit square polygon [0,0] to [1,1]
        let query_wkb =
            polygon_wkb(&[&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]]);

        // Row geometries:
        //   row 0: POINT(0.5, 0.5) -- inside the query polygon -> intersects
        //   row 1: POINT(2.0, 2.0) -- outside the query polygon -> does not intersect
        //   row 2: null geometry -> null output (SQL three-valued logic)
        let batch = make_geom_batch(vec![
            Some(point_wkb(0.5, 0.5)),
            Some(point_wkb(2.0, 2.0)),
            None,
        ]);

        let pred = Predicate::arrow_opaque(
            StIntersectsOp,
            [
                Expression::column(["geom"]),
                Expression::literal(Scalar::geometry(GeometryType::default(), query_wkb)),
            ],
        );

        let result = evaluate_predicate(&pred, &batch, false).unwrap();
        assert_eq!(
            result,
            BooleanArray::from(vec![Some(true), Some(false), None])
        );
    }

    #[test]
    fn test_st_intersects_inverted() {
        // Same setup, but inverted (NOT ST_Intersects = ST_Disjoint semantics).
        let query_wkb =
            polygon_wkb(&[&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]]);

        let batch = make_geom_batch(vec![
            Some(point_wkb(0.5, 0.5)),
            Some(point_wkb(2.0, 2.0)),
            None,
        ]);

        let pred = Predicate::arrow_opaque(
            StIntersectsOp,
            [
                Expression::column(["geom"]),
                Expression::literal(Scalar::geometry(GeometryType::default(), query_wkb)),
            ],
        );

        // inverted=true: the inside point becomes false, the outside point becomes true.
        let result = evaluate_predicate(&pred, &batch, true).unwrap();
        assert_eq!(
            result,
            BooleanArray::from(vec![Some(false), Some(true), None])
        );
    }

    /// Evaluates an `StXOp`/`StYOp` over a BinaryArray of WKB bytes by wrapping it in a
    /// single-column `geom` RecordBatch and routing the op through `evaluate_expression`.
    fn eval_coord_op<T: ArrowOpaqueExpressionOp + 'static>(
        op: T,
        wkb_values: Vec<Option<Vec<u8>>>,
    ) -> Float64Array {
        let batch = make_geom_batch(wkb_values);
        let expr = Expression::arrow_opaque(op, [Expression::column(["geom"])]);
        let arr = evaluate_expression(&expr, &batch, None).unwrap();
        arr.as_any().downcast_ref::<Float64Array>().unwrap().clone()
    }

    #[test]
    fn test_st_x_basic() {
        let out = eval_coord_op(
            StXOp,
            vec![Some(point_wkb(1.0, 2.0)), Some(point_wkb(3.0, 4.0)), None],
        );
        assert_eq!(out, Float64Array::from(vec![Some(1.0), Some(3.0), None]));
    }

    #[test]
    fn test_st_y_basic() {
        let out = eval_coord_op(
            StYOp,
            vec![Some(point_wkb(1.0, 2.0)), Some(point_wkb(3.0, 4.0)), None],
        );
        assert_eq!(out, Float64Array::from(vec![Some(2.0), Some(4.0), None]));
    }

    #[test]
    fn test_st_x_malformed_wkb_gives_null() {
        // Row 0: valid point. Row 1: garbage bytes -- WKB parse fails, conservative null out.
        let out = eval_coord_op(
            StXOp,
            vec![
                Some(point_wkb(7.5, 8.5)),
                Some(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ],
        );
        assert_eq!(out, Float64Array::from(vec![Some(7.5), None]));
    }

    #[test]
    fn test_st_x_non_point_gives_null() {
        // Polygon parses successfully but is not a Point -- conservative null.
        let poly = polygon_wkb(&[&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]]);
        let out = eval_coord_op(StXOp, vec![Some(poly)]);
        assert_eq!(out, Float64Array::from(vec![Option::<f64>::None]));
    }

    #[test]
    fn test_as_data_skipping_predicate_inverted_returns_none() {
        // NOT ST_Intersects is not bbox-prunable: a file whose bbox overlaps the query may
        // still contain only non-intersecting geometries, so the bbox cannot prove ST_Disjoint.
        let poly_wkb =
            polygon_wkb(&[&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]]);
        let pred = Predicate::not(
            crate::engine::arrow_expression::st_intersects::st_intersects_predicate(
                ColumnName::new(["geom"]),
                poly_wkb,
            ),
        );
        // The top-level `Not` pushes through to the opaque predicate as `inverted = true`,
        // which is short-circuited to None inside `as_data_skipping_predicate`. The junction
        // wrapping turns that None into a NULL literal inside the parent junction; but here
        // we have no surrounding junction, so the whole transform returns None.
        assert!(as_data_skipping_predicate(&pred).is_none());
    }

    #[test]
    fn test_as_data_skipping_predicate_both_literals_returns_none() {
        // Two Scalar::Geometry literals -- no column stats in play, so skipping is a no-op.
        // We cannot drive this through the public `st_intersects_predicate` builder (which
        // forces a column as the first arg), so we call `as_data_skipping_predicate` on the
        // op directly with a mock evaluator that is never actually consulted.
        let poly_wkb =
            polygon_wkb(&[&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]]);
        let other = point_wkb(0.5, 0.5);
        let exprs = [
            Expression::literal(Scalar::geometry(GeometryType::default(), poly_wkb)),
            Expression::literal(Scalar::geometry(GeometryType::default(), other)),
        ];
        let evaluator = UnreachableIndirectEvaluator;
        let result = StIntersectsOp.as_data_skipping_predicate(&evaluator, &exprs, false);
        assert!(result.is_none());
    }

    /// An [`IndirectDataSkippingPredicateEvaluator`] whose every method panics. Used in tests
    /// that expect early short-circuiting before any evaluator call happens.
    struct UnreachableIndirectEvaluator;

    impl DataSkippingPredicateEvaluator for UnreachableIndirectEvaluator {
        type Output = Predicate;
        type ColumnStat = Expression;

        fn get_min_stat(&self, _col: &ColumnName, _dt: &DataType) -> Option<Expression> {
            unreachable!("get_min_stat must not be called")
        }
        fn get_max_stat(&self, _col: &ColumnName, _dt: &DataType) -> Option<Expression> {
            unreachable!("get_max_stat must not be called")
        }
        fn get_nullcount_stat(&self, _col: &ColumnName) -> Option<Expression> {
            unreachable!()
        }
        fn get_rowcount_stat(&self) -> Option<Expression> {
            unreachable!()
        }
        fn eval_pred_scalar(&self, _v: &Scalar, _i: bool) -> Option<Predicate> {
            unreachable!()
        }
        fn eval_pred_scalar_is_null(&self, _v: &Scalar, _i: bool) -> Option<Predicate> {
            unreachable!()
        }
        fn eval_pred_is_null(&self, _c: &ColumnName, _i: bool) -> Option<Predicate> {
            unreachable!()
        }
        fn eval_pred_binary_scalars(
            &self,
            _op: BinaryPredicateOp,
            _l: &Scalar,
            _r: &Scalar,
            _i: bool,
        ) -> Option<Predicate> {
            unreachable!()
        }
        fn eval_pred_opaque(
            &self,
            _op: &crate::expressions::OpaquePredicateOpRef,
            _e: &[Expression],
            _i: bool,
        ) -> Option<Predicate> {
            unreachable!()
        }
        fn eval_partial_cmp(
            &self,
            _o: std::cmp::Ordering,
            _c: Expression,
            _v: &Scalar,
            _i: bool,
        ) -> Option<Predicate> {
            unreachable!()
        }
        fn finish_eval_pred_junction(
            &self,
            _op: crate::expressions::JunctionPredicateOp,
            _ps: &mut dyn Iterator<Item = Option<Predicate>>,
            _i: bool,
        ) -> Option<Predicate> {
            unreachable!()
        }
    }

    #[test]
    fn test_as_data_skipping_predicate_col_vs_literal_shape() {
        // ST_Intersects(col("geom"), POLYGON): expect AND of 4 binary comparisons, each
        // comparing an `st_x`/`st_y` opaque expression against an f64 literal.
        let poly_wkb =
            polygon_wkb(&[&[(2.0, 3.0), (6.0, 3.0), (6.0, 7.0), (2.0, 7.0), (2.0, 3.0)]]);
        let pred = crate::engine::arrow_expression::st_intersects::st_intersects_predicate(
            ColumnName::new(["geom"]),
            poly_wkb,
        );

        // Drive the transform via the standard data-skipping entry point.
        let skipping = as_data_skipping_predicate(&pred).expect("predicate should be skippable");

        // Shape: Junction(And, [4 preds]). Each pred is either Binary(LT/GT, ...) or
        // Not(Binary(LT/GT, ...)) because Predicate::le / Predicate::ge are sugared via NOT.
        let Predicate::Junction(j) = &skipping else {
            panic!("expected AND junction, got {skipping:#?}");
        };
        assert_eq!(j.op, crate::expressions::JunctionPredicateOp::And);
        assert_eq!(j.preds.len(), 4, "expected 4 bbox inequalities");

        for (i, sub) in j.preds.iter().enumerate() {
            // Unwrap one optional NOT wrapper.
            let inner = match sub {
                Predicate::Not(p) => p.as_ref(),
                p => p,
            };
            let Predicate::Binary(bp) = inner else {
                panic!("sub-predicate {i} is not a binary predicate: {sub:#?}");
            };
            assert!(
                matches!(
                    bp.op,
                    BinaryPredicateOp::LessThan | BinaryPredicateOp::GreaterThan
                ),
                "sub-predicate {i} op is not a comparison: {:?}",
                bp.op
            );
            // Exactly one side should be an `st_x`/`st_y` opaque expression; the other an
            // f64 literal. Both corners of an AABB overlap check have this shape here.
            let (col_side, lit_side) = match (bp.left.as_ref(), bp.right.as_ref()) {
                (Expression::Opaque(_), Expression::Literal(_)) => (&*bp.left, &*bp.right),
                (Expression::Literal(_), Expression::Opaque(_)) => (&*bp.right, &*bp.left),
                _ => panic!("sub-predicate {i}: expected one opaque and one literal side"),
            };
            let Expression::Opaque(op_expr) = col_side else {
                unreachable!()
            };
            let name = op_expr.op.name();
            assert!(
                name == "st_x" || name == "st_y",
                "sub-predicate {i}: opaque op is {name}"
            );
            assert!(
                matches!(lit_side, Expression::Literal(Scalar::Double(_))),
                "sub-predicate {i}: literal is not Double: {lit_side:#?}"
            );
        }
    }

    // TODO(followup): end-to-end eval over stats batch -- requires building a RecordBatch with
    // the nested `stats_parsed.{minValues,maxValues}.geom` struct schema, which is non-trivial
    // to assemble by hand. Covered instead by the shape tests above and the Bbox unit tests.
}
