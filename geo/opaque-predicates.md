# Geo Types Implementation Plan

## Context

Add first-class `geometry` and `geography` support to delta-kernel-rs per the Delta Lake geo RFC.
The goal is to parse and round-trip geo-typed columns in Delta table schemas, enforce the
`"geospatial"` table feature, represent geo data as GeoArrow WKB in the Arrow default engine,
and enable file-level data skipping for `ST_Intersects` predicates via the existing opaque
predicate infrastructure.

**Confirmed design decisions:**
- `PrimitiveType::Geometry(GeometryType)` + `PrimitiveType::Geography(GeographyType)` — first-class type variants
- No new `Scalar` variant — geo values travel as `Scalar::Binary(WKB bytes)`
- Arrow layer: GeoArrow field-level metadata (`ARROW:extension:name = "geoarrow.wkb"`) on a `Binary` physical array
- Table feature name: `"geospatial"` (confirmed from DBR `GeoSpatialTableFeature`, RFC-aligned), `ReaderWriter`
- Stats format: `minValues.col = "POINT(xMin yMin)"`, `maxValues.col = "POINT(xMax yMax)"` WKT strings in the existing stats JSON fields; Geography gets null count only
- Data skipping: opaque predicate in the default engine (`StIntersectsOp` implementing `ArrowOpaquePredicateOp`)

**Existing plan reference:** `geo/plan-with-binary-scalar.md` (in repo) covers phases 1-3 in detail.
This plan extends it with the stats schema changes (phase 4) and opaque predicate (phase 5).

---

## Phase 1 — Type System (`kernel/src/schema/mod.rs`)

Define (before `DecimalType`):

```rust
pub enum EdgeInterpolationAlgorithm { Spherical, Vincenty, Thomas, Andoyer, Karney }
// Display: lowercase; from_str: parse lowercase, error on unknown

pub struct GeometryType { srid: String }
// DEFAULT_SRID = "OGC:CRS84"; try_new(srid) -> DeltaResult<Self>; srid(&self) -> &str

pub struct GeographyType { srid: String, algorithm: EdgeInterpolationAlgorithm }
// DEFAULT_SRID = "OGC:CRS84"; DEFAULT_ALGORITHM = Spherical
```

Add to `PrimitiveType` enum:
```rust
Geometry(GeometryType),
Geography(GeographyType),
```

Serde: custom serialize to `"geometry(OGC:CRS84)"` / `"geography(OGC:CRS84, spherical)"`.
Deserialize: match `"geometry"` prefix and `"geography"` prefix before the unsupported fallback.
Display: same format as serialization.
Accept bare `"geometry"` on read (default to OGC:CRS84).

**Run `cargo build --workspace --all-features` after this step** — all exhaustive match sites will
fail to compile; fix them in phase 2.

---

## Phase 2 — Compiler Error Fixes (exhaustive match sites)

| File | Change |
|---|---|
| `kernel/src/expressions/scalars.rs` | `parse_scalar` match: `Geometry(_) \| Geography(_) => Err("geo types cannot be partition column values")` |
| `kernel/src/engine/arrow_conversion.rs` | `TryFromKernel<&DataType>` for `ArrowDataType`: both geo variants → `Ok(ArrowDataType::Binary)`. Also: `TryFromKernel<&StructField>` for `ArrowField`: inject GeoArrow metadata when field type is geo. `TryFromArrow<&ArrowField>`: detect `ARROW:extension:name = "geoarrow.wkb"` before falling through to type conversion. |
| `kernel/src/engine/ensure_data_types.rs` | Add 4 arms: `(Geometry(_), Binary)`, `(Geometry(_), LargeBinary)`, `(Geography(_), Binary)`, `(Geography(_), LargeBinary)` → `Ok(Identical)` |
| `kernel/src/engine/parquet_row_group_skipping.rs` | `get_parquet_min_stat` and `get_parquet_max_stat`: add `(Geometry(_), _) \| (Geography(_), _) => return None` |
| `kernel/src/engine/arrow_expression/mod.rs` | `append_null`: geo variants → `append_nulls_as!(array::BinaryBuilder)` |
| `kernel/src/engine/arrow_data.rs` | Add geo arm alongside `Binary` in any exhaustive `DataType` match |
| `kernel/src/transaction/stats_verifier.rs` | Add geo arms returning an error (geo columns not valid clustering/stats columns) in `col_types_for` exhaustive matches |
| `ffi/src/schema.rs` | `visit_schema_item`: map both geo variants to `call!(visit_binary)` (avoids breaking ABI) |

### GeoArrow field metadata details (for `arrow_conversion.rs`)
- Inject on write: `ARROW:extension:name = "geoarrow.wkb"`, `ARROW:extension:metadata = {"crs":"OGC:CRS84"}` (geometry) or `{"crs":"OGC:CRS84","edges":"spherical"}` (geography — `"edges"` key distinguishes geo from geom)
- Parse on read: if `ARROW:extension:name == "geoarrow.wkb"` → parse CRS and optional `"edges"` from metadata JSON → reconstruct `PrimitiveType::Geometry` or `PrimitiveType::Geography`
- `TryFromArrow<&ArrowDataType>` (data-type only): no change — Binary stays Binary; geo recovery is field-level

---

## Phase 3 — Table Feature

**`kernel/src/table_features/mod.rs`:**
```rust
/// Geospatial type support: geometry and geography columns with WKB encoding.
#[strum(serialize = "geospatial")]
#[serde(rename = "geospatial")]
GeospatialType,
```
Add `GEOSPATIAL_TYPE_INFO: FeatureInfo` with `feature_type: ReaderWriter`, `kernel_support: Supported`,
`enablement_check: AlwaysIfSupported`. Update `feature_type()` and `info()` match arms.

**`kernel/src/table_features/geospatial.rs` (NEW — pattern: `timestamp_ntz.rs`):**
```rust
pub(crate) fn validate_geospatial_feature_support(tc: &TableConfiguration) -> DeltaResult<()>
pub(crate) fn schema_contains_geospatial(schema: &Schema) -> bool
// schema_contains_geospatial: walks schema via SchemaTransform, returns true on Geometry(_)|Geography(_)
```

**`kernel/src/table_configuration.rs`:** Add `validate_geospatial_feature_support(&table_config)?;`
alongside the `validate_timestamp_ntz_feature_support` call.

---

## Phase 4 — Stats Schema Changes

**`kernel/src/scan/data_skipping/stats_schema/mod.rs`:**

`is_skipping_eligible_datatype` — no change (geo is not eligible in the generic sense).

`MinMaxStatsTransform::transform_primitive` — add explicit handling BEFORE the `is_skipping_eligible_datatype` gate:
```rust
// Geometry min/max stats are WKT POINT strings, not WKB binary values
PrimitiveType::Geometry(_) => Some(Cow::Owned(PrimitiveType::String)),
// Geography: spherical bbox is meaningless — no min/max stats
PrimitiveType::Geography(_) => None,
```

This ensures the stats schema for a geometry column emits `minValues.col: STRING` and
`maxValues.col: STRING`, matching how the Delta log actually stores WKT POINT strings.
No min/max columns for geography columns (only null count, which is automatic).

---

## Phase 5 — ST_Intersects Opaque Predicate (default engine)

**New file: `kernel/src/engine/arrow_expression/st_intersects.rs`**

Implement `ArrowOpaquePredicateOp` for `StIntersectsOp`:

```rust
pub struct StIntersectsOp;

// Predicate::arrow_opaque(StIntersectsOp, [col_expr, wkb_literal])
// args[0] = Expression::Column(geo_column_path)
// args[1] = Expression::Literal(Scalar::Binary(wkb_bytes))
```

Methods:

### `name()`
Returns `"ST_Intersects"`.

### `eval_pred(args, batch, inverted)`
Row-level evaluation over an Arrow `RecordBatch`:
1. Cast `args[0]` to column name, read `BinaryArray` from batch
2. Extract WKB bytes from `args[1]` literal, compute its bounding box (xmin/xmax/ymin/ymax)
   using a WKB parser (see dependencies below)
3. For each row: parse WKB bytes → compute bbox → test for intersection (4 double comparisons)
4. Return `BooleanArray`, XOR with `inverted` if needed

### `eval_pred_scalar(eval_expr, eval_pred, args, inverted)`
Returns `Ok(None)` — scalars don't apply here.

### `eval_as_data_skipping_predicate(predicate_evaluator, args, inverted)`
File-level skipping against stats:
1. Extract column name from `args[0]`
2. Call `predicate_evaluator.get_min_stat(col, &DataType::STRING)` → `Scalar::String("POINT(xMin yMin)")`
3. Call `predicate_evaluator.get_max_stat(col, &DataType::STRING)` → `Scalar::String("POINT(xMax yMax)")`
4. Parse both POINT strings to extract `file_xmin/ymin` and `file_xmax/ymax`
5. Extract WKB bytes from `args[1]` literal, compute `q_xmin/ymin/q_xmax/ymax`
6. Intersection test: `NOT (file_xmax < q_xmin OR file_xmin > q_xmax OR file_ymax < q_ymin OR file_ymin > q_ymax)`
7. Apply `inverted` if needed; return `Some(bool)` or `None` if any stat is missing

### `as_data_skipping_predicate(predicate_evaluator, args, inverted)`
Returns `None` — the WKT parsing cannot be expressed in kernel's predicate language.

### WKT POINT parsing (private helper, no crate dependency)
`"POINT(x y)"` format is trivial to parse with `str::trim`/`split` — no geo crate needed here.
**A new crate dependency is only required for WKB parsing of the query literal bbox.**

### New dependency
Add to `kernel/Cargo.toml` under `[target.'cfg(feature = "default-engine")'.dependencies]`:
- `geo` or `geozero` crate for WKB parsing (WKB → bbox extraction)
- Alternative: `wkb` crate (lighter)
- Keep it behind the `default-engine` feature flag — core kernel has no geo dep

### Public API
Export a constructor so connectors can build the predicate easily:
```rust
// in kernel/src/engine/arrow_expression/mod.rs or a dedicated pub module
pub fn st_intersects_predicate(col: impl Into<ColumnName>, wkb_literal: Vec<u8>) -> Predicate {
    Predicate::arrow_opaque(StIntersectsOp, [
        Expression::column(col),
        Expression::literal(Scalar::Binary(wkb_literal)),
    ])
}
```

---

## `StructData` Type-Check Hazard

`StructData::try_new` checks `field.data_type() == scalar.data_type()`. A geo field reports
`DataType::Primitive(Geometry(crs))` but `Scalar::Binary` reports `DataType::BINARY` — not equal.
Write a targeted test early to see if this path is hit. If it is, relax the check to treat
`Binary` as compatible with `Geometry`/`Geography`.

---

## Implementation Sequence

1. Define `EdgeInterpolationAlgorithm`, `GeometryType`, `GeographyType`; add to `PrimitiveType`; serde + Display
2. `cargo build --workspace --all-features` → fix all exhaustive match errors (phase 2)
3. `ensure_data_types.rs` geo-as-binary compatibility arms (not a compile error, add explicitly)
4. GeoArrow metadata inject/parse in `arrow_conversion.rs` (field-level)
5. Table feature: `GeospatialType` + `geospatial.rs` + `table_configuration.rs` wiring
6. Stats schema: `MinMaxStatsTransform` geo handling (STRING for Geometry, None for Geography)
7. `StIntersectsOp`: `eval_as_data_skipping_predicate` + `eval_pred`
8. Write tests (see below)

---

## Files to Change (complete list)

| File | Nature |
|---|---|
| `kernel/src/schema/mod.rs` | New types + `PrimitiveType` variants + serde + Display + factory methods |
| `kernel/src/expressions/scalars.rs` | `parse_scalar` error arms for geo |
| `kernel/src/engine/arrow_conversion.rs` | DataType arms + GeoArrow metadata inject/parse at field level |
| `kernel/src/engine/ensure_data_types.rs` | 4 geo-as-binary compatibility arms |
| `kernel/src/engine/parquet_row_group_skipping.rs` | No-op arms in min/max stat functions |
| `kernel/src/engine/arrow_expression/mod.rs` | `append_null` geo arm + re-export `st_intersects_predicate` |
| `kernel/src/engine/arrow_data.rs` | Geo arm alongside Binary |
| `kernel/src/transaction/stats_verifier.rs` | Error arms for geo in exhaustive matches |
| `kernel/src/scan/data_skipping/stats_schema/mod.rs` | `MinMaxStatsTransform` geo handling |
| `kernel/src/table_features/mod.rs` | `GeospatialType` variant + `FeatureInfo` + match arms |
| `kernel/src/table_configuration.rs` | `validate_geospatial_feature_support` call |
| `ffi/src/schema.rs` | Map geo to `visit_binary` |
| `kernel/src/table_features/geospatial.rs` | **NEW** — validation + schema walk |
| `kernel/src/engine/arrow_expression/st_intersects.rs` | **NEW** — `StIntersectsOp` |
| `kernel/Cargo.toml` | Add WKB crate under default-engine feature |

---

## Verification Plan

```bash
# Step 2 (surface match sites)
cargo build --workspace --all-features

# After all phases
cargo nextest run -p delta_kernel --lib --all-features
cargo fmt && cargo clippy --workspace --benches --tests --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
```

**Key tests to write:**
1. Schema round-trip: `"geometry(OGC:CRS84)"` → `PrimitiveType::Geometry` → Display → `"geometry(OGC:CRS84)"`; geography with algorithm; bare `"geometry"` → defaults to OGC:CRS84; unknown algorithm → error
2. Arrow round-trip: `PrimitiveType::Geometry(crs)` → `ArrowField` (check `ARROW:extension:name` metadata) → `TryFromArrow` → `PrimitiveType::Geometry(crs)`
3. `ensure_data_types`: `(Geometry(crs), ArrowDataType::Binary)` → `Identical`
4. Table feature validation (negative): geo column + no `"geospatial"` feature → error
5. Table feature validation (positive): geo column + feature present → ok
6. Parquet stats: geo column → `get_parquet_min_stat` returns `None`
7. Stats schema: geo column included in `minValues`/`maxValues` schema as `STRING`; geography excluded from min/max
8. `StIntersectsOp::eval_as_data_skipping_predicate`: mock stats with `"POINT(-1 -1)"` min and `"POINT(3 4)"` max; intersecting query → Some(true); non-intersecting → Some(false); missing stats → None
9. `StructData` geo value test: construct `StructData` with geo column + `Scalar::Binary` — pass or identify the fix needed
