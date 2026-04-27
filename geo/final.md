# Geo Types: Two Implementation Plans

Phases 1–3 are identical in both plans. The plans diverge at Phase 4 (stats schema) and Phase 5 (ST_Intersects predicate).

---

## Shared Context

Add `PrimitiveType::Geometry` / `PrimitiveType::Geography`, GeoArrow Arrow layer, `"geospatial"` table feature, and an `ST_Intersects` opaque predicate with file-level stats skipping.

Confirmed decisions (both plans):
- Geo values travel as `Scalar::Binary(WKB bytes)` — no new `Scalar` variant
- Arrow: `ARROW:extension:name = "geoarrow.wkb"` on a `Binary` physical array
- Delta log stats format: `minValues.col = "POINT(xMin yMin)"`, `maxValues.col = "POINT(xMax yMax)"` — RFC-mandated WKT strings on disk
- Row-level `eval_pred`: geoarrow crate (vectorized, zero-copy `WKBArray`)
- `eval_as_data_skipping_predicate` (row-group level): same in both plans — parses WKT scalars directly

---

## Phases 1–3 (identical in both plans)

### Phase 1 — Type System (`kernel/src/schema/mod.rs`)

Insert before `DecimalType`:

```rust
pub enum EdgeInterpolationAlgorithm { Spherical, Vincenty, Thomas, Andoyer, Karney }
// Display: lowercase; FromStr: parse lowercase, error on unknown

pub struct GeometryType { srid: String }
// DEFAULT_SRID = "OGC:CRS84"; try_new(srid) -> DeltaResult<Self>; Default = OGC:CRS84

pub struct GeographyType { srid: String, algorithm: EdgeInterpolationAlgorithm }
// DEFAULT_SRID = "OGC:CRS84"; DEFAULT_ALGORITHM = Spherical; Default = OGC:CRS84 + Spherical
```

Add to `PrimitiveType` after `Decimal`:
```rust
Geometry(GeometryType),
Geography(GeographyType),
```

**Serde:**
- Serialize: `"geometry(OGC:CRS84)"` / `"geography(OGC:CRS84, spherical)"` (comma+space between CRS and algorithm)
- Deserialize: extend existing custom deserializer before `unsupported` catch-all:
  - `"geometry"` → default; `"geometry(...)"` → parse SRID; `"geography(...)"` → parse SRID + algorithm via `rfind(',')`;  `"geography"` → default
- Display: same format as serialization

Run `cargo build --workspace --all-features` after this step to surface all exhaustive match errors.

### Phase 2 — Compiler Error Fixes

| File | Change |
|---|---|
| `kernel/src/expressions/scalars.rs` | `parse_scalar`: `Geometry(_) \| Geography(_) => Err("geo types cannot be partition column values")` |
| `kernel/src/engine/arrow_conversion.rs` | `TryFromKernel<&DataType>`: both → `Ok(ArrowDataType::Binary)` |
| `kernel/src/engine/arrow_conversion.rs` | `TryFromKernel<&StructField>`: inject `ARROW:extension:name = "geoarrow.wkb"` + `ARROW:extension:metadata = {"crs":"..."}` (+ `"edges"` for Geography) |
| `kernel/src/engine/arrow_conversion.rs` | `TryFromArrow<&ArrowField>`: detect `ARROW:extension:name == "geoarrow.wkb"`, parse CRS + optional `"edges"` → reconstruct `Geometry`/`Geography` before falling through to data-type conversion |
| `kernel/src/engine/ensure_data_types.rs` | 4 arms: `(Geometry(_), Binary)`, `(Geometry(_), LargeBinary)`, `(Geography(_), Binary)`, `(Geography(_), LargeBinary)` → `Ok(Identical)` |
| `kernel/src/engine/parquet_row_group_skipping.rs` | Both stat functions: `(Geometry(_), _) \| (Geography(_), _) => return None` |
| `kernel/src/engine/arrow_expression/mod.rs` | `append_null`: `Geometry(_) \| Geography(_) => append_nulls_as!(array::BinaryBuilder)` |
| `kernel/src/engine/arrow_data.rs` | Geo arm alongside `Binary` in any exhaustive match — treat geo as Binary |
| `kernel/src/transaction/stats_verifier.rs` | `column_types_for` + `is_stat_present`: both → `Err(internal_error("Geo columns cannot be clustering/stats columns"))` |
| `ffi/src/schema.rs` | `visit_schema_item`: both → `call!(visit_binary)` |

`TryFromArrow<&ArrowDataType>`: no change — Binary stays Binary at data-type level.

### Phase 3 — Table Feature

**`kernel/src/table_features/mod.rs`:**
- `GeospatialType` variant: `#[strum(serialize = "geospatial")]`
- `GEOSPATIAL_TYPE_INFO`: `feature_type: ReaderWriter`, `kernel_support: Supported`, `enablement_check: AlwaysIfSupported`, `min_legacy_version: None`, `feature_requirements: &[]`
- Add to `feature_type()` and `info()` match arms

**New file `kernel/src/table_features/geospatial.rs`** (pattern: `timestamp_ntz.rs`):
```rust
pub(crate) fn validate_geospatial_feature_support(tc: &TableConfiguration) -> DeltaResult<()>
pub(crate) fn schema_contains_geospatial(schema: &Schema) -> bool
// schema_contains_geospatial: walks schema via SchemaTransform, true on Geometry(_)|Geography(_)
```

**`kernel/src/table_configuration.rs`:** call `validate_geospatial_feature_support(&table_config)?;` in `try_new` after the `validate_timestamp_ntz_feature_support` call.

---

---

# Plan A — Opaque Predicate Approach

## A: How file-level skipping works

`StIntersectsOp::as_data_skipping_predicate` returns a *second internal opaque predicate* (`StIntersectsSkipOp`) that parses WKT strings from the stats batch at Arrow batch evaluation time.

- **No changes** to `log_replay.rs`, `DataSkippingFilter`, or `DataSkippingPredicateCreator`
- Stats schema still maps `Geometry → String` (one STRING column per geo field in minValues/maxValues)
- WKT parsing happens inside `StIntersectsSkipOp::eval_pred` when the stats batch is available

## A-Phase 4 — Stats Schema

`MinMaxStatsTransform::transform_primitive` — add before `is_skipping_eligible_datatype` gate:
```rust
PrimitiveType::Geometry(_) => Some(Cow::Owned(PrimitiveType::String)),
PrimitiveType::Geography(_) => None,
```

Stats schema for a geometry column `geom`:
```
nullCount.geom: LONG
minValues.geom: STRING    <- "POINT(xmin ymin)"
maxValues.geom: STRING    <- "POINT(xmax ymax)"
```

No other changes. `parse_json` produces `StringArray` columns which `StIntersectsSkipOp::eval_pred` reads directly.

## A-Phase 5 — ST_Intersects (`kernel/src/engine/arrow_expression/st_intersects.rs`)

Two structs in one file.

### `StIntersectsOp` (public)

**`eval_pred(args, batch, inverted)`** — row-level via geoarrow:
1. Extract column name from `args[0]`, look up `BinaryArray` from `batch`
2. Wrap zero-copy: `WKBArray::new(binary_array, Metadata::default())`
3. Extract WKB bytes from `args[1]` literal; parse → geometry via `geoarrow::io::wkb::read_wkb`
4. `Intersects::intersects(&wkb_array, &query_geom)` → `BooleanArray`
5. If `inverted`: `arrow::compute::not(&result)?`

**`eval_pred_scalar`** → `Ok(None)`

**`eval_as_data_skipping_predicate(evaluator, args, inverted)`** — row-group scalar stats:
1. `evaluator.get_min_stat(col, &DataType::STRING)` → `Scalar::String("POINT(xmin ymin)")`
2. `evaluator.get_max_stat(col, &DataType::STRING)` → `Scalar::String("POINT(xmax ymax)")`
3. Parse both via `parse_wkt_point`; extract query bbox from `args[1]` via `extract_wkb_bbox`
4. Bbox test; return `Some(intersects XOR inverted)` or `None` if any stat missing

**`as_data_skipping_predicate(evaluator, args, inverted)`** — **file-level, returns second opaque predicate**:
```rust
fn as_data_skipping_predicate(...) -> Option<Predicate> {
    let col = extract_col_name(&exprs[0])?;
    let min_expr = evaluator.get_min_stat(col, &DataType::STRING)?;
    // e.g. Expression::Column("stats_parsed.minValues.geom")
    let max_expr = evaluator.get_max_stat(col, &DataType::STRING)?;
    Some(Predicate::arrow_opaque(
        StIntersectsSkipOp { inverted },
        [min_expr, max_expr, exprs[1].clone()],
    ))
}
```

---

### `StIntersectsSkipOp` (internal)

```rust
struct StIntersectsSkipOp { inverted: bool }
```

**`eval_pred(args, batch, inverted)`** — batch-level WKT parsing (each row = one file's stats):
1. Evaluate `args[0]` against `batch` → `StringArray` of `"POINT(xmin ymin)"`
2. Evaluate `args[1]` against `batch` → `StringArray` of `"POINT(xmax ymax)"`
3. Extract query bbox from `args[2]` WKB literal via `extract_wkb_bbox` (done once)
4. Per row: if null → `true` (keep); parse WKT → if parse fails → `true`; bbox test; XOR with `inverted ^ self.inverted`
5. Return `BooleanArray`

**All other methods** → `Ok(None)` / `None`

### Private helpers (no crate needed for WKT)

```rust
fn parse_wkt_point(s: &str) -> Option<(f64, f64)> {
    let inner = s.trim().strip_prefix("POINT(")?.strip_suffix(')')?;
    let mut parts = inner.split_whitespace();
    let x: f64 = parts.next()?.parse().ok()?;
    let y: f64 = parts.next()?.parse().ok()?;
    parts.next().is_none().then_some((x, y))
}

fn extract_wkb_bbox(wkb: &[u8]) -> Option<(f64, f64, f64, f64)> {
    // uses geoarrow::io::wkb::read_wkb + geo::BoundingRect
}
```

## A: Files changed (delta from shared phases 1–3)

| File | Change |
|---|---|
| `kernel/src/scan/data_skipping/stats_schema/mod.rs` | `MinMaxStatsTransform`: `Geometry -> String`, `Geography -> None` |
| `kernel/src/engine/arrow_expression/st_intersects.rs` | **NEW** — `StIntersectsOp` + `StIntersectsSkipOp` + helpers |
| `kernel/src/engine/arrow_expression/mod.rs` | `mod st_intersects; pub use st_intersects::st_intersects_predicate;` |
| `kernel/Cargo.toml` | `geoarrow` under `default-engine-base` |

**Not touched:** `log_replay.rs`, `data_skipping.rs`, `DataSkippingPredicateCreator`

## A: Pros and cons

**Pros:**
- Minimal blast radius — 4 files changed beyond shared phases
- Self-contained: all geo-specific skipping logic lives in `st_intersects.rs`
- No new infrastructure concepts

**Cons:**
- WKT parsing is hidden inside an opaque predicate — less transparent, harder to debug
- The "file-level skip" predicate is itself an opaque predicate (two levels of opaque nesting); unusual pattern
- If another predicate type ever needs geo stats, it can't reuse the float columns (they don't exist as columns)
- The `StIntersectsSkipOp` struct is a non-obvious implementation detail

---

---

# Plan B — Post-Parse Transform Approach

## B: How file-level skipping works

A post-parse transform step runs in `log_replay.rs` after `stats_parsed` is built. For each geometry column, it parses `"POINT(x y)"` WKT strings and adds 4 Float64 sub-columns to the stats batch. `as_data_skipping_predicate` then uses those float columns directly via standard numeric predicates — no second opaque predicate.

- **Changes required** in `log_replay.rs` (inject transform), `data_skipping.rs`/`DataSkippingPredicateCreator` (return float column refs for geo), `stats_schema/mod.rs` (expand geo to 4 float sub-columns in schema)
- `as_data_skipping_predicate` returns a plain `Predicate::and_from([gt(...), lt(...), gt(...), lt(...)])` — all numeric comparisons, no opaque wrapping

## B-Phase 4 — Stats Schema

`MinMaxStatsTransform::transform_primitive`:
```rust
PrimitiveType::Geometry(_) => Some(Cow::Owned(PrimitiveType::String)),
// Still String — parse_json needs this to match the on-disk WKT format
PrimitiveType::Geography(_) => None,
```

**Additionally:** define a naming convention for the float sub-columns. For a geometry column named `geom`, the post-parse transform produces these columns in the stats batch:
```
minValues.geom        : STRING   (original WKT, still present)
minValues.geom.__xmin : DOUBLE   (parsed x from minValues WKT)
minValues.geom.__ymin : DOUBLE   (parsed y from minValues WKT)
maxValues.geom        : STRING   (original WKT, still present)
maxValues.geom.__xmax : DOUBLE   (parsed x from maxValues WKT)
maxValues.geom.__ymax : DOUBLE   (parsed y from maxValues WKT)
```

The double-underscore prefix (`__`) marks these as kernel-internal synthetic columns that are not part of the Delta schema.

The stats schema must declare these float sub-columns so `DataSkippingPredicateCreator::get_min_stat` can reference them. Add a helper in `stats_schema/mod.rs`:

```rust
// Geo bbox sub-column names for a geometry column
pub(crate) fn geo_xmin_col(col: &ColumnName) -> ColumnName { col.child("__xmin") }
pub(crate) fn geo_ymin_col(col: &ColumnName) -> ColumnName { col.child("__ymin") }
pub(crate) fn geo_xmax_col(col: &ColumnName) -> ColumnName { col.child("__xmax") }
pub(crate) fn geo_ymax_col(col: &ColumnName) -> ColumnName { col.child("__ymax") }
```

The stats schema for a geometry column becomes:
```
minValues.geom: STRING          <- for parse_json
minValues.geom.__xmin: DOUBLE   <- added by transform
minValues.geom.__ymin: DOUBLE   <- added by transform
maxValues.geom: STRING
maxValues.geom.__xmax: DOUBLE
maxValues.geom.__ymax: DOUBLE
```

## B-Phase 4b — Post-Parse Transform (`kernel/src/scan/log_replay.rs`)

**Injection point:** after each call to `transform.evaluate(...)` that produces `stats_parsed`, before `self.build_selection_vector(...)`. Two identical injection sites (parallel path ~line 672, main path ~line 742).

New helper function `apply_geo_stats_transform`:

```rust
fn apply_geo_stats_transform(
    batch: Box<dyn EngineData>,
    engine: &dyn Engine,
    geo_columns: &[ColumnName],  // geometry columns in the table schema
) -> DeltaResult<Box<dyn EngineData>>
```

For each geometry column:
1. Read `stats_parsed.minValues.{col}` → `StringArray` of `"POINT(x y)"`
2. Parse each string → `(x, y)` via `parse_wkt_point`; null on missing stats
3. Produce `Float64Array` for xmin and ymin
4. Read `stats_parsed.maxValues.{col}` → parse → produce xmax and ymax arrays
5. Add the 4 new columns to the batch as sub-fields of `minValues.{col}` / `maxValues.{col}`

Pass `geo_columns: Vec<ColumnName>` into `DataSkippingFilter::new` (constructed at ~line 202 in `LogReplayProcessor`) so `DataSkippingPredicateCreator` can reference the float sub-columns.

## B-Phase 4c — `DataSkippingPredicateCreator` (`kernel/src/scan/data_skipping.rs`)

Add `geo_columns: &'a [ColumnName]` field. `StIntersectsOp::as_data_skipping_predicate` calls `get_min_stat` / `get_max_stat` with the derived sub-column names directly:

```rust
fn as_data_skipping_predicate(...) -> Option<Predicate> {
    let col = extract_col_name(&exprs[0])?;
    let xmin = evaluator.get_min_stat(&geo_xmin_col(col), &DataType::DOUBLE)?;
    let ymin = evaluator.get_min_stat(&geo_ymin_col(col), &DataType::DOUBLE)?;
    let xmax = evaluator.get_max_stat(&geo_xmax_col(col), &DataType::DOUBLE)?;
    let ymax = evaluator.get_max_stat(&geo_ymax_col(col), &DataType::DOUBLE)?;
    let (q_xmin, q_ymin, q_xmax, q_ymax) = extract_wkb_bbox(wkb_from_literal(&exprs[1])?)?;
    // intersects iff: fxmax >= qxmin AND fxmin <= qxmax AND fymax >= qymin AND fymin <= qymax
    Some(Predicate::and_from([
        Predicate::ge(xmax, Expression::literal(q_xmin)),
        Predicate::le(xmin, Expression::literal(q_xmax)),
        Predicate::ge(ymax, Expression::literal(q_ymin)),
        Predicate::le(ymin, Expression::literal(q_ymax)),
    ]))
}
```

`evaluator.get_min_stat(&geo_xmin_col(col), &DataType::DOUBLE)` returns
`Expression::Column("stats_parsed.minValues.geom.__xmin")` — valid because the post-parse transform added that column.

## B-Phase 5 — ST_Intersects (`kernel/src/engine/arrow_expression/st_intersects.rs`)

**Only one struct** — `StIntersectsOp` (public). No `StIntersectsSkipOp`.

**`eval_pred`**: same as Plan A (geoarrow).
**`eval_pred_scalar`** → `Ok(None)`
**`eval_as_data_skipping_predicate`**: same as Plan A (scalar WKT parsing for row-group).
**`as_data_skipping_predicate`**: returns plain numeric `Predicate` (4 float comparisons, see above).

Private helpers: same `parse_wkt_point` (no dep) and `extract_wkb_bbox` (geoarrow) as Plan A.

## B: Files changed (delta from shared phases 1–3)

| File | Change |
|---|---|
| `kernel/src/scan/data_skipping/stats_schema/mod.rs` | `MinMaxStatsTransform`: `Geometry -> String`, `Geography -> None`; add geo sub-column name helpers |
| `kernel/src/scan/log_replay.rs` | Inject `apply_geo_stats_transform` after stats_parsed build (2 sites); pass geo columns to `DataSkippingFilter` |
| `kernel/src/scan/data_skipping.rs` | `DataSkippingFilter::new` accepts `geo_columns`; `DataSkippingPredicateCreator` stores them |
| `kernel/src/engine/arrow_expression/st_intersects.rs` | **NEW** — `StIntersectsOp` only + helpers |
| `kernel/src/engine/arrow_expression/mod.rs` | `mod st_intersects; pub use st_intersects::st_intersects_predicate;` |
| `kernel/Cargo.toml` | `geoarrow` under `default-engine-base` |

## B: Pros and cons

**Pros:**
- `as_data_skipping_predicate` returns a plain numeric `Predicate` — transparent, consistent with every other column type
- Float columns are real columns in the stats batch — reusable by future geo predicates
- Only one `StIntersectsOp` struct (simpler public API)

**Cons:**
- More invasive: touches `log_replay.rs`, `data_skipping.rs`, and stats schema structurally
- `DataSkippingFilter::new` signature changes (adds `geo_columns` param)
- `__xmin` naming convention for synthetic sub-columns is a new concept
- `apply_geo_stats_transform` requires rebuilding the Arrow RecordBatch with added columns

---

---

## Shared remainder (both plans)

### Dependencies (`kernel/Cargo.toml`)

```toml
geoarrow = { version = "0.4", optional = true, features = ["wkb", "geo"] }
```
Add `"dep:geoarrow"` to `default-engine-base` feature list.

### Public constructor (both plans, identical)

```rust
pub fn st_intersects_predicate(col: impl Into<ColumnName>, wkb_literal: Vec<u8>) -> Predicate {
    Predicate::arrow_opaque(StIntersectsOp, [
        Expression::column(col),
        Expression::literal(Scalar::Binary(wkb_literal)),
    ])
}
```

### `StructData` type-check hazard (both plans)

Write test after Phase 2: `StructData::try_new` with geo field + `Scalar::Binary`. If it fails, relax equality check:
```rust
let compatible = field.data_type() == &scalar.data_type()
    || matches!((field.data_type(), &scalar.data_type()),
        (DataType::Primitive(PrimitiveType::Geometry(_)), DataType::BINARY)
        | (DataType::Primitive(PrimitiveType::Geography(_)), DataType::BINARY));
```

### Implementation sequence (both plans)

1. Phase 1 — type system + serde + Display
2. Phase 2 — `cargo build`; fix all exhaustive match errors
3. StructData hazard test (early)
4. Phase 3 — table feature
5. Phase 4 — stats schema (+ transform injection for Plan B)
6. Add `geoarrow` to `Cargo.toml`
7. Phase 5 — `st_intersects.rs`
8. Tests

### Test plan (both plans)

1. Schema round-trip: `"geometry(OGC:CRS84)"` <-> `PrimitiveType::Geometry`; bare `"geometry"` -> default; unknown algorithm -> error; EPSG round-trip
2. Arrow field round-trip: kernel -> `ArrowField` (check `ARROW:extension:name`) -> back to kernel; Geography preserves `"edges"`
3. `ensure_data_types`: `(Geometry(_), Binary)` -> `Identical`; `(Geography(_), LargeBinary)` -> `Identical`
4. Table feature validation: geo + feature -> ok; geo + no feature -> error
5. Parquet stats: `get_parquet_min_stat` returns `None` for geo
6. Stats schema: geometry -> `minValues.col: STRING`; geography -> absent from min/max
7. `eval_as_data_skipping_predicate` (scalar): intersecting -> `Some(true)`; non-intersecting -> `Some(false)`; missing -> `None`
8. **Plan A only:** `StIntersectsSkipOp::eval_pred`: RecordBatch with WKT StringArrays -> correct `BooleanArray`; null stats -> keep
   **Plan B only:** `as_data_skipping_predicate`: verify returns 4-comparison `Predicate` with correct float column refs; end-to-end test with transformed stats batch
9. `StructData` geo compatibility

### Verification

```bash
cargo build --workspace --all-features        # after Phase 2
cargo nextest run -p delta_kernel --lib --all-features
cargo fmt && cargo clippy --workspace --benches --tests --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
```
