# Geo Types Implementation Plan (Scalar::Binary, no GeoArrow Arrow metadata)

> Also saved at: `/home/lorena.rosati/.claude/plans/geo-plan-with-binary-scalar.md`
>
> Plan 2 (Scalar::Geo + plain Binary) preserved at: `geo/plan-scalar-geo-no-geoarrow.md`

## Context

Add geometry and geography support to delta-kernel-rs. Geo values at the scalar/expression
level reuse the existing `Scalar::Binary(Vec<u8>)` variant -- no new Scalar variant. Geo columns
are plain `Binary` in Arrow -- the Delta log schema is the sole source of truth for the
geometry/geography distinction. Minimum blast radius: no Scalar match sites change, no Arrow
field-level metadata code.

---

## Design Decisions

| Decision | Chosen Approach |
|---|---|
| Type system | `PrimitiveType::Geometry(GeometryType)` + `PrimitiveType::Geography(GeographyType)` |
| Scalar literal | `Scalar::Binary(Vec<u8>)` -- no new Scalar variant |
| Arrow representation | Plain `ArrowDataType::Binary` -- no GeoArrow extension metadata |
| Table feature | New `ReaderWriter` feature `GeospatialType` (confirm name against Delta RFC) |
| Preview feature variant | None |

**vs. plan-with-geoarrow.md** (Scalar::Geo + GeoArrow): saves ~5 Scalar match sites AND ~4
arrow_conversion changes (no GeoArrow helpers). Trade-off: `StructData::try_new` type-check
hazard (see below).

**vs. plan-scalar-geo-no-geoarrow.md** (Scalar::Geo + plain Binary): same Arrow story, but
no `Scalar::Geo` variant means no changes to `data_type()`, `Display`, `logical_partial_cmp`,
or FFI expression visitor. Saves ~5 Scalar/FFI match sites.

---

## New Types (all in `kernel/src/schema/mod.rs`)

```rust
pub enum EdgeInterpolationAlgorithm { Spherical, Vincenty, Thomas, Andoyer, Karney }
// Display: lowercase ("spherical", "vincenty", ...)
// from_str: parse from lowercase string, error on unknown value

pub struct GeometryType { srid: String }
// DEFAULT_SRID = "OGC:CRS84"
// try_new(srid) -> DeltaResult<Self>  // reject empty
// srid(&self) -> &str

pub struct GeographyType { srid: String, algorithm: EdgeInterpolationAlgorithm }
// DEFAULT_SRID = "OGC:CRS84", DEFAULT_ALGORITHM = Spherical
```

### Delta JSON serialization format (matching Java Kernel)

| Type | Serialized string |
|---|---|
| `Geometry(OGC:CRS84)` | `"geometry(OGC:CRS84)"` |
| `Geography(OGC:CRS84, Spherical)` | `"geography(OGC:CRS84, spherical)"` |
| Bare `"geometry"` on read | accept, default to OGC:CRS84 |

---

## File-by-File Changes

### 1. `kernel/src/schema/mod.rs`

- Define `EdgeInterpolationAlgorithm`, `GeometryType`, `GeographyType` (above `DecimalType`)
- **`PrimitiveType` enum** (lines 1428-1453): add `Geometry(GeometryType)` and
  `Geography(GeographyType)` variants with `#[serde(serialize_with = "...")]`
- Add `serialize_geometry` + `serialize_geography` (follow `serialize_decimal` at line 1487)
- **`PrimitiveType::Deserialize`** (lines 1504-1555): add branches before `unsupported` fallback:
  - `"geometry" | starts_with "geometry("` -> parse `GeometryType`
  - `"geography" | starts_with "geography("` -> parse `GeographyType`
  - CRS can contain colons (`OGC:CRS84`, `EPSG:3857`); use first comma as CRS/algorithm separator
- **`Display for PrimitiveType`** (lines 1560-1577): add arms
- **`can_widen_to`**: no change -- existing explicit opt-in logic excludes geo automatically
- Factory methods (no `const` possible since `String` is non-const):
  ```rust
  pub fn geometry(srid: impl Into<String>) -> DeltaResult<DataType>
  pub fn geography(srid: impl Into<String>, alg: EdgeInterpolationAlgorithm) -> DeltaResult<DataType>
  ```

---

### 2. `kernel/src/expressions/scalars.rs`

**No new variant.** Only change:

- **`parse_scalar()` on `PrimitiveType`** (lines 703-757): exhaustive match -- add arms that error:
  ```rust
  Geometry(_) | Geography(_) => Err(/* "geo types cannot be used as partition column values" */),
  ```

Note: the empty-string early return at line 699 fires first, so
`Scalar::Null(DataType::Primitive(PrimitiveType::Geometry(crs)))` is returned for empty stats
entries -- this is correct for null counts.

No changes to `data_type()`, `Display`, `logical_partial_cmp`, or arithmetic methods.

---

### 3. `kernel/src/engine/arrow_conversion.rs`

**`TryFromKernel<&DataType> for ArrowDataType`** (lines 146-200): add two arms:
```rust
PrimitiveType::Geometry(_) => Ok(ArrowDataType::Binary),
PrimitiveType::Geography(_) => Ok(ArrowDataType::Binary),
```

No changes to `TryFromKernel<&StructField> for ArrowField`, `TryFromArrow<&ArrowField> for
StructField`, or `TryFromArrow<&ArrowDataType> for DataType`. The Arrow layer has zero geo
awareness in this plan -- geo fields become plain Binary with no extension metadata.

When the default engine reads a Parquet file, a geo column comes back as `ArrowDataType::Binary`
and `TryFromArrow` returns `DataType::BINARY`. The kernel uses the Delta log schema (not the
Arrow schema) to know the column is `Geometry(crs)`. This requires `ensure_data_types` to
accept `(Geometry(crs), Binary)` as compatible -- see next section.

---

### 4. `kernel/src/engine/ensure_data_types.rs`

**Hard blocker without this change.** `DataType::Primitive(PrimitiveType::Geometry(crs))` vs
`ArrowDataType::Binary` hits the catch-all error because (a) `Binary.is_primitive()` is false
in Arrow and (b) `Geometry(crs) != DataType::BINARY`. Add explicit arms BEFORE the existing
`DataType::BINARY` arm:

```rust
(DataType::Primitive(PrimitiveType::Geometry(_)), ArrowDataType::Binary)
| (DataType::Primitive(PrimitiveType::Geometry(_)), ArrowDataType::LargeBinary)
| (DataType::Primitive(PrimitiveType::Geography(_)), ArrowDataType::Binary)
| (DataType::Primitive(PrimitiveType::Geography(_)), ArrowDataType::LargeBinary)
    => Ok(DataTypeCompat::Identical),
```

---

### 5. `kernel/src/engine/parquet_row_group_skipping.rs`

**`get_parquet_min_stat`** (lines 111-150) and **`get_parquet_max_stat`** (lines 157-196):
add no-op arms:
```rust
(Geometry(_), _) | (Geography(_), _) => return None,
```

---

### 6. `kernel/src/engine/arrow_expression/mod.rs`

**`append_null`** (lines 171-218): exhaustive `DataType` match, no wildcard -- add:
```rust
DataType::Primitive(PrimitiveType::Geometry(_))
| DataType::Primitive(PrimitiveType::Geography(_)) => append_nulls_as!(array::BinaryBuilder),
```

---

### 7. `kernel/src/engine/arrow_data.rs`

Check around line 458 for exhaustive `DataType` match on column pushing. Add geo variants
alongside the existing `Binary` arm if the match is exhaustive (no-op if already wildcarded).

---

### 8. `kernel/src/transaction/stats_verifier.rs`

**No changes needed.** `col_types_for` uses wildcard `_ => Err(...)` catch-alls at lines ~172
and ~203. Geo columns are excluded by `is_skipping_eligible_datatype` before reaching these
sites in practice.

---

### 9. `kernel/src/scan/data_skipping/stats_schema/mod.rs`

**No changes needed.**

- `is_skipping_eligible_datatype`: positive `matches!` allowlist -- geo automatically excluded.
- `NullCountSchemaTransformer`: uses wildcard at struct field level -- handles geo correctly.
- Null counts are valid for geo columns and work automatically.

---

### 10. `kernel/src/table_features/mod.rs`

Add in the `ReaderWriter` section:
```rust
/// Geospatial type support: geometry and geography columns with WKB encoding.
#[strum(serialize = "geospatialType")]
#[serde(rename = "geospatialType")]
GeospatialType,
```

Add static:
```rust
static GEOSPATIAL_TYPE_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};
```

Update `feature_type()` and `info()` match arms.

---

### 11. `kernel/src/table_features/geospatial.rs` (NEW FILE)

Pattern: identical to `timestamp_ntz.rs`.

```rust
pub(crate) fn validate_geospatial_feature_support(tc: &TableConfiguration) -> DeltaResult<()>
pub(crate) fn schema_contains_geospatial(schema: &Schema) -> bool
// walks schema via SchemaTransform, checks for Geometry(_) | Geography(_) variants
```

Register in `table_features/mod.rs`:
```rust
mod geospatial;
pub(crate) use geospatial::{schema_contains_geospatial, validate_geospatial_feature_support};
```

---

### 12. `kernel/src/table_configuration.rs`

Add call alongside `validate_timestamp_ntz_feature_support`:
```rust
validate_geospatial_feature_support(&table_config)?;
```

---

### 13. FFI layer (`ffi/src/schema.rs`)

The `visit_schema_item` match on `DataType` is exhaustive. Map geo types to `visit_binary` to
avoid a breaking ABI change:
```rust
DataType::Primitive(PrimitiveType::Geometry(_))
| DataType::Primitive(PrimitiveType::Geography(_)) => call!(visit_binary),
```

No changes needed to `ffi/src/expressions/` -- `Scalar::Binary` is already handled there.

---

## StructData Type Check (Watch Out)

`StructData::try_new` checks `f.data_type() == &a.data_type()`. For a geo column, the field
declares `DataType::Primitive(PrimitiveType::Geometry(crs))` but a `Scalar::Binary` value
reports `DataType::BINARY`. These are not equal.

**When does this matter?** Only if kernel internally constructs a `StructData` containing a
geo column value. In the normal scan read path, data flows as Arrow arrays via the visitor
pattern, never through `StructData`. Not a blocker today, but write a targeted test to surface
this early. If it fails: relax the equality check to treat `Binary` as compatible with
`Geometry`/`Geography`.

---

## Blast Radius Summary

| File | Nature of Change |
|---|---|
| `kernel/src/schema/mod.rs` | New structs + 2 PrimitiveType variants + serde + Display + factory methods |
| `kernel/src/expressions/scalars.rs` | `parse_scalar` +2 error arms only (no new Scalar variant) |
| `kernel/src/engine/arrow_conversion.rs` | +2 DataType arms only (no StructField metadata helpers) |
| `kernel/src/engine/ensure_data_types.rs` | +4 arms: (Geometry/Geography, Binary/LargeBinary) => Identical |
| `kernel/src/engine/parquet_row_group_skipping.rs` | +1 combined no-op arm in each of min/max stat functions |
| `kernel/src/engine/arrow_expression/mod.rs` | `append_null` +1 arm for geo (BinaryBuilder) |
| `kernel/src/engine/arrow_data.rs` | +1 arm for geo alongside Binary (if exhaustive match) |
| `kernel/src/transaction/stats_verifier.rs` | No change (wildcard catch-alls) |
| `kernel/src/scan/data_skipping/stats_schema/mod.rs` | No change (positive allowlist + wildcards) |
| `kernel/src/table_features/mod.rs` | GeospatialType variant + FeatureInfo + match arms |
| `kernel/src/table_features/geospatial.rs` | **New file** |
| `kernel/src/table_configuration.rs` | +1 validation call |
| `ffi/src/schema.rs` | `visit_schema_item` +1 arm (map to visit_binary) |

Estimated: ~11 source files + 1 new file. **Run `cargo build` after step 1 to surface all
exhaustive match sites.**

---

## Type Conversion Flows

### Read path
```
Delta log JSON ("geometry(OGC:CRS84)")
  -> PrimitiveType::Deserialize -> PrimitiveType::Geometry(GeometryType { srid: "OGC:CRS84" })
  -> kernel StructType (held in Snapshot -- authoritative source of geo type)

Parquet read (default engine):
  Arrow field: plain Binary (no extension metadata)
    -> TryFromArrow<&ArrowField>: DataType::BINARY (no special handling)
  ensure_data_types: (Geometry(crs) expected, Binary delivered) -> Identical [explicit arm]
  Arrow BinaryArray data
    -> EngineData -> connector reads via get_binary() -> opaque WKB bytes
```

### Write path
```
User provides Binary data (WKB bytes) for a geo column
  -> PrimitiveType::Geometry(crs) in kernel schema
  -> TryFromKernel<&DataType>: ArrowDataType::Binary (no extension metadata on field)
  -> written as plain Binary in Parquet
  -> Delta log: schema includes "geometry(OGC:CRS84)" column type string
```

---

## Open Questions

1. **Exact Delta RFC table feature name**: Confirm `"geospatialType"` against the Delta RFC.
   Public API, cannot change after shipping.
2. **`StructData` type check**: Write a targeted test to surface whether
   `StructData::try_new` is hit for geo columns in any current code path.

---

## Implementation Sequence

1. Define `EdgeInterpolationAlgorithm`, `GeometryType`, `GeographyType`; add to `PrimitiveType`;
   implement serde + Display; add factory methods
2. **Run `cargo build --workspace --all-features`** -- all exhaustive match sites will fail to compile
3. Fix all compiler errors (`arrow_conversion.rs`, `parquet_row_group_skipping.rs`, `scalars.rs`,
   `arrow_expression/mod.rs`, `arrow_data.rs`, `ffi/src/schema.rs`)
4. Add `ensure_data_types.rs` explicit geo-as-binary compatibility arms
5. Add `GeospatialType` table feature + `GEOSPATIAL_TYPE_INFO` + match arms
6. Create `geospatial.rs` + wire up validation in `table_configuration.rs`
7. Write tests (see verification plan below)

---

## Verification Plan

1. **Schema round-trip**: `"geometry(OGC:CRS84)"` -> `PrimitiveType::Geometry` -> `Display` ->
   `"geometry(OGC:CRS84)"`. Cover: default CRS, geography with algorithm, unknown algorithm = error.
2. **`ensure_data_types`**: `(Geometry(crs), ArrowDataType::Binary)` returns `Identical`.
3. **Arrow conversion**: `PrimitiveType::Geometry(crs)` -> `ArrowDataType::Binary` (no field
   extension metadata).
4. **Feature validation (negative)**: geo columns in schema + protocol missing `GeospatialType`
   -> error.
5. **Feature validation (positive)**: geo columns + feature present -> ok.
6. **Parquet stats**: geo column in predicate -> `get_parquet_min_stat` returns `None`.
7. **`StructData` test**: construct a `StructData` with a geo column value (as `Scalar::Binary`)
   -- does it fail? If yes, fix the type check and add regression test.

```bash
cargo build --workspace --all-features         # step 2: surface all match sites
cargo nextest run -p delta_kernel --lib --all-features
cargo fmt && cargo clippy --workspace --benches --tests --all-features -- -D warnings
```
