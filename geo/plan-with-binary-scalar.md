# Geo Types Implementation Plan (Binary Scalar variant)

> Also saved at: `/home/lorena.rosati/.claude/plans/geo-plan-with-binary-scalar.md` (the Claude plans directory)

## Context

Add first-class geometry and geography support to delta-kernel-rs per the Delta Lake geo RFC. Same as the GeoArrow plan except geo values at the scalar/expression level are `Scalar::Binary(Vec<u8>)` — no new Scalar variant. This reduces blast radius at the cost of requiring explicit "geo is compatible with Binary" special-cases in a few type-checking spots.

---

## Design Decisions

| Decision | Chosen Approach |
|---|---|
| Type system | `PrimitiveType::Geometry(GeometryType)` + `PrimitiveType::Geography(GeographyType)` |
| Scalar literal | `Scalar::Binary(Vec<u8>)` — no new Scalar variant |
| Arrow representation | GeoArrow convention: `ARROW:extension:name = "geoarrow.wkb"` as field-level metadata, physical type = `Binary` |
| Table feature | New `ReaderWriter` feature `GeospatialType` (confirm exact name against Delta RFC) |
| Preview feature variant | None |

**Blast radius vs previous plan**: No changes to `Scalar` match sites or FFI scalar dispatch (~6 match sites saved). Trade-off: requires explicit "Binary is compatible with Geometry" special-casing in `ensure_data_types.rs` and `append_null`.

---

## New Types (all in `kernel/src/schema/mod.rs`)

Define before `DecimalType`, following its pattern:

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
- **`PrimitiveType` enum** (lines 1428-1453): add `Geometry(GeometryType)` and `Geography(GeographyType)` variants with `#[serde(serialize_with = "...")]`
- Add `serialize_geometry` + `serialize_geography` (follow `serialize_decimal` at line 1487)
- **`PrimitiveType::Deserialize`** (lines 1504-1555): add branches before the `unsupported` fallback:
  - `"geometry" | str starts_with "geometry("` -> parse `GeometryType`
  - `"geography" | str starts_with "geography("` -> parse `GeographyType`
  - CRS can contain colons (`OGC:CRS84`, `EPSG:3857`); use first comma as CRS/algorithm separator
- **`Display for PrimitiveType`** (lines 1560-1577): add arms
- **`can_widen_to`**: no change -- existing explicit-opt-in logic excludes geo automatically
- Add factory methods (no `const` possible since `String` is non-const):
  ```rust
  pub fn geometry(srid: impl Into<String>) -> DeltaResult<DataType>
  pub fn geography(srid: impl Into<String>, alg: EdgeInterpolationAlgorithm) -> DeltaResult<DataType>
  ```

---

### 2. `kernel/src/expressions/scalars.rs`

**No new variant.** Only change:

- **`PrimitiveType::parse_scalar()`** (lines 703-757): exhaustive match -- add arms that error:
  ```rust
  Geometry(_) | Geography(_) => Err(/* "geo types cannot be used as partition column values" */),
  ```
  Note: the empty-string early return at line 699 fires first, so `Scalar::Null(DataType::Primitive(PrimitiveType::Geometry(crs)))` is returned for empty stats entries -- this is correct for null counts.

---

### 3. `kernel/src/engine/arrow_conversion.rs`

#### Important: GeoArrow is field-level metadata

`ArrowDataType::Extension` does not exist in arrow-rs 56/57. Physical type is `Binary`; geo metadata lives in Arrow field metadata:
- `ARROW:extension:name = "geoarrow.wkb"`
- `ARROW:extension:metadata = {"crs":"OGC:CRS84"}` (geometry)
- `ARROW:extension:metadata = {"crs":"OGC:CRS84","edges":"spherical"}` (geography)
  - `edges` key distinguishes geometry (absent) from geography (present)

**`TryFromKernel<&DataType> for ArrowDataType`** (lines 146-200): add two arms:
```rust
PrimitiveType::Geometry(_) => Ok(ArrowDataType::Binary),
PrimitiveType::Geography(_) => Ok(ArrowDataType::Binary),
```

**`TryFromKernel<&StructField> for ArrowField`** (around line 106): after building the Arrow field, inject GeoArrow extension metadata when the field type is geo. Add helper:
```rust
fn inject_geoarrow_metadata(ptype: &PrimitiveType, metadata: &mut HashMap<String, String>)
```
This adds `ARROW:extension:name` and `ARROW:extension:metadata` entries.

**`TryFromArrow<&ArrowField> for StructField`** (lines 220-255): check field metadata for GeoArrow extension type BEFORE converting the data type:
```rust
if metadata.get("ARROW:extension:name") == Some(&"geoarrow.wkb".to_string()) {
    let data_type = parse_geoarrow_datatype(metadata)?;
    return Ok(StructField::new(name, data_type, nullable));
}
// fall through to existing conversion
```
Add helper:
```rust
fn parse_geoarrow_datatype(metadata: &HashMap<String, String>) -> DeltaResult<DataType>
// parses ARROW:extension:metadata JSON
// absent/null "edges" -> Geometry(crs)
// present "edges" -> Geography(crs, algorithm)
```

**`TryFromArrow<&ArrowDataType> for DataType`** (lines 258-351): no change -- `Binary -> DataType::BINARY` as before. The geo type recovery happens at the field level (above).

---

### 4. `kernel/src/engine/ensure_data_types.rs`

**Hard blocker without this change**: `DataType::Primitive(PrimitiveType::Geometry(crs))` vs `ArrowDataType::Binary` hits the catch-all error since (a) `Binary.is_primitive()` is false and (b) `Geometry(crs) != DataType::BINARY`. Add explicit arms BEFORE the existing `DataType::BINARY` arm:

```rust
(DataType::Primitive(PrimitiveType::Geometry(_)), ArrowDataType::Binary)
| (DataType::Primitive(PrimitiveType::Geometry(_)), ArrowDataType::LargeBinary)
| (DataType::Primitive(PrimitiveType::Geography(_)), ArrowDataType::Binary)
| (DataType::Primitive(PrimitiveType::Geography(_)), ArrowDataType::LargeBinary)
    => Ok(DataTypeCompat::Identical),
```

---

### 5. `kernel/src/engine/parquet_row_group_skipping.rs`

**`get_parquet_min_stat`** (lines 111-150) and **`get_parquet_max_stat`** (lines 157-196): add no-op arms:
```rust
(Geometry(_), _) | (Geography(_), _) => return None,
```

---

### 6. `kernel/src/engine/arrow_expression/mod.rs`

**`append_null`** (lines 171-218): exhaustive DataType match, no wildcard -- add arms for geo types that use `BinaryBuilder`:
```rust
DataType::Primitive(PrimitiveType::Geometry(_))
| DataType::Primitive(PrimitiveType::Geography(_)) => append_nulls_as!(array::BinaryBuilder),
```

---

### 7. `kernel/src/engine/arrow_data.rs`

Check around line 458 for a DataType match when pushing columns. Add geo variants alongside the Binary arm if the match is exhaustive.

---

### 8. `kernel/src/transaction/stats_verifier.rs`

Exhaustive DataType match in `col_types_for` (around lines 172, 203). Add geo arms returning an error (geo columns are not valid clustering/stats columns):
```rust
DataType::Primitive(PrimitiveType::Geometry(_))
| DataType::Primitive(PrimitiveType::Geography(_)) => Err(/* "geo not supported for stats" */),
```

---

### 9. `kernel/src/scan/data_skipping/stats_schema/mod.rs`

**`is_skipping_eligible_datatype`**: positive `matches!` allowlist -- geo automatically excluded. No change.

**`MinMaxStatsTransform`**: if exhaustive match on PrimitiveType, add geo variants returning `None`. If it uses `is_skipping_eligible_datatype` as a gate, no change.

**`NullCountSchemaTransformer`**: null counts are valid for geo columns, handled automatically.

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

The `visit_schema_item` match on `DataType` is exhaustive. Map geo types to `visit_binary` to avoid a breaking ABI change (adding new function pointers to `#[repr(C)]` structs is breaking):
```rust
DataType::Primitive(PrimitiveType::Geometry(_))
| DataType::Primitive(PrimitiveType::Geography(_)) => call!(visit_binary),
```

Dedicated FFI geo callbacks can be added in a future phase with a version bump.

---

## StructData Type Check (Watch Out)

`StructData::try_new` checks `f.data_type() == &a.data_type()`. For a geo column, the field declares `DataType::Primitive(PrimitiveType::Geometry(crs))` but a `Scalar::Binary` value reports `DataType::BINARY`. These are not equal.

**When does this matter?** Only if kernel internally constructs a `StructData` containing a geo column value. In the normal scan read path, data flows as Arrow arrays via the visitor pattern, never through `StructData`. If this turns out to be a blocker during implementation, the fix is to relax the check to treat Binary as compatible with Geometry/Geography. Write a targeted test to surface this early.

---

## Blast Radius Summary

| File | Nature of Change |
|---|---|
| `kernel/src/schema/mod.rs` | New structs + 2 PrimitiveType variants + serde + Display + factory methods |
| `kernel/src/expressions/scalars.rs` | `parse_scalar` +2 error arms only (no new Scalar variant) |
| `kernel/src/engine/arrow_conversion.rs` | +2 DataType arms; GeoArrow metadata inject/parse at StructField level; 2 helpers |
| `kernel/src/engine/ensure_data_types.rs` | +4 arms: (Geometry/Geography, Binary/LargeBinary) => Identical |
| `kernel/src/engine/parquet_row_group_skipping.rs` | +1 combined no-op arm in each of min/max stat functions |
| `kernel/src/engine/arrow_expression/mod.rs` | `append_null` +1 arm for geo (BinaryBuilder) |
| `kernel/src/engine/arrow_data.rs` | +1 arm for geo alongside Binary (if exhaustive match) |
| `kernel/src/transaction/stats_verifier.rs` | +1 arm per exhaustive match (error arms) |
| `kernel/src/scan/data_skipping/stats_schema/mod.rs` | +1 arm if MinMaxStatsTransform match is exhaustive, else no change |
| `kernel/src/table_features/mod.rs` | GeospatialType variant + FeatureInfo + match arms |
| `kernel/src/table_features/geospatial.rs` | **New file** |
| `kernel/src/table_configuration.rs` | +1 validation call |
| `ffi/src/schema.rs` | `visit_schema_item` +1 arm (map to visit_binary) |

Estimated: ~13 source files + 1 new file. **Run `cargo build` after step 1 to surface all exhaustive match sites.**

---

## Type Conversion Flows

### Read path
```
Delta log JSON ("geometry(OGC:CRS84)")
  -> PrimitiveType::Deserialize -> PrimitiveType::Geometry(GeometryType { srid: "OGC:CRS84" })
  -> kernel StructType (held in Snapshot)

Parquet read (default engine):
  Arrow field (Binary + ARROW:extension:name=geoarrow.wkb + ARROW:extension:metadata)
    -> TryFromArrow<&ArrowField>: detect geoarrow.wkb metadata -> parse_geoarrow_datatype
    -> PrimitiveType::Geometry(crs)  [field schema reconstructed from Arrow metadata]
  Arrow BinaryArray data
    -> EngineData -> connector reads via get_binary() -> opaque WKB bytes
```

### Write path
```
User provides Binary data (WKB bytes) for a geo column
  -> PrimitiveType::Geometry(crs) in kernel schema
  -> TryFromKernel<&StructField> for ArrowField: inject ARROW:extension:name/metadata
  -> written as Binary in Parquet; Arrow schema field carries geoarrow.wkb metadata
  -> Delta log: schema includes "geometry(OGC:CRS84)" column type string
```

---

## Open Questions

1. **Exact Delta RFC table feature name**: Confirm `"geospatialType"` against the Delta RFC. Public API, cannot change after shipping.
2. **`StructData` type check**: Investigate if the `StructData::try_new` data_type equality check is actually hit for geo columns. Write a targeted test to surface this.

---

## Implementation Sequence

1. Define `EdgeInterpolationAlgorithm`, `GeometryType`, `GeographyType`; add to `PrimitiveType`; implement serde + Display; add factory methods
2. **Run `cargo build --workspace --all-features`** -- all exhaustive match sites will fail to compile
3. Fix all compiler errors (add arms in `arrow_conversion.rs`, `parquet_row_group_skipping.rs`, `scalars.rs`, `arrow_expression/mod.rs`, `arrow_data.rs`, `stats_verifier.rs`, `stats_schema/mod.rs`, `ffi/src/schema.rs`)
4. Add `ensure_data_types.rs` explicit geo-as-binary compatibility arms
5. Add `GeospatialType` table feature + `GEOSPATIAL_TYPE_INFO` + match arms
6. Create `geospatial.rs` + wire up validation in `table_configuration.rs`
7. Add GeoArrow metadata injection in `TryFromKernel<&StructField> for ArrowField`
8. Add GeoArrow metadata parsing in `TryFromArrow<&ArrowField> for StructField`
9. Write tests (see verification plan below)

---

## Verification Plan

1. **Schema round-trip**: `"geometry(OGC:CRS84)"` -> `PrimitiveType::Geometry` -> `Display` -> `"geometry(OGC:CRS84)"`. Cover: default CRS, geography with algorithm, unknown algorithm = error.
2. **Arrow conversion round-trip**: `PrimitiveType::Geometry(crs)` -> `ArrowField` with geoarrow.wkb metadata -> `TryFromArrow` -> `PrimitiveType::Geometry(crs)`.
3. **`ensure_data_types`**: `(Geometry(crs), ArrowDataType::Binary)` returns `Identical`.
4. **Feature validation (negative)**: geo columns in schema + protocol missing `GeospatialType` -> error.
5. **Feature validation (positive)**: geo columns + feature present -> ok.
6. **Parquet stats**: geo column in predicate -> `get_parquet_min_stat` returns `None`.
7. **`StructData` test**: construct a StructData with a geo column value (as `Scalar::Binary`) -- does it fail? If yes, fix the type check and add regression test.

```bash
cargo build --workspace --all-features         # step 2: surface all match sites
cargo nextest run -p delta_kernel --lib --all-features
cargo fmt && cargo clippy --workspace --benches --tests --all-features -- -D warnings
```
