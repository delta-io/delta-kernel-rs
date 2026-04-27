# Geo Types Implementation Plan for delta-kernel-rs

> Also saved at: `/home/lorena.rosati/.claude/plans/lively-squishing-pie.md`


## Context

Add first-class geometry and geography support to delta-kernel-rs per the Delta Lake geo RFC. The implementation introduces two new parameterized `PrimitiveType` variants, a new `Scalar::Geo` literal, a new reader+writer table feature, and GeoArrow extension type support in the Arrow conversion layer. Physical storage is WKB bytes in Parquet/Arrow.

---

## Design Decisions (Confirmed)

| Decision | Chosen Approach |
|---|---|
| Type system | `PrimitiveType::Geometry(GeometryType)` + `PrimitiveType::Geography(GeographyType)` |
| Scalar literal | `Scalar::Geo(DataType, Vec<u8>)` — single variant carrying type info, WKB bytes |
| Arrow representation | GeoArrow extension type convention: `ARROW:extension:name = "geoarrow.wkb"` as field-level metadata |
| Table feature | New `ReaderWriter` feature, name TBD (probably `"geospatialType"` matching Delta RFC) |
| Preview feature variant | GeospatialType only (no preview variant) |

### `Scalar::Geo(DataType, Vec<u8>)`

Analogous to `Scalar::Null(DataType)`. The caller provides the full `DataType` (which carries CRS/algorithm), so `data_type()` is correct. Single variant, no type erasure.

---

## New Types (all in `kernel/src/schema/mod.rs`)

### `GeometryType` (follow `DecimalType` pattern)

```rust
pub struct GeometryType { srid: String }
impl GeometryType {
    pub const DEFAULT_SRID: &str = "OGC:CRS84";
    pub fn try_new(srid: impl Into<String>) -> DeltaResult<Self>  // reject empty
    pub fn srid(&self) -> &str
}
```

### `GeographyType`

```rust
pub struct GeographyType { srid: String, algorithm: EdgeInterpolationAlgorithm }
// same DEFAULT_SRID = "OGC:CRS84", DEFAULT_ALGORITHM = Spherical
```

### `EdgeInterpolationAlgorithm` (closed enum matching Java Kernel)

```rust
pub enum EdgeInterpolationAlgorithm { Spherical, Vincenty, Thomas, Andoyer, Karney }
```

The Java Kernel (`GeographyType.java`) validates against exactly these five values.

### Delta JSON serialization format (matching Java Kernel `simpleString()`)

| Type | Serialized string |
|---|---|
| `Geometry(OGC:CRS84)` | `"geometry(OGC:CRS84)"` |
| `Geography(OGC:CRS84, Spherical)` | `"geography(OGC:CRS84, spherical)"` |
| Bare geometry (default CRS) | accept `"geometry"` on read, write with explicit CRS |

---

## File-by-File Changes

### 1. `kernel/src/schema/mod.rs`

**Define `GeometryType`, `GeographyType`, `EdgeInterpolationAlgorithm`** before `DecimalType` definition.

**`PrimitiveType` enum** (lines 1428-1453): Add two variants:
```rust
Geometry(GeometryType),
Geography(GeographyType),
```

**Serde changes:**
- Add `serialize_geometry` + `serialize_geography` helper functions (follow `serialize_decimal` at line 1487)
- `PrimitiveType::Deserialize` (lines 1504-1555): Add branches before the `unsupported` fallback:
  ```
  "geometry" | str starts_with "geometry(" -> parse GeometryType
  "geography" | str starts_with "geography(" -> parse GeographyType
  ```
  CRS can contain colons (`OGC:CRS84`, `EPSG:3857`). Use comma as separator between CRS and algorithm.

**`Display for PrimitiveType`** (lines 1560-1577): Add arms:
```rust
Geometry(g) => write!(f, "geometry({})", g.srid()),
Geography(g) => write!(f, "geography({}, {})", g.srid(), g.algorithm()),
```

**`DataType` factory methods**: Cannot add `const` for parameterized types. Add:
```rust
pub fn geometry(srid: impl Into<String>) -> DeltaResult<DataType>
pub fn geography(srid: impl Into<String>, alg: EdgeInterpolationAlgorithm) -> DeltaResult<DataType>
```

**`can_widen_to`** (type widening, lines 1468-1484): Geo types cannot widen. No new match arm needed -- existing explicit-opt-in logic handles this.

---

### 2. `kernel/src/expressions/scalars.rs`

**`Scalar` enum** (lines 223-258): Add variant:
```rust
/// Geospatial value stored as WKB bytes. Carries the column DataType for correct type dispatch.
Geo(DataType, Vec<u8>),
```

**`data_type()`** (lines 262-281):
```rust
Self::Geo(dt, _) => dt.clone(),
```

**`Display for Scalar`** (lines 360-426):
```rust
Self::Geo(_, bytes) => write!(f, "Geo({} WKB bytes)", bytes.len()),
```

**`logical_partial_cmp`** (lines 465-500): Geo values are NOT orderable (WKB byte comparison is semantically wrong for geometric equality):
```rust
(Geo(..), _) => None,
```

**`parse_scalar` on PrimitiveType** (lines 703-757): Geo columns should NOT be partition columns. Add arms that return an error:
```rust
Geometry(_) | Geography(_) => Err(/* "geo columns are not supported as partition values" */),
```

**Arithmetic methods** (`try_add/sub/mul/div`): These use `_ => None` catch-alls, no changes needed.

---

### 3. `kernel/src/engine/arrow_conversion.rs`

#### Important: GeoArrow is field-level metadata, NOT a DataType variant

`ArrowDataType::Extension(...)` does NOT exist in arrow-rs 56/57. GeoArrow extension types work by convention: physical type is `Binary`, and two field metadata keys are set:
- `ARROW:extension:name = "geoarrow.wkb"`
- `ARROW:extension:metadata = <JSON string>`

The JSON metadata distinguishes geometry from geography via the `edges` field:
- No `edges` (or `edges: null`) = geometry
- `edges: "spherical"` (or vincenty/thomas/andoyer/karney) = geography

#### Kernel -> Arrow (two layers)

**`TryFromKernel<&DataType> for ArrowDataType`** (lines 146-200): Geo maps to Binary at the physical level:
```rust
PrimitiveType::Geometry(_) => Ok(ArrowDataType::Binary),
PrimitiveType::Geography(_) => Ok(ArrowDataType::Binary),
```

**`TryFromKernel<&StructField> for ArrowField`** (around line 106): When converting a kernel `StructField` whose type is `Geometry(crs)` or `Geography(crs, alg)`, inject GeoArrow metadata into the Arrow field:
```rust
// Metadata to inject:
field_metadata.insert("ARROW:extension:name".into(), "geoarrow.wkb".into());
field_metadata.insert("ARROW:extension:metadata".into(), geoarrow_metadata_json(kernel_type));
```

GeoArrow metadata JSON:
```json
// Geometry:
{"crs": "OGC:CRS84"}
// Geography:
{"crs": "OGC:CRS84", "edges": "spherical"}
```

The CRS value may need to be a PROJJSON object for full spec compliance, but authority string shorthand is acceptable for now.

#### Arrow -> Kernel (field-level inspection)

**`TryFromArrow<&ArrowField> for StructField`** (lines 220-255): Check extension metadata BEFORE converting the data type:
```rust
if let Some(ext_name) = field.metadata().get("ARROW:extension:name") {
    if ext_name == "geoarrow.wkb" {
        let kernel_type = parse_geoarrow_metadata(field.metadata())?;
        return Ok(StructField::new(name, kernel_type, nullable));
    }
}
// fall through to existing conversion...
```

Add helper `parse_geoarrow_metadata(metadata: &HashMap<String, String>) -> DeltaResult<DataType>`:
- Parse `ARROW:extension:metadata` JSON
- If `edges` key is absent/null -> `DataType::Primitive(PrimitiveType::Geometry(...))`
- If `edges` key is present -> `DataType::Primitive(PrimitiveType::Geography(...))`

**`TryFromArrow<&ArrowDataType> for DataType`** (lines 258-351): No change needed for `Binary`. The field-level check above intercepts geo columns before this is called.

---

### 4. `kernel/src/engine/parquet_row_group_skipping.rs`

**`get_parquet_min_stat`** (lines 111-150) and **`get_parquet_max_stat`** (lines 157-196): Add arms at the start of the PrimitiveType match:
```rust
(Geometry(_), _) | (Geography(_), _) => return None,
```
Geo values have no ordering, so no row group skipping. Returning `None` means "no stat available, don't skip anything."

---

### 5. `kernel/src/scan/data_skipping/stats_schema/mod.rs`

**`is_skipping_eligible_datatype`**: Uses positive `matches!` allowlist -- geo types automatically excluded. No change needed.

**`MinMaxStatsSchema` / `NullCountSchemaTransformer`**: Null counts ARE meaningful for geo columns and will work automatically. Min/max schema transformation: the function that builds the stats schema walks `PrimitiveType` variants. If it uses exhaustive matching, add geo variants that return `None` (no min/max stats). If it already uses a positive allowlist, no change needed.

---

### 6. `kernel/src/table_features/mod.rs`

Add new variant in the `ReaderWriter` section:
```rust
/// Geospatial type support: geometry and geography columns with WKB encoding.
GeospatialType,
```

Serialization name: `"geospatialType"` (follow the Delta RFC; parallels `"variantType"`). Confirm exact name against spec before implementing.

Add `FeatureInfo` static:
```rust
static GEOSPATIAL_TYPE_INFO: FeatureInfo = FeatureInfo {
    feature_type: FeatureType::ReaderWriter,
    min_legacy_version: None,  // new feature, no legacy version inference
    feature_requirements: &[],
    kernel_support: KernelSupport::Supported,
    enablement_check: EnablementCheck::AlwaysIfSupported,
};
```

Update all exhaustive match arms on `TableFeature` to add `GeospatialType`.

---

### 7. `kernel/src/table_features/geospatial.rs` (NEW FILE)

Pattern: identical to `kernel/src/table_features/timestamp_ntz.rs`.

```rust
/// Checks whether `schema` contains any geometry or geography columns (recursively).
pub(crate) fn schema_contains_geospatial(schema: &Schema) -> bool { ... }

/// Returns an error if the schema contains geo columns but the protocol lacks GeospatialType.
pub(crate) fn validate_geospatial_feature_support(tc: &TableConfiguration) -> DeltaResult<()> { ... }
```

Register in `table_features/mod.rs`:
```rust
mod geospatial;
pub(crate) use geospatial::{schema_contains_geospatial, validate_geospatial_feature_support};
```

---

### 8. `kernel/src/table_configuration.rs`

Find where `validate_timestamp_ntz_feature_support` is called and add `validate_geospatial_feature_support` alongside it.

---

### 9. FFI layer (`ffi/src/schema.rs`, `ffi/src/schema_visitor.rs`)

The FFI layer exposes `PrimitiveType` and `Scalar` to C/C++. New variants need handling. Search for `PrimitiveType::` and `Scalar::` match sites and add `Geometry`/`Geography`/`Geo` arms. Lower priority but required for FFI completeness.

---

## Blast Radius Summary

| File | Nature of Change |
|---|---|
| `kernel/src/schema/mod.rs` | New structs + 2 new `PrimitiveType` variants + serde + Display + factory methods |
| `kernel/src/expressions/scalars.rs` | New `Scalar::Geo` variant + `data_type()` + Display + `logical_partial_cmp` + `parse_scalar` |
| `kernel/src/engine/arrow_conversion.rs` | Kernel->Arrow: 2 new `DataType` arms + field metadata injection; Arrow->Kernel: field metadata inspection |
| `kernel/src/engine/parquet_row_group_skipping.rs` | 2 new no-op arms in min/max stat functions |
| `kernel/src/scan/data_skipping/stats_schema/mod.rs` | Possible arm additions depending on match exhaustiveness |
| `kernel/src/table_features/mod.rs` | New variant + FeatureInfo + match arm updates |
| `kernel/src/table_features/geospatial.rs` | **New file** -- schema validation |
| `kernel/src/table_configuration.rs` | Wire up geospatial validation |
| `ffi/src/schema.rs`, `ffi/src/schema_visitor.rs` | FFI match arm additions |

Estimated: ~12-15 source files modified + 1 new file. Any exhaustive `match PrimitiveType` without a wildcard will cause a compile error after step 1, flagging all additional sites automatically.

---

## Type Conversion Flow (Read Path)

```
Delta log JSON ("geometry(OGC:CRS84)")
  -> PrimitiveType::Deserialize -> PrimitiveType::Geometry(GeometryType { srid: "OGC:CRS84" })
  -> kernel StructType (held in Snapshot)

Parquet read (default engine):
  Arrow schema field (Binary + geoarrow.wkb extension metadata)
    -> TryFromArrow<&ArrowField> for StructField -> PrimitiveType::Geometry(crs)  [field metadata inspection]
  Arrow BinaryArray
    -> EngineData -> Scalar::Geo(DataType::geometry("OGC:CRS84")?, wkb_bytes)
```

## Type Conversion Flow (Write Path)

```
User provides geo column as Binary data (WKB)
  -> kernel write path: PrimitiveType::Geometry(crs) in schema
  -> TryFromKernel<&StructField> for ArrowField: inject ARROW:extension:name + ARROW:extension:metadata
  -> written as Binary in Parquet; Arrow schema field carries geoarrow.wkb metadata
  -> Delta log: schema includes "geometry(OGC:CRS84)" column type
```

---

## Open Questions

1. **Exact Delta RFC table feature name**: Is it `"geospatialType"`, `"geometryType"`, or something else? Check the Delta RFC/issue tracker. The name is public API and must match the spec exactly.

2. **CRS round-trip in GeoArrow metadata**: For full spec compliance, the CRS in GeoArrow metadata should be PROJJSON. For pragmatic first cut: store the CRS string directly (e.g., `{"crs": "OGC:CRS84"}`). This can be evolved later.

---

## Implementation Sequence

1. **Define new schema types** (`GeometryType`, `GeographyType`, `EdgeInterpolationAlgorithm`) + add to `PrimitiveType`
   - Run `cargo build` after this step -- all exhaustive match sites will fail to compile, identifying the full blast radius
2. **Fix all `PrimitiveType` match sites** surfaced by the compile errors
3. **Add `Scalar::Geo(DataType, Vec<u8>)`** + fix all `Scalar` match sites
4. **Add `GeospatialType` table feature** + create `geospatial.rs` + wire up validation
5. **Arrow conversion** -- geo -> Binary at DataType level, inject/parse GeoArrow field metadata
6. **Parquet row group skipping** -- no-op arms
7. **FFI** -- match arm additions
8. **Tests** -- see verification plan below

---

## Verification Plan

1. **Schema round-trip** (`schema/mod.rs` tests): `geometry(OGC:CRS84)` -> JSON -> deserialize -> equality. Cover: default CRS (`"geometry"`), geography with algorithm, unknown algorithm fails.

2. **Arrow conversion round-trip**: `PrimitiveType::Geometry(crs)` -> `ArrowField` with geoarrow.wkb metadata -> `TryFromArrow` -> back to `PrimitiveType::Geometry(crs)`.

3. **Table feature validation (negative)**: Schema with geo columns + protocol missing `GeospatialType` -> error.

4. **Table feature validation (positive)**: Schema with geo columns + protocol with `GeospatialType` -> succeeds.

5. **Parquet stats**: Geo column in scan with predicate -> `get_parquet_min_stat` returns `None`.

6. **Scalar ordering**: `Scalar::Geo(..).logical_partial_cmp(anything)` returns `None`.

7. **Compile check after step 1**:
```bash
cargo build --workspace --all-features         # surfaces all blast-radius match sites
cargo nextest run -p delta_kernel --lib --all-features
cargo fmt && cargo clippy --workspace --benches --tests --all-features -- -D warnings
```
