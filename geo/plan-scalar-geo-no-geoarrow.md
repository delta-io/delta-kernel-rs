# Geo Types Implementation Plan (Scalar::Geo, no GeoArrow Arrow metadata)

> Also saved at: `/home/lorena.rosati/.claude/plans/lively-squishing-pie.md` (the Claude plans directory)

## Context

Add geometry and geography support to delta-kernel-rs. Uses `Scalar::Geo(DataType, Vec<u8>)` at the expression level (typed, no erasure), but deliberately does NOT inject GeoArrow extension metadata into Arrow fields. Geo columns are plain `Binary` in Arrow — the Delta log schema is the sole source of truth for the geometry/geography distinction. Simplest Arrow integration at the cost of losing type info at the Arrow layer.

---

## Design Decisions

| Decision | Chosen Approach |
|---|---|
| Type system | `PrimitiveType::Geometry(GeometryType)` + `PrimitiveType::Geography(GeographyType)` |
| Scalar literal | `Scalar::Geo(DataType, Vec<u8>)` — carries full DataType, correct `data_type()` |
| Arrow representation | Plain `ArrowDataType::Binary` — no GeoArrow extension metadata |
| Table feature | New `ReaderWriter` feature `GeospatialType` (confirm name against Delta RFC) |
| Preview feature variant | None |

**vs. plan-with-geoarrow.md**: Same Scalar design, but skips all GeoArrow field-level metadata work (no `inject_geoarrow_metadata`, no `parse_geoarrow_datatype`). Trade-off: simpler Arrow code, but Arrow schema cannot reconstruct geo type without consulting the Delta log.

**vs. plan-with-binary-scalar.md**: Has `Scalar::Geo` (typed), so no `StructData` type-check hazard. But same "plain Binary" Arrow story.

---

## New Types (all in `kernel/src/schema/mod.rs`)

```rust
pub enum EdgeInterpolationAlgorithm { Spherical, Vincenty, Thomas, Andoyer, Karney }
// Display: lowercase; from_str: reject unknown values

pub struct GeometryType { srid: String }
// DEFAULT_SRID = "OGC:CRS84"; try_new rejects empty; srid() getter

pub struct GeographyType { srid: String, algorithm: EdgeInterpolationAlgorithm }
```

### Delta JSON serialization format

| Type | Serialized string |
|---|---|
| `Geometry(OGC:CRS84)` | `"geometry(OGC:CRS84)"` |
| `Geography(OGC:CRS84, Spherical)` | `"geography(OGC:CRS84, spherical)"` |
| Bare `"geometry"` on read | accept, default to OGC:CRS84 |

---

## File-by-File Changes

### 1. `kernel/src/schema/mod.rs`

- Define `EdgeInterpolationAlgorithm`, `GeometryType`, `GeographyType` above `DecimalType`
- **`PrimitiveType` enum** (lines 1428-1453): add `Geometry(GeometryType)` + `Geography(GeographyType)` with `#[serde(serialize_with = "...")]`
- Add `serialize_geometry` + `serialize_geography` (follow `serialize_decimal` at line 1487)
- **`PrimitiveType::Deserialize`** (lines 1504-1555): add branches before `unsupported`:
  - `"geometry" | starts_with "geometry("` -> parse GeometryType
  - `"geography" | starts_with "geography("` -> parse GeographyType
- **`Display for PrimitiveType`** (lines 1560-1577): add arms
- **`can_widen_to`**: no change -- positive opt-in list excludes geo automatically
- Factory methods:
  ```rust
  pub fn geometry(srid: impl Into<String>) -> DeltaResult<DataType>
  pub fn geography(srid: impl Into<String>, alg: EdgeInterpolationAlgorithm) -> DeltaResult<DataType>
  ```

---

### 2. `kernel/src/expressions/scalars.rs`

**`Scalar` enum** (lines 223-258): add variant:
```rust
/// Geospatial value as WKB bytes. Carries column DataType for correct type dispatch.
Geo(DataType, Vec<u8>),
```

**`data_type()`** (lines 262-281): `Self::Geo(dt, _) => dt.clone()`

**`Display for Scalar`** (lines 360-426): `Self::Geo(_, b) => write!(f, "Geo({} WKB bytes)", b.len())`

**`logical_partial_cmp`** (lines 465-500): `(Geo(..), _) => None` -- geo values are not orderable

**`parse_scalar` on PrimitiveType** (lines 703-757): add error arms -- geo columns cannot be partition columns:
```rust
Geometry(_) | Geography(_) => Err(/* "geo types cannot be used as partition column values" */),
```

**Arithmetic** (`try_add/sub/mul/div`): catch-all `_ => None` already handles this, no change.

---

### 3. `kernel/src/engine/arrow_conversion.rs`

**`TryFromKernel<&DataType> for ArrowDataType`** (lines 146-200): geo maps to plain Binary:
```rust
PrimitiveType::Geometry(_) => Ok(ArrowDataType::Binary),
PrimitiveType::Geography(_) => Ok(ArrowDataType::Binary),
```

**`TryFromKernel<&StructField> for ArrowField`**: no special handling -- geo fields become plain Binary Arrow fields with no extension metadata.

**`TryFromArrow<&ArrowField> for StructField`** (lines 220-255): no special handling -- Binary Arrow fields stay `DataType::BINARY`. Geo type recovery is the kernel's responsibility via the Delta log schema.

**`TryFromArrow<&ArrowDataType> for DataType`** (lines 258-351): no change.

#### Implication
When the default engine reads a Parquet file, a geo column comes back as `ArrowDataType::Binary`. The `TryFromArrow` layer returns `DataType::BINARY`. The kernel uses the Delta log schema (not the Arrow schema) to know the column is `Geometry(crs)`. This requires that `ensure_data_types` accept `(Geometry(crs), Binary)` as compatible.

---

### 4. `kernel/src/engine/ensure_data_types.rs`

**Hard blocker without this change.** Add explicit arms BEFORE the existing `DataType::BINARY` arm:

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

**`append_null`** (lines 171-218): exhaustive match, no wildcard -- add:
```rust
DataType::Primitive(PrimitiveType::Geometry(_))
| DataType::Primitive(PrimitiveType::Geography(_)) => append_nulls_as!(array::BinaryBuilder),
```

---

### 7. `kernel/src/engine/arrow_data.rs`

Check around line 458 for exhaustive DataType match on column pushing. Add geo variants alongside the existing Binary arm.

---

### 8. `kernel/src/transaction/stats_verifier.rs`

Exhaustive DataType match in `col_types_for` (lines ~172, 203). Add error arms:
```rust
DataType::Primitive(PrimitiveType::Geometry(_))
| DataType::Primitive(PrimitiveType::Geography(_)) => Err(/* "geo not supported for stats" */),
```

---

### 9. `kernel/src/scan/data_skipping/stats_schema/mod.rs`

**`is_skipping_eligible_datatype`**: positive `matches!` allowlist -- geo automatically excluded, no change.

**`MinMaxStatsTransform`**: if exhaustive match on PrimitiveType, add geo variants returning `None`.

---

### 10. `kernel/src/table_features/mod.rs`

```rust
/// Geospatial type support: geometry and geography columns with WKB encoding.
#[strum(serialize = "geospatialType")]
#[serde(rename = "geospatialType")]
GeospatialType,
```

Add `GEOSPATIAL_TYPE_INFO` static (ReaderWriter, no min_legacy_version, Supported). Update `feature_type()` and `info()` match arms.

---

### 11. `kernel/src/table_features/geospatial.rs` (NEW FILE)

Pattern: identical to `timestamp_ntz.rs`.

```rust
pub(crate) fn validate_geospatial_feature_support(tc: &TableConfiguration) -> DeltaResult<()>
pub(crate) fn schema_contains_geospatial(schema: &Schema) -> bool
```

Register in `table_features/mod.rs`.

---

### 12. `kernel/src/table_configuration.rs`

Add `validate_geospatial_feature_support(&table_config)?` alongside `validate_timestamp_ntz_feature_support`.

---

### 13. FFI layer (`ffi/src/schema.rs`)

Map geo to `visit_binary` in the exhaustive `visit_schema_item` match to avoid breaking ABI:
```rust
DataType::Primitive(PrimitiveType::Geometry(_))
| DataType::Primitive(PrimitiveType::Geography(_)) => call!(visit_binary),
```

Also update FFI expression visitor for new `Scalar::Geo` variant.

---

## Blast Radius Summary

| File | Nature of Change |
|---|---|
| `kernel/src/schema/mod.rs` | New structs + 2 PrimitiveType variants + serde + Display + factory methods |
| `kernel/src/expressions/scalars.rs` | `Scalar::Geo(DataType, Vec<u8>)` + `data_type()` + Display + `logical_partial_cmp` + `parse_scalar` |
| `kernel/src/engine/arrow_conversion.rs` | +2 DataType arms only (no metadata helpers needed) |
| `kernel/src/engine/ensure_data_types.rs` | +4 arms: (Geometry/Geography, Binary/LargeBinary) => Identical |
| `kernel/src/engine/parquet_row_group_skipping.rs` | +1 no-op arm in each of min/max stat functions |
| `kernel/src/engine/arrow_expression/mod.rs` | `append_null` +1 arm (BinaryBuilder) |
| `kernel/src/engine/arrow_data.rs` | +1 arm for geo alongside Binary |
| `kernel/src/transaction/stats_verifier.rs` | +1 arm per exhaustive match (error) |
| `kernel/src/scan/data_skipping/stats_schema/mod.rs` | +1 arm if exhaustive, else no change |
| `kernel/src/table_features/mod.rs` | GeospatialType variant + FeatureInfo + match arms |
| `kernel/src/table_features/geospatial.rs` | **New file** |
| `kernel/src/table_configuration.rs` | +1 validation call |
| `ffi/src/schema.rs` | +1 arm (map geo to visit_binary) |
| `ffi/src/expressions/engine_visitor.rs` (approx) | `Scalar::Geo` match arm |

Estimated: ~13-14 source files + 1 new file.

---

## Type Conversion Flows

### Read path
```
Delta log JSON ("geometry(OGC:CRS84)")
  -> PrimitiveType::Geometry(GeometryType { srid: "OGC:CRS84" })
  -> kernel StructType (held in Snapshot -- authoritative source of geo type)

Parquet read (default engine):
  Arrow field: plain Binary (no extension metadata)
    -> TryFromArrow: DataType::BINARY
  ensure_data_types: (Geometry(crs) expected, Binary delivered) -> Identical [explicit arm]
  Arrow BinaryArray data -> EngineData -> connector reads via get_binary() -> WKB bytes
  Scalar construction: Scalar::Geo(DataType::geometry("OGC:CRS84")?, wkb_bytes)
```

### Write path
```
User provides WKB bytes for a geo column
  -> PrimitiveType::Geometry(crs) in kernel schema
  -> TryFromKernel<&DataType>: ArrowDataType::Binary (no extension metadata)
  -> written as plain Binary in Parquet
  -> Delta log: schema includes "geometry(OGC:CRS84)" column type string
```

---

## Open Questions

1. **Exact Delta RFC table feature name**: Confirm `"geospatialType"` against the spec.
2. **Arrow schema round-trip loss**: With no GeoArrow metadata, tools reading only Arrow/Parquet schema (not the Delta log) see plain Binary. Acceptable?

---

## Implementation Sequence

1. Define new schema types + add `PrimitiveType` variants + serde + Display + factory methods
2. **`cargo build --workspace --all-features`** -- surfaces all exhaustive match sites
3. Fix all compiler errors
4. Add `ensure_data_types.rs` geo-as-binary compatibility arms
5. Add `GeospatialType` table feature + create `geospatial.rs` + wire up validation
6. Write tests

---

## Verification Plan

1. **Schema round-trip**: `"geometry(OGC:CRS84)"` -> deserialize -> `Display` -> `"geometry(OGC:CRS84)"`.
2. **`ensure_data_types`**: `(Geometry(crs), ArrowDataType::Binary)` -> `Identical`.
3. **Arrow conversion**: `PrimitiveType::Geometry(crs)` -> `ArrowDataType::Binary`, no field metadata.
4. **Feature validation (negative)**: geo columns + protocol missing `GeospatialType` -> error.
5. **Feature validation (positive)**: geo columns + feature present -> ok.
6. **Parquet stats**: `get_parquet_min_stat` for geo column returns `None`.
7. **Scalar ordering**: `Scalar::Geo(..).logical_partial_cmp(anything)` returns `None`.

```bash
cargo build --workspace --all-features
cargo nextest run -p delta_kernel --lib --all-features
cargo fmt && cargo clippy --workspace --benches --tests --all-features -- -D warnings
```
