# Schemas and Data Types

Kernel defines its own type system that mirrors the Delta protocol specification. This
type system is independent of any engine's type system (such as Arrow). Your engine
converts to and from Kernel types as needed.

## Data types

The `DataType` enum represents all types supported by the Delta protocol:

### Primitive types

| Constant | Rust equivalent | Description |
|----------|-----------------|-------------|
| `DataType::BOOLEAN` | `bool` | True or false |
| `DataType::BYTE` | `i8` | 8-bit signed integer |
| `DataType::SHORT` | `i16` | 16-bit signed integer |
| `DataType::INTEGER` | `i32` | 32-bit signed integer |
| `DataType::LONG` | `i64` | 64-bit signed integer |
| `DataType::FLOAT` | `f32` | 32-bit IEEE 754 float |
| `DataType::DOUBLE` | `f64` | 64-bit IEEE 754 float |
| `DataType::STRING` | `String` | UTF-8 string |
| `DataType::BINARY` | `Vec<u8>` | Arbitrary bytes |
| `DataType::DATE` | N/A | Calendar date (days since epoch) |
| `DataType::TIMESTAMP` | N/A | Microsecond precision, adjusted to UTC |
| `DataType::TIMESTAMP_NTZ` | N/A | Microsecond precision, no timezone |

#### Decimal

Decimals have a precision (1 to 38 inclusive) and a scale (0 to precision inclusive):

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::DeltaResult;
# use delta_kernel::schema::DataType;
# fn main() -> DeltaResult<()> {
let price_type = DataType::decimal(18, 2)?;
# Ok(())
# }
```

### Complex types

#### Array

An ordered sequence of elements, all of the same type:

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::schema::{ArrayType, DataType};
// Array of nullable strings
let array_type = DataType::from(ArrayType::new(DataType::STRING, true));
```

The `contains_null` parameter indicates whether elements can be null.

#### Map

A collection of key-value pairs:

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::schema::{DataType, MapType};
// Map from string keys to nullable integer values
let map_type = DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, true));
```

Map keys are never null. The `value_contains_null` parameter controls whether values
can be null.

#### Struct

A named collection of fields (see [Schemas](#schemas) below). Structs can be nested:

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::schema::{DataType, StructField, StructType};
# use delta_kernel::DeltaResult;
# fn main() -> DeltaResult<()> {
let address_type = StructType::try_new([
    StructField::nullable("street", DataType::STRING),
    StructField::nullable("city", DataType::STRING),
    StructField::nullable("zip", DataType::STRING),
])?;

let person_type = StructType::try_new([
    StructField::not_null("name", DataType::STRING),
    StructField::nullable("address", address_type),
])?;
# Ok(())
# }
```

#### Variant

A semi-structured type that can hold any value. The physical representation uses a struct
with `metadata` and `value` fields (both binary). To create an unshredded variant column:

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::schema::DataType;
let variant_type = DataType::unshredded_variant();
```

## Schemas

A schema is a `StructType`, an ordered collection of named, typed fields. The type
aliases `Schema` and `SchemaRef` (`Arc<StructType>`) are used throughout the API.

### Creating a schema

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::DeltaResult;
# use delta_kernel::schema::{DataType, StructField, StructType};
# fn main() -> DeltaResult<()> {
let schema = StructType::try_new([
    StructField::not_null("id", DataType::LONG),
    StructField::nullable("name", DataType::STRING),
    StructField::nullable("score", DataType::DOUBLE),
])?;
# Ok(())
# }
```

`try_new` returns an error if the schema contains duplicate field names
(case-insensitive, since Delta column names are case-insensitive).

`StructType::builder()` provides a builder for incremental construction:

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::DeltaResult;
# use delta_kernel::schema::{DataType, StructField, StructType};
# fn main() -> DeltaResult<()> {
let schema = StructType::builder()
    .add_field(StructField::not_null("id", DataType::LONG))
    .add_field(StructField::nullable("name", DataType::STRING))
    .build()?;
# Ok(())
# }
```

### Querying a schema

```rust,ignore
// Look up a field by name
if let Some(field) = schema.field("name") {
    println!("{}: {:?}, nullable={}", field.name(), field.data_type(), field.is_nullable());
}

// Check if a field exists
assert!(schema.contains("id"));

// Get the positional index of a field
let idx = schema.index_of("name"); // Some(1)

// Iterate over all fields
for field in schema.fields() {
    println!("{}", field.name());
}

// Number of fields
let n = schema.num_fields();
```

### Projecting a schema

`project()` creates a new schema with a subset of fields. The output preserves the
order you specify:

```rust,ignore
// Table schema: [id, name, email, created_at]
// Select only [email, id] in that order
let projected = schema.project(&["email", "id"])?;
```

See [Column Selection](../reading/column_selection.md) for how this is used in scans.

## Fields

A `StructField` has a name, data type, nullability flag, and optional metadata:

```rust,ignore
// Non-nullable field
let id = StructField::not_null("id", DataType::LONG);

// Nullable field
let name = StructField::nullable("name", DataType::STRING);

// Field with explicit nullability
let score = StructField::new("score", DataType::DOUBLE, true);
```

### Field metadata

Fields can carry arbitrary key-value metadata:

```rust,ignore
let field = StructField::nullable("price", DataType::decimal(18, 2)?)
    .with_metadata([("description", "Unit price in USD")]);
```

Metadata is stored as a `HashMap<String, MetadataValue>`. The `MetadataValue` enum
supports strings, numbers (`i64`), booleans, and arbitrary JSON.

## Reading a table's schema

Every `Snapshot` exposes the table's schema:

```rust,no_run
# extern crate delta_kernel;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn main() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let engine = DefaultEngine::builder(store_from_url(&url)?).build();
let snapshot = Snapshot::builder_for(url).build(&engine)?;
let schema = snapshot.schema();

for field in schema.fields() {
    println!("{}: {:?}", field.name(), field.data_type());
}
# Ok(())
# }
```

## What's next

- [Column Selection](../reading/column_selection.md): projecting columns in a scan
- [Creating a Table](../writing/create_table.md): defining a schema for a new table
