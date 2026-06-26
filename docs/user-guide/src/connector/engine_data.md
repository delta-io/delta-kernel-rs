# The EngineData trait

The `EngineData` trait is the kernel's interface for columnar data. Any data that flows between the
kernel and your connector is represented as `EngineData`.

If you use the `DefaultEngine`, you get `ArrowEngineData`, a wrapper around an arrow `RecordBatch`
that implements the `EngineData` trait, and don't need to implement this trait. This page is for
connector builders who want to use a different columnar data format.

## The trait

```rust,ignore
pub trait EngineData: AsAny {
    fn visit_rows(
        &self,
        column_names: &[ColumnName],
        visitor: &mut dyn RowVisitor,
    ) -> DeltaResult<()>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn append_columns(
        &self,
        schema: SchemaRef,
        columns: Vec<ArrayData>,
    ) -> DeltaResult<Box<dyn EngineData>>;

    fn apply_selection_vector(
        self: Box<Self>,
        selection_vector: Vec<bool>,
    ) -> DeltaResult<Box<dyn EngineData>>;

    fn has_field(&self, name: &ColumnName) -> bool;
}
```

The five required methods to implement are `visit_rows`, `len`, `append_columns`,
`apply_selection_vector`, and `has_field`. The `is_empty` method has a default
implementation that delegates to `len`.

`has_field` returns `true` if a field at the given (possibly nested) path exists in the
data's schema. For a top-level field named `"foo"`, pass `ColumnName::new(["foo"])`. For
nested fields, each non-leaf element of the path must be a struct field at that level.

## visit_rows and the visitor pattern

Kernel uses a visitor pattern to access the actual data that's inside an `EngineData`. This pattern
means the connector can call into kernel with a reference to the data, which makes reasoning about
data lifetimes simpler. In particular, engines don't need to worry about keeping data alive past the
invocation of the visitor.

`visit_rows` is the core data extraction method. The kernel never inspects your columns
directly. Instead, it passes a `RowVisitor` that knows which columns it needs, and your
implementation provides typed accessors (`GetData`) for those columns.

The flow:

```text
Kernel                              Your EngineData
------                              ---------------
"I need columns [path, size]"
    visit_rows(column_names, visitor)
                                    1. Look up the requested columns
                                    2. Create a GetData accessor per column
                                    3. Call visitor.visit(row_count, &getters)

Kernel (inside the visitor)
    for row in 0..row_count:
        path = getters[0].get_str(row, "path")?
        size = getters[1].get_long(row, "size")?
```

### GetData

`GetData` is the typed accessor trait. Each accessor handles one column. The kernel calls
the appropriate typed method based on the column's data type:

| Method | Return type | Delta type |
|--------|-------------|------------|
| `get_bool(row, name)` | `bool` | BOOLEAN |
| `get_byte(row, name)` | `i8` | BYTE |
| `get_short(row, name)` | `i16` | SHORT |
| `get_int(row, name)` | `i32` | INTEGER |
| `get_long(row, name)` | `i64` | LONG |
| `get_float(row, name)` | `f32` | FLOAT |
| `get_double(row, name)` | `f64` | DOUBLE |
| `get_date(row, name)` | `i32` | DATE |
| `get_timestamp(row, name)` | `i64` | TIMESTAMP |
| `get_decimal(row, name)` | `i128` | DECIMAL |
| `get_str(row, name)` | `&str` | STRING |
| `get_binary(row, name)` | `&[u8]` | BINARY |
| `get_list(row, name)` | `ListItem` | ARRAY (of strings) |
| `get_map(row, name)` | `MapItem` | MAP (string keys and values) |

All methods return `DeltaResult<Option<T>>`. A `None` value means the field is null. By
default, every method returns an "unexpected type" error, so you only need to implement the
one that matches your column's type.

`ListItem` provides access to a row's list of strings. Call `get(index)` to retrieve a
single element, or `materialize()` to collect all elements into a `Vec<String>`.

`MapItem` provides access to a row's string-to-string map. Call `get(key)` to look up a
value by key, or `materialize()` to collect all entries into a `HashMap<String, String>`.
If a value is null, `get` returns `None` and `materialize` drops that entry.

Not every possible data-type is covered (i.e. no `Map<Int, Int>`). The trait only covers the data
types the kernel needs to fuction, and no more.

### TypedGetData

The `TypedGetData` trait provides a convenience wrapper which is automatically implemented for most
types that implement `GetData`. Instead of calling the specific `get_*` method for each type, you
can write generic code that dispatches based on the Rust type:

```rust,ignore
// Without TypedGetData: explicit method per type
let path: Option<&str> = getters[0].get_str(row, "path")?;
let size: Option<i64> = getters[1].get_long(row, "size")?;

// With TypedGetData: type-driven dispatch
let path: Option<String> = getters[0].get_opt(row, "path")?;
let size: Option<i64> = getters[1].get_opt(row, "size")?;
```

`TypedGetData` also provides a `get` method that returns `DeltaResult<T>` (without
`Option`), returning an error if the value is null.

### RowVisitor

Engines don't need to implement `RowVisitor`. The kernel provides its own visitors. Your
`visit_rows` implementation needs construct the correct getters array and then call
`visitor.visit(row_count, &getters)` once.

The visitor declares which columns and types it expects via `selected_column_names_and_types()`,
which returns `(&'static [ColumnName], &'static [DataType])`. Your implementation should validate
that the requested columns exist and have compatible types before creating getters.

`RowVisitor` also provides a convenience method `visit_rows_of(&mut self, data: &dyn EngineData)`.
This calls `data.visit_rows(...)` with the column names from
`selected_column_names_and_types()`, saving you from extracting them manually.

## append_columns

The kernel calls `append_columns` to add new columns to your data. This happens when committing to
the table, as the kernel needs to build up the commit log entries. For example, row-tracking
information is added to the data that needs to be written into the delta log this way.

```rust,ignore
fn append_columns(
    &self,
    schema: SchemaRef,
    columns: Vec<ArrayData>,
) -> DeltaResult<Box<dyn EngineData>>;
```

- **`schema`** describes only the new columns being appended (not the full result schema)
- **`columns`** contains the data as `ArrayData`, the kernel's generic columnar
  representation that you will need to convert to your engine's format.
- Returns a new `EngineData` with the original columns plus the appended columns
- The row count of the new columns must match the existing data

## apply_selection_vector

The kernel calls `apply_selection_vector` to filter rows. This is used for
deletion vector support and other row-level filtering.

```rust,ignore
fn apply_selection_vector(
    self: Box<Self>,
    selection_vector: Vec<bool>,
) -> DeltaResult<Box<dyn EngineData>>;
```

- Within the selection_vector, `true` means keep the row, `false` means remove it
- If the selection vector is shorter than the data, remaining rows are kept
- This consumes the `EngineData` (`self: Box<Self>`), so you can implement this in-place if your
  format supports it

## FilteredEngineData

The kernel pairs `EngineData` with a selection vector in `FilteredEngineData`. This is just a
convenience type since the two appear together in many situations. For example, this is used in
write paths (e.g., `JsonHandler::write_json_file` receives an iterator of `FilteredEngineData`).

Construct a `FilteredEngineData` with `try_new`, which validates that the selection vector
is not longer than the data:

```rust,ignore
let filtered = FilteredEngineData::try_new(data, selection_vector)?;
```

You can also convert a `Box<dyn EngineData>` directly with `.into()`, which selects all
rows (equivalent to an empty selection vector):

```rust,ignore
let filtered: FilteredEngineData = engine_data.into();
```

Key methods:

| Method | Purpose |
|--------|---------|
| `try_new(data, selection_vector)` | Construct with validation (errors if vector is longer than data) |
| `with_all_rows_selected(data)` | Wrap data with an empty selection vector (all rows kept) |
| `data()` | Access the underlying `EngineData` |
| `selection_vector()` | Get the boolean selection vector as `&[bool]` |
| `apply_selection_vector()` | Applies the filter, removing unselected rows and consuming self |
| `into_parts()` | Decompose into `(Box<dyn EngineData>, Vec<bool>)`. |

> [!WARNING]
> If you call `into_parts()`, you MUST keep the returned parts paired. That is, the `Box<dyn
> EngineData>` _is not_ valid for every row, only for those rows as desribed below.

In the selection vector, `true` means keep the row, `false` meands remove it. If the selection
vector is shorter than the data, uncovered rows are implicitly selected.  If the selection vector is
longer than the data, `try_new` returns an error.

### FilteredRowVisitor

The `FilteredRowVisitor` trait processes `FilteredEngineData` with automatic row filtering.
Instead of receiving a raw row count, your visitor gets a `RowIndexIterator` that yields
only the indices of selected rows:

```rust,ignore
pub trait FilteredRowVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]);

    fn visit_filtered<'a>(
        &mut self,
        getters: &[&'a dyn GetData<'a>],
        rows: RowIndexIterator<'_>,
    ) -> DeltaResult<()>;

    fn visit_rows_of(&mut self, data: &FilteredEngineData) -> DeltaResult<()>;
}
```

The default `visit_rows_of` method handles all the plumbing: extracting the selection
vector, building the bridge to `RowVisitor`, and calling `visit_rows`. Your implementation
only needs to provide `selected_column_names_and_types` and `visit_filtered`.

Call `rows.num_rows()` inside `visit_filtered` to get the total row count (including
deselected rows), which is useful for sizing output vectors.

## ArrowEngineData (the default)

The `DefaultEngine` uses `ArrowEngineData`, which wraps an Arrow `RecordBatch`:

```rust,ignore
use delta_kernel::engine::arrow_data::ArrowEngineData;

// Wrap a RecordBatch
let engine_data = ArrowEngineData::new(record_batch);

// Access the underlying RecordBatch by reference
let batch_ref = engine_data.record_batch();

// Convert back from a Box<dyn EngineData> (requires the arrow-related default features)
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
let batch = engine_data_box.try_into_record_batch()?;
```

`ArrowEngineData` also provides `try_from_engine_data(engine_data)` to downcast a
`Box<dyn EngineData>` to `Box<ArrowEngineData>`, returning an error if the underlying type
is not `ArrowEngineData`.

If you use the default engine, you work with `ArrowEngineData` and never need to implement
`EngineData` yourself. `EngineDataArrowExt` provides `try_into_record_batch()` for
converting the opaque `EngineData` trait object back to an Arrow `RecordBatch` for your
connector's use. This trait is implemented for both `Box<dyn EngineData>` and
`DeltaResult<Box<dyn EngineData>>`, so you can call it directly on scan results.

## What's next

- [Implementing the Engine trait](./implementing_engine.md) covers the four handler traits
  your engine must provide
- [Building a Scan](../reading/building_a_scan.md) shows how scan results flow through
  `EngineData`
