use std::os::raw::c_void;

use delta_kernel::schema::{ArrayType, DataType, MapType, PrimitiveType, StructType};

use crate::handle::Handle;
use crate::scan::CStringMap;
use crate::{kernel_string_slice, KernelStringSlice, SharedSchema};

/// The `EngineSchemaVisitor` defines a visitor system to allow engines to build their own
/// representation of a schema from a particular schema within kernel.
///
/// The model is list based. When the kernel needs a list, it will ask engine to allocate one of a
/// particular size. Once allocated the engine returns an `id`, which can be any integer identifier
/// ([`usize`]) the engine wants, and will be passed back to the engine to identify the list in the
/// future.
///
/// Every schema element the kernel visits belongs to some list of "sibling" elements. The schema
/// itself is a list of schema elements, and every complex type (struct, map, array) contains a list
/// of "child" elements.
///  1. Before visiting schema or any complex type, the kernel asks the engine to allocate a list to
///     hold its children
///  2. When visiting any schema element, the kernel passes its parent's "child list" as the
///     "sibling list" the element should be appended to:
///      - For the top-level schema, visit each top-level column, passing the column's name and type
///      - For a struct, first visit each struct field, passing the field's name, type, nullability,
///        and metadata
///      - For a map, visit the key and value, passing its special name ("map_key" or "map_value"),
///        type, and value nullability (keys are never nullable)
///      - For a list, visit the element, passing its special name ("array_element"), type, and
///        nullability
///  3. When visiting a complex schema element, the kernel also passes the "child list" containing
///     that element's (already-visited) children.
///  4. The [`visit_schema`] method returns the id of the list of top-level columns
// WARNING: the visitor MUST NOT retain internal references to the string slices passed to visitor
// methods
#[repr(C)]
pub struct EngineSchemaVisitor {
    /// opaque state pointer
    pub data: *mut c_void,
    /// Creates a new field list, optionally reserving capacity up front
    pub make_field_list: extern "C" fn(data: *mut c_void, reserve: usize) -> usize,

    // visitor methods that should instantiate and append the appropriate type to the field list
    /// Indicate that the schema contains a `Struct` type. The top level of a Schema is always a
    /// `Struct`. The fields of the `Struct` are in the list identified by `child_list_id`.
    pub visit_struct: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
        child_list_id: usize,
    ),

    /// Indicate that the schema contains an Array type. `child_list_id` will be a _one_ item list
    /// with the array's element type
    pub visit_array: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
        child_list_id: usize,
    ),

    /// Indicate that the schema contains an Map type. `child_list_id` will be a _two_ item list
    /// where the first element is the map's key type and the second element is the
    /// map's value type
    pub visit_map: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
        child_list_id: usize,
    ),

    /// visit a `decimal` with the specified `precision` and `scale`
    pub visit_decimal: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
        precision: u8,
        scale: u8,
    ),

    /// Visit a `string` belonging to the list identified by `sibling_list_id`.
    pub visit_string: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit a `long` belonging to the list identified by `sibling_list_id`.
    pub visit_long: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit an `integer` belonging to the list identified by `sibling_list_id`.
    pub visit_integer: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit a `short` belonging to the list identified by `sibling_list_id`.
    pub visit_short: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit a `byte` belonging to the list identified by `sibling_list_id`.
    pub visit_byte: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit a `float` belonging to the list identified by `sibling_list_id`.
    pub visit_float: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit a `double` belonging to the list identified by `sibling_list_id`.
    pub visit_double: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit a `boolean` belonging to the list identified by `sibling_list_id`.
    pub visit_boolean: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit `binary` belonging to the list identified by `sibling_list_id`.
    pub visit_binary: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit a `date` belonging to the list identified by `sibling_list_id`.
    pub visit_date: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit a `timestamp` belonging to the list identified by `sibling_list_id`.
    pub visit_timestamp: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit a `timestamp` with no timezone belonging to the list identified by `sibling_list_id`.
    pub visit_timestamp_ntz: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit an `interval year to month` belonging to the list identified by `sibling_list_id`.
    pub visit_interval_year_month: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit an `interval day to second` belonging to the list identified by `sibling_list_id`.
    pub visit_interval_day_time: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit a `void` belonging to the list identified by `sibling_list_id`.
    pub visit_void: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),

    /// Visit a `variant` belonging to the list identified by `sibling_list_id`.
    pub visit_variant: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CStringMap,
    ),
}

/// Visit the given `schema` using the provided `visitor`. See the documentation of
/// [`EngineSchemaVisitor`] for a description of how this visitor works.
///
/// This method returns the id of the list allocated to hold the top level schema columns.
///
/// # Safety
///
/// Caller is responsible for passing a valid schema handle and schema visitor.
#[no_mangle]
pub unsafe extern "C" fn visit_schema(
    schema: Handle<SharedSchema>,
    visitor: &mut EngineSchemaVisitor,
) -> usize {
    let schema = unsafe { schema.as_ref() };
    visit_schema_impl(schema, visitor)
}

fn visit_schema_impl(schema: &StructType, visitor: &mut EngineSchemaVisitor) -> usize {
    // Visit all the fields of a struct and return the list of children
    fn visit_struct_fields(visitor: &EngineSchemaVisitor, s: &StructType) -> usize {
        let child_list_id = (visitor.make_field_list)(visitor.data, s.num_fields());
        for field in s.fields() {
            visit_schema_item(
                field.name(),
                field.data_type(),
                field.is_nullable(),
                &field.metadata_with_string_values().into(),
                visitor,
                child_list_id,
            );
        }
        child_list_id
    }

    fn visit_array_item(
        visitor: &EngineSchemaVisitor,
        at: &ArrayType,
        contains_null: bool,
    ) -> usize {
        let child_list_id = (visitor.make_field_list)(visitor.data, 1);
        let metadata = CStringMap::default();
        visit_schema_item(
            "array_element",
            &at.element_type,
            contains_null,
            &metadata,
            visitor,
            child_list_id,
        );
        child_list_id
    }

    fn visit_map_types(
        visitor: &EngineSchemaVisitor,
        mt: &MapType,
        value_contains_null: bool,
    ) -> usize {
        let child_list_id = (visitor.make_field_list)(visitor.data, 2);
        let metadata = CStringMap::default();
        visit_schema_item(
            "map_key",
            &mt.key_type,
            false,
            &metadata,
            visitor,
            child_list_id,
        );
        visit_schema_item(
            "map_value",
            &mt.value_type,
            value_contains_null,
            &metadata,
            visitor,
            child_list_id,
        );
        child_list_id
    }

    // Visit a struct field (recursively) and add the result to the list of siblings.
    fn visit_schema_item(
        name: &str,
        data_type: &DataType,
        is_nullable: bool,
        metadata: &CStringMap,
        visitor: &EngineSchemaVisitor,
        sibling_list_id: usize,
    ) {
        macro_rules! call {
            ( $visitor_fn:ident $(, $extra_args:expr) *) => {
                (visitor.$visitor_fn)(
                    visitor.data,
                    sibling_list_id,
                    kernel_string_slice!(name),
                    is_nullable,
                    metadata
                    $(, $extra_args) *
                )
            };
        }
        match data_type {
            DataType::Struct(st) => call!(visit_struct, visit_struct_fields(visitor, st)),
            DataType::Map(mt) => {
                call!(
                    visit_map,
                    visit_map_types(visitor, mt, mt.value_contains_null)
                )
            }
            DataType::Array(at) => {
                call!(visit_array, visit_array_item(visitor, at, at.contains_null))
            }
            DataType::Primitive(PrimitiveType::Decimal(d)) => {
                call!(visit_decimal, d.precision(), d.scale())
            }
            &DataType::Variant(_) => call!(visit_variant),
            &DataType::STRING => call!(visit_string),
            &DataType::LONG => call!(visit_long),
            &DataType::INTEGER => call!(visit_integer),
            &DataType::SHORT => call!(visit_short),
            &DataType::BYTE => call!(visit_byte),
            &DataType::FLOAT => call!(visit_float),
            &DataType::DOUBLE => call!(visit_double),
            &DataType::BOOLEAN => call!(visit_boolean),
            &DataType::BINARY => call!(visit_binary),
            &DataType::DATE => call!(visit_date),
            &DataType::TIMESTAMP => call!(visit_timestamp),
            &DataType::TIMESTAMP_NTZ => call!(visit_timestamp_ntz),
            &DataType::INTERVAL_YEAR_MONTH => call!(visit_interval_year_month),
            &DataType::INTERVAL_DAY_TIME => call!(visit_interval_day_time),
            &DataType::VOID => call!(visit_void),
            #[cfg(feature = "geo-type-in-dev")]
            DataType::Primitive(PrimitiveType::Geometry(_))
            | DataType::Primitive(PrimitiveType::Geography(_)) => {
                // TODO(#2949): add visit_geometry / visit_geography callbacks carrying the CRS;
                // skipping silently drops the column
                tracing::warn!("Skipping unsupported geo field '{name}' in FFI schema visit");
            }
        }
    }

    visit_struct_fields(visitor, schema)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use delta_kernel::schema::{ArrayType, StructField};

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct VisitedField {
        name: String,
        data_type: &'static str,
        is_nullable: bool,
        children: Option<usize>,
    }

    #[derive(Default)]
    struct TestSchemaBuilder {
        lists: Vec<Vec<VisitedField>>,
    }

    extern "C" fn make_field_list(data: *mut c_void, reserve: usize) -> usize {
        let builder = unsafe { &mut *(data as *mut TestSchemaBuilder) };
        let list_id = builder.lists.len();
        builder.lists.push(Vec::with_capacity(reserve));
        list_id
    }

    fn field_name(name: KernelStringSlice) -> String {
        unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                name.ptr as *const u8,
                name.len,
            ))
            .to_string()
        }
    }

    fn add_field(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        data_type: &'static str,
        children: Option<usize>,
    ) {
        let builder = unsafe { &mut *(data as *mut TestSchemaBuilder) };
        builder.lists[sibling_list_id].push(VisitedField {
            name: field_name(name),
            data_type,
            is_nullable,
            children,
        });
    }

    extern "C" fn visit_struct(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        _metadata: &CStringMap,
        child_list_id: usize,
    ) {
        add_field(
            data,
            sibling_list_id,
            name,
            is_nullable,
            "struct",
            Some(child_list_id),
        );
    }

    extern "C" fn visit_array(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        _metadata: &CStringMap,
        child_list_id: usize,
    ) {
        add_field(
            data,
            sibling_list_id,
            name,
            is_nullable,
            "array",
            Some(child_list_id),
        );
    }

    extern "C" fn visit_map(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        _metadata: &CStringMap,
        child_list_id: usize,
    ) {
        add_field(
            data,
            sibling_list_id,
            name,
            is_nullable,
            "map",
            Some(child_list_id),
        );
    }

    extern "C" fn visit_decimal(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        _metadata: &CStringMap,
        _precision: u8,
        _scale: u8,
    ) {
        add_field(data, sibling_list_id, name, is_nullable, "decimal", None);
    }

    macro_rules! visit_simple_type {
        ($fn_name:ident, $type_name:expr) => {
            extern "C" fn $fn_name(
                data: *mut c_void,
                sibling_list_id: usize,
                name: KernelStringSlice,
                is_nullable: bool,
                _metadata: &CStringMap,
            ) {
                add_field(data, sibling_list_id, name, is_nullable, $type_name, None);
            }
        };
    }

    visit_simple_type!(visit_string, "string");
    visit_simple_type!(visit_long, "long");
    visit_simple_type!(visit_integer, "integer");
    visit_simple_type!(visit_short, "short");
    visit_simple_type!(visit_byte, "byte");
    visit_simple_type!(visit_float, "float");
    visit_simple_type!(visit_double, "double");
    visit_simple_type!(visit_boolean, "boolean");
    visit_simple_type!(visit_binary, "binary");
    visit_simple_type!(visit_date, "date");
    visit_simple_type!(visit_timestamp, "timestamp");
    visit_simple_type!(visit_timestamp_ntz, "timestamp_ntz");
    visit_simple_type!(visit_interval_year_month, "interval year to month");
    visit_simple_type!(visit_interval_day_time, "interval day to second");
    visit_simple_type!(visit_void, "void");
    visit_simple_type!(visit_variant, "variant");

    fn test_visitor(builder: &mut TestSchemaBuilder) -> EngineSchemaVisitor {
        EngineSchemaVisitor {
            data: builder as *mut _ as *mut c_void,
            make_field_list,
            visit_struct,
            visit_array,
            visit_map,
            visit_decimal,
            visit_string,
            visit_long,
            visit_integer,
            visit_short,
            visit_byte,
            visit_float,
            visit_double,
            visit_boolean,
            visit_binary,
            visit_date,
            visit_timestamp,
            visit_timestamp_ntz,
            visit_interval_year_month,
            visit_interval_day_time,
            visit_void,
            visit_variant,
        }
    }

    #[test]
    fn visit_schema_preserves_interval_fields() {
        let schema = StructType::try_new(vec![
            StructField::nullable("ym", DataType::INTERVAL_YEAR_MONTH),
            StructField::not_null("dt", DataType::INTERVAL_DAY_TIME),
            StructField::nullable(
                "nested",
                StructType::try_new(vec![StructField::nullable(
                    "inner_ym",
                    DataType::INTERVAL_YEAR_MONTH,
                )])
                .unwrap(),
            ),
            StructField::nullable(
                "intervals",
                ArrayType::new(DataType::INTERVAL_DAY_TIME, false),
            ),
        ])
        .unwrap();

        let mut builder = TestSchemaBuilder::default();
        let mut visitor = test_visitor(&mut builder);
        let top_level_id = visit_schema_impl(&schema, &mut visitor);

        assert_eq!(top_level_id, 0);
        assert_eq!(builder.lists[0].len(), 4);
        assert_eq!(
            builder.lists[0][0],
            VisitedField {
                name: "ym".to_string(),
                data_type: "interval year to month",
                is_nullable: true,
                children: None,
            }
        );
        assert_eq!(
            builder.lists[0][1],
            VisitedField {
                name: "dt".to_string(),
                data_type: "interval day to second",
                is_nullable: false,
                children: None,
            }
        );

        let nested_child_list_id = builder.lists[0][2].children.unwrap();
        assert_eq!(
            builder.lists[nested_child_list_id][0],
            VisitedField {
                name: "inner_ym".to_string(),
                data_type: "interval year to month",
                is_nullable: true,
                children: None,
            }
        );

        let array_child_list_id = builder.lists[0][3].children.unwrap();
        assert_eq!(
            builder.lists[array_child_list_id][0],
            VisitedField {
                name: "array_element".to_string(),
                data_type: "interval day to second",
                is_nullable: false,
                children: None,
            }
        );
    }
}
