use std::os::raw::c_void;

use delta_kernel::schema::{ArrayType, DataType, MapType, PrimitiveType, StructType};

use crate::handle::Handle;
use crate::scan::CMetadataMap;
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
        metadata: &CMetadataMap,
        child_list_id: usize,
    ),

    /// Indicate that the schema contains an Array type. `child_list_id` will be a _one_ item list
    /// with the array's element type
    pub visit_array: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
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
        metadata: &CMetadataMap,
        child_list_id: usize,
    ),

    /// visit a `decimal` with the specified `precision` and `scale`
    pub visit_decimal: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
        precision: u8,
        scale: u8,
    ),

    /// Visit a `string` belonging to the list identified by `sibling_list_id`.
    pub visit_string: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `long` belonging to the list identified by `sibling_list_id`.
    pub visit_long: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit an `integer` belonging to the list identified by `sibling_list_id`.
    pub visit_integer: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `short` belonging to the list identified by `sibling_list_id`.
    pub visit_short: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `byte` belonging to the list identified by `sibling_list_id`.
    pub visit_byte: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `float` belonging to the list identified by `sibling_list_id`.
    pub visit_float: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `double` belonging to the list identified by `sibling_list_id`.
    pub visit_double: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `boolean` belonging to the list identified by `sibling_list_id`.
    pub visit_boolean: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit `binary` belonging to the list identified by `sibling_list_id`.
    pub visit_binary: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `date` belonging to the list identified by `sibling_list_id`.
    pub visit_date: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `timestamp` belonging to the list identified by `sibling_list_id`.
    pub visit_timestamp: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `timestamp` with no timezone belonging to the list identified by `sibling_list_id`.
    pub visit_timestamp_ntz: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit an `interval year to month` belonging to the list identified by `sibling_list_id`.
    pub visit_interval_year_month: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit an `interval day to second` belonging to the list identified by `sibling_list_id`.
    pub visit_interval_day_time: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `void` belonging to the list identified by `sibling_list_id`.
    pub visit_void: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `variant` belonging to the list identified by `sibling_list_id`.
    pub visit_variant: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
    ),

    /// Visit a `geometry` belonging to the list identified by `sibling_list_id`.
    ///
    /// `crs` is the coordinate reference system string for the geometry type.
    pub visit_geometry: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
        crs: KernelStringSlice,
    ),

    /// Visit a `geography` belonging to the list identified by `sibling_list_id`.
    ///
    /// `crs` is the coordinate reference system string for the geography type. `algorithm` is the
    /// lowercase Delta protocol token for edge interpolation: `spherical`, `vincenty`, `thomas`,
    /// `andoyer`, or `karney`.
    pub visit_geography: extern "C" fn(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        metadata: &CMetadataMap,
        crs: KernelStringSlice,
        algorithm: KernelStringSlice,
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
                &field.metadata().clone().into(),
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
        let metadata = CMetadataMap::default();
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
        let metadata = CMetadataMap::default();
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
        metadata: &CMetadataMap,
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
            DataType::Primitive(PrimitiveType::Geometry(geometry)) => {
                let crs = geometry.crs();
                call!(visit_geometry, kernel_string_slice!(crs))
            }
            #[cfg(feature = "geo-type-in-dev")]
            DataType::Primitive(PrimitiveType::Geography(geography)) => {
                let crs = geography.crs();
                let algorithm = geography.algorithm().to_string();
                call!(
                    visit_geography,
                    kernel_string_slice!(crs),
                    kernel_string_slice!(algorithm)
                )
            }
        }
    }

    visit_struct_fields(visitor, schema)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use delta_kernel::schema::schema;
    #[cfg(feature = "geo-type-in-dev")]
    use delta_kernel::schema::{
        EdgeInterpolationAlgorithm, GeographyType, GeometryType, StructField,
    };

    use super::*;
    use crate::TryFromStringSlice;

    #[derive(Debug, PartialEq, Eq)]
    struct VisitedField {
        name: String,
        data_type: &'static str,
        is_nullable: bool,
        children: Option<usize>,
        geo: Option<VisitedGeoType>,
    }

    impl VisitedField {
        fn new(
            name: &str,
            data_type: &'static str,
            is_nullable: bool,
            children: Option<usize>,
        ) -> Self {
            Self {
                name: name.to_string(),
                data_type,
                is_nullable,
                children,
                geo: None,
            }
        }

        fn geo(
            name: &str,
            data_type: &'static str,
            is_nullable: bool,
            crs: &str,
            algorithm: Option<&str>,
        ) -> Self {
            Self {
                name: name.to_string(),
                data_type,
                is_nullable,
                children: None,
                geo: Some(VisitedGeoType {
                    crs: crs.to_string(),
                    algorithm: algorithm.map(str::to_string),
                }),
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct VisitedGeoType {
        crs: String,
        algorithm: Option<String>,
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

    fn add_field(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        data_type: &'static str,
        children: Option<usize>,
    ) {
        let builder = unsafe { &mut *(data as *mut TestSchemaBuilder) };
        let name = unsafe { String::try_from_slice(&name) }.unwrap();
        builder.lists[sibling_list_id].push(VisitedField::new(
            &name,
            data_type,
            is_nullable,
            children,
        ));
    }

    macro_rules! visit_nested_type {
        ($fn_name:ident, $type_name:expr) => {
            extern "C" fn $fn_name(
                data: *mut c_void,
                sibling_list_id: usize,
                name: KernelStringSlice,
                is_nullable: bool,
                _metadata: &CMetadataMap,
                child_list_id: usize,
            ) {
                add_field(
                    data,
                    sibling_list_id,
                    name,
                    is_nullable,
                    $type_name,
                    Some(child_list_id),
                );
            }
        };
    }

    visit_nested_type!(visit_struct, "struct");
    visit_nested_type!(visit_array, "array");
    visit_nested_type!(visit_map, "map");

    extern "C" fn visit_decimal(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        _metadata: &CMetadataMap,
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
                _metadata: &CMetadataMap,
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

    extern "C" fn visit_geometry(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        _metadata: &CMetadataMap,
        crs: KernelStringSlice,
    ) {
        let builder = unsafe { &mut *(data as *mut TestSchemaBuilder) };
        let name = unsafe { String::try_from_slice(&name) }.unwrap();
        let crs = unsafe { String::try_from_slice(&crs) }.unwrap();
        builder.lists[sibling_list_id].push(VisitedField::geo(
            &name,
            "geometry",
            is_nullable,
            &crs,
            None,
        ));
    }

    extern "C" fn visit_geography(
        data: *mut c_void,
        sibling_list_id: usize,
        name: KernelStringSlice,
        is_nullable: bool,
        _metadata: &CMetadataMap,
        crs: KernelStringSlice,
        algorithm: KernelStringSlice,
    ) {
        let builder = unsafe { &mut *(data as *mut TestSchemaBuilder) };
        let name = unsafe { String::try_from_slice(&name) }.unwrap();
        let crs = unsafe { String::try_from_slice(&crs) }.unwrap();
        let algorithm = unsafe { String::try_from_slice(&algorithm) }.unwrap();
        builder.lists[sibling_list_id].push(VisitedField::geo(
            &name,
            "geography",
            is_nullable,
            &crs,
            Some(&algorithm),
        ));
    }

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
            visit_geometry,
            visit_geography,
        }
    }

    #[test]
    fn visit_schema_preserves_interval_fields() {
        let schema = schema! {
            nullable "ym": INTERVAL_YEAR_MONTH,
            not_null "dt": INTERVAL_DAY_TIME,
            nullable "nested": {
                nullable "inner_ym": INTERVAL_YEAR_MONTH,
            },
            nullable "intervals": [ not_null INTERVAL_DAY_TIME ],
        };

        let mut builder = TestSchemaBuilder::default();
        let mut visitor = test_visitor(&mut builder);
        let top_level_id = visit_schema_impl(&schema, &mut visitor);

        assert_eq!(top_level_id, 0);
        assert_eq!(builder.lists[0].len(), 4);
        assert_eq!(
            builder.lists[0][0],
            VisitedField::new("ym", "interval year to month", true, None)
        );
        assert_eq!(
            builder.lists[0][1],
            VisitedField::new("dt", "interval day to second", false, None)
        );

        let nested_child_list_id = builder.lists[0][2].children.unwrap();
        assert_eq!(
            builder.lists[nested_child_list_id][0],
            VisitedField::new("inner_ym", "interval year to month", true, None)
        );

        let array_child_list_id = builder.lists[0][3].children.unwrap();
        assert_eq!(
            builder.lists[array_child_list_id][0],
            VisitedField::new("array_element", "interval day to second", false, None)
        );
    }

    #[cfg(feature = "geo-type-in-dev")]
    #[test]
    fn visit_schema_preserves_geo_fields() {
        let schema = StructType::try_new(vec![
            StructField::nullable("geom", GeometryType::try_new("OGC:CRS84").unwrap()),
            StructField::not_null(
                "geog",
                GeographyType::try_new("OGC:CRS84", EdgeInterpolationAlgorithm::Spherical).unwrap(),
            ),
        ])
        .unwrap();

        let mut builder = TestSchemaBuilder::default();
        let mut visitor = test_visitor(&mut builder);
        let top_level_id = visit_schema_impl(&schema, &mut visitor);

        assert_eq!(top_level_id, 0);
        assert_eq!(builder.lists[0].len(), 2);
        assert_eq!(
            builder.lists[0][0],
            VisitedField::geo("geom", "geometry", true, "OGC:CRS84", None)
        );
        assert_eq!(
            builder.lists[0][1],
            VisitedField::geo("geog", "geography", false, "OGC:CRS84", Some("spherical"))
        );
    }

    #[cfg(feature = "geo-type-in-dev")]
    #[rstest::rstest]
    #[case(EdgeInterpolationAlgorithm::Spherical, "spherical")]
    #[case(EdgeInterpolationAlgorithm::Vincenty, "vincenty")]
    #[case(EdgeInterpolationAlgorithm::Thomas, "thomas")]
    #[case(EdgeInterpolationAlgorithm::Andoyer, "andoyer")]
    #[case(EdgeInterpolationAlgorithm::Karney, "karney")]
    fn visit_schema_preserves_geography_algorithm_protocol_token(
        #[case] algorithm: EdgeInterpolationAlgorithm,
        #[case] expected: &str,
    ) {
        let schema = StructType::try_new(vec![StructField::not_null(
            "geog",
            GeographyType::try_new("OGC:CRS84", algorithm).unwrap(),
        )])
        .unwrap();

        let mut builder = TestSchemaBuilder::default();
        let mut visitor = test_visitor(&mut builder);
        let top_level_id = visit_schema_impl(&schema, &mut visitor);

        assert_eq!(top_level_id, 0);
        assert_eq!(builder.lists[0].len(), 1);
        assert_eq!(
            builder.lists[0][0]
                .geo
                .as_ref()
                .and_then(|geo| geo.algorithm.as_deref()),
            Some(expected)
        );
    }
}
