//! Helpers to ensure that kernel data types match arrow data types

use std::collections::{HashMap, HashSet};
use std::ops::Deref;

use delta_kernel_derive::internal_api;
use itertools::Itertools;

use super::arrow_conversion::TryIntoArrow as _;
use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};
use crate::engine::arrow_utils::make_arrow_error;
use crate::schema::{DataType, MetadataValue, StructField};
use crate::utils::require;
use crate::{DeltaResult, Error};

/// Controls how `ensure_data_types` validates struct fields and metadata.
#[derive(Clone, Copy)]
#[internal_api]
pub(crate) enum ValidationMode {
    /// Check types only. Struct fields are matched by ordinal position, not by name.
    /// Nullability and metadata are not checked.
    #[allow(dead_code)]
    TypesOnly,
    /// Check types and match struct fields by name, but skip nullability and metadata.
    /// Used by the parquet reader where fields are already resolved by name upstream.
    TypesAndNames,
    /// Check types, names, nullability, and metadata.
    Full,
}

/// Ensure a kernel data type matches an arrow data type. This only ensures that the actual "type"
/// is the same, but does so recursively into structs, and ensures lists and maps have the correct
/// associated types as well.
///
/// The `mode` parameter controls how struct fields are matched and whether nullability/metadata
/// are checked. See [`ValidationMode`] for details.
///
/// This returns an `Ok(DataTypeCompat)` if the types are compatible, and
/// will indicate what kind of compatibility they have, or an error if the types do not match. If
/// there is a `struct` type included and the mode uses name-based matching, we only ensure that
/// the named fields that the kernel is asking for exist, and that for those fields the types
/// match. Un-selected fields are ignored.
#[internal_api]
pub(crate) fn ensure_data_types(
    kernel_type: &DataType,
    arrow_type: &ArrowDataType,
    mode: ValidationMode,
) -> DeltaResult<DataTypeCompat> {
    let check = EnsureDataTypes { mode };
    check.ensure_data_types(kernel_type, arrow_type)
}

struct EnsureDataTypes {
    mode: ValidationMode,
}

/// Capture the compatibility between two data-types, as passed to [`ensure_data_types`]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[internal_api]
pub(crate) enum DataTypeCompat {
    /// The two types are the same
    Identical,
    /// What is read from parquet needs to be cast to the associated type
    NeedsCast(ArrowDataType),
    /// Types are compatible, but are nested types. This is used when comparing types where casting
    /// is not desired (i.e. in the expression evaluator)
    Nested,
}

impl EnsureDataTypes {
    // Perform the check. See documentation for `ensure_data_types` entry point method above
    fn ensure_data_types(
        &self,
        kernel_type: &DataType,
        arrow_type: &ArrowDataType,
    ) -> DeltaResult<DataTypeCompat> {
        match (kernel_type, arrow_type) {
            (DataType::Primitive(_), _) if arrow_type.is_primitive() => {
                check_cast_compat(kernel_type.try_into_arrow()?, arrow_type)
            }
            (&DataType::Variant(_), _) => {
                check_cast_compat(kernel_type.try_into_arrow()?, arrow_type)
            }
            // strings, bools, binary, and void aren't primitive in arrow
            (&DataType::BOOLEAN, ArrowDataType::Boolean)
            | (&DataType::STRING, ArrowDataType::LargeUtf8)
            | (&DataType::STRING, ArrowDataType::Utf8)
            | (&DataType::STRING, ArrowDataType::Utf8View)
            | (&DataType::BINARY, ArrowDataType::LargeBinary)
            | (&DataType::BINARY, ArrowDataType::BinaryView)
            | (&DataType::BINARY, ArrowDataType::Binary)
            | (&DataType::VOID, ArrowDataType::Null) => Ok(DataTypeCompat::Identical),
            (DataType::Array(inner_type), ArrowDataType::List(arrow_list_field))
            | (DataType::Array(inner_type), ArrowDataType::LargeList(arrow_list_field))
            | (DataType::Array(inner_type), ArrowDataType::ListView(arrow_list_field))
            | (DataType::Array(inner_type), ArrowDataType::LargeListView(arrow_list_field)) => {
                self.ensure_nullability(
                    "List",
                    inner_type.contains_null,
                    arrow_list_field.is_nullable(),
                )?;
                self.ensure_data_types(&inner_type.element_type, arrow_list_field.data_type())
            }
            (DataType::Map(kernel_map_type), ArrowDataType::Map(arrow_map_type, _)) => {
                let ArrowDataType::Struct(fields) = arrow_map_type.data_type() else {
                    return Err(make_arrow_error("Arrow map type wasn't a struct."));
                };
                let [key_type, value_type] = fields.deref() else {
                    return Err(make_arrow_error(
                        "Arrow map type didn't have expected key/value fields",
                    ));
                };
                self.ensure_data_types(&kernel_map_type.key_type, key_type.data_type())?;
                self.ensure_nullability(
                    "Map",
                    kernel_map_type.value_contains_null,
                    value_type.is_nullable(),
                )?;
                self.ensure_data_types(&kernel_map_type.value_type, value_type.data_type())?;
                Ok(DataTypeCompat::Nested)
            }
            (DataType::Struct(kernel_fields), ArrowDataType::Struct(arrow_fields)) => {
                match self.mode {
                    ValidationMode::TypesOnly => {
                        // Ordinal matching: check field count and types by position,
                        // ignore names. Column mapping can cause name mismatches between
                        // physical (arrow) and logical (kernel) field names.
                        require!(kernel_fields.num_fields() == arrow_fields.len(), {
                            make_arrow_error(format!(
                                "Struct field count mismatch: expected {}, got {}",
                                kernel_fields.num_fields(),
                                arrow_fields.len()
                            ))
                        });
                        for (kernel_field, arrow_field) in
                            kernel_fields.fields().zip(arrow_fields.iter())
                        {
                            self.ensure_data_types(
                                &kernel_field.data_type,
                                arrow_field.data_type(),
                            )?;
                        }
                    }
                    ValidationMode::TypesAndNames | ValidationMode::Full => {
                        // Name-based matching: look up kernel fields by arrow field name.
                        // Full mode additionally checks nullability and metadata.
                        let mapped_fields = arrow_fields
                            .iter()
                            .filter_map(|f| kernel_fields.field(f.name()));

                        let mut found_fields = 0;
                        for (kernel_field, arrow_field) in mapped_fields.zip(arrow_fields) {
                            self.ensure_nullability_and_metadata(kernel_field, arrow_field)?;
                            self.ensure_data_types(
                                &kernel_field.data_type,
                                arrow_field.data_type(),
                            )?;
                            found_fields += 1;
                        }

                        require!(kernel_fields.num_fields() == found_fields, {
                            let arrow_field_map: HashSet<&String> =
                                HashSet::from_iter(arrow_fields.iter().map(|f| f.name()));
                            let missing_field_names = kernel_fields
                                .field_names()
                                .filter(|kernel_field| !arrow_field_map.contains(kernel_field))
                                .take(5)
                                .join(", ");
                            make_arrow_error(format!(
                                "Missing Struct fields {missing_field_names} \
                                 (Up to five missing fields shown)"
                            ))
                        });
                    }
                }
                Ok(DataTypeCompat::Nested)
            }
            _ => Err(make_arrow_error(format!(
                "Incorrect datatype. Expected {kernel_type}, got {arrow_type}"
            ))),
        }
    }

    fn ensure_nullability(
        &self,
        desc: &str,
        kernel_field_is_nullable: bool,
        arrow_field_is_nullable: bool,
    ) -> DeltaResult<()> {
        if matches!(self.mode, ValidationMode::Full)
            && kernel_field_is_nullable != arrow_field_is_nullable
        {
            Err(Error::Generic(format!(
                "{desc} has nullablily {kernel_field_is_nullable} in kernel and {arrow_field_is_nullable} in arrow",
            )))
        } else {
            Ok(())
        }
    }

    fn ensure_nullability_and_metadata(
        &self,
        kernel_field: &StructField,
        arrow_field: &ArrowField,
    ) -> DeltaResult<()> {
        self.ensure_nullability(
            &kernel_field.name,
            kernel_field.nullable,
            arrow_field.is_nullable(),
        )?;
        if matches!(self.mode, ValidationMode::Full)
            && !metadata_eq(&kernel_field.metadata, arrow_field.metadata())
        {
            Err(Error::Generic(format!(
                "Field {} has metadata {:?} in kernel and {:?} in arrow",
                kernel_field.name,
                kernel_field.metadata,
                arrow_field.metadata(),
            )))
        } else {
            Ok(())
        }
    }
}

// Check if two types can be cast
fn check_cast_compat(
    target_type: ArrowDataType,
    source_type: &ArrowDataType,
) -> DeltaResult<DataTypeCompat> {
    use ArrowDataType::*;

    match (source_type, &target_type) {
        (source_type, target_type) if source_type == target_type => Ok(DataTypeCompat::Identical),
        (&ArrowDataType::Timestamp(_, _), &ArrowDataType::Timestamp(_, _)) => {
            // timestamps are able to be cast between each other
            Ok(DataTypeCompat::NeedsCast(target_type))
        }
        // Allow up-casting to a larger type if it's safe and can't cause overflow or loss of
        // precision.
        (Int8, Int16 | Int32 | Int64 | Float64) => Ok(DataTypeCompat::NeedsCast(target_type)),
        (Int16, Int32 | Int64 | Float64) => Ok(DataTypeCompat::NeedsCast(target_type)),
        (Int32, Int64 | Float64) => Ok(DataTypeCompat::NeedsCast(target_type)),
        (Float32, Float64) => Ok(DataTypeCompat::NeedsCast(target_type)),
        (_, Decimal128(p, s)) if can_upcast_to_decimal(source_type, *p, *s) => {
            Ok(DataTypeCompat::NeedsCast(target_type))
        }
        (Date32, Timestamp(_, None)) => Ok(DataTypeCompat::NeedsCast(target_type)),
        // Physical type reinterpretation: some checkpoint writers store date/timestamp columns
        // as plain integers without Parquet logical type annotations. The Delta protocol
        // guarantees data files conform to the table schema, so a schema mismatch (e.g. Int32
        // where Date32 is expected) would not occur in normal data files.
        //
        // NOTE: The kernel-type equivalent lives in `PrimitiveType::is_stats_type_compatible_with`
        // in `schema/mod.rs`. Changes here must be mirrored there.
        (Int32, Date32) => Ok(DataTypeCompat::NeedsCast(target_type)),
        (Int64, Timestamp(TimeUnit::Microsecond, _)) => Ok(DataTypeCompat::NeedsCast(target_type)),
        _ => Err(make_arrow_error(format!(
            "Incorrect datatype. Expected {target_type}, got {source_type}"
        ))),
    }
}

// Returns whether the given source type can be safely cast to a decimal with the given precision
// and scale without loss of information.
fn can_upcast_to_decimal(
    source_type: &ArrowDataType,
    target_precision: u8,
    target_scale: i8,
) -> bool {
    use ArrowDataType::*;

    let (source_precision, source_scale) = match source_type {
        Decimal128(p, s) => (*p, *s),
        // Allow converting integers to a decimal that can hold all possible values.
        Int8 => (3u8, 0i8),
        Int16 => (5u8, 0i8),
        Int32 => (10u8, 0i8),
        Int64 => (20u8, 0i8),
        _ => return false,
    };

    target_precision >= source_precision
        && target_scale >= source_scale
        && target_precision - source_precision >= (target_scale - source_scale) as u8
}

impl PartialEq<String> for MetadataValue {
    fn eq(&self, other: &String) -> bool {
        self.to_string().eq(other)
    }
}

// allow for comparing our metadata maps to arrow ones. We can't implement PartialEq because both
// are HashMaps which aren't defined in this crate
fn metadata_eq(
    kernel_metadata: &HashMap<String, MetadataValue>,
    arrow_metadata: &HashMap<String, String>,
) -> bool {
    let kernel_len = kernel_metadata.len();
    if kernel_len != arrow_metadata.len() {
        return false;
    }
    if kernel_len == 0 {
        // lens are equal, so two empty maps are equal
        return true;
    }
    kernel_metadata
        .iter()
        .all(|(key, value)| arrow_metadata.get(key).is_some_and(|v| *value == *v))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Fields};
    use crate::engine::arrow_conversion::TryFromKernel as _;
    use crate::engine::arrow_data::unshredded_variant_arrow_type;
    use crate::schema::{ArrayType, DataType, MapType, StructField};
    use crate::utils::test_utils::assert_result_error_with_message;

    #[test]
    fn accepts_safe_decimal_casts() {
        use ArrowDataType::*;

        use super::can_upcast_to_decimal;

        assert!(can_upcast_to_decimal(&Decimal128(1, 0), 2u8, 0i8));
        assert!(can_upcast_to_decimal(&Decimal128(1, 0), 2u8, 1i8));
        assert!(can_upcast_to_decimal(&Decimal128(5, -2), 6u8, -2i8));
        assert!(can_upcast_to_decimal(&Decimal128(5, -2), 6u8, -1i8));
        assert!(can_upcast_to_decimal(&Decimal128(5, 1), 6u8, 1i8));
        assert!(can_upcast_to_decimal(&Decimal128(5, 1), 6u8, 2i8));
        assert!(can_upcast_to_decimal(
            &Decimal128(10, 5),
            crate::arrow::datatypes::DECIMAL128_MAX_PRECISION,
            crate::arrow::datatypes::DECIMAL128_MAX_SCALE - 5
        ));

        assert!(can_upcast_to_decimal(&Int8, 3u8, 0i8));
        assert!(can_upcast_to_decimal(&Int8, 4u8, 0i8));
        assert!(can_upcast_to_decimal(&Int8, 4u8, 1i8));
        assert!(can_upcast_to_decimal(&Int8, 7u8, 2i8));

        assert!(can_upcast_to_decimal(&Int16, 5u8, 0i8));
        assert!(can_upcast_to_decimal(&Int16, 6u8, 0i8));
        assert!(can_upcast_to_decimal(&Int16, 6u8, 1i8));
        assert!(can_upcast_to_decimal(&Int16, 9u8, 2i8));

        assert!(can_upcast_to_decimal(&Int32, 10u8, 0i8));
        assert!(can_upcast_to_decimal(&Int32, 11u8, 0i8));
        assert!(can_upcast_to_decimal(&Int32, 11u8, 1i8));
        assert!(can_upcast_to_decimal(&Int32, 14u8, 2i8));

        assert!(can_upcast_to_decimal(&Int64, 20u8, 0i8));
        assert!(can_upcast_to_decimal(&Int64, 21u8, 0i8));
        assert!(can_upcast_to_decimal(&Int64, 21u8, 1i8));
        assert!(can_upcast_to_decimal(&Int64, 24u8, 2i8));
    }

    #[test]
    fn rejects_unsafe_decimal_casts() {
        use ArrowDataType::*;

        use super::can_upcast_to_decimal;

        assert!(!can_upcast_to_decimal(&Decimal128(2, 0), 2u8, 1i8));
        assert!(!can_upcast_to_decimal(&Decimal128(2, 0), 2u8, -1i8));
        assert!(!can_upcast_to_decimal(&Decimal128(5, 2), 6u8, 4i8));

        assert!(!can_upcast_to_decimal(&Int8, 2u8, 0i8));
        assert!(!can_upcast_to_decimal(&Int8, 3u8, 1i8));
        assert!(!can_upcast_to_decimal(&Int16, 4u8, 0i8));
        assert!(!can_upcast_to_decimal(&Int16, 5u8, 1i8));
        assert!(!can_upcast_to_decimal(&Int32, 9u8, 0i8));
        assert!(!can_upcast_to_decimal(&Int32, 10u8, 1i8));
        assert!(!can_upcast_to_decimal(&Int64, 19u8, 0i8));
        assert!(!can_upcast_to_decimal(&Int64, 20u8, 1i8));
    }

    #[test]
    fn ensure_variants() {
        fn incorrect_variant_arrow_type() -> ArrowDataType {
            let metadata_field = ArrowField::new("field_1", ArrowDataType::Binary, true);
            let value_field = ArrowField::new("field_2", ArrowDataType::Binary, true);
            let fields = vec![metadata_field, value_field];
            ArrowDataType::Struct(fields.into())
        }

        assert!(ensure_data_types(
            &DataType::unshredded_variant(),
            &unshredded_variant_arrow_type(),
            ValidationMode::Full
        )
        .is_ok());
        assert_result_error_with_message(
            ensure_data_types(
                &DataType::unshredded_variant(),
                &incorrect_variant_arrow_type(),
                ValidationMode::Full,
            ),
            "Invalid argument error: Incorrect datatype",
        )
    }

    #[test]
    fn ensure_decimals() {
        assert!(ensure_data_types(
            &DataType::decimal(5, 2).unwrap(),
            &ArrowDataType::Decimal128(5, 2),
            ValidationMode::TypesAndNames
        )
        .is_ok());
        assert_result_error_with_message(
            ensure_data_types(
                &DataType::decimal(5, 2).unwrap(),
                &ArrowDataType::Decimal128(5, 3),
                ValidationMode::TypesAndNames,
            ),
            "Invalid argument error: Incorrect datatype. Expected Decimal128(5, 2), got Decimal128(5, 3)",
        )
    }

    #[test]
    fn ensure_map() {
        let arrow_field = ArrowField::new_map(
            "map",
            "entries",
            ArrowField::new("key", ArrowDataType::Int64, false),
            ArrowField::new("val", ArrowDataType::Utf8, true),
            false,
            false,
        );
        assert!(ensure_data_types(
            &DataType::Map(Box::new(MapType::new(
                DataType::LONG,
                DataType::STRING,
                true
            ))),
            arrow_field.data_type(),
            ValidationMode::TypesAndNames
        )
        .is_ok());

        assert_result_error_with_message(
            ensure_data_types(
                &DataType::Map(Box::new(MapType::new(
                    DataType::LONG,
                    DataType::STRING,
                    false,
                ))),
                arrow_field.data_type(),
                ValidationMode::Full,
            ),
            "Generic delta kernel error: Map has nullablily false in kernel and true in arrow",
        );
        assert_result_error_with_message(
            ensure_data_types(
                &DataType::Map(Box::new(MapType::new(DataType::LONG, DataType::LONG, true))),
                arrow_field.data_type(),
                ValidationMode::TypesAndNames,
            ),
            "Invalid argument error: Incorrect datatype. Expected long, got Utf8",
        );
    }

    #[test]
    fn ensure_list() {
        assert!(ensure_data_types(
            &DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
            &ArrowDataType::new_list(ArrowDataType::Int64, true),
            ValidationMode::TypesAndNames
        )
        .is_ok());
        assert!(ensure_data_types(
            &DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
            &ArrowDataType::new_large_list(ArrowDataType::Int64, true),
            ValidationMode::TypesAndNames
        )
        .is_ok());
        assert_result_error_with_message(
            ensure_data_types(
                &DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
                &ArrowDataType::new_list(ArrowDataType::Int64, true),
                ValidationMode::TypesAndNames,
            ),
            "Invalid argument error: Incorrect datatype. Expected Utf8, got Int64",
        );
        assert_result_error_with_message(
            ensure_data_types(
                &DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
                &ArrowDataType::new_list(ArrowDataType::Int64, false),
                ValidationMode::Full,
            ),
            "Generic delta kernel error: List has nullablily true in kernel and false in arrow",
        );
    }

    #[test]
    fn ensure_struct() {
        let schema = DataType::struct_type_unchecked([StructField::nullable(
            "a",
            ArrayType::new(
                DataType::struct_type_unchecked([
                    StructField::nullable("w", DataType::LONG),
                    StructField::nullable("x", ArrayType::new(DataType::LONG, true)),
                    StructField::nullable(
                        "y",
                        MapType::new(DataType::LONG, DataType::STRING, true),
                    ),
                    StructField::nullable(
                        "z",
                        DataType::struct_type_unchecked([
                            StructField::nullable("n", DataType::LONG),
                            StructField::nullable("m", DataType::STRING),
                        ]),
                    ),
                ]),
                true,
            ),
        )]);
        let arrow_struct = ArrowDataType::try_from_kernel(&schema).unwrap();
        assert!(ensure_data_types(&schema, &arrow_struct, ValidationMode::Full).is_ok());

        let kernel_simple = DataType::struct_type_unchecked([
            StructField::nullable("w", DataType::LONG),
            StructField::nullable("x", DataType::LONG),
        ]);

        let arrow_simple_ok = ArrowField::new_struct(
            "arrow_struct",
            Fields::from(vec![
                ArrowField::new("w", ArrowDataType::Int64, true),
                ArrowField::new("x", ArrowDataType::Int64, true),
            ]),
            true,
        );
        assert!(ensure_data_types(
            &kernel_simple,
            arrow_simple_ok.data_type(),
            ValidationMode::Full
        )
        .is_ok());

        let arrow_missing_simple = ArrowField::new_struct(
            "arrow_struct",
            Fields::from(vec![ArrowField::new("w", ArrowDataType::Int64, true)]),
            true,
        );
        assert_result_error_with_message(
            ensure_data_types(
                &kernel_simple,
                arrow_missing_simple.data_type(),
                ValidationMode::Full,
            ),
            "Invalid argument error: Missing Struct fields x (Up to five missing fields shown)",
        );

        let arrow_nullable_mismatch_simple = ArrowField::new_struct(
            "arrow_struct",
            Fields::from(vec![
                ArrowField::new("w", ArrowDataType::Int64, false),
                ArrowField::new("x", ArrowDataType::Int64, true),
            ]),
            true,
        );
        assert_result_error_with_message(
            ensure_data_types(
                &kernel_simple,
                arrow_nullable_mismatch_simple.data_type(),
                ValidationMode::Full,
            ),
            "Generic delta kernel error: w has nullablily true in kernel and false in arrow",
        );
    }

    #[test]
    fn ensure_views() {
        assert_eq!(
            ensure_data_types(
                &DataType::STRING,
                &ArrowDataType::Utf8View,
                ValidationMode::Full
            )
            .unwrap(),
            DataTypeCompat::Identical
        );
        assert_eq!(
            ensure_data_types(
                &DataType::BINARY,
                &ArrowDataType::BinaryView,
                ValidationMode::Full
            )
            .unwrap(),
            DataTypeCompat::Identical
        );
        assert_eq!(
            ensure_data_types(
                &DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
                &ArrowDataType::ListView(Arc::new(ArrowField::new_list_field(
                    ArrowDataType::Int64,
                    true
                ))),
                ValidationMode::Full
            )
            .unwrap(),
            DataTypeCompat::Identical
        );
        assert_eq!(
            ensure_data_types(
                &DataType::Array(Box::new(ArrayType::new(DataType::LONG, true))),
                &ArrowDataType::LargeListView(Arc::new(ArrowField::new_list_field(
                    ArrowDataType::Int64,
                    true
                ))),
                ValidationMode::Full
            )
            .unwrap(),
            DataTypeCompat::Identical
        );
    }

    #[test]
    fn ensure_large_strings_and_binary() {
        assert_eq!(
            ensure_data_types(
                &DataType::STRING,
                &ArrowDataType::LargeUtf8,
                ValidationMode::Full
            )
            .unwrap(),
            DataTypeCompat::Identical
        );
        assert_eq!(
            ensure_data_types(
                &DataType::BINARY,
                &ArrowDataType::LargeBinary,
                ValidationMode::Full
            )
            .unwrap(),
            DataTypeCompat::Identical
        );
    }

    #[test]
    fn ensure_int32_to_date_reinterpretation() {
        // Int32 -> Date32: checkpoint writers may omit the DATE logical type annotation
        assert_eq!(
            ensure_data_types(
                &DataType::DATE,
                &ArrowDataType::Int32,
                ValidationMode::TypesAndNames
            )
            .unwrap(),
            DataTypeCompat::NeedsCast(ArrowDataType::Date32)
        );
        // Reverse is not supported: Date32 -> Int32
        assert_result_error_with_message(
            ensure_data_types(
                &DataType::INTEGER,
                &ArrowDataType::Date32,
                ValidationMode::TypesAndNames,
            ),
            "Incorrect datatype",
        );
    }

    #[test]
    fn ensure_int64_to_timestamp_reinterpretation() {
        // Int64 -> Timestamp (with UTC timezone, i.e. kernel `timestamp`)
        let ts_utc = ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            ensure_data_types(
                &DataType::TIMESTAMP,
                &ArrowDataType::Int64,
                ValidationMode::TypesAndNames
            )
            .unwrap(),
            DataTypeCompat::NeedsCast(ts_utc)
        );
        // Int64 -> TimestampNtz (no timezone, i.e. kernel `timestamp_ntz`)
        let ts_ntz = ArrowDataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(
            ensure_data_types(
                &DataType::TIMESTAMP_NTZ,
                &ArrowDataType::Int64,
                ValidationMode::TypesAndNames
            )
            .unwrap(),
            DataTypeCompat::NeedsCast(ts_ntz)
        );
        // Reverse is not supported
        assert_result_error_with_message(
            ensure_data_types(
                &DataType::LONG,
                &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                ValidationMode::TypesAndNames,
            ),
            "Incorrect datatype",
        );
    }

    /// Ensures that every kernel-level checkpoint reinterpretation rule in
    /// `PrimitiveType::is_checkpoint_cast_compatible` has a corresponding Arrow cast
    /// in `check_cast_compat`. If one side is updated without the other, this test fails.
    #[test]
    fn checkpoint_reinterpretation_rules_match_arrow_cast_rules() {
        use crate::schema::PrimitiveType;

        let reinterpretation_pairs: &[(PrimitiveType, PrimitiveType)] = &[
            (PrimitiveType::Integer, PrimitiveType::Date),
            (PrimitiveType::Long, PrimitiveType::Timestamp),
            (PrimitiveType::Long, PrimitiveType::TimestampNtz),
        ];
        for (source_kernel, target_kernel) in reinterpretation_pairs {
            // Verify the kernel-level rule holds
            assert!(
                source_kernel.is_checkpoint_cast_compatible(target_kernel),
                "Kernel should allow {source_kernel:?} -> {target_kernel:?}"
            );

            // Verify the Arrow-level rule holds
            let source_arrow =
                ArrowDataType::try_from_kernel(&DataType::Primitive(source_kernel.clone()))
                    .unwrap();
            let target_arrow =
                ArrowDataType::try_from_kernel(&DataType::Primitive(target_kernel.clone()))
                    .unwrap();
            assert!(
                check_cast_compat(target_arrow.clone(), &source_arrow).is_ok(),
                "Arrow check_cast_compat should allow {source_arrow:?} -> {target_arrow:?} \
                 to match kernel rule {source_kernel:?} -> {target_kernel:?}"
            );
        }
    }

    #[test]
    fn types_only_matches_struct_by_ordinal_ignoring_names() {
        let kernel = DataType::struct_type_unchecked([
            StructField::nullable("logical_a", DataType::LONG),
            StructField::nullable("logical_b", DataType::STRING),
        ]);
        let arrow = ArrowDataType::Struct(
            vec![
                ArrowField::new("physical_x", ArrowDataType::Int64, true),
                ArrowField::new("physical_y", ArrowDataType::Utf8, true),
            ]
            .into(),
        );
        assert!(ensure_data_types(&kernel, &arrow, ValidationMode::TypesOnly).is_ok());
    }

    #[test]
    fn types_only_rejects_struct_field_count_mismatch() {
        let kernel = DataType::struct_type_unchecked([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
        ]);
        let arrow =
            ArrowDataType::Struct(vec![ArrowField::new("x", ArrowDataType::Int64, true)].into());
        assert_result_error_with_message(
            ensure_data_types(&kernel, &arrow, ValidationMode::TypesOnly),
            "Struct field count mismatch",
        );
    }

    #[test]
    fn types_only_rejects_struct_type_mismatch_by_ordinal() {
        let kernel = DataType::struct_type_unchecked([
            StructField::nullable("a", DataType::LONG),
            StructField::nullable("b", DataType::LONG),
        ]);
        let arrow = ArrowDataType::Struct(
            vec![
                ArrowField::new("x", ArrowDataType::Int64, true),
                ArrowField::new("y", ArrowDataType::Utf8, true),
            ]
            .into(),
        );
        assert_result_error_with_message(
            ensure_data_types(&kernel, &arrow, ValidationMode::TypesOnly),
            "Incorrect datatype",
        );
    }
}
