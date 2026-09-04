//! FFI surface for building the partition values of a partitioned write.
//!
//! Engines build a [`PartitionValueMap`] by inserting one typed value per partition column
//! (keyed by the column's logical name), then pass it to `get_partitioned_write_context`. The
//! typed inserts mirror the `visit_expression_literal_*` family: one entry point per scalar type.
//! The kernel validates the supplied values against the table's partition schema and serializes
//! them per the Delta protocol when the write context is built.

use std::collections::HashMap;

use delta_kernel::expressions::Scalar;
use delta_kernel::DeltaResult;
use delta_kernel_ffi_macros::handle_descriptor;

use crate::error::{ExternResult, IntoExternResult};
use crate::expressions::kernel_visitor::NullTypeTag;
use crate::handle::Handle;
use crate::{KernelStringSlice, SharedExternEngine, TryFromStringSlice};

/// Owns a map from a partition column's logical name to its [`Scalar`] value. Engines build the
/// map with the `partition_value_map_insert_*` functions and pass it to
/// `get_partitioned_write_context` (or its create-table counterpart), which consumes it.
pub struct PartitionValueMap {
    pub(crate) inner: HashMap<String, Scalar>,
}

/// Mutable handle for a `PartitionValueMap`.
#[handle_descriptor(target=PartitionValueMap, mutable=true, sized=true)]
pub struct ExclusivePartitionValueMap;

/// Allocate an empty partition value map. The returned handle must be released either by
/// [`free_partition_value_map`] or by `get_partitioned_write_context` (which consumes the map).
#[no_mangle]
pub extern "C" fn partition_value_map_new() -> Handle<ExclusivePartitionValueMap> {
    Box::new(PartitionValueMap {
        inner: HashMap::new(),
    })
    .into()
}

/// Free a partition value map handle.
///
/// # Safety
///
/// Caller must pass a valid handle previously returned by [`partition_value_map_new`] that has
/// not already been consumed by a write-context call.
#[no_mangle]
pub unsafe extern "C" fn free_partition_value_map(map: Handle<ExclusivePartitionValueMap>) {
    map.drop_handle();
}

// =============================================================================
// Typed inserts
// =============================================================================
//
// Each insert keys on the partition column's *logical* name (the kernel translates to physical
// names). Re-inserting the same name replaces the previous value. Inserts can fail when the `name`
// slice is not valid UTF-8 (and, for decimal/null, when the type parameters are invalid), hence the
// `ExternResult<bool>` return; the engine handle is used to allocate any error. The map handle is
// borrowed, never consumed, so the caller keeps ownership across inserts. These are written out
// per-type rather than macro-generated because cbindgen scans source syntactically and cannot see
// `extern "C" fn`s produced by macro expansion, so they would be absent from the generated header.
// See issue #255 for more info.

/// Common insertion path: validate the name, then store the scalar under it. Returns `true` on
/// success.
fn partition_value_map_insert_impl(
    map: &mut PartitionValueMap,
    name: DeltaResult<&str>,
    scalar: DeltaResult<Scalar>,
) -> DeltaResult<bool> {
    map.inner.insert(name?.to_string(), scalar?);
    Ok(true)
}

/// Insert a `string` partition value under `name`.
///
/// # Safety
///
/// Caller must pass valid handles and string slices.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_string(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let value = unsafe { String::try_from_slice(&value) };
    partition_value_map_insert_impl(map, name, value.map(Scalar::from)).into_extern_result(&engine)
}

/// Insert an `integer` (32-bit) partition value under `name`.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_int(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: i32,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    partition_value_map_insert_impl(map, name, Ok(Scalar::from(value))).into_extern_result(&engine)
}

/// Insert a `long` (64-bit) partition value under `name`.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_long(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    partition_value_map_insert_impl(map, name, Ok(Scalar::from(value))).into_extern_result(&engine)
}

/// Insert a `short` (16-bit) partition value under `name`.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_short(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: i16,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    partition_value_map_insert_impl(map, name, Ok(Scalar::from(value))).into_extern_result(&engine)
}

/// Insert a `byte` (8-bit) partition value under `name`.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_byte(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: i8,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    partition_value_map_insert_impl(map, name, Ok(Scalar::from(value))).into_extern_result(&engine)
}

/// Insert a `float` (32-bit) partition value under `name`.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_float(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: f32,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    partition_value_map_insert_impl(map, name, Ok(Scalar::from(value))).into_extern_result(&engine)
}

/// Insert a `double` (64-bit) partition value under `name`.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_double(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: f64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    partition_value_map_insert_impl(map, name, Ok(Scalar::from(value))).into_extern_result(&engine)
}

/// Insert a `boolean` partition value under `name`.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_bool(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: bool,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    partition_value_map_insert_impl(map, name, Ok(Scalar::from(value))).into_extern_result(&engine)
}

/// Insert a `date` partition value (`value` = days since the Unix epoch) under `name`.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_date(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: i32,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    partition_value_map_insert_impl(map, name, Ok(Scalar::Date(value))).into_extern_result(&engine)
}

/// Insert a `timestamp` partition value (`value` = microseconds since the Unix epoch, UTC) under
/// `name`.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_timestamp(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    partition_value_map_insert_impl(map, name, Ok(Scalar::Timestamp(value)))
        .into_extern_result(&engine)
}

/// Insert a `timestamp_ntz` partition value (`value` = microseconds since the Unix epoch, no
/// timezone) under `name`.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_timestamp_ntz(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    partition_value_map_insert_impl(map, name, Ok(Scalar::TimestampNtz(value)))
        .into_extern_result(&engine)
}

/// Insert a `binary` partition value under `name`, copying `len` bytes from `value`.
///
/// # Safety
///
/// Caller must pass valid handles, a valid `name` slice, and (when `len > 0`) a `value` pointer to
/// at least `len` readable bytes. An empty value may be passed as `(null, 0)`.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_binary(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value: *const u8,
    len: usize,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    // `from_raw_parts` requires a non-null pointer even when `len == 0`, so guard the empty case.
    let bytes = if value.is_null() || len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(value, len) }
    };
    partition_value_map_insert_impl(map, name, Ok(Scalar::Binary(bytes.to_vec())))
        .into_extern_result(&engine)
}

/// Insert a `decimal` partition value under `name`. The unscaled 128-bit value is supplied as two
/// 64-bit halves (`value_hi << 64 | value_lo`). Returns an error if the precision/scale
/// combination is invalid.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_decimal(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    value_hi: u64,
    value_lo: u64,
    precision: u8,
    scale: u8,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let value = ((value_hi as i128) << 64) | (value_lo as i128);
    partition_value_map_insert_impl(map, name, Scalar::decimal(value, precision, scale))
        .into_extern_result(&engine)
}

/// Insert a typed `null` partition value under `name`. The `type_tag` identifies the column's
/// data type using the same `NullTypeTag` encoding as `visit_expression_literal_null` in
/// [`kernel_visitor`](crate::expressions::kernel_visitor). For the decimal tag, `precision` and
/// `scale` specify the decimal parameters; for all other types, pass 0 for both.
///
/// Returns an error if the tag is unrecognized, is the non-primitive sentinel (255), or carries an
/// invalid decimal precision/scale. A null partition value is only legal for a nullable partition
/// column; the kernel rejects it otherwise when the write context is built.
///
/// # Safety
///
/// Caller must pass valid handles and a valid `name` slice.
#[no_mangle]
pub unsafe extern "C" fn partition_value_map_insert_null(
    mut map: Handle<ExclusivePartitionValueMap>,
    name: KernelStringSlice,
    type_tag: u8,
    precision: u8,
    scale: u8,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map = unsafe { map.as_mut() };
    let engine = unsafe { engine.as_ref() };
    let name = unsafe { TryFromStringSlice::try_from_slice(&name) };
    let scalar = NullTypeTag::try_from(type_tag)
        .and_then(|tag| tag.to_data_type(precision, scale))
        .map(Scalar::Null);
    partition_value_map_insert_impl(map, name, scalar).into_extern_result(&engine)
}

#[cfg(test)]
mod tests {
    use delta_kernel::Error;

    use super::*;

    #[test]
    fn insert_stores_typed_scalars_under_logical_names() {
        let mut map = PartitionValueMap {
            inner: HashMap::new(),
        };
        partition_value_map_insert_impl(&mut map, Ok("year"), Ok(Scalar::from(2024i32))).unwrap();
        partition_value_map_insert_impl(&mut map, Ok("region"), Ok(Scalar::from("US"))).unwrap();

        assert_eq!(map.inner.len(), 2);
        assert_eq!(map.inner.get("year"), Some(&Scalar::Integer(2024)));
        assert_eq!(map.inner.get("region"), Some(&Scalar::String("US".into())));
    }

    #[test]
    fn insert_replaces_existing_value_for_same_name() {
        let mut map = PartitionValueMap {
            inner: HashMap::new(),
        };
        partition_value_map_insert_impl(&mut map, Ok("p"), Ok(Scalar::from(1i32))).unwrap();
        partition_value_map_insert_impl(&mut map, Ok("p"), Ok(Scalar::from(2i32))).unwrap();

        assert_eq!(map.inner.len(), 1);
        assert_eq!(map.inner.get("p"), Some(&Scalar::Integer(2)));
    }

    #[test]
    fn insert_propagates_name_error_without_mutating_map() {
        let mut map = PartitionValueMap {
            inner: HashMap::new(),
        };
        let result = partition_value_map_insert_impl(
            &mut map,
            Err(Error::generic("bad name")),
            Ok(Scalar::from(1i32)),
        );
        assert!(result.is_err());
        assert!(map.inner.is_empty());
    }

    #[test]
    fn insert_propagates_scalar_error_without_mutating_map() {
        let mut map = PartitionValueMap {
            inner: HashMap::new(),
        };
        // precision 0 is invalid for a decimal.
        let result = partition_value_map_insert_impl(&mut map, Ok("d"), Scalar::decimal(1, 0, 0));
        assert!(result.is_err());
        assert!(map.inner.is_empty());
    }

    #[test]
    fn null_insert_builds_typed_null_scalar() {
        let mut map = PartitionValueMap {
            inner: HashMap::new(),
        };
        let scalar = NullTypeTag::try_from(NullTypeTag::Integer as u8)
            .and_then(|tag| tag.to_data_type(0, 0))
            .map(Scalar::Null);
        partition_value_map_insert_impl(&mut map, Ok("p"), scalar).unwrap();
        assert_eq!(
            map.inner.get("p"),
            Some(&Scalar::Null(delta_kernel::schema::DataType::INTEGER))
        );
    }

    // The following tests drive the typed `extern "C"` wrappers through their real signatures
    // (which require an engine handle for error allocation), guarding against the easy
    // copy-paste hazards: a wrong `Scalar` variant in a wrapper, a botched decimal hi/lo
    // recombination, or a mis-dispatched null type tag. They build a throwaway in-memory engine.
    #[cfg(feature = "default-engine-base")]
    mod ffi {
        use delta_kernel::schema::{DataType, PrimitiveType};

        use super::*;
        use crate::error::{ExternResult, KernelError};
        use crate::ffi_test_utils::{ok_or_panic, recover_error};
        use crate::kernel_string_slice;
        use crate::tests::get_default_engine;

        #[test]
        fn typed_wrappers_construct_expected_scalars() {
            let engine = get_default_engine("memory:///partition_value_test/");
            let map = partition_value_map_new();
            let binary = [1u8, 2, 3];
            // A decimal value whose magnitude exceeds 64 bits, so the high word must be used.
            let big = 1i128 << 64;

            unsafe {
                let n = "i";
                ok_or_panic(partition_value_map_insert_int(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    9,
                    engine.shallow_copy(),
                ));
                let n = "l";
                ok_or_panic(partition_value_map_insert_long(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    10,
                    engine.shallow_copy(),
                ));
                let n = "sh";
                ok_or_panic(partition_value_map_insert_short(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    11,
                    engine.shallow_copy(),
                ));
                let n = "by";
                ok_or_panic(partition_value_map_insert_byte(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    12,
                    engine.shallow_copy(),
                ));
                let n = "f";
                ok_or_panic(partition_value_map_insert_float(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    1.5,
                    engine.shallow_copy(),
                ));
                let n = "d";
                ok_or_panic(partition_value_map_insert_double(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    2.5,
                    engine.shallow_copy(),
                ));
                let n = "b";
                ok_or_panic(partition_value_map_insert_bool(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    true,
                    engine.shallow_copy(),
                ));
                let n = "dt";
                ok_or_panic(partition_value_map_insert_date(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    19723,
                    engine.shallow_copy(),
                ));
                let n = "ts";
                ok_or_panic(partition_value_map_insert_timestamp(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    1000,
                    engine.shallow_copy(),
                ));
                let n = "tsntz";
                ok_or_panic(partition_value_map_insert_timestamp_ntz(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    2000,
                    engine.shallow_copy(),
                ));
                let n = "bin";
                ok_or_panic(partition_value_map_insert_binary(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    binary.as_ptr(),
                    binary.len(),
                    engine.shallow_copy(),
                ));
                // An empty binary value may arrive as (null, 0); it must not deref the pointer.
                let n = "bin_empty";
                ok_or_panic(partition_value_map_insert_binary(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    std::ptr::null(),
                    0,
                    engine.shallow_copy(),
                ));
                let n = "dec";
                ok_or_panic(partition_value_map_insert_decimal(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    1, // value_hi
                    0, // value_lo
                    38,
                    0,
                    engine.shallow_copy(),
                ));
                let n = "s";
                let v = "hello";
                ok_or_panic(partition_value_map_insert_string(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    kernel_string_slice!(v),
                    engine.shallow_copy(),
                ));
            }

            let inner = &unsafe { map.as_ref() }.inner;
            assert_eq!(inner.get("i"), Some(&Scalar::Integer(9)));
            assert_eq!(inner.get("l"), Some(&Scalar::Long(10)));
            assert_eq!(inner.get("sh"), Some(&Scalar::Short(11)));
            assert_eq!(inner.get("by"), Some(&Scalar::Byte(12)));
            assert_eq!(inner.get("f"), Some(&Scalar::Float(1.5)));
            assert_eq!(inner.get("d"), Some(&Scalar::Double(2.5)));
            assert_eq!(inner.get("b"), Some(&Scalar::Boolean(true)));
            assert_eq!(inner.get("dt"), Some(&Scalar::Date(19723)));
            assert_eq!(inner.get("ts"), Some(&Scalar::Timestamp(1000)));
            assert_eq!(inner.get("tsntz"), Some(&Scalar::TimestampNtz(2000)));
            assert_eq!(inner.get("bin"), Some(&Scalar::Binary(vec![1, 2, 3])));
            assert_eq!(inner.get("bin_empty"), Some(&Scalar::Binary(vec![])));
            assert_eq!(
                inner.get("dec"),
                Some(&Scalar::decimal(big, 38, 0).unwrap())
            );
            assert_eq!(inner.get("s"), Some(&Scalar::String("hello".into())));

            unsafe {
                free_partition_value_map(map);
                crate::free_engine(engine);
            }
        }

        #[test]
        fn null_wrapper_dispatches_type_tags_and_rejects_invalid() {
            let engine = get_default_engine("memory:///partition_value_null_test/");
            let map = partition_value_map_new();

            // Decimal null carries precision/scale through to the data type.
            let n = "dec_null";
            ok_or_panic(unsafe {
                partition_value_map_insert_null(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    NullTypeTag::Decimal as u8,
                    10,
                    2,
                    engine.shallow_copy(),
                )
            });
            let expected =
                Scalar::Null(DataType::Primitive(PrimitiveType::decimal(10, 2).unwrap()));
            assert_eq!(
                unsafe { map.as_ref() }.inner.get("dec_null"),
                Some(&expected)
            );

            // The non-primitive sentinel and unrecognized tags are rejected.
            let expect_err = |result: ExternResult<bool>| match result {
                ExternResult::Err(e) => unsafe { recover_error(e) }.etype,
                ExternResult::Ok(_) => panic!("expected error"),
            };
            let n = "bad";
            let err = expect_err(unsafe {
                partition_value_map_insert_null(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    NullTypeTag::NonPrimitive as u8,
                    0,
                    0,
                    engine.shallow_copy(),
                )
            });
            assert_eq!(err, KernelError::GenericError);
            let err = expect_err(unsafe {
                partition_value_map_insert_null(
                    map.shallow_copy(),
                    kernel_string_slice!(n),
                    200, // unrecognized tag
                    0,
                    0,
                    engine.shallow_copy(),
                )
            });
            assert_eq!(err, KernelError::GenericError);
            // Failed inserts did not add the "bad" key.
            assert!(!unsafe { map.as_ref() }.inner.contains_key("bad"));

            unsafe {
                free_partition_value_map(map);
                crate::free_engine(engine);
            }
        }
    }
}
