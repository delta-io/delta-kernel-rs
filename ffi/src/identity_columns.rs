//! FFI for Concurrent Identity Columns (CIC) helpers.
//!
//! Exposes pure-data kernel primitives that engines reuse without taking on
//! kernel's Rust dependency surface:
//! - [`reservation_next_values`]: wraps [`ReservedRange::values`] so engines don't reimplement the
//!   overflow-checked `start + step * i` arithmetic (which kernel gets right at i64 boundary
//!   cases).
//!
//! Kept outside the `delta-kernel-unity-catalog` feature gate because these
//! are core CIC primitives, not UC-specific. The UC-specific plumbing
//! (CreateSequence/ReserveIds HTTP, committer callbacks) lives in
//! `delta_kernel_unity_catalog.rs` behind that feature.

use delta_kernel::identity_columns::ReservedRange;
use delta_kernel::{DeltaResult, Error};

use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{KernelStringSlice, SharedExternEngine, TryFromStringSlice};

/// Generates `count` consecutive identity values from a reservation into a
/// caller-allocated i64 buffer. Value at index `i` of the output is
/// `range_start + step * (offset + i)`.
///
/// This is a thin pure-data wrapper over [`ReservedRange::values`] so
/// engines can read the same overflow-checked arithmetic kernel uses without
/// binding to kernel types. The caller provides the reservation bounds + step
/// (which kernel stores on [`ReservedRange`]), the row offset into the
/// reservation, and the number of values to write.
///
/// # Parameters
///
/// - `column_name`: the logical column name. Used only for diagnostics in error messages -- not by
///   the value-generation arithmetic.
/// - `range_start`, `range_end`, `step`: the reservation. `step` must be non-zero and the range
///   must be consistent with the step's sign.
/// - `offset`: rows already consumed from this reservation.
/// - `count`: rows to produce in this call. `offset + count` must not exceed the reservation's row
///   capacity.
/// - `out`: caller-allocated buffer holding `count * sizeof(int64_t)` bytes. Written with the
///   generated values.
/// - `engine`: used only to allocate any returned error in the caller's memory space. Caller
///   retains ownership.
///
/// # Errors
///
/// - Returns `GenericError` if the reservation is invalid (zero step, inconsistent bounds) or
///   exhausted (offset + count overflows the available range).
/// - Returns `GenericError` on i64 arithmetic overflow (very large reservations near i64::MAX /
///   i64::MIN).
///
/// # Safety
///
/// - `engine` must be a valid handle previously returned from kernel and not yet freed; ownership
///   remains with the caller.
/// - `out` must point to at least `count` `i64` slots when `count > 0`. When `count == 0` the
///   function is a no-op and `out` may be null.
/// - `column_name` must point to a valid UTF-8 kernel string slice for the duration of the call
///   when `count > 0`. When `count == 0`, `column_name` is not dereferenced.
///
/// Return type is `ExternResult<bool>` (always `Ok(true)` on success) rather
/// than `ExternResult<()>` because cbindgen does not translate Rust's `()`
/// into a usable C++ template argument, leaving `ExternResult` bare and
/// breaking the generated C++ header.
#[no_mangle]
pub unsafe extern "C" fn reservation_next_values(
    column_name: KernelStringSlice,
    range_start: i64,
    range_end: i64,
    step: i64,
    offset: u64,
    count: u64,
    out: *mut i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let engine = unsafe { engine.as_ref() };
    let result = unsafe {
        reservation_next_values_impl(
            column_name,
            range_start,
            range_end,
            step,
            offset,
            count,
            out,
        )
    };
    result.into_extern_result(&engine)
}

/// # Safety
///
/// `out` must be writable for at least `count` `i64` values when `count > 0`.
/// `column_name` must point to a valid UTF-8 kernel string slice when
/// `count > 0`; it is not dereferenced when `count == 0`.
unsafe fn reservation_next_values_impl(
    column_name: KernelStringSlice,
    range_start: i64,
    range_end: i64,
    step: i64,
    offset: u64,
    count: u64,
    out: *mut i64,
) -> DeltaResult<bool> {
    if count == 0 {
        return Ok(true);
    }
    if out.is_null() {
        return Err(Error::generic(
            "reservation_next_values: out buffer is null but count > 0",
        ));
    }
    // Safety: caller affirms `column_name` is a valid kernel string slice
    // when `count > 0`, which we just confirmed.
    let column_name: String = unsafe { TryFromStringSlice::try_from_slice(&column_name) }?;
    let reservation = ReservedRange {
        range_start,
        range_end,
        step,
    };
    let values = reservation.values(&column_name, offset, count)?;
    // Safety: caller guaranteed `out` has at least `count` i64 slots.
    unsafe {
        std::ptr::copy_nonoverlapping(values.as_ptr(), out, values.len());
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::error::{ExternResult, KernelError};
    use crate::ffi_test_utils::{ok_or_panic, recover_error};
    use crate::kernel_string_slice;
    use crate::tests::get_default_engine;

    fn engine() -> Handle<SharedExternEngine> {
        let tmp = tempdir().unwrap();
        get_default_engine(tmp.path().to_str().unwrap())
    }

    #[test]
    fn step_1_produces_consecutive_values() {
        let e = engine();
        let mut buf = [0i64; 5];
        let column_name = "id";
        let ok = ok_or_panic(unsafe {
            reservation_next_values(
                kernel_string_slice!(column_name),
                1,
                10,
                1,
                0,
                5,
                buf.as_mut_ptr(),
                e.shallow_copy(),
            )
        });
        assert!(ok);
        assert_eq!(buf, [1, 2, 3, 4, 5]);
        unsafe { crate::free_engine(e) };
    }

    #[test]
    fn offset_advances_cursor() {
        let e = engine();
        let mut buf = [0i64; 4];
        let column_name = "id";
        let _ = ok_or_panic(unsafe {
            reservation_next_values(
                kernel_string_slice!(column_name),
                1,
                10,
                1,
                3,
                4,
                buf.as_mut_ptr(),
                e.shallow_copy(),
            )
        });
        assert_eq!(buf, [4, 5, 6, 7]);
        unsafe { crate::free_engine(e) };
    }

    #[test]
    fn step_greater_than_1_strides() {
        let e = engine();
        let mut buf = [0i64; 5];
        let column_name = "id";
        let _ = ok_or_panic(unsafe {
            reservation_next_values(
                kernel_string_slice!(column_name),
                0,
                20,
                5,
                0,
                5,
                buf.as_mut_ptr(),
                e.shallow_copy(),
            )
        });
        assert_eq!(buf, [0, 5, 10, 15, 20]);
        unsafe { crate::free_engine(e) };
    }

    #[test]
    fn negative_step_descends() {
        let e = engine();
        let mut buf = [0i64; 6];
        let column_name = "id";
        let _ = ok_or_panic(unsafe {
            reservation_next_values(
                kernel_string_slice!(column_name),
                10,
                0,
                -2,
                0,
                6,
                buf.as_mut_ptr(),
                e.shallow_copy(),
            )
        });
        assert_eq!(buf, [10, 8, 6, 4, 2, 0]);
        unsafe { crate::free_engine(e) };
    }

    #[test]
    fn exhausted_range_errors_includes_column_name() {
        let e = engine();
        let mut buf = [0i64; 4];
        let column_name = "user_id";
        let result = unsafe {
            reservation_next_values(
                kernel_string_slice!(column_name),
                1,
                3,
                1,
                0,
                4,
                buf.as_mut_ptr(),
                e.shallow_copy(),
            )
        };
        match result {
            ExternResult::Err(err_ptr) => {
                let err = unsafe { recover_error(err_ptr) };
                assert_eq!(err.etype, KernelError::GenericError);
                assert!(
                    err.message.contains("reservation exhausted"),
                    "unexpected error message: {}",
                    err.message
                );
                assert!(
                    err.message.contains("user_id"),
                    "error should include column name, got: {}",
                    err.message
                );
            }
            ExternResult::Ok(_) => panic!("expected error for exhausted range"),
        }
        unsafe { crate::free_engine(e) };
    }

    #[test]
    fn count_zero_is_noop() {
        let e = engine();
        let column_name = "id";
        let _ = ok_or_panic(unsafe {
            reservation_next_values(
                kernel_string_slice!(column_name),
                1,
                10,
                1,
                0,
                0,
                std::ptr::null_mut(),
                e.shallow_copy(),
            )
        });
        unsafe { crate::free_engine(e) };
    }
}
