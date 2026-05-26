//! Batch stats accessor handed to the engine during file pruning.
//!
//! Two complementary lanes are exposed:
//!
//! - The **scalar lane** -- per-row, per-column getters (`batch_stats_{min,max}_{string,long}`,
//!   `batch_stats_null_count`, `batch_stats_row_count`). Engines that don't speak the Arrow C Data
//!   Interface can still prune by reading one stat at a time.
//! - The **Arrow lane** -- a borrowed `ArrowFFIData` for engines that want to consume the whole
//!   batch through the Arrow C Data Interface. Gated on `default-engine-base`.
//!
//! Both lanes are valid for the duration of the surrounding callback. The
//! scalar lane caches interned strings in a per-accessor [`StrArena`] so
//! returned slices remain stable across calls.

use delta_kernel::expressions::Scalar;
use delta_kernel::schema::DataType;

#[cfg(feature = "default-engine-base")]
use crate::engine_data::ArrowFFIData;
use crate::{kernel_string_slice, KernelStringSlice, TryFromStringSlice};

/// Per-batch stats provider. Implementations resolve column lookups against a
/// metadata batch indexed by `row`.
pub(crate) trait BatchStatsProvider {
    /// Read the min stat for the cell at `row` in column `col`. `dtype` is the
    /// requested kind (caller dispatches on it). Returns `None` if the column
    /// or row is absent. May return a narrower-typed [`Scalar`] (e.g.
    /// [`Scalar::Integer`] when `dtype == LONG`); callers widen via
    /// [`scalar_as_i64`].
    fn min(&self, row: usize, col: &str, dtype: &DataType) -> Option<Scalar>;

    /// Read the max stat for the cell at `row` in column `col`. Same
    /// type-narrowing and widening rules as [`Self::min`].
    fn max(&self, row: usize, col: &str, dtype: &DataType) -> Option<Scalar>;

    /// Returns the column's null count at `row`, or `None` if absent.
    fn null_count(&self, row: usize, col: &str) -> Option<i64>;

    /// Returns the file row count at `row`, or `None` if absent.
    fn row_count(&self, row: usize) -> Option<i64>;
}

/// Append-only `!Sync` string arena. Stores raw `*mut str` so interned
/// slices keep stable provenance across `Vec` reallocation under Miri.
#[derive(Default)]
pub(crate) struct StrArena {
    boxes: std::cell::RefCell<Vec<*mut str>>,
}

impl StrArena {
    pub(crate) fn intern(&self, s: String) -> &str {
        let raw: *mut str = Box::into_raw(s.into_boxed_str());
        self.boxes.borrow_mut().push(raw);
        // SAFETY: `raw` is a leaked `Box<str>` reclaimed in `Drop`; its
        // provenance is stable for `&self`'s lifetime.
        unsafe { &*raw }
    }
}

impl Drop for StrArena {
    fn drop(&mut self) {
        for raw in self.boxes.borrow().iter() {
            // SAFETY: paired with `Box::into_raw` in `intern`.
            unsafe { drop(Box::from_raw(*raw)) };
        }
    }
}

/// Accessor handed to the engine during batched file pruning. Interned
/// slices returned by the scalar getters live until the callback returns.
pub struct BatchStatsAccessor<'a> {
    provider: &'a dyn BatchStatsProvider,
    arena: StrArena,
    /// Optional Arrow-C-Data view; consumed via [`batch_stats_as_arrow`].
    #[cfg(feature = "default-engine-base")]
    arrow_ffi: Option<ArrowFFIData>,
}

impl<'a> BatchStatsAccessor<'a> {
    #[allow(dead_code)] // used under default-engine-base
    pub(crate) fn new(provider: &'a dyn BatchStatsProvider) -> Self {
        Self {
            provider,
            arena: StrArena::default(),
            #[cfg(feature = "default-engine-base")]
            arrow_ffi: None,
        }
    }

    /// Attach the Arrow-C-Data view exposed via [`batch_stats_as_arrow`].
    /// `None` leaves it unset.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn with_arrow_ffi(mut self, arrow_ffi: Option<ArrowFFIData>) -> Self {
        self.arrow_ffi = arrow_ffi;
        self
    }

    fn intern(&self, s: String) -> &str {
        self.arena.intern(s)
    }
}

/// Read the string min stat for `(row, col)`.
///
/// # Safety
/// `acc` and `out` must be valid pointers; `col` a valid string slice. The
/// returned slice points into the accessor's per-callback arena.
#[no_mangle]
pub unsafe extern "C" fn batch_stats_min_string(
    acc: *const BatchStatsAccessor<'_>,
    row: usize,
    col: KernelStringSlice,
    out: *mut KernelStringSlice,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let Some(Scalar::String(s)) = acc.provider.min(row, &name, &DataType::STRING) else {
        return false;
    };
    let interned = acc.intern(s);
    unsafe { *out = kernel_string_slice!(interned) };
    true
}

/// Read the string max stat for `(row, col)`.
///
/// # Safety
/// See [`batch_stats_min_string`].
#[no_mangle]
pub unsafe extern "C" fn batch_stats_max_string(
    acc: *const BatchStatsAccessor<'_>,
    row: usize,
    col: KernelStringSlice,
    out: *mut KernelStringSlice,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let Some(Scalar::String(s)) = acc.provider.max(row, &name, &DataType::STRING) else {
        return false;
    };
    let interned = acc.intern(s);
    unsafe { *out = kernel_string_slice!(interned) };
    true
}

/// Read the i64 min stat for `(row, col)`. See [`scalar_as_i64`] for the
/// accepted stat types.
///
/// # Safety
/// See [`batch_stats_min_string`].
#[no_mangle]
pub unsafe extern "C" fn batch_stats_min_long(
    acc: *const BatchStatsAccessor<'_>,
    row: usize,
    col: KernelStringSlice,
    out: *mut i64,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let Some(scalar) = acc.provider.min(row, &name, &DataType::LONG) else {
        return false;
    };
    let Some(v) = scalar_as_i64(&scalar) else {
        return false;
    };
    unsafe { *out = v };
    true
}

/// Read the i64 max stat for `(row, col)`. Same widening rules as
/// [`batch_stats_min_long`].
///
/// # Safety
/// See [`batch_stats_min_string`].
#[no_mangle]
pub unsafe extern "C" fn batch_stats_max_long(
    acc: *const BatchStatsAccessor<'_>,
    row: usize,
    col: KernelStringSlice,
    out: *mut i64,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let Some(scalar) = acc.provider.max(row, &name, &DataType::LONG) else {
        return false;
    };
    let Some(v) = scalar_as_i64(&scalar) else {
        return false;
    };
    unsafe { *out = v };
    true
}

/// Read the null count for `(row, col)`.
///
/// # Safety
/// See [`batch_stats_min_string`].
#[no_mangle]
pub unsafe extern "C" fn batch_stats_null_count(
    acc: *const BatchStatsAccessor<'_>,
    row: usize,
    col: KernelStringSlice,
    out: *mut i64,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(name) = (unsafe { column_name_from_slice(&col) }) else {
        return false;
    };
    let Some(v) = acc.provider.null_count(row, &name) else {
        return false;
    };
    unsafe { *out = v };
    true
}

/// Read the row count for the file at `row`.
///
/// # Safety
/// See [`batch_stats_min_string`].
#[no_mangle]
pub unsafe extern "C" fn batch_stats_row_count(
    acc: *const BatchStatsAccessor<'_>,
    row: usize,
    out: *mut i64,
) -> bool {
    if acc.is_null() || out.is_null() {
        return false;
    }
    let acc = unsafe { &*acc };
    let Some(v) = acc.provider.row_count(row) else {
        return false;
    };
    unsafe { *out = v };
    true
}

/// Arrow C Data Interface view of the stats batch. Pointer is valid for the
/// surrounding `eval_against_stats` callback. Release callbacks are
/// idempotent, so importing or not importing is both safe.
///
/// Returns null when the accessor isn't Arrow-backed.
///
/// # Safety
/// `acc` must be a valid [`BatchStatsAccessor`] pointer if non-null.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn batch_stats_as_arrow(
    acc: *const BatchStatsAccessor<'_>,
) -> *const ArrowFFIData {
    let Some(acc) = (unsafe { acc.as_ref() }) else {
        return std::ptr::null();
    };
    acc.arrow_ffi
        .as_ref()
        .map_or(std::ptr::null(), |d| d as *const _)
}

unsafe fn column_name_from_slice(col: &KernelStringSlice) -> Option<String> {
    // SAFETY: caller guarantees slice validity for the duration of the call.
    unsafe { String::try_from_slice(col) }.ok()
}

/// Widen any signed integer scalar (`Long`, `Integer`, `Short`, `Byte`) to
/// `i64`. Returns `None` for any other variant.
pub(crate) fn scalar_as_i64(scalar: &Scalar) -> Option<i64> {
    match scalar {
        Scalar::Long(v) => Some(*v),
        Scalar::Integer(v) => Some(i64::from(*v)),
        Scalar::Short(v) => Some(i64::from(*v)),
        Scalar::Byte(v) => Some(i64::from(*v)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::ptr;

    use delta_kernel::expressions::Scalar;
    use delta_kernel::schema::DataType;

    use super::*;
    use crate::expressions::pruning::tests::FixedBatchStats;
    use crate::TryFromStringSlice;

    #[test]
    fn batch_stats_accessor_round_trips_numeric_getters() {
        let stats_provider = FixedBatchStats {
            min: Some(Scalar::Long(7)),
            max: Some(Scalar::Long(42)),
            nulls: Some(3),
            rows: Some(100),
        };
        let acc = BatchStatsAccessor::new(&stats_provider);
        let col_name = "any_col";

        let mut min_out = 0_i64;
        assert!(unsafe {
            batch_stats_min_long(&acc, 0, kernel_string_slice!(col_name), &mut min_out)
        });
        assert_eq!(min_out, 7);

        let mut max_out = 0_i64;
        assert!(unsafe {
            batch_stats_max_long(&acc, 0, kernel_string_slice!(col_name), &mut max_out)
        });
        assert_eq!(max_out, 42);

        let mut null_out = 0_i64;
        assert!(unsafe {
            batch_stats_null_count(&acc, 0, kernel_string_slice!(col_name), &mut null_out)
        });
        assert_eq!(null_out, 3);

        let mut row_out = 0_i64;
        assert!(unsafe { batch_stats_row_count(&acc, 0, &mut row_out) });
        assert_eq!(row_out, 100);
    }

    #[test]
    fn batch_stats_accessor_string_getters_round_trip() {
        let stats_provider = FixedBatchStats {
            min: Some(Scalar::String("apple".to_string())),
            max: Some(Scalar::String("zebra".to_string())),
            nulls: None,
            rows: None,
        };
        let acc = BatchStatsAccessor::new(&stats_provider);
        let col_name = "any_col";

        let mut out = KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        };
        assert!(unsafe {
            batch_stats_min_string(&acc, 0, kernel_string_slice!(col_name), &mut out)
        });
        assert_eq!(unsafe { String::try_from_slice(&out) }.unwrap(), "apple");

        assert!(unsafe {
            batch_stats_max_string(&acc, 0, kernel_string_slice!(col_name), &mut out)
        });
        assert_eq!(unsafe { String::try_from_slice(&out) }.unwrap(), "zebra");
    }

    #[test]
    fn batch_stats_accessor_returns_false_when_stat_absent() {
        let stats_provider = FixedBatchStats {
            min: None,
            max: None,
            nulls: None,
            rows: None,
        };
        let acc = BatchStatsAccessor::new(&stats_provider);
        let col_name = "any_col";

        let mut out = 0_i64;
        assert!(!unsafe {
            batch_stats_min_long(&acc, 0, kernel_string_slice!(col_name), &mut out)
        });
        assert!(!unsafe { batch_stats_row_count(&acc, 0, &mut out) });
    }

    // === Integer widening through batch_stats_min_long / max_long =================

    #[test]
    fn batch_stats_min_long_widens_integer_scalar() {
        let stats_provider = FixedBatchStats {
            min: Some(Scalar::Integer(5_i32)),
            max: Some(Scalar::Integer(7_i32)),
            nulls: None,
            rows: None,
        };
        let acc = BatchStatsAccessor::new(&stats_provider);
        let col_name = "c";
        let mut min_out = 0_i64;
        assert!(unsafe {
            batch_stats_min_long(&acc, 0, kernel_string_slice!(col_name), &mut min_out)
        });
        assert_eq!(min_out, 5);
        let mut max_out = 0_i64;
        assert!(unsafe {
            batch_stats_max_long(&acc, 0, kernel_string_slice!(col_name), &mut max_out)
        });
        assert_eq!(max_out, 7);
    }

    #[test]
    fn batch_stats_min_long_widens_short_scalar() {
        let stats_provider = FixedBatchStats {
            min: Some(Scalar::Short(5_i16)),
            max: Some(Scalar::Short(7_i16)),
            nulls: None,
            rows: None,
        };
        let acc = BatchStatsAccessor::new(&stats_provider);
        let col_name = "c";
        let mut min_out = 0_i64;
        assert!(unsafe {
            batch_stats_min_long(&acc, 0, kernel_string_slice!(col_name), &mut min_out)
        });
        assert_eq!(min_out, 5);
        let mut max_out = 0_i64;
        assert!(unsafe {
            batch_stats_max_long(&acc, 0, kernel_string_slice!(col_name), &mut max_out)
        });
        assert_eq!(max_out, 7);
    }

    #[test]
    fn batch_stats_min_long_widens_byte_scalar() {
        let stats_provider = FixedBatchStats {
            min: Some(Scalar::Byte(5_i8)),
            max: Some(Scalar::Byte(7_i8)),
            nulls: None,
            rows: None,
        };
        let acc = BatchStatsAccessor::new(&stats_provider);
        let col_name = "c";
        let mut min_out = 0_i64;
        assert!(unsafe {
            batch_stats_min_long(&acc, 0, kernel_string_slice!(col_name), &mut min_out)
        });
        assert_eq!(min_out, 5);
        let mut max_out = 0_i64;
        assert!(unsafe {
            batch_stats_max_long(&acc, 0, kernel_string_slice!(col_name), &mut max_out)
        });
        assert_eq!(max_out, 7);
    }

    // === Null-pointer guards on the scalar getters ===============================

    #[test]
    fn batch_stats_getters_handle_null_acc_and_out_pointers() {
        // With a valid accessor we still get false when out is null.
        let stats_provider = FixedBatchStats {
            min: Some(Scalar::Long(1)),
            max: Some(Scalar::Long(2)),
            nulls: Some(0),
            rows: Some(1),
        };
        let acc = BatchStatsAccessor::new(&stats_provider);
        let col_name = "c";

        // Null acc returns false for every getter.
        let mut long_out = 0_i64;
        let mut str_out = KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        };
        assert!(!unsafe {
            batch_stats_min_long(
                std::ptr::null(),
                0,
                kernel_string_slice!(col_name),
                &mut long_out,
            )
        });
        assert!(!unsafe {
            batch_stats_max_long(
                std::ptr::null(),
                0,
                kernel_string_slice!(col_name),
                &mut long_out,
            )
        });
        assert!(!unsafe {
            batch_stats_min_string(
                std::ptr::null(),
                0,
                kernel_string_slice!(col_name),
                &mut str_out,
            )
        });
        assert!(!unsafe {
            batch_stats_max_string(
                std::ptr::null(),
                0,
                kernel_string_slice!(col_name),
                &mut str_out,
            )
        });
        assert!(!unsafe {
            batch_stats_null_count(
                std::ptr::null(),
                0,
                kernel_string_slice!(col_name),
                &mut long_out,
            )
        });
        assert!(!unsafe { batch_stats_row_count(std::ptr::null(), 0, &mut long_out) });

        // Null out returns false for every getter (using a valid accessor).
        assert!(!unsafe {
            batch_stats_min_long(
                &acc,
                0,
                kernel_string_slice!(col_name),
                std::ptr::null_mut(),
            )
        });
        assert!(!unsafe {
            batch_stats_max_long(
                &acc,
                0,
                kernel_string_slice!(col_name),
                std::ptr::null_mut(),
            )
        });
        assert!(!unsafe {
            batch_stats_min_string(
                &acc,
                0,
                kernel_string_slice!(col_name),
                std::ptr::null_mut(),
            )
        });
        assert!(!unsafe {
            batch_stats_max_string(
                &acc,
                0,
                kernel_string_slice!(col_name),
                std::ptr::null_mut(),
            )
        });
        assert!(!unsafe {
            batch_stats_null_count(
                &acc,
                0,
                kernel_string_slice!(col_name),
                std::ptr::null_mut(),
            )
        });
        assert!(!unsafe { batch_stats_row_count(&acc, 0, std::ptr::null_mut()) });
    }

    // === Recording provider verifies DataType passed to provider ==================

    struct RecordingBatchStats {
        last_min: std::cell::RefCell<Option<(usize, String, DataType)>>,
        last_max: std::cell::RefCell<Option<(usize, String, DataType)>>,
    }

    impl BatchStatsProvider for RecordingBatchStats {
        fn min(&self, row: usize, col: &str, dtype: &DataType) -> Option<Scalar> {
            *self.last_min.borrow_mut() = Some((row, col.to_string(), dtype.clone()));
            // Return matching variant so the caller succeeds.
            match dtype {
                d if *d == DataType::LONG => Some(Scalar::Long(0)),
                d if *d == DataType::STRING => Some(Scalar::String(String::new())),
                _ => None,
            }
        }
        fn max(&self, row: usize, col: &str, dtype: &DataType) -> Option<Scalar> {
            *self.last_max.borrow_mut() = Some((row, col.to_string(), dtype.clone()));
            match dtype {
                d if *d == DataType::LONG => Some(Scalar::Long(0)),
                d if *d == DataType::STRING => Some(Scalar::String(String::new())),
                _ => None,
            }
        }
        fn null_count(&self, _row: usize, _col: &str) -> Option<i64> {
            Some(0)
        }
        fn row_count(&self, _row: usize) -> Option<i64> {
            Some(0)
        }
    }

    #[test]
    fn ffi_getters_pass_expected_datatype_to_provider() {
        let provider = RecordingBatchStats {
            last_min: std::cell::RefCell::new(None),
            last_max: std::cell::RefCell::new(None),
        };
        let acc = BatchStatsAccessor::new(&provider);
        let col_name = "c";

        let mut long_out = 0_i64;
        assert!(unsafe {
            batch_stats_min_long(&acc, 0, kernel_string_slice!(col_name), &mut long_out)
        });
        let recorded = provider.last_min.borrow().clone().unwrap();
        assert_eq!(recorded.0, 0);
        assert_eq!(recorded.1, "c");
        assert_eq!(recorded.2, DataType::LONG);

        assert!(unsafe {
            batch_stats_max_long(&acc, 0, kernel_string_slice!(col_name), &mut long_out)
        });
        let recorded = provider.last_max.borrow().clone().unwrap();
        assert_eq!(recorded.2, DataType::LONG);

        let mut str_out = KernelStringSlice {
            ptr: ptr::null(),
            len: 0,
        };
        assert!(unsafe {
            batch_stats_min_string(&acc, 0, kernel_string_slice!(col_name), &mut str_out)
        });
        let recorded = provider.last_min.borrow().clone().unwrap();
        assert_eq!(recorded.2, DataType::STRING);

        assert!(unsafe {
            batch_stats_max_string(&acc, 0, kernel_string_slice!(col_name), &mut str_out)
        });
        let recorded = provider.last_max.borrow().clone().unwrap();
        assert_eq!(recorded.2, DataType::STRING);
    }
}
