//! Batch stats accessor handed to the engine during file pruning.
//!
//! Exposes the per-batch stats payload to the engine callback. Currently the
//! only view is an Arrow-C-Data interface, gated on `default-engine-base`.

#[cfg(feature = "default-engine-base")]
use crate::engine_data::ArrowFFIData;

/// Accessor handed to the engine during batched file pruning. Exposes an
/// Arrow-C-Data view of the stats batch via [`batch_stats_as_arrow`]
/// (`default-engine-base` only).
pub struct BatchStatsAccessor<'a> {
    /// Optional Arrow-C-Data view; consumed via [`batch_stats_as_arrow`].
    #[cfg(feature = "default-engine-base")]
    arrow_ffi: Option<ArrowFFIData>,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> BatchStatsAccessor<'a> {
    #[allow(dead_code)] // used under default-engine-base
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(feature = "default-engine-base")]
            arrow_ffi: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Attach the Arrow-C-Data view exposed via [`batch_stats_as_arrow`].
    /// `None` leaves it unset.
    #[cfg(feature = "default-engine-base")]
    pub(crate) fn with_arrow_ffi(mut self, arrow_ffi: Option<ArrowFFIData>) -> Self {
        self.arrow_ffi = arrow_ffi;
        self
    }
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
