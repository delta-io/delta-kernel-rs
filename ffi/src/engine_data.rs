//! EngineData related ffi code
use std::ffi::c_void;

#[cfg(feature = "default-engine-base")]
use delta_kernel::arrow;
#[cfg(feature = "default-engine-base")]
use delta_kernel::arrow::array::{
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ArrayData, RecordBatch, StructArray,
};
#[cfg(feature = "default-engine-base")]
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
#[cfg(feature = "default-engine-base")]
use delta_kernel::DeltaResult;
use delta_kernel::EngineData;

use super::handle::Handle;
#[cfg(feature = "default-engine-base")]
use crate::error::AllocateErrorFn;
use crate::ExclusiveEngineData;
#[cfg(feature = "default-engine-base")]
use crate::{ExternResult, IntoExternResult, SharedExternEngine};

/// Get the number of rows in an engine data
///
/// # Safety
/// `data_handle` must be a valid pointer to a kernel allocated `ExclusiveEngineData`
#[no_mangle]
pub unsafe extern "C" fn engine_data_length(data: &mut Handle<ExclusiveEngineData>) -> usize {
    let data = unsafe { data.as_mut() };
    data.len()
}

/// Allow an engine to "unwrap" an [`ExclusiveEngineData`] into the raw pointer for the case it
/// wants to use its own engine data format
///
/// # Safety
///
/// `data_handle` must be a valid pointer to a kernel allocated `ExclusiveEngineData`. The Engine
/// must ensure the handle outlives the returned pointer.
// TODO(frj): What is the engine actually doing with this method?? If we need access to raw extern
// pointers, we will need to define an `ExternEngineData` trait that exposes such capability, along
// with an ExternEngineDataVtable that implements it. See `ExternEngine` and `ExternEngineVtable`
// for examples of how that works.
#[no_mangle]
pub unsafe extern "C" fn get_raw_engine_data(mut data: Handle<ExclusiveEngineData>) -> *mut c_void {
    let ptr = get_raw_engine_data_impl(&mut data) as *mut dyn EngineData;
    ptr as _
}

unsafe fn get_raw_engine_data_impl(data: &mut Handle<ExclusiveEngineData>) -> &mut dyn EngineData {
    let _data = unsafe { data.as_mut() };
    todo!() // See TODO comment for EngineData
}

/// Struct to allow binding to the arrow [C Data
/// Interface](https://arrow.apache.org/docs/format/CDataInterface.html). This includes the data and
/// the schema.
#[cfg(feature = "default-engine-base")]
#[repr(C)]
pub struct ArrowFFIData {
    pub array: FFI_ArrowArray,
    pub schema: FFI_ArrowSchema,
}

#[cfg(feature = "default-engine-base")]
impl ArrowFFIData {
    pub fn empty() -> Self {
        Self {
            array: FFI_ArrowArray::empty(),
            schema: FFI_ArrowSchema::empty(),
        }
    }

    /// Convert opaque engine data into Arrow C Data Interface structs.
    ///
    /// The returned `ArrowFFIData` owns the exported data. The caller is responsible for
    /// either importing it (via `from_ffi`) or dropping it (the `Drop` impls on
    /// `FFI_ArrowArray`/`FFI_ArrowSchema` call their release callbacks).
    pub fn try_from_engine_data(data: Box<dyn EngineData>) -> DeltaResult<Self> {
        let record_batch = data.try_into_record_batch()?;
        let sa: StructArray = record_batch.into();
        let array_data: ArrayData = sa.into();
        let array = FFI_ArrowArray::new(&array_data);
        let schema = FFI_ArrowSchema::try_from(array_data.data_type())?;
        Ok(Self { array, schema })
    }
}

// TODO: This should use a callback to avoid having to have the engine free the struct
/// Get an [`ArrowFFIData`] to allow binding to the arrow [C Data
/// Interface](https://arrow.apache.org/docs/format/CDataInterface.html). This includes the data and
/// the schema. If this function returns an `Ok` variant the _engine_ must free the returned struct
/// via [`free_arrow_ffi_data`] exactly once.
///
/// # Safety
/// data_handle must be a valid ExclusiveEngineData as read by the
/// [`delta_kernel::engine::default::DefaultEngine`] obtained from `get_default_engine`.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn get_raw_arrow_data(
    data: Handle<ExclusiveEngineData>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<*mut ArrowFFIData> {
    // TODO(frj): This consumes the handle. Is that what we really want?
    let data = unsafe { data.into_inner() };
    get_raw_arrow_data_impl(data).into_extern_result(&engine.as_ref())
}

#[cfg(feature = "default-engine-base")]
fn get_raw_arrow_data_impl(data: Box<dyn EngineData>) -> DeltaResult<*mut ArrowFFIData> {
    Ok(Box::into_raw(Box::new(ArrowFFIData::try_from_engine_data(
        data,
    )?)))
}

/// Free an [`ArrowFFIData`] pointer produced by a kernel FFI function (e.g.
/// [`get_raw_arrow_data`] or [`crate::table_changes::scan_table_changes_next`]).
///
/// If the consumer has already imported the inner `FFI_ArrowArray` / `FFI_ArrowSchema` via a
/// foreign Arrow layer (e.g. arrow-glib's `garrow_record_batch_import`), that import has
/// moved ownership of the release callbacks out of the structs; dropping the `Box` here is
/// then a cheap no-op on the arrays. If the consumer has not imported them, the structs'
/// `Drop` impls will call their release callbacks so no memory is leaked.
///
/// A null pointer is a no-op, matching the convention used by
/// [`crate::scan::free_scan_metadata_arrow_result`].
///
/// # Safety
///
/// `result` must be either null, or a pointer returned by a kernel FFI function that produces
/// `*mut ArrowFFIData`. Must be called at most once per non-null pointer.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn free_arrow_ffi_data(result: *mut ArrowFFIData) {
    if result.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(result) };
}

/// Creates engine data from Arrow C Data Interface array and schema.
///
/// Converts the provided Arrow C Data Interface array and schema into delta-kernel's internal
/// engine data format. Note that ownership of the array is transferred to the kernel, whereas the
/// ownership of the schema stays the engine's.
///
/// # Safety
/// - `array` must be a valid FFI_ArrowArray
/// - `schema` must be a valid pointer to a FFI_ArrowSchema
/// - `engine` must be a valid Handle to a SharedExternEngine
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn get_engine_data(
    array: FFI_ArrowArray,
    schema: &FFI_ArrowSchema,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<ExclusiveEngineData>> {
    get_engine_data_impl(array, schema).into_extern_result(&allocate_error)
}

#[cfg(feature = "default-engine-base")]
unsafe fn get_engine_data_impl(
    array: FFI_ArrowArray,
    schema: &FFI_ArrowSchema,
) -> DeltaResult<Handle<ExclusiveEngineData>> {
    let array_data = unsafe { arrow::array::ffi::from_ffi(array, schema) };
    let record_batch: RecordBatch = StructArray::from(array_data?).into();
    let arrow_engine_data: ArrowEngineData = record_batch.into();
    let engine_data: Box<dyn EngineData> = Box::new(arrow_engine_data);
    Ok(engine_data.into())
}

#[cfg(all(test, feature = "default-engine-base"))]
mod tests {
    use std::sync::Arc;

    use delta_kernel::arrow::array::{Array, Int32Array, StructArray};
    use delta_kernel::arrow::datatypes::{DataType, Field};
    use delta_kernel::arrow::ffi::to_ffi;

    use super::*;

    /// Build a one-column `ArrowFFIData` we can use as a free-target. Mirrors the layout
    /// produced by [`ArrowFFIData::try_from_engine_data`] (a struct array exported via
    /// [`to_ffi`]) without going through the engine.
    fn make_arrow_ffi_data() -> ArrowFFIData {
        let column = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let field = Arc::new(Field::new("x", DataType::Int32, true));
        let struct_array = StructArray::from(vec![(field, column as _)]);
        let array_data = struct_array.into_data();
        let (array, schema) = to_ffi(&array_data).expect("to_ffi");
        ArrowFFIData { array, schema }
    }

    #[test]
    fn free_null_is_safe() {
        // Mirrors the convention enforced by `free_scan_metadata_arrow_result`.
        unsafe { free_arrow_ffi_data(std::ptr::null_mut()) };
    }

    #[test]
    fn free_drops_box_and_invokes_release_callbacks() {
        // Verifies the common path: kernel allocates, engine immediately frees. The Drop
        // impls on FFI_ArrowArray / FFI_ArrowSchema invoke their release callbacks; this
        // test passes if there is no double-free / leak under sanitizers.
        let ptr = Box::into_raw(Box::new(make_arrow_ffi_data()));
        unsafe { free_arrow_ffi_data(ptr) };
    }

    #[test]
    fn free_after_consumer_imported_arrays() {
        // Mirrors the read-table example flow: an engine imports the Arrow arrays via
        // `from_ffi` (which moves the release callbacks out, leaving the inner structs in
        // a release-callback-less state), then frees the now-empty box. The Drop impls
        // become no-ops on the consumed structs, but the Box itself still needs reclaiming.
        let mut data = make_arrow_ffi_data();
        // Move the array+schema out via from_ffi -- this is what an engine does to import
        // the data into its own arrow runtime. After this, `data.array` and `data.schema`
        // have been replaced with empty stand-ins whose Drop impls are no-ops.
        let array = std::mem::replace(&mut data.array, FFI_ArrowArray::empty());
        let schema = std::mem::replace(&mut data.schema, FFI_ArrowSchema::empty());
        // Consume them to release the underlying arrow allocations.
        let _ = unsafe { arrow::array::ffi::from_ffi(array, &schema) }.expect("from_ffi");
        // Now free the now-empty box.
        let ptr = Box::into_raw(Box::new(data));
        unsafe { free_arrow_ffi_data(ptr) };
    }
}
