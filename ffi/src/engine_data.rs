//! EngineData related ffi code

#[cfg(feature = "default-engine-base")]
use delta_kernel::arrow;
#[cfg(feature = "default-engine-base")]
use delta_kernel::arrow::array::{
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ArrayData, RecordBatch, StructArray,
};
#[cfg(feature = "default-engine-base")]
use delta_kernel::engine::arrow_data::ArrowEngineData;
#[cfg(feature = "default-engine-base")]
use delta_kernel::DeltaResult;
use delta_kernel::EngineData;
use std::ffi::c_void;

#[cfg(feature = "default-engine-base")]
use crate::error::AllocateErrorFn;
use crate::ExclusiveEngineData;
#[cfg(feature = "default-engine-base")]
use crate::{ExternResult, IntoExternResult, SharedExternEngine};

use super::handle::Handle;

/// Get the number of rows in an engine data
///
/// # Safety
/// `data_handle` must be a valid pointer to a kernel allocated `ExclusiveEngineData`
#[no_mangle]
pub unsafe extern "C" fn engine_data_length(data: &mut Handle<ExclusiveEngineData>) -> usize {
    let data = unsafe { data.as_mut() };
    data.len()
}

/// Allow an engine to "unwrap" an [`ExclusiveEngineData`] into the raw pointer for the case it wants
/// to use its own engine data format
///
/// # Safety
///
/// `data_handle` must be a valid pointer to a kernel allocated `ExclusiveEngineData`. The Engine must
/// ensure the handle outlives the returned pointer.
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
}

// TODO: This should use a callback to avoid having to have the engine free the struct
/// Get an [`ArrowFFIData`] to allow binding to the arrow [C Data
/// Interface](https://arrow.apache.org/docs/format/CDataInterface.html). This includes the data and
/// the schema. If this function returns an `Ok` variant the _engine_ must free the returned struct.
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

// TODO: This method leaks the returned pointer memory. How will the engine free it?
#[cfg(feature = "default-engine-base")]
fn get_raw_arrow_data_impl(data: Box<dyn EngineData>) -> DeltaResult<*mut ArrowFFIData> {
    let record_batch: delta_kernel::arrow::array::RecordBatch = data
        .into_any()
        .downcast::<ArrowEngineData>()
        .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
        .into();
    let sa: StructArray = record_batch.into();
    let array_data: ArrayData = sa.into();
    // these call `clone`. is there a way to not copy anything and what exactly are they cloning?
    let array = FFI_ArrowArray::new(&array_data);
    let schema = FFI_ArrowSchema::try_from(array_data.data_type())?;
    let ret_data = Box::new(ArrowFFIData { array, schema });
    Ok(Box::leak(ret_data))
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
