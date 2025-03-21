//! EngineData related ffi code

#[cfg(feature = "default-engine")]
use delta_kernel::arrow::array::{
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ArrayData, StructArray,
};
#[cfg(feature = "default-engine")]
use delta_kernel::DeltaResult;
use delta_kernel::EngineData;
use std::ffi::c_void;

use crate::ExclusiveEngineData;
#[cfg(feature = "default-engine")]
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
#[cfg(feature = "default-engine")]
#[repr(C)]
pub struct ArrowFFIData {
    pub array: FFI_ArrowArray,
    pub schema: FFI_ArrowSchema,
}

// TODO: This should use a callback to avoid having to have the engine free the struct
/// Get an [`ArrowFFIData`] to allow binding to the arrow [C Data
/// Interface](https://arrow.apache.org/docs/format/CDataInterface.html). This includes the data and
/// the schema. If this function returns an `Ok` variant the _engine_ must free the returned struct.
///
/// # Safety
/// data_handle must be a valid ExclusiveEngineData as read by the
/// [`delta_kernel::engine::default::DefaultEngine`] obtained from `get_default_engine`.
#[cfg(feature = "default-engine")]
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
#[cfg(feature = "default-engine")]
fn get_raw_arrow_data_impl(data: Box<dyn EngineData>) -> DeltaResult<*mut ArrowFFIData> {
    let record_batch: delta_kernel::arrow::array::RecordBatch = data
        .into_any()
        .downcast::<delta_kernel::engine::arrow_data::ArrowEngineData>()
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
