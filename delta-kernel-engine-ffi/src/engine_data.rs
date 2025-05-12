use crate::{ExternResult, IntoExternResult, SharedExternEngine};
use delta_kernel::arrow::array::{
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ArrayData, StructArray,
};
use delta_kernel::DeltaResult;

/// Struct to allow binding to the arrow [C Data
/// Interface](https://arrow.apache.org/docs/format/CDataInterface.html). This includes the data and
/// the schema.
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
