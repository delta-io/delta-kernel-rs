use datafusion_common::DataFusionError;
use delta_kernel::error::Error as DeltaKernelError;

pub(crate) fn to_df_err(e: DeltaKernelError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
