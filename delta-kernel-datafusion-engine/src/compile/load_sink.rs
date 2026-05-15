//! [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load) helper kept after the
//! physical-compile sweep: the executor calls [`physical_read_schema`] to learn which columns
//! the per-row parquet/json reader should request.

use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::LoadSink;

pub(crate) fn physical_read_schema(
    load: &LoadSink,
) -> Result<delta_kernel::schema::SchemaRef, DeltaError> {
    Ok(load.file_schema.clone())
}
