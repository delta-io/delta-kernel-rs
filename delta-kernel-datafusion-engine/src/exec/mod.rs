//! Custom physical operators and table providers for the DataFusion engine.

mod field_id_adapter;
mod file_listing;
mod load_exec;
mod load_helpers;
mod load_provider;
mod metadata_stamper;

pub(crate) use field_id_adapter::FieldIdPhysicalExprAdapterFactory;
pub(crate) use file_listing::FileListingExec;
pub(crate) use load_exec::LoadExec;
pub(crate) use load_provider::LoadTableProvider;
pub(crate) use metadata_stamper::stamp_batch_metadata;
