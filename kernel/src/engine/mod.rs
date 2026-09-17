//! Engine infrastructure shared by `Engine` implementations.
//!
//! The default Arrow/Tokio engine lives in the separate `delta_kernel_default_engine` crate.
//! `SyncEngine` is included only in test builds.

#[cfg(feature = "arrow-expression")]
use delta_kernel_derive::internal_api;

#[cfg(feature = "arrow-expression")]
use crate::parquet::arrow::arrow_reader::ArrowReaderOptions;
#[cfg(feature = "arrow-expression")]
use crate::parquet::arrow::arrow_writer::ArrowWriterOptions;
#[cfg(feature = "arrow-expression")]
use crate::parquet::basic::Compression;
#[cfg(feature = "arrow-expression")]
use crate::parquet::file::properties::WriterProperties;
#[cfg(feature = "arrow-expression")]
use crate::table_properties::{ParquetCompressionCodec, ParquetWriterConfig};

/// Returns the standard [`ArrowReaderOptions`] for all default engine parquet reads.
///
/// Skipping the embedded Arrow IPC schema avoids dependence on Arrow-specific metadata and
/// ensures that type resolution is driven by the kernel schema rather than the file's schema.
#[cfg(feature = "arrow-expression")]
#[internal_api]
pub(crate) fn reader_options() -> ArrowReaderOptions {
    ArrowReaderOptions::new().with_skip_arrow_metadata(true)
}

/// Maps a Delta [`ParquetCompressionCodec`] onto the Arrow/parquet [`Compression`] the writer
/// emits.
///
/// Lives here (not in an engine crate) because kernel owns [`ParquetCompressionCodec`]: the orphan
/// rule forbids either engine crate from implementing this conversion on the foreign
/// [`Compression`].
#[cfg(feature = "arrow-expression")]
impl From<ParquetCompressionCodec> for Compression {
    fn from(codec: ParquetCompressionCodec) -> Self {
        match codec {
            ParquetCompressionCodec::Snappy => Compression::SNAPPY,
            ParquetCompressionCodec::Zstd => Compression::ZSTD(Default::default()),
            ParquetCompressionCodec::Uncompressed => Compression::UNCOMPRESSED,
            ParquetCompressionCodec::Gzip => Compression::GZIP(Default::default()),
            ParquetCompressionCodec::Lz4 => Compression::LZ4,
            ParquetCompressionCodec::Lz4Raw => Compression::LZ4_RAW,
        }
    }
}

/// Returns the standard [`ArrowWriterOptions`] for all kernel parquet writes.
///
/// Sets the compression codec from `config` and omits the Arrow IPC schema from the file metadata,
/// which keeps Delta files interoperable with non-Arrow readers and avoids encoding Arrow-specific
/// type information.
#[cfg(feature = "arrow-expression")]
#[internal_api]
pub(crate) fn writer_options(config: &ParquetWriterConfig) -> ArrowWriterOptions {
    let props = WriterProperties::builder()
        .set_compression(config.compression.into())
        .build();
    ArrowWriterOptions::new()
        .with_properties(props)
        .with_skip_arrow_metadata(true)
}
#[cfg(feature = "arrow-conversion")]
pub mod arrow_conversion;

#[cfg(all(feature = "arrow-expression", feature = "default-engine-base"))]
pub mod arrow_expression;
#[cfg(all(feature = "arrow-expression", feature = "internal-api"))]
pub mod arrow_utils;
#[cfg(all(feature = "arrow-expression", not(feature = "internal-api")))]
pub(crate) mod arrow_utils;
#[cfg(all(feature = "internal-api", feature = "arrow-expression"))]
pub use self::arrow_utils::{parse_json, to_json_bytes};

// The plan executor support modules read Arrow data (`arrow_utils`, `arrow_data`), so they
// require the Arrow engine base in addition to the declarative-plans IR.
#[cfg(all(feature = "declarative-plans", feature = "default-engine-base"))]
pub mod plans;

#[cfg(test)]
pub(crate) mod sync;

#[cfg(test)]
pub(crate) mod test_delegating;

#[cfg(feature = "default-engine-base")]
pub mod arrow_data;
#[cfg(feature = "default-engine-base")]
pub(crate) mod arrow_get_data;

#[cfg(all(feature = "default-engine-base", feature = "internal-api"))]
pub mod ensure_data_types;
#[cfg(all(feature = "default-engine-base", not(feature = "internal-api")))]
pub(crate) mod ensure_data_types;
#[cfg(feature = "default-engine-base")]
// module is always pub; trait inside is gated by #[internal_api]
pub mod parquet_row_group_skipping;
#[cfg(all(test, feature = "default-engine-base"))]
pub(crate) mod test_utils;
