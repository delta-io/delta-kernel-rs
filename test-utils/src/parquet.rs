//! Helpers for writing toy parquet files and producing [`FileMeta`] for them.
//!
//! Used by engine integration tests that need to materialize a tiny on-disk parquet file as a
//! scan source.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int64Array, RecordBatch};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
use delta_kernel::FileMeta;
use url::Url;

/// Build a [`FileMeta`] pointing at a local parquet file.
///
/// Reads `path`'s size from the filesystem and produces a `file://` URL. Panics if `path` does
/// not exist or cannot be converted to a URL.
pub fn file_meta(path: &Path) -> FileMeta {
    FileMeta::new(
        Url::from_file_path(path).expect("path is absolute"),
        0,
        std::fs::metadata(path).expect("file exists").len(),
    )
}

/// Write a single-row-group parquet file with one non-null `Int64` column named `field`.
///
/// The schema is `(field: Int64, NOT NULL)`. Used by scan/parity tests to seed a tiny on-disk
/// parquet input.
pub fn write_i64_parquet(path: &Path, field: &str, values: &[i64]) {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        field,
        ArrowDataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(
            values.iter().copied(),
        ))],
    )
    .expect("record batch");
    let file = File::create(path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
}
