use delta_kernel::arrow::array::{RecordBatch, StructArray};
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::StructType;
use delta_kernel::DeltaResult;
use delta_kernel_default_engine::parquet::DataFileMetadata;
use delta_kernel_default_engine::stats::collect_stats;

#[test]
fn downstream_can_collect_stats_and_inspect_file_metadata() {
    let _collect_stats: fn(&RecordBatch, &[ColumnName], &StructType) -> DeltaResult<StructArray> =
        collect_stats;

    fn inspect_file_metadata(metadata: &DataFileMetadata) {
        let _ = &metadata.file_meta.location;
        let _ = metadata.file_meta.size;
    }

    let _inspect_file_metadata = inspect_file_metadata;
}
