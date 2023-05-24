use crate::delta_log::{parse_record_batch, DataFile};
use crate::parquet_reader::arrow_parquet_reader::ArrowParquetReader;
use crate::parquet_reader::ParquetReader;
use crate::storage::StorageClient;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use bytes::Bytes;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::debug;

#[derive(Debug)]
pub struct DeltaReader {
    location: PathBuf,
}

impl<'a> DeltaReader {
    pub(crate) fn new(location: PathBuf) -> Self {
        DeltaReader { location }
    }

    /// given a record batch representing files to read (path, dv, partition values, etc.), this
    /// reads each corresponding scan data file and returns an iterator over valid table rows
    pub fn read_batch<S: StorageClient + 'a>(
        &'a self,
        files_batch: RecordBatch,
        storage_client: &'a S,
    ) -> impl Iterator<Item = Result<RecordBatch, ArrowError>> + 'a {
        // FIXME
        let parquet = Box::leak(Box::new(ArrowParquetReader::default()));
        parse_record_batch(files_batch).flat_map(|action| match action {
            Some(crate::delta_log::Action::AddFile(path, dv)) => ScanDataIterator::new(
                DataFile::new(path, dv),
                self.location.clone(),
                storage_client,
                parquet,
            ),
            _ => panic!("unexpected"),
        })
    }
}

impl RecordBatchReader for ScanDataIterator<'_> {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::new(arrow::datatypes::Schema::empty())
    }
}

/// An iterator over valid arrow record batches for a scan of a Delta table snapshot.
pub(crate) struct ScanDataIterator<'a> {
    batch_iter: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>> + 'a>,
}

impl Debug for ScanDataIterator<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (lower_bound, _) = self.batch_iter.size_hint();
        write!(
            f,
            "Delta scan data iterator over at least {lower_bound} record batches"
        )
    }
}

impl Iterator for ScanDataIterator<'_> {
    type Item = Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.batch_iter.next()
    }
}

impl<'a> ScanDataIterator<'a> {
    pub(crate) fn new<S: StorageClient, P: ParquetReader + 'a>(
        data_file: DataFile,
        location: PathBuf,
        storage_client: &'a S,
        parquet: &'a P,
    ) -> Self {
        // iter over record batches in parquet
        let data_iter = read::<S, P>(&data_file, location, storage_client, parquet).map(Ok);
        ScanDataIterator {
            batch_iter: Box::new(data_iter),
        }
    }
}

pub(crate) fn read<S: StorageClient, P: ParquetReader>(
    data_file: &DataFile,
    location: PathBuf,
    storage_client: &S,
    parquet: &P,
) -> impl Iterator<Item = RecordBatch> {
    // read data from storage
    let mut p = location.clone();
    p.push(data_file.path.clone());
    let data = storage_client.read(&p);
    let bytes = Bytes::from(data);

    // deserialize DV
    let dv = data_file.dv.as_ref().map(|dv| {
        let z = z85::decode(dv).unwrap();
        let uuid = uuid::Uuid::from_slice(&z).unwrap();
        let dv_suffix = format!("deletion_vector_{uuid}.bin");
        let mut dv_path = PathBuf::from(&location);
        dv_path.push(dv_suffix);
        debug!("deletion vector path: {dv_path:?}");
        let dv = std::fs::read(dv_path).unwrap();
        // TODO implement the 64-bit packed version
        roaring::RoaringBitmap::deserialize_from(&dv[21..]).unwrap()
    });
    debug!("deletion vector bitmap: {:?}", dv);

    // parse the parquet file from bytes
    let reader = parquet.read(bytes);
    // apply the DV
    reader.map(move |batch| match &dv {
        Some(dv) => {
            let batch = batch.unwrap();
            let vec: Vec<_> = (0..batch.num_rows())
                .map(|i| {
                    Some(if dv.contains(i.try_into().unwrap()) {
                        false
                    } else {
                        true
                    })
                })
                .collect();
            let dv = arrow::array::BooleanArray::from(vec);
            arrow::compute::filter_record_batch(&batch, &dv).unwrap()
        }
        None => batch.unwrap(),
    })
}
