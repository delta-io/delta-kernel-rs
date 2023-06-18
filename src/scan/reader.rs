use crate::delta_log::{from_actions_batch, Action, DataFile};

use crate::parquet_reader::arrow_parquet_reader::ArrowParquetReader;
use crate::parquet_reader::ParquetReader;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use futures::prelude::*;
use object_store::path::Path;
use object_store::ObjectStore;
use tracing::debug;

use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct DeltaReader {
    location: Path,
}

impl DeltaReader {
    pub(crate) fn new(location: Path) -> Self {
        DeltaReader { location }
    }

    /// Provide a stream of read [RecordBatch] from the data files on the files_stream
    pub fn with_files(
        &self,
        storage: Arc<dyn ObjectStore>,
        files_stream: impl Stream<Item = RecordBatch> + std::marker::Unpin,
        /*
         * Unpin because of the async closure here: https://stackoverflow.com/a/61222654
         */
    ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> + std::marker::Unpin {
        // XXX: parquet should be provided by the caller
        let location = self.location.clone();
        Box::pin(
            from_actions_batch(files_stream)
                .map(move |action| (action, location.clone(), storage.clone()))
                .map(|(action, location, storage)| async move {
                    match action {
                        Some(Action::AddFile(path, dv)) => {
                            let parquet = ArrowParquetReader::default();
                            read(DataFile::new(path, dv), location, storage, parquet).await
                        }
                        _ => panic!("Unexpected action from stream! {action:?}"),
                    }
                })
                .then(|a| a)
                .map(|a| stream::iter(a.map(Ok)))
                .flatten(),
        )
    }
}

pub(crate) async fn read<P: ParquetReader>(
    data_file: DataFile,
    location: Path,
    storage: Arc<dyn ObjectStore>,
    parquet: P,
) -> impl Iterator<Item = RecordBatch> {
    // read data from storage
    let data = storage
        .get(&data_file.path)
        .await
        .expect("Failed to get bytes");
    // XXX: Error needs to bubble up
    let bytes = data.bytes().await.expect("Failed to get bytes for read()");

    // deserialize DV
    let dv = match &data_file.dv {
        Some(dv) => {
            debug!("processing deletion vector for {:?}", data_file);
            let z = z85::decode(dv).unwrap();
            let uuid = uuid::Uuid::from_slice(&z).unwrap();
            let dv_suffix = format!("deletion_vector_{uuid}.bin");
            let dv_path = location.child(dv_suffix);
            debug!("deletion vector path: {dv_path:?}");
            let dv = storage
                .get(&dv_path)
                .await
                .expect("Failed to get deletion vector");
            let dv = dv.bytes().await.expect("Failed to get bytes for the DV");
            // TODO implement the 64-bit packed version
            Some(roaring::RoaringBitmap::deserialize_from(&dv[21..]).unwrap())
        }
        None => None,
    };
    debug!("deletion vector bitmap: {:?}", dv);

    // parse the parquet file from bytes
    let reader = parquet.read(bytes);
    // apply the DV
    reader.map(move |batch| match &dv {
        Some(dv) => {
            let batch = batch.unwrap();
            let vec: Vec<_> = (0..batch.num_rows())
                .map(|i| Some(!dv.contains(i.try_into().unwrap())))
                .collect();
            let dv = arrow::array::BooleanArray::from(vec);
            arrow::compute::filter_record_batch(&batch, &dv).unwrap()
        }
        None => batch.unwrap(),
    })
}
