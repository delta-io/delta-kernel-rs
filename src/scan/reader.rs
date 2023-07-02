use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_select::filter::filter_record_batch;
use futures::prelude::*;
use object_store::path::Path;
use object_store::ObjectStore;
use tracing::debug;

use crate::error::{DeltaResult, Error};
use crate::parquet_reader::arrow_parquet_reader::ArrowParquetReader;
use crate::parquet_reader::ParquetReader;
use crate::snapshot::replay::{from_actions_batch, Action, DataFile};

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
        files_stream: impl Stream<Item = DeltaResult<RecordBatch>> + std::marker::Unpin,
        /*
         * Unpin because of the async closure here: https://stackoverflow.com/a/61222654
         */
    ) -> DeltaResult<impl Stream<Item = DeltaResult<RecordBatch>> + std::marker::Unpin> {
        // XXX: parquet should be provided by the caller
        let location = self.location.clone();
        Ok(Box::pin(
            from_actions_batch(files_stream)
                .map(move |action| (action, location.clone(), storage.clone()))
                .map(|(action, location, storage)| async move {
                    match action {
                        Ok(Some(Action::AddFile(path, dv))) => {
                            let parquet = ArrowParquetReader::default();
                            read(DataFile::new(path, dv), location, storage, parquet).await
                        }
                        _ => panic!("Unexpected action from stream! {action:?}"),
                    }
                })
                .then(|a| a)
                .map_ok(stream::iter)
                .try_flatten(),
        ))
    }
}

pub(crate) async fn read<P: ParquetReader>(
    data_file: DataFile,
    location: Path,
    storage: Arc<dyn ObjectStore>,
    parquet: P,
) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>> {
    // read data from storage
    let bytes = storage.get(&data_file.path).await?.bytes().await?;

    // deserialize DV
    let dv = match &data_file.dv {
        Some(dv) => {
            debug!("processing deletion vector for {:?}", data_file);
            let z =
                z85::decode(dv).map_err(|_| Error::DeletionVector("failed to decode,".into()))?;
            let uuid =
                uuid::Uuid::from_slice(&z).map_err(|err| Error::DeletionVector(err.to_string()))?;
            let dv_suffix = format!("deletion_vector_{uuid}.bin");
            let dv_path = location.child(dv_suffix);
            debug!("deletion vector path: {dv_path:?}");
            let dv = storage.get(&dv_path).await?.bytes().await?;
            // TODO implement the 64-bit packed version
            Some(
                roaring::RoaringBitmap::deserialize_from(&dv[21..])
                    .map_err(|err| Error::DeletionVector(err.to_string()))?,
            )
        }
        None => None,
    };
    debug!("deletion vector bitmap: {:?}", dv);

    // parse the parquet file from bytes
    let reader = parquet.read(bytes)?;
    // apply the DV
    Ok(reader.map(move |batch| match &dv {
        Some(dv) => {
            let batch = batch?;
            let vec: Vec<_> = (0..batch.num_rows())
                .map(|i| Some(!dv.contains(i.try_into().expect("fit into u32"))))
                .collect();
            let dv = BooleanArray::from(vec);
            Ok(filter_record_batch(&batch, &dv)?)
        }
        None => Ok(batch?),
    }))
}
