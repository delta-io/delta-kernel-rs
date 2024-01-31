use std::sync::Arc;

use roaring::RoaringTreemap;

use self::file_stream::log_replay_iter;
use crate::actions::action_definitions::Add;
use crate::expressions::Expression;
use crate::schema::{SchemaRef, StructType};
use crate::snapshot::Snapshot;
use crate::{DeltaResult, EngineClient, EngineData, FileDataReadResultIterator, FileMeta};

mod data_skipping;
pub mod file_stream;

// TODO projection: something like fn select(self, columns: &[&str])
/// Builder to scan a snapshot of a table.
pub struct ScanBuilder {
    snapshot: Arc<Snapshot>,
    schema: Option<SchemaRef>,
    predicate: Option<Expression>,
}

impl std::fmt::Debug for ScanBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("ScanBuilder")
            .field("schema", &self.schema)
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl ScanBuilder {
    /// Create a new [`ScanBuilder`] instance.
    pub fn new(snapshot: Arc<Snapshot>) -> Self {
        Self {
            snapshot,
            schema: None,
            predicate: None,
        }
    }

    /// Provide [`Schema`] for columns to select from the [`Snapshot`].
    ///
    /// A table with columns `[a, b, c]` could have a scan which reads only the first
    /// two columns by using the schema `[a, b]`.
    ///
    /// [`Schema`]: crate::schema::Schema
    /// [`Snapshot`]: crate::snapshot::Snapshot
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Predicates specified in this crate's [`Expression`] type.
    ///
    /// Can be used to filter the rows in a scan. For example, using the predicate
    /// `x < 4` to return a subset of the rows in the scan which satisfy the filter.
    pub fn with_predicate(mut self, predicate: Expression) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Build the [`Scan`].
    ///
    /// This is lazy and performs no 'work' at this point. The [`Scan`] type itself can be used
    /// to fetch the files and associated metadata required to perform actual data reads.
    pub fn build(self) -> Scan {
        // if no schema is provided, use snapshot's entire schema (e.g. SELECT *)
        let read_schema = self
            .schema
            .unwrap_or_else(|| self.snapshot.schema().clone().into());
        Scan {
            snapshot: self.snapshot,
            read_schema,
            predicate: self.predicate,
        }
    }
}

/// Rows can be dropped from a scan due to deletion vectors, so we communicate back both EngineData
/// and information regarding whether a row should be included or not
//pub type ScanResultIter = Box<dyn Iterator<Item = DeltaResult<ScanResult>> + Send>;
pub struct ScanResult {
    pub raw_data: DeltaResult<Box<dyn EngineData>>,
    offset: u64,
    mask: Option<RoaringTreemap>,
}

impl ScanResult {
    pub fn contains(&self, row_index: u64) -> bool {
        match self.mask.as_ref() {
            Some(mask) => {
                let index = row_index + self.offset;
                !mask.contains(index)
            }
            None => true,
        }
    }
}

pub struct Scan {
    snapshot: Arc<Snapshot>,
    read_schema: SchemaRef,
    predicate: Option<Expression>,
}

impl std::fmt::Debug for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Scan")
            .field("schema", &self.read_schema)
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl Scan {
    /// Get a shred reference to the [`Schema`] of the scan.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn schema(&self) -> &SchemaRef {
        &self.read_schema
    }

    /// Get the predicate [`Expression`] of the scan.
    pub fn predicate(&self) -> &Option<Expression> {
        &self.predicate
    }

    /// This is the main method to 'materialize' the scan. It returns a `ScanFileBatchIterator`
    /// which yields record batches of scan files and their associated metadata. Rows of the scan
    /// files batches correspond to data reads, and the DeltaReader is used to materialize the scan
    /// files into actual table data.
    pub fn files(
        &self,
        engine_client: &dyn EngineClient,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<Add>>> {
        let action_schema = Arc::new(StructType::new(vec![
            crate::actions::schemas::ADD_FIELD.clone(),
            crate::actions::schemas::REMOVE_FIELD.clone(),
        ]));

        let log_iter = self.snapshot.log_segment.replay(
            engine_client,
            action_schema,
            self.predicate.clone(),
        )?;

        Ok(log_replay_iter(
            log_iter,
            engine_client.get_data_extactor(),
            &self.read_schema,
            &self.predicate,
        ))
    }

    // TODO: Docs for this, also, return type is... wonky
    pub fn execute(&self, engine_client: &dyn EngineClient) -> DeltaResult<Vec<ScanResult>> {
        let parquet_handler = engine_client.get_parquet_handler();
        let data_extractor = engine_client.get_data_extactor();
        let mut results: Vec<ScanResult> = vec![];
        let files = self.files(engine_client)?;
        for add_result in files {
            let add = add_result?;
            let meta = FileMeta {
                last_modified: add.modification_time,
                size: add.size as usize,
                location: self.snapshot.table_root.join(&add.path)?,
            };
            let read_results =
                parquet_handler.read_parquet_files(&[meta], self.read_schema.clone(), None)?;
            let dv_mask = add
                .deletion_vector
                .as_ref()
                .map(|dv_descriptor| {
                    let fs_client = engine_client.get_file_system_client();
                    dv_descriptor.read(fs_client, self.snapshot.table_root.clone())
                })
                .transpose()?;

            let mut offset = 0;
            for read_result in read_results {
                let len = if let Ok(ref res) = read_result {
                    data_extractor.length(&**res)
                } else {
                    0
                };
                let scan_result = ScanResult {
                    raw_data: read_result,
                    offset,
                    mask: dv_mask.clone(),
                };
                offset += len as u64;
                results.push(scan_result);
            }
        }
        Ok(results)
        // let v: Vec<ScanResultIter> = self
        //     .files(engine_client)?
        //     .flat_map(|res| {
        //         let add = res?;
        //         let meta = FileMeta {
        //             last_modified: add.modification_time,
        //             size: add.size as usize,
        //             location: self.snapshot.table_root.join(&add.path)?,
        //         };

        //         let read_results = parquet_handler.read_parquet_files(&[meta], self.read_schema.clone(), None);
        //         let dv_mask = add.deletion_vector.as_ref().map(|dv_descriptor| {
        //             let fs_client = engine_client.get_file_system_client();
        //             dv_descriptor.read(fs_client, self.snapshot.table_root.clone())
        //         }).transpose()?;
        //             //  TODO(nick) settle on a way to communicate the DV

        //             // let mask: BooleanArray = (0..v.len())
        //             //     .map(|i| Some(!dv.contains(i.try_into().expect("fit into u32"))))
        //             //     .collect();
        //             //Ok(Some(filter_record_batch(&batch, &mask)?))
        //         //}
        //         let ret: DeltaResult<Box<dyn Iterator<Item = DeltaResult<ScanResult>>>> = read_results.map(|result| result.map(|data| {
        //             ScanResult {
        //                 raw_data: data,
        //                 offset: 0,
        //                 mask: dv_mask,
        //             }
        //         }));
        //     })
        //     .collect();
        // Ok(v)
        // if batches.is_empty() {
        //     return Ok(None);
        // }
        // let schema = batches[0].schema();
        // let batch = concat_batches(&schema, &batches)?;

        // })
        //.filter_map_ok(|batch| batch)
        //.collect()
    }
}

#[cfg(all(test, feature = "default-client"))]
mod tests {
    use itertools::Itertools;
    use std::path::PathBuf;

    use super::*;
    use crate::simple_client::SimpleClient;
    use crate::Table;

    #[test]
    fn test_scan_files() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine_client = SimpleClient::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine_client, None).unwrap();
        let scan = ScanBuilder::new(snapshot).build();
        let files: Vec<Add> = scan.files(&engine_client).unwrap().try_collect().unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(
            &files[0].path,
            "part-00000-517f5d32-9c95-48e8-82b4-0229cc194867-c000.snappy.parquet"
        );
        //TODO assert!(&files[0].deletion_vector.is_none());
    }

    #[test]
    fn test_scan_data() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine_client = SimpleClient::new();

        let table = Table::new(url);
        let snapshot = table.snapshot(&engine_client, None).unwrap();
        let scan = ScanBuilder::new(snapshot).build();
        let files = scan.execute(&engine_client).unwrap();

        assert_eq!(files.len(), 1);
        //assert_eq!(files[0].num_rows(), 10)
    }
}
