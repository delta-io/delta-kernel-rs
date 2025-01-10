//! # The Default Engine
//!
//! The default implementation of [`Engine`] is [`DefaultEngine`].
//!
//! The underlying implementations use asynchronous IO. Async tasks are run on
//! a separate thread pool, provided by the [`TaskExecutor`] trait. Read more in
//! the [executor] module.

use std::collections::HashMap;
use std::sync::Arc;

use self::storage::parse_url_opts;
use object_store::{path::Path, DynObjectStore};
use url::Url;

use self::executor::TaskExecutor;
use self::filesystem::ObjectStoreFileSystemClient;
use self::json::DefaultJsonHandler;
use self::parquet::DefaultParquetHandler;
use self::parquet2::AsyncParquetHandler;
use super::arrow_data::ArrowEngineData;
use super::arrow_expression::ArrowExpressionHandler;
use crate::schema::Schema;
use crate::transaction::WriteContext;
use crate::{
    DeltaResult, Engine, EngineData, ExpressionHandler, FileSystemClient, JsonHandler,
    ParquetHandler,
};

pub mod executor;
pub mod file_stream;
pub mod filesystem;
pub mod json;
pub mod parquet;
pub mod parquet2;
pub mod storage;

// #[derive(Debug)]
pub struct DefaultEngine<E: TaskExecutor> {
    store: Arc<DynObjectStore>,
    file_system: Arc<ObjectStoreFileSystemClient<E>>,
    json: Arc<DefaultJsonHandler<E>>,
    parquet: Arc<dyn ParquetHandler>,
    expression: Arc<ArrowExpressionHandler>,
}

impl<E: TaskExecutor> DefaultEngine<E> {
    /// Create a new [`DefaultEngine`] instance
    ///
    /// # Parameters
    ///
    /// - `table_root`: The URL of the table within storage.
    /// - `options`: key/value pairs of options to pass to the object store.
    /// - `task_executor`: Used to spawn async IO tasks. See [executor::TaskExecutor].
    pub fn try_new<K, V>(
        table_root: &Url,
        options: impl IntoIterator<Item = (K, V)>,
        task_executor: Arc<E>,
    ) -> DeltaResult<Self>
    where
        K: AsRef<str>,
        V: Into<String>,
    {
        // table root is the path of the table in the ObjectStore
        let (store, table_root) = parse_url_opts(table_root, options)?;
        Ok(Self::new(Arc::new(store), table_root, task_executor))
    }

    /// Create a new [`DefaultEngine`] instance
    ///
    /// # Parameters
    ///
    /// - `store`: The object store to use.
    /// - `table_root_path`: The root path of the table within storage.
    /// - `task_executor`: Used to spawn async IO tasks. See [executor::TaskExecutor].
    pub fn new(store: Arc<DynObjectStore>, table_root: Path, task_executor: Arc<E>) -> Self {
        // HACK to check if we're using a LocalFileSystem from ObjectStore. We need this because
        // local filesystem doesn't return a sorted list by default. Although the `object_store`
        // crate explicitly says it _does not_ return a sorted listing, in practice all the cloud
        // implementations actually do:
        // - AWS:
        //   [`ListObjectsV2`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
        //   states: "For general purpose buckets, ListObjectsV2 returns objects in lexicographical
        //   order based on their key names." (Directory buckets are out of scope for now)
        // - Azure: Docs state
        //   [here](https://learn.microsoft.com/en-us/rest/api/storageservices/enumerating-blob-resources):
        //   "A listing operation returns an XML response that contains all or part of the requested
        //   list. The operation returns entities in alphabetical order."
        // - GCP: The [main](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) doc
        //   doesn't indicate order, but [this
        //   page](https://cloud.google.com/storage/docs/xml-api/get-bucket-list) does say: "This page
        //   shows you how to list the [objects](https://cloud.google.com/storage/docs/objects) stored
        //   in your Cloud Storage buckets, which are ordered in the list lexicographically by name."
        // So we just need to know if we're local and then if so, we sort the returned file list in
        // `filesystem.rs`
        let store_str = format!("{}", store);
        let is_local = store_str.starts_with("LocalFileSystem");
        let parquet = Arc::new(DefaultParquetHandler::new(
            store.clone(),
            task_executor.clone(),
        ));
        let parquet = Arc::new(AsyncParquetHandler::new(store.clone()));

        Self {
            file_system: Arc::new(ObjectStoreFileSystemClient::new(
                store.clone(),
                !is_local,
                table_root,
                task_executor.clone(),
            )),
            json: Arc::new(DefaultJsonHandler::new(
                store.clone(),
                task_executor.clone(),
            )),
            parquet,
            store,
            expression: Arc::new(ArrowExpressionHandler {}),
        }
    }

    pub fn get_object_store_for_url(&self, _url: &Url) -> Option<Arc<DynObjectStore>> {
        Some(self.store.clone())
    }

    pub async fn write_parquet(
        &self,
        data: &ArrowEngineData,
        write_context: &WriteContext,
        _partition_values: HashMap<String, String>,
        _data_change: bool,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let transform = write_context.logical_to_physical();
        let input_schema: Schema = data.record_batch().schema().try_into()?;
        let output_schema = write_context.schema();
        let logical_to_physical_expr = self.get_expression_handler().get_evaluator(
            input_schema.into(),
            transform.clone(),
            output_schema.clone().into(),
        );
        let _physical_data = logical_to_physical_expr.evaluate(data)?;
        unimplemented!()
        // self.parquet
        //     .write_parquet_file(
        //         write_context.target_dir(),
        //         physical_data,
        //         partition_values,
        //         data_change,
        //     )
        //     .await
    }
}

impl<E: TaskExecutor> Engine for DefaultEngine<E> {
    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
        self.expression.clone()
    }

    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
        self.file_system.clone()
    }

    fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::FileMeta;
    use crate::Url;

    #[test]
    fn test_big_parquet_read() {
        let files = [
            "part-00000-1eed20e0-ffed-42ec-9459-cd7e9920ae86-c000.snappy.parquet",
            "part-00001-e7571634-7b4e-4b36-aa8b-1dabd2023b1b-c000.snappy.parquet",
            "part-00002-b5002f7e-c190-4a8d-aac5-73699d4708c6-c000.snappy.parquet",
            "part-00003-76e19cf6-40c5-4eb1-82aa-7e4eb1a05461-c000.snappy.parquet",
            "part-00004-b97e958b-287d-4d2e-a5cc-61ee5313f5cb-c000.snappy.parquet",
            "part-00005-3e43af0a-cc9d-4d4a-98da-c64e2f3f1c0c-c000.snappy.parquet",
            "part-00006-2db9d154-8105-423a-9789-86db387d5d52-c000.snappy.parquet",
            "part-00007-8be8674c-8113-40a0-bea8-14720ebe0334-c000.snappy.parquet",
            "part-00008-9dda491d-f90e-4780-ace5-ff7f9a509288-c000.snappy.parquet",
            "part-00009-eb0301f3-4e74-47f3-b175-9e5cb13c6b25-c000.snappy.parquet",
            "part-00010-e4d63187-a542-4d08-ae48-030a2f16d4b5-c000.snappy.parquet",
            "part-00011-78bf0375-5c6e-4716-96ed-f46be99f299b-c000.snappy.parquet",
            "part-00012-eb0e01b4-2c14-41ba-a423-ad9c4dd8d59e-c000.snappy.parquet",
            "part-00013-509ea10e-ce58-4e72-83d8-5c384cce6f24-c000.snappy.parquet",
            "part-00014-dbe072f7-6de5-417a-a0cc-6580b71863f6-c000.snappy.parquet",
            "part-00015-d32aaafc-0c0b-4549-bd86-9b00aaa87c7d-c000.snappy.parquet",
            "part-00016-6752753d-d6fd-46d0-ad60-93891b7e650e-c000.snappy.parquet",
            "part-00017-0a813b31-f101-4446-b035-5716e3856a63-c000.snappy.parquet",
            "part-00018-41c0dc65-15de-4340-bc2c-3135094168d8-c000.snappy.parquet",
            "part-00019-d242aa3b-30aa-42c7-a68d-7afb39e25e92-c000.snappy.parquet",
            "part-00020-5ffce99f-e8af-457a-b948-b7b0848007a3-c000.snappy.parquet",
            "part-00021-a6a7e366-88ea-473e-b8c1-3836926a2692-c000.snappy.parquet",
            "part-00022-df1f04f7-e310-4e7d-b696-13ea8b96f208-c000.snappy.parquet",
            "part-00023-12fdef69-9e84-4c1e-b70a-b0efe351cbed-c000.snappy.parquet",
            "part-00024-5ea7aca5-e3bf-4bb4-a151-11700110bc88-c000.snappy.parquet",
            "part-00025-939fe0df-9b5b-4686-b9e6-6663868dd0e0-c000.snappy.parquet",
            "part-00026-53fbc359-37c5-43c4-8893-0e5de66ebc8a-c000.snappy.parquet",
            "part-00027-0e67a127-09cc-4566-b2c6-e68acd09ac86-c000.snappy.parquet",
            "part-00028-8a1bd6c4-8793-4f2c-8457-c33498f0d58a-c000.snappy.parquet",
            "part-00029-0e81d1fc-51f5-4027-bb1e-fd78322ced9f-c000.snappy.parquet",
            "part-00030-5e1e7572-8502-4c40-a9a7-bb6e1ece7d4f-c000.snappy.parquet",
            "part-00031-3e1e2862-fffa-4cf1-877a-d71bb7154e1d-c000.snappy.parquet",
            "part-00032-69f795b5-3b14-47dc-a838-1beb630d3248-c000.snappy.parquet",
            "part-00033-169c0e30-781a-40f9-8d67-a03a11c5686f-c000.snappy.parquet",
            "part-00034-2c1543cd-2c96-48d2-8632-368bedc93cf7-c000.snappy.parquet",
            "part-00035-2e3bade5-f50c-476f-9ab7-b2e79c54e3a0-c000.snappy.parquet",
            "part-00036-e0fde4bb-1a68-4c14-93d4-b4faf6a69523-c000.snappy.parquet",
            "part-00037-3ccd9985-7635-404f-886c-5e930cdcf391-c000.snappy.parquet",
            "part-00038-24e21c26-b016-4377-b2c4-f9a36c4b54d3-c000.snappy.parquet",
            "part-00039-a18c6f27-751e-4e7c-9b46-879be8e218dc-c000.snappy.parquet",
            "part-00040-e7a9fad5-72d5-4ea0-81dd-d7d567004e5f-c000.snappy.parquet",
            "part-00041-291b7825-a746-44b6-90d4-5e7fe25d9860-c000.snappy.parquet",
            "part-00042-13a834db-1ca8-41ba-b506-3c1b49a3433c-c000.snappy.parquet",
            "part-00043-d8bc1430-0ecf-417d-aca1-2cf84b228ba2-c000.snappy.parquet",
            "part-00044-808a7ef3-5021-40f7-ab60-fb54028e53a7-c000.snappy.parquet",
            "part-00045-f3caa90c-9ff5-4c30-888c-f6c048a28b7a-c000.snappy.parquet",
            "part-00046-b93fc157-93d0-4d97-bb87-d3c8a7364af3-c000.snappy.parquet",
            "part-00047-b6e433ae-02e1-4d40-9f4f-b44a7d00af35-c000.snappy.parquet",
            "part-00048-934224c8-03a4-4f6d-8a1d-ac3a33be8936-c000.snappy.parquet",
            "part-00049-35fadcf8-dd37-4371-87b9-ecb20b3b10ca-c000.snappy.parquet",
            "part-00050-748f1818-814e-4ee3-9381-155d984fed1f-c000.snappy.parquet",
            "part-00051-75508184-80bf-4c55-a105-a19f6b4cc325-c000.snappy.parquet",
            "part-00052-e5a811c7-1df4-41f0-8e96-88ff10937e5b-c000.snappy.parquet",
            "part-00053-25c84512-0b76-45aa-8303-f73da5aa1a10-c000.snappy.parquet",
            "part-00054-40b6bd80-f9b1-437f-9a61-db509aef4cff-c000.snappy.parquet",
            "part-00055-c03b109b-c0df-4307-949c-63e04444a4bb-c000.snappy.parquet",
            "part-00056-54613c5b-f2ed-4ac7-8366-e49989147a2b-c000.snappy.parquet",
            "part-00057-0b5851af-dc8b-4dff-b2cb-4fa3e4bce90c-c000.snappy.parquet",
            "part-00058-3b289d75-7d06-445f-a282-968e2fadac77-c000.snappy.parquet",
            "part-00059-c0baadc0-cf06-455a-9899-6c1662716c55-c000.snappy.parquet",
            "part-00060-57baf0ce-7c7e-4b74-a696-cf5cfa95478f-c000.snappy.parquet",
            "part-00061-b1b94ff5-a6c8-453d-9a46-89928d00c219-c000.snappy.parquet",
            "part-00062-7fcf3eb1-21a9-47bd-b0c4-b8e9ebfb45f5-c000.snappy.parquet",
            "part-00063-f8c75bb2-6b67-4880-899f-f7049422f8e4-c000.snappy.parquet",
            "part-00064-35f87c88-7ea6-45a1-a8f8-6a969642e8db-c000.snappy.parquet",
            "part-00065-64ff3648-b6b4-4da2-afc5-19fd7f9dda86-c000.snappy.parquet",
            "part-00066-c8112b58-40fc-4224-80a3-c57d50fe6a5d-c000.snappy.parquet",
            "part-00067-86415a13-af6b-46b1-840b-5fe17f99f428-c000.snappy.parquet",
            "part-00068-d88eb4b4-fef3-49ee-a613-3ec371638d14-c000.snappy.parquet",
            "part-00069-38cf2c6d-9c71-4028-a515-58dbacff00d0-c000.snappy.parquet",
            "part-00070-a016a9eb-5493-4522-9072-f6aa6725d071-c000.snappy.parquet",
            "part-00071-169a0900-63b3-4da7-b064-6dbefb5c03ac-c000.snappy.parquet",
            "part-00072-fdc17060-5aab-47a4-8ae3-be7536c441ff-c000.snappy.parquet",
            "part-00073-ff014ad4-b4aa-4627-85c7-5d8309e14716-c000.snappy.parquet",
            "part-00074-db8aad8f-3521-4a01-9802-2fd6dada4a3c-c000.snappy.parquet",
            "part-00075-d4e9b806-677b-468a-808b-152ccbf39cb6-c000.snappy.parquet",
            "part-00076-193b7c62-886d-4d6f-a6b8-f47ffaada74a-c000.snappy.parquet",
            "part-00077-89d3b0ed-455c-4790-b773-22b7fbf508d2-c000.snappy.parquet",
            "part-00078-f254064d-987e-4464-8dc2-370aff954738-c000.snappy.parquet",
            "part-00079-2fd5ce5c-e3fc-4e8b-8753-178f5831f002-c000.snappy.parquet",
            "part-00080-5378d594-1048-4e13-afad-a4ca8f1dc42c-c000.snappy.parquet",
            "part-00081-9b4f2e4a-ec98-4876-8c0f-3f5944589908-c000.snappy.parquet",
            "part-00082-027c187c-1c55-4de2-b0b4-296ec58c1ba7-c000.snappy.parquet",
            "part-00083-11f7de5e-9421-4b40-acbb-5c7f98fd200e-c000.snappy.parquet",
            "part-00084-f2af393c-0dbd-4d3b-93cf-a92ce2de43dc-c000.snappy.parquet",
            "part-00085-61562868-01a3-45fc-be1e-524ff1bbcca9-c000.snappy.parquet",
            "part-00086-487df01a-4b4c-4dc9-8c6c-b9dd2b65d269-c000.snappy.parquet",
            "part-00087-812f8992-a896-44e5-aba4-f505abb3492a-c000.snappy.parquet",
            "part-00088-c8e927e4-2510-4e43-aefe-9bc86e1b2371-c000.snappy.parquet",
            "part-00089-2ba2c768-300b-48ee-ab81-dcb5716c8a72-c000.snappy.parquet",
            "part-00090-bdeca86e-ce91-45ea-b82a-95a266baeaa4-c000.snappy.parquet",
            "part-00091-41d71af6-5e80-4dee-b068-9154d90e120e-c000.snappy.parquet",
            "part-00092-53359e14-81de-40ea-85d4-1ac0f8df2e00-c000.snappy.parquet",
            "part-00093-454ab78c-ed1a-4a9d-b7b1-064ea5f49747-c000.snappy.parquet",
            "part-00094-a02961df-d6dc-49c1-b6af-7490db161a78-c000.snappy.parquet",
            "part-00095-a8f95229-df71-4795-9788-6ddeeb4c00c1-c000.snappy.parquet",
            "part-00096-a1c24e11-51ef-41b5-ad8b-d219731b37d0-c000.snappy.parquet",
            "part-00097-8ca73b6e-8f95-4e47-a5c9-f5a2db6bb406-c000.snappy.parquet",
            "part-00098-4b8e9732-a65b-4e33-ab13-ec66e9323300-c000.snappy.parquet",
            "part-00099-61972ac8-b84a-4bea-adc1-42e614cb7e47-c000.snappy.parquet",
        ];

        let files: Vec<FileMeta> = files
            .iter()
            .map(|f| {
                let location = Url::parse(&format!(
                    "file:///Users/zach.schuermann/Desktop/100_file_table/{}",
                    f
                ))
                .unwrap();
                FileMeta {
                    location,
                    size: 0,
                    last_modified: 0,
                }
            })
            .collect();

        let store = Arc::new(object_store::local::LocalFileSystem::new());
        use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
        let engine = super::DefaultEngine::new(
            store,
            object_store::path::Path::from_filesystem_path(
                "/Users/zach.schuermann/Desktop/100_file_table/",
            )
            .unwrap(),
            Arc::new(TokioBackgroundExecutor::new()),
        );

        use crate::Engine;

        // schema is just id int
        let schema = Arc::new(crate::schema::StructType::new(vec![
            crate::schema::StructField::new("id", crate::schema::DataType::LONG, false),
        ]));

        let res = engine
            .get_parquet_handler()
            .read_parquet_files(&files, schema, None)
            .unwrap();

        println!("read {:?} rows", res.map(|r| r.unwrap().len()).sum::<usize>());
    }
}
