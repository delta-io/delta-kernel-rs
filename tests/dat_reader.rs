use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use deltakernel::client::DefaultTableClient;
use deltakernel::{Table, Version};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct TableVersionMetaData {
    version: Version,
    properties: HashMap<String, String>,
    min_reader_version: u32,
    min_writer_version: u32,
}

datatest_stable::harness!(
    reader_test,
    "tests/dat/out/reader_tests/generated/",
    r"test_case_info\.json"
);
fn reader_test(path: &Path) -> datatest_stable::Result<()> {
    let root_dir = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        path.parent().unwrap().to_str().unwrap()
    );
    let expected_tvm_path = format!("{}/expected/latest/table_version_metadata.json", root_dir);
    let file = File::open(expected_tvm_path).expect("Oops");
    let reader = BufReader::new(file);
    let expected_tvm: TableVersionMetaData = serde_json::from_reader(reader).unwrap();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let path =
                std::fs::canonicalize(PathBuf::from(format!("{}/delta/", root_dir))).unwrap();
            let url = url::Url::from_directory_path(path).unwrap();
            let table_client = Arc::new(
                DefaultTableClient::try_new(&url, std::iter::empty::<(&str, &str)>()).unwrap(),
            );

            // assert correct version is loaded
            let table = Table::new(url, table_client);
            let snapshot = table.snapshot(None).await.unwrap();
            assert_eq!(snapshot.version(), expected_tvm.version);

            // assert correct metadata is read
            let metadata = snapshot.metadata().await.unwrap();
            let protocol = snapshot.protocol().await.unwrap();
            let tvm = TableVersionMetaData {
                version: snapshot.version(),
                properties: metadata
                    .configuration
                    .into_iter()
                    .map(|(k, v)| (k, v.unwrap()))
                    .collect(),
                min_reader_version: protocol.min_reader_version as u32,
                min_writer_version: protocol.min_wrriter_version as u32,
            };
            assert_eq!(tvm, expected_tvm);
        });
    Ok(())
}
