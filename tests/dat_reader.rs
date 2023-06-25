use deltakernel::Table;
use deltakernel::Version;
use object_store::local::LocalFileSystem;
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
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
    let storage = Arc::new(LocalFileSystem::new_with_prefix(&root_dir)?);
    let expected_tvm_path = format!("{}/expected/latest/table_version_metadata.json", root_dir);
    let file = File::open(expected_tvm_path).expect("Oops");
    let reader = BufReader::new(file);
    let expected_tvm: TableVersionMetaData = serde_json::from_reader(reader).unwrap();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let table = Table::with_store(storage.clone())
                .at("delta")
                .build()
                .expect("Failed to build table");
            let snapshot = table.get_latest_snapshot().await.unwrap();

            assert_eq!(snapshot.version(), expected_tvm.version);
        });
    Ok(())
}
