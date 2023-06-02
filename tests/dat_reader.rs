use delta_kernel::delta_table::DeltaTable;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use test_case::test_case;

use delta_kernel::storage::StorageClient;
use delta_kernel::Version;

// TODO: common to dv.rs
#[derive(Default, Debug, Clone)]
struct LocalStorageClient {}

impl StorageClient for LocalStorageClient {
    fn list(&self, prefix: &str) -> Vec<std::path::PathBuf> {
        let path = PathBuf::from(prefix);
        std::fs::read_dir(path)
            .unwrap()
            // .crc files break this, filter them out
            .filter_map(|entry| match entry {
                Ok(entry) if entry.path().extension().and_then(|s| s.to_str()) != Some("crc") => {
                    Some(entry.path())
                }
                _ => None,
            })
            .collect()
    }

    fn read(&self, path: &std::path::Path) -> Vec<u8> {
        std::fs::read(path).unwrap()
    }
}

#[derive(Serialize, Deserialize)]
struct TableVersionMetaData {
    version: Version,
    properties: HashMap<String, String>,
    min_reader_version: u32,
    min_writer_version: u32,
}

// TODO: macro generate this?
#[test_case("tests/dat/reader_tests/generated/all_primitive_types")]
#[test_case("tests/dat/reader_tests/generated/basic_append")]
#[test_case("tests/dat/reader_tests/generated/basic_partitioned")]
#[test_case("tests/dat/reader_tests/generated/multi_partitioned")]
#[test_case("tests/dat/reader_tests/generated/multi_partitioned_2")]
#[test_case("tests/dat/reader_tests/generated/nested_types")]
#[test_case("tests/dat/reader_tests/generated/no_replay")]
#[test_case("tests/dat/reader_tests/generated/no_stats")]
#[test_case("tests/dat/reader_tests/generated/stats_as_struct")]
#[test_case("tests/dat/reader_tests/generated/with_checkpoint")]
#[test_case("tests/dat/reader_tests/generated/with_schema_change")]
fn reader_test(root_dir: &str) {
    let expected_tvm_path = format!("{}/expected/latest/table_version_metadata.json", root_dir);
    let file = File::open(expected_tvm_path).expect("Oops");
    let reader = BufReader::new(file);
    let expected_tvm: TableVersionMetaData = serde_json::from_reader(reader).unwrap();

    let dt_path = format!("{}/delta", root_dir);
    let storage_client = LocalStorageClient::default();
    let table = DeltaTable::new(&dt_path);
    let snapshot = table.get_latest_snapshot(&storage_client);

    assert_eq!(snapshot.version(), expected_tvm.version);
}
