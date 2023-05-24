use delta_core::{storage::StorageClient, Version};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Clone)]
pub(crate) struct MockStorageClient {
    files: HashMap<PathBuf, Vec<u8>>,
}

impl StorageClient for MockStorageClient {
    fn list(&self, prefix: &str) -> Vec<PathBuf> {
        self.files
            .keys()
            .filter(|p| p.starts_with(prefix))
            .cloned()
            .map(|f| PathBuf::from(f))
            .collect()
    }
    fn read(&self, path: &Path) -> Vec<u8> {
        self.files.get(path).unwrap().to_owned()
    }
}

impl MockStorageClient {
    pub(crate) fn add_commit(&mut self, version: Version, commit: &str) {
        let path = PathBuf::from(format!("_delta_log/{:0>20}.json", version));
        self.files.insert(path, commit.as_bytes().to_vec());
    }
    pub(crate) fn add_data(&mut self, path: PathBuf, data: Vec<u8>) {
        self.files.insert(path, data);
    }
}
