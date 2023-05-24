use std::path::{Path, PathBuf};

/// trait encapsulating all storage interaction
// TODO: don't use Path(Buf), don't use Vec<u8>
pub trait StorageClient {
    fn list(&self, prefix: &str) -> Vec<PathBuf>;
    fn read(&self, path: &Path) -> Vec<u8>;
}
