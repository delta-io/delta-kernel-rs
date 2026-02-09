//! Lazy CRC loading support.
//!
//! Provides thread-safe lazy loading of CRC files, ensuring they are read at most once and the
//! result is shared across all consumers.

use std::sync::{Arc, OnceLock};

use tracing::warn;

use super::{try_read_crc_file, Crc};
use crate::path::ParsedLogPath;
use crate::{Engine, Version};

/// Result of attempting to load a CRC file.
///
/// The "not yet loaded" state is represented by `OnceLock::get()` returning `None`, not as an enum
/// variant.
#[allow(unused)] // TODO: remove after we complete CRC support
#[derive(Debug, Clone)]
pub(crate) enum CrcLoadResult {
    /// No CRC file exists for this log segment.
    DoesNotExist,
    /// CRC file exists but failed to read/parse (corrupted or I/O error).
    CorruptOrFailed,
    /// CRC file was successfully loaded.
    Loaded(Arc<Crc>),
}

#[allow(unused)] // TODO: remove after we complete CRC support
impl CrcLoadResult {
    /// Returns the CRC if successfully loaded.
    pub(crate) fn get(&self) -> Option<&Arc<Crc>> {
        match self {
            CrcLoadResult::Loaded(crc) => Some(crc),
            _ => None,
        }
    }
}

/// Lazy loader for CRC info that ensures it's only read once.
///
/// Uses `OnceLock` to ensure thread-safe initialization that happens at most once.
#[allow(unused)] // TODO: remove after we complete CRC support
#[derive(Debug)]
pub(crate) struct LazyCrc {
    /// The CRC file path, if one exists in the log segment.
    crc_file: Option<ParsedLogPath>,
    /// Cached load result (loaded lazily, at most once).
    cached: OnceLock<CrcLoadResult>,
}

#[allow(unused)] // TODO: remove after we complete CRC support
impl LazyCrc {
    /// Create a new lazy CRC loader.
    ///
    /// If `crc_file` is `None`, the loader will immediately return `DoesNotExist` when accessed.
    pub(crate) fn new(crc_file: Option<ParsedLogPath>) -> Self {
        Self {
            crc_file,
            cached: OnceLock::new(),
        }
    }

    /// Returns the CRC load result, loading if necessary.
    ///
    /// The loading closure is only called once, even across threads. Subsequent calls return the
    /// cached result.
    pub(crate) fn get_or_load(&self, engine: &dyn Engine) -> &CrcLoadResult {
        self.cached.get_or_init(|| match &self.crc_file {
            None => CrcLoadResult::DoesNotExist,
            Some(crc_path) => match try_read_crc_file(engine, crc_path) {
                Ok(crc) => CrcLoadResult::Loaded(Arc::new(crc)),
                Err(e) => {
                    warn!(
                        "Failed to read CRC file {:?}: {}. Falling back to log replay.",
                        crc_path.location.location, e
                    );
                    CrcLoadResult::CorruptOrFailed
                }
            },
        })
    }

    /// Check if CRC has been loaded (without triggering loading).
    pub(crate) fn is_loaded(&self) -> bool {
        self.cached.get().is_some()
    }

    /// Returns the CRC version if a CRC file exists (without loading content).
    ///
    /// This can be used to check if a CRC exists at the snapshot version before deciding whether
    /// to load it.
    pub(crate) fn crc_version(&self) -> Option<Version> {
        self.crc_file.as_ref().map(|f| f.version)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rstest::rstest;

    use super::*;
    use crate::actions::{Metadata, Protocol};
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::{DefaultEngine, DefaultEngineBuilder};
    use object_store::memory::InMemory;

    fn table_root() -> url::Url {
        url::Url::parse("memory:///").unwrap()
    }

    fn test_engine() -> DefaultEngine<TokioBackgroundExecutor> {
        DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()
    }

    // ===== CrcLoadResult Tests =====

    #[test]
    fn test_crc_load_result_loaded() {
        let crc = Crc {
            table_size_bytes: 100,
            num_files: 10,
            num_metadata: 1,
            num_protocol: 1,
            metadata: Metadata::default(),
            protocol: Protocol::default(),
            txn_id: None,
            in_commit_timestamp_opt: None,
            set_transactions: None,
            domain_metadata: None,
            file_size_histogram: None,
            all_files: None,
            num_deleted_records_opt: None,
            num_deletion_vectors_opt: None,
            deleted_record_counts_histogram_opt: None,
        };
        let loaded = CrcLoadResult::Loaded(Arc::new(crc));
        assert!(loaded.get().is_some());
        assert_eq!(loaded.get().unwrap().table_size_bytes, 100);
    }

    #[rstest]
    #[case::does_not_exist(CrcLoadResult::DoesNotExist)]
    #[case::corrupt(CrcLoadResult::CorruptOrFailed)]
    fn test_crc_load_result(#[case] result: CrcLoadResult) {
        assert!(result.get().is_none());
    }

    // ===== LazyCrc Tests =====

    #[test]
    fn test_lazy_crc_no_file() {
        let engine = test_engine();

        let lazy = LazyCrc::new(None);
        assert!(!lazy.is_loaded());
        assert_eq!(lazy.crc_version(), None);

        let result = lazy.get_or_load(&engine);
        assert!(matches!(result, CrcLoadResult::DoesNotExist));
        assert!(result.get().is_none());
        assert!(lazy.is_loaded());
    }

    #[test]
    fn test_lazy_crc_missing_file() {
        let engine = test_engine();

        let lazy = LazyCrc::new(Some(ParsedLogPath::create_parsed_crc(&table_root(), 5)));
        assert!(!lazy.is_loaded());
        assert_eq!(lazy.crc_version(), Some(5));

        let result = lazy.get_or_load(&engine);
        assert!(matches!(result, CrcLoadResult::CorruptOrFailed));
        assert!(result.get().is_none());
        assert!(lazy.is_loaded());
    }

    fn test_table_root(dir: &str) -> url::Url {
        let path = std::fs::canonicalize(PathBuf::from(dir)).unwrap();
        url::Url::from_directory_path(path).unwrap()
    }

    #[test]
    fn test_lazy_crc_loads_real_file() {
        let engine = crate::engine::sync::SyncEngine::new();
        let table_root = test_table_root("./tests/data/crc-full/");

        let lazy = LazyCrc::new(Some(ParsedLogPath::create_parsed_crc(&table_root, 0)));
        assert!(!lazy.is_loaded());
        assert_eq!(lazy.crc_version(), Some(0));

        let result = lazy.get_or_load(&engine);
        assert!(lazy.is_loaded());

        let crc = result.get().unwrap();
        assert_eq!(crc.table_size_bytes, 5259);
    }

    #[test]
    fn test_lazy_crc_malformed_file() {
        let engine = crate::engine::sync::SyncEngine::new();
        let table_root = test_table_root("./tests/data/crc-malformed/");

        let lazy = LazyCrc::new(Some(ParsedLogPath::create_parsed_crc(&table_root, 0)));
        assert!(!lazy.is_loaded());
        assert_eq!(lazy.crc_version(), Some(0));

        let result = lazy.get_or_load(&engine);
        assert!(matches!(result, CrcLoadResult::CorruptOrFailed));
        assert!(result.get().is_none());
        assert!(lazy.is_loaded());
    }
}
