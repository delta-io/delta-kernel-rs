//! Lazy CRC loading helper for the protocol+metadata replay shortcut.
//!
//! Used internally by [`LogSegment::read_protocol_metadata_opt`](crate::log_segment::LogSegment)
//! to read a CRC file at most once and skip log replay if Protocol/Metadata are present.
//! The `Snapshot`'s eager `Arc<Crc>` (constructed during snapshot load) is unrelated to
//! this helper -- this exists solely as a "transient cache" for the P&M shortcut path.
//!
//! Once the eager-CRC + reverse-replay accumulator handles all snapshot-load paths, this
//! helper can be folded into the P&M-replay path or removed entirely. Until then, it's
//! the minimal surface needed to keep that path working.

use std::sync::{Arc, OnceLock};

use tracing::warn;

use super::{try_read_crc_file, Crc};
use crate::path::ParsedLogPath;
use crate::{Engine, Version};

/// Result of attempting to load a CRC file.
///
/// "Not yet loaded" is represented by `OnceLock::get()` returning `None`, not as an enum
/// variant.
#[derive(Debug, Clone)]
pub(crate) enum CrcLoadResult {
    /// No CRC file exists for this log segment.
    DoesNotExist,
    /// CRC file exists but failed to read/parse (corrupted or I/O error).
    CorruptOrFailed,
    /// CRC file was successfully loaded.
    Loaded(Arc<Crc>),
}

/// Lazy loader for a CRC file. Ensures the file is read at most once and the result is
/// cached for subsequent accesses.
#[derive(Debug)]
pub(crate) struct LazyCrc {
    /// The CRC file path, if one exists in the log segment.
    crc_file: Option<ParsedLogPath>,
    /// Cached load result (loaded lazily, at most once).
    cached: OnceLock<CrcLoadResult>,
}

impl LazyCrc {
    /// Create a new lazy CRC loader. If `crc_file` is `None`, the loader will return
    /// [`CrcLoadResult::DoesNotExist`] on access without any I/O.
    pub(crate) fn new(crc_file: Option<ParsedLogPath>) -> Self {
        Self {
            crc_file,
            cached: OnceLock::new(),
        }
    }

    /// Returns the CRC load result, loading from storage if not yet cached. The loading
    /// closure runs at most once across all callers.
    pub(crate) fn get_or_load(&self, engine: &dyn Engine) -> &CrcLoadResult {
        self.cached.get_or_init(|| match &self.crc_file {
            None => CrcLoadResult::DoesNotExist,
            Some(crc_path) => match try_read_crc_file(engine, crc_path) {
                Ok(crc) => CrcLoadResult::Loaded(Arc::new(crc)),
                Err(e) => {
                    warn!(
                        "Failed to read CRC file {:?}: {}.",
                        crc_path.location.location, e
                    );
                    CrcLoadResult::CorruptOrFailed
                }
            },
        })
    }

    /// Returns the version of the CRC file backing this loader, if any. No I/O.
    pub(crate) fn crc_version(&self) -> Option<Version> {
        self.crc_file.as_ref().map(|f| f.version)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::crc::FileStatsState;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::{DefaultEngine, DefaultEngineBuilder};
    use crate::object_store::memory::InMemory;

    fn table_root() -> url::Url {
        url::Url::parse("memory:///").unwrap()
    }

    fn test_engine() -> DefaultEngine<TokioBackgroundExecutor> {
        DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()
    }

    fn test_table_root(dir: &str) -> url::Url {
        let path = std::fs::canonicalize(PathBuf::from(dir)).unwrap();
        url::Url::from_directory_path(path).unwrap()
    }

    #[test]
    fn lazy_crc_with_no_file_returns_does_not_exist() {
        let engine = test_engine();
        let lazy = LazyCrc::new(None);
        assert_eq!(lazy.crc_version(), None);

        let result = lazy.get_or_load(&engine);
        assert!(matches!(result, CrcLoadResult::DoesNotExist));
    }

    #[test]
    fn lazy_crc_with_missing_file_path_returns_corrupt_or_failed() {
        let engine = test_engine();
        let lazy = LazyCrc::new(Some(ParsedLogPath::create_parsed_crc(&table_root(), 5)));
        assert_eq!(lazy.crc_version(), Some(5));

        let result = lazy.get_or_load(&engine);
        assert!(matches!(result, CrcLoadResult::CorruptOrFailed));
    }

    #[test]
    fn lazy_crc_loads_real_file() {
        let engine = crate::engine::sync::SyncEngine::new();
        let table_root = test_table_root("./tests/data/crc-full/");
        let lazy = LazyCrc::new(Some(ParsedLogPath::create_parsed_crc(&table_root, 0)));
        assert_eq!(lazy.crc_version(), Some(0));

        let result = lazy.get_or_load(&engine);
        let CrcLoadResult::Loaded(crc) = result else {
            panic!("expected Loaded, got {result:?}");
        };
        assert_eq!(crc.file_stats().unwrap().table_size_bytes(), 5259);
    }

    #[test]
    fn lazy_crc_with_malformed_file_returns_corrupt_or_failed() {
        let engine = crate::engine::sync::SyncEngine::new();
        let table_root = test_table_root("./tests/data/crc-malformed/");
        let lazy = LazyCrc::new(Some(ParsedLogPath::create_parsed_crc(&table_root, 0)));

        let result = lazy.get_or_load(&engine);
        assert!(matches!(result, CrcLoadResult::CorruptOrFailed));
    }

    #[test]
    fn lazy_crc_caches_result_across_calls() {
        // Build a CRC with a real file behind it. Calling get_or_load twice must return
        // the same Loaded result without doing the I/O twice (covered indirectly: a
        // second call doesn't error even if the file vanished, because the result was
        // cached on the first call).
        let engine = crate::engine::sync::SyncEngine::new();
        let table_root = test_table_root("./tests/data/crc-full/");
        let lazy = LazyCrc::new(Some(ParsedLogPath::create_parsed_crc(&table_root, 0)));
        let first = lazy.get_or_load(&engine);
        let second = lazy.get_or_load(&engine);
        // OnceLock returns the same reference both times.
        assert!(std::ptr::eq(first, second));

        // Sanity: still Loaded.
        let CrcLoadResult::Loaded(crc) = first else {
            panic!("expected Loaded");
        };
        assert!(matches!(crc.file_stats, FileStatsState::Valid { .. }));
    }
}
