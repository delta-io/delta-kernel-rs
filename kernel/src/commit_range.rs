//! Read a contiguous range of commit files from a Delta log.
//!
//! [`CommitRange`] is a thin reader over `[start_version, end_version]` that produces
//! [`CommitDataBatch`]es tagged with the source commit version. It does no deduplication,
//! protocol validation, or schema interpretation; consumers (CDF, table changes, incremental
//! scan, ...) layer their own semantics on top.
//!
//! Two iteration directions:
//! - [`CommitRange::iter`]: forward, ascending commit version (oldest first).
//! - [`CommitRange::iter_rev`]: backward, descending commit version (newest first). Useful for
//!   readers that do `(path, dv_unique_id)` deduplication where the newest action wins.
//!
//! Within a single commit, batches arrive in the engine's natural read order; only the
//! cross-commit ordering changes between `iter` and `iter_rev`.
//!
//! # Example
//!
//! ```rust,no_run
//! # use std::sync::Arc;
//! # use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
//! # use delta_kernel::engine::default::storage::store_from_url;
//! # use delta_kernel::commit_range::CommitRange;
//! # use delta_kernel::schema::SchemaRef;
//! # use delta_kernel::{DeltaResult, Error};
//! # let url = delta_kernel::try_parse_uri("./tests/data/basic_partitioned")?;
//! # let engine = Arc::new(DefaultEngineBuilder::new(store_from_url(&url)?).build());
//! # let log_root = url.join("_delta_log/")?;
//! # let schema: SchemaRef = unimplemented!("the schema of action columns to project");
//! let range = CommitRange::try_new(engine.as_ref(), log_root, 0, Some(1))?;
//! for batch in range.iter_rev(engine.as_ref(), schema) {
//!     let batch = batch?;
//!     // newest commit first; `batch.version` carries the source commit version
//! }
//! # Ok::<(), Error>(())
//! ```

use url::Url;

use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Engine, EngineData, Version};

/// A contiguous range of commit files, `[start_version, end_version]` (both inclusive).
///
/// Construction validates that every commit JSON in the range is present (no gaps); a
/// missing commit -- typically from log retention removing JSONs that a checkpoint already
/// covers -- surfaces as an `Err` from [`CommitRange::try_new`].
pub struct CommitRange {
    log_segment: LogSegment,
    start_version: Version,
}

impl CommitRange {
    /// List commit files in `[start_version, end_version]`. If `end_version` is `None`,
    /// the latest available version is used.
    ///
    /// # Errors
    /// - `start_version > end_version` (caller error).
    /// - Any commit JSON in the requested range is missing.
    /// - Any I/O failure during listing.
    pub fn try_new(
        engine: &dyn Engine,
        log_root: Url,
        start_version: Version,
        end_version: impl Into<Option<Version>>,
    ) -> DeltaResult<Self> {
        let log_segment = LogSegment::for_table_changes(
            engine.storage_handler().as_ref(),
            log_root,
            start_version,
            end_version,
        )?;
        Ok(Self {
            log_segment,
            start_version,
        })
    }

    /// First version covered by this range (inclusive).
    pub fn start_version(&self) -> Version {
        self.start_version
    }

    /// Last version covered by this range (inclusive).
    pub fn end_version(&self) -> Version {
        self.log_segment.end_version
    }

    /// Iterate commits forward (oldest version first). Each yielded [`CommitDataBatch`]
    /// carries the source commit version. Within one commit, batches arrive in the
    /// engine's natural read order.
    pub fn iter<'a>(
        &'a self,
        engine: &'a dyn Engine,
        schema: SchemaRef,
    ) -> impl Iterator<Item = DeltaResult<CommitDataBatch>> + 'a {
        let commits: Vec<ParsedLogPath> = self.log_segment.listed.ascending_commit_files.clone();
        CommitRangeIter::new(engine, schema, commits)
    }

    /// Iterate commits backward (newest version first). Useful for readers that do
    /// `(path, dv_unique_id)` deduplication where the newest action wins.
    ///
    /// Cross-commit ordering is reversed; within a single commit batches arrive in the
    /// engine's natural read order. That ordering does not affect newest-wins dedup
    /// because a single commit cannot contain two actions with the same `(path, dv)`.
    pub fn iter_rev<'a>(
        &'a self,
        engine: &'a dyn Engine,
        schema: SchemaRef,
    ) -> impl Iterator<Item = DeltaResult<CommitDataBatch>> + 'a {
        let commits: Vec<ParsedLogPath> = self
            .log_segment
            .listed
            .ascending_commit_files
            .iter()
            .rev()
            .cloned()
            .collect();
        CommitRangeIter::new(engine, schema, commits)
    }
}

/// One JSON batch read from a commit, tagged with the source commit version.
pub struct CommitDataBatch {
    /// Version of the commit this batch was read from.
    pub version: Version,
    /// Raw action data for this batch. The schema matches what was passed to
    /// [`CommitRange::iter`] / [`CommitRange::iter_rev`].
    pub data: Box<dyn EngineData>,
}

// === Implementation ===

/// Lazy iterator: opens commit JSONs one at a time in the supplied order, yielding each
/// batch tagged with the source commit version.
struct CommitRangeIter<'a> {
    engine: &'a dyn Engine,
    schema: SchemaRef,
    remaining: std::vec::IntoIter<ParsedLogPath>,
    current: Option<CurrentCommit>,
}

struct CurrentCommit {
    version: Version,
    batches: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>,
}

impl<'a> CommitRangeIter<'a> {
    fn new(engine: &'a dyn Engine, schema: SchemaRef, commits: Vec<ParsedLogPath>) -> Self {
        Self {
            engine,
            schema,
            remaining: commits.into_iter(),
            current: None,
        }
    }
}

impl Iterator for CommitRangeIter<'_> {
    type Item = DeltaResult<CommitDataBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.current {
                Some(current) => match current.batches.next() {
                    Some(Ok(data)) => {
                        let version = current.version;
                        return Some(Ok(CommitDataBatch { version, data }));
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => self.current = None,
                },
                None => {
                    let commit = self.remaining.next()?;
                    match self.engine.json_handler().read_json_files(
                        std::slice::from_ref(&commit.location),
                        self.schema.clone(),
                        None,
                    ) {
                        Ok(batches) => {
                            self.current = Some(CurrentCommit {
                                version: commit.version,
                                batches,
                            });
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::*;
    use crate::actions::get_commit_schema;
    use crate::engine::sync::SyncEngine;

    fn basic_partitioned_log_root() -> Url {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
        let table_url = url::Url::from_directory_path(path).unwrap();
        table_url.join("_delta_log/").unwrap()
    }

    #[test]
    fn iter_forward_yields_oldest_first() {
        let engine = Arc::new(SyncEngine::new());
        let range = CommitRange::try_new(engine.as_ref(), basic_partitioned_log_root(), 0, Some(1))
            .unwrap();
        assert_eq!(range.start_version(), 0);
        assert_eq!(range.end_version(), 1);

        let versions: Vec<Version> = range
            .iter(engine.as_ref(), get_commit_schema().clone())
            .map(|b| b.unwrap().version)
            .collect();
        assert_eq!(versions, vec![0, 1]);
    }

    #[test]
    fn iter_rev_yields_newest_first() {
        let engine = Arc::new(SyncEngine::new());
        let range = CommitRange::try_new(engine.as_ref(), basic_partitioned_log_root(), 0, Some(1))
            .unwrap();

        let versions: Vec<Version> = range
            .iter_rev(engine.as_ref(), get_commit_schema().clone())
            .map(|b| b.unwrap().version)
            .collect();
        assert_eq!(versions, vec![1, 0]);
    }

    #[test]
    fn missing_commit_is_an_error() {
        let engine = Arc::new(SyncEngine::new());
        // Range starting past the last commit -> listing fails.
        let res = CommitRange::try_new(engine.as_ref(), basic_partitioned_log_root(), 999, None);
        assert!(res.is_err());
    }
}
