use std::sync::Arc;

use crate::path::ParsedLogPath;
use crate::{DeltaResult, Engine, EngineDataResultIterator, Error, Version};

use url::Url;

#[derive(Debug)]
pub struct CommitMetadata {
    pub(crate) commit_path: ParsedLogPath<Url>,
    pub(crate) version: Version,
}

impl CommitMetadata {
    pub(crate) fn new(commit_path: ParsedLogPath<Url>, version: Version) -> Self {
        Self {
            commit_path,
            version,
        }
    }
}

#[derive(Debug)]
/// Result of committing a transaction.
pub enum CommitResponse {
    Committed { version: Version },
    Conflict { version: Version },
}

pub trait Committer: Send + Sync {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: EngineDataResultIterator<'_>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse>;
}

pub(crate) struct FileSystemCommitter;

impl FileSystemCommitter {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

impl Committer for FileSystemCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: EngineDataResultIterator<'_>,
        commit_metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        let json_handler = engine.json_handler();
        match json_handler.write_json_file(
            &commit_metadata.commit_path.location,
            Box::new(actions),
            false,
        ) {
            Ok(()) => Ok(CommitResponse::Committed {
                version: commit_metadata.version,
            }),
            Err(Error::FileAlreadyExists(_)) => Ok(CommitResponse::Conflict {
                version: commit_metadata.version,
            }),
            Err(e) => Err(e),
        }
    }
}
