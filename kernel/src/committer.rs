use std::sync::Arc;

use crate::path::ParsedLogPath;
use crate::{DeltaResult, Engine, EngineDataResultIterator, Error, Version};

use url::Url;

#[derive(Debug)]
pub struct CommitMetadata {
    pub commit_path: ParsedLogPath<Url>,
    pub version: Version,
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
    type Context;

    fn commit(
        &self,
        engine: &dyn Engine,
        actions: EngineDataResultIterator<'_>,
        version: Version,
        context: &Self::Context,
    ) -> DeltaResult<CommitResponse>;
}

pub struct FileSystemCommitter;

impl Committer for FileSystemCommitter {
    type Context = Url; // Table root
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: EngineDataResultIterator<'_>,
        version: Version,
        context: &Self::Context,
    ) -> DeltaResult<CommitResponse> {
        let path = ParsedLogPath::new_commit(context, version)?;
        let json_handler = engine.json_handler();
        match json_handler.write_json_file(&path.location, Box::new(actions), false) {
            Ok(()) => Ok(CommitResponse::Committed { version }),
            Err(Error::FileAlreadyExists(_)) => Ok(CommitResponse::Conflict { version }),
            Err(e) => Err(e),
        }
    }
}
