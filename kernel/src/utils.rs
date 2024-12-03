//! Various utility functions/macros used throughout the kernel

/// convenient way to return an error if a condition isn't true
macro_rules! require {
    ( $cond:expr, $err:expr ) => {
        if !($cond) {
            return Err($err);
        }
    };
}

pub(crate) use require;

#[cfg(test)]
pub(crate) mod test_utils {
    use itertools::Itertools;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use serde::Serialize;
    use std::{path::Path, sync::Arc};
    use tempfile::TempDir;
    use test_utils::delta_path_for_version;

    use crate::actions::{Add, Cdc, CommitInfo, Metadata, Protocol, Remove};

    #[derive(Serialize)]
    pub(crate) enum Action {
        #[serde(rename = "add")]
        Add(Add),
        #[serde(rename = "remove")]
        Remove(Remove),
        #[serde(rename = "cdc")]
        Cdc(Cdc),
        #[serde(rename = "metaData")]
        Metadata(Metadata),
        #[serde(rename = "protocol")]
        Protocol(Protocol),
        #[serde(rename = "commitInfo")]
        CommitInfo(CommitInfo),
    }

    impl From<Add> for Action {
        fn from(value: Add) -> Self {
            Self::Add(value)
        }
    }
    impl From<Remove> for Action {
        fn from(value: Remove) -> Self {
            Self::Remove(value)
        }
    }
    impl From<Cdc> for Action {
        fn from(value: Cdc) -> Self {
            Self::Cdc(value)
        }
    }
    impl From<Metadata> for Action {
        fn from(value: Metadata) -> Self {
            Self::Metadata(value)
        }
    }
    impl From<Protocol> for Action {
        fn from(value: Protocol) -> Self {
            Self::Protocol(value)
        }
    }
    impl From<CommitInfo> for Action {
        fn from(value: CommitInfo) -> Self {
            Self::CommitInfo(value)
        }
    }

    pub(crate) struct MockTable {
        commit_num: u64,
        store: Arc<LocalFileSystem>,
        dir: TempDir,
    }

    impl MockTable {
        pub(crate) fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
            Self {
                commit_num: 0,
                store,
                dir,
            }
        }
        pub(crate) async fn commit(&mut self, actions: impl Iterator<Item = Action>) {
            let data = actions
                .map(|action| serde_json::to_string(&action).unwrap())
                .join("\n");

            let path = delta_path_for_version(self.commit_num, "json");
            self.commit_num += 1;

            self.store
                .put(&path, data.into())
                .await
                .expect("put log file in store");
        }
        pub(crate) fn table_root(&self) -> &Path {
            self.dir.path()
        }
    }
}
