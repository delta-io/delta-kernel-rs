//! UCCatalog implements a high-level interface for interacting with Delta Tables in Unity Catalog.

use std::sync::Arc;

use delta_kernel::{Engine, LogPath, Snapshot, Version};

use uc_client::models::{Commit, CommitRequest};
use uc_client::prelude::*;

use itertools::Itertools;
use tracing::info;
use url::Url;

/// The [UCCatalog] provides a high-level interface to interact with Delta Tables stored in Unity
/// Catalog.
pub struct UCCatalog<'a> {
    client: &'a UCClient,
}

impl<'a> UCCatalog<'a> {
    pub fn new(client: &'a UCClient) -> Self {
        UCCatalog { client }
    }

    pub async fn load_snapshot(
        &self,
        table_id: &str,
        table_uri: &str,
        engine: &dyn Engine,
    ) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
        self.load_snapshot_inner(table_id, table_uri, None, engine)
            .await
    }

    pub async fn load_snapshot_at(
        &self,
        table_id: &str,
        table_uri: &str,
        version: Version,
        engine: &dyn Engine,
    ) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
        self.load_snapshot_inner(table_id, table_uri, Some(version), engine)
            .await
    }

    pub(crate) async fn load_snapshot_inner(
        &self,
        table_id: &str,
        table_uri: &str,
        version: Option<Version>,
        engine: &dyn Engine,
    ) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
        let table_uri = table_uri.to_string();
        let req = CommitsRequest {
            table_id: table_id.to_string(),
            table_uri: table_uri.clone(),
            start_version: Some(0),
            end_version: version.and_then(|v| v.try_into().ok()),
        };
        // TODO: does it paginate?
        let commits = self.client.get_commits(req).await?;

        // sort them
        let mut commits = commits;
        if let Some(c) = commits.commits.as_mut() {
            c.sort_by_key(|c| c.version)
        }

        // if commits are present, we ensure they are sorted+contiguous
        if let Some(commits) = &commits.commits {
            if !commits.windows(2).all(|w| w[1].version == w[0].version + 1) {
                return Err("Received non-contiguous commit versions".into());
            }
        }

        // we always get back the latest version from commits response, and pass that in to
        // kernel's Snapshot builder. basically, load_table for the latest version always looks
        // like a time travel query since we know the latest version ahead of time.
        //
        // note there is a weird edge case: if the table was just created it will return
        // latest_table_version = -1, but the 0.json will exist in the _delta_log.
        let version: Version = match version {
            Some(v) => v,
            None => match commits.latest_table_version {
                -1 => 0,
                i => i.try_into()?,
            },
        };

        // consume uc-client's Commit and hand back a delta_kernel LogPath
        let table_url = Url::parse(&table_uri)?;
        let commits: Vec<_> = commits
            .commits
            .unwrap_or_default()
            .into_iter()
            .map(|c| -> Result<LogPath, Box<dyn std::error::Error>> {
                LogPath::staged_commit(
                    table_url.clone(),
                    &c.file_name,
                    c.file_modification_timestamp,
                    c.file_size.try_into()?,
                )
                .map_err(|e| e.into())
            })
            .try_collect()?;

        info!("commits for kernel: {:?}\n", commits);

        Snapshot::builder_for(Url::parse(&(table_uri + "/"))?)
            .at_version(version)
            .with_log_tail(commits)
            .build(engine)
            .map_err(|e| e.into())
    }
}

/// A [UCCommitter] is a Unity Catalog [Committer] implementation for committing to delta tables in
/// UC.
pub struct UCCommitter {
    client: Arc<UCClient>,
}

impl UCCommitter {
    pub fn new(client: Arc<UCClient>) -> Self {
        UCCommitter { client }
    }
}

use delta_kernel::committer::Committer;
use delta_kernel::EngineDataResultIterator;

impl Committer for UCCommitter {
    fn commit<'a>(
        &self,
        engine: &'a dyn Engine,
        actions: EngineDataResultIterator<'a>,
        commit_metadata: delta_kernel::committer::CommitMetadata,
    ) -> delta_kernel::DeltaResult<delta_kernel::committer::CommitResponse> {
        // let commit_req = CommitRequest::new(
        //     commit_metadata.table_id,
        //     commit_metadata.table_uri,
        //     Commit::new(
        //         commit_metadata.version as i64,
        //         commit_metadata.timestamp,
        //         format!("{}.json", commit_metadata.version),
        //         123,
        //         123456789,
        //     ),
        // );
        // self.client.commit(commit_req) // ASYNC!
        todo!()
    }

    fn published(
        &self,
        version: Version,
        context: &UCCommitterContext,
    ) -> delta_kernel::DeltaResult<()> {
        let request =
            CommitRequest::ack_publish(&context.table_id, &context.table_uri, version as i64);
        let handle = tokio::runtime::Handle::current();
        tokio::task::block_in_place(move || {
            handle.block_on(async move {
                match self.client.commit(request).await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(delta_kernel::Error::Generic(format!(
                        "publish_commit failed: {e}"
                    ))),
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
    use delta_kernel::engine::default::DefaultEngine;
    use delta_kernel::transaction::CommitResult;
    use object_store::ObjectStore;

    use super::*;

    // We could just re-export UCClient's get_table to not require consumers to directly import
    // uc_client themselves.
    async fn get_table(
        client: &UCClient,
        table_name: &str,
    ) -> Result<(String, String), Box<dyn std::error::Error>> {
        let res = client.get_table(table_name).await?;
        let table_id = res.table_id;
        let table_uri = res.storage_location;

        info!(
            "[GET TABLE] got table_id: {}, table_uri: {}\n",
            table_id, table_uri
        );

        Ok((table_id, table_uri))
    }

    // ignored test which you can run manually to play around with reading a UC table. run with:
    // `ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo t read_uc_table --nocapture -- --ignored`
    #[ignore]
    #[tokio::test]
    async fn read_uc_table() -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = env::var("ENDPOINT").expect("ENDPOINT environment variable not set");
        let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        let table_name = env::var("TABLENAME").expect("TABLENAME environment variable not set");

        // build UC client, get table info and credentials
        let client = UCClient::builder(endpoint, &token).build()?;
        let (table_id, table_uri) = get_table(&client, &table_name).await?;
        let creds = client
            .get_credentials(&table_id, Operation::Read)
            .await
            .map_err(|e| format!("Failed to get credentials: {}", e))?;

        // build catalog
        let catalog = UCCatalog::new(&client);

        // TODO: support non-AWS
        let creds = creds
            .aws_temp_credentials
            .ok_or("No AWS temporary credentials found")?;

        let options = [
            ("region", "us-west-2"),
            ("access_key_id", &creds.access_key_id),
            ("secret_access_key", &creds.secret_access_key),
            ("session_token", &creds.session_token),
        ];

        let table_url = Url::parse(&table_uri)?;
        let (store, path) = object_store::parse_url_opts(&table_url, options)?;
        let store: Arc<_> = store.into();

        info!("created object store: {:?}\npath: {:?}\n", store, path);

        let engine = DefaultEngine::new(store, Arc::new(TokioBackgroundExecutor::new()));

        // read table
        let snapshot = catalog
            .load_snapshot(&table_id, &table_uri, &engine)
            .await?;
        // or time travel
        // let snapshot = catalog.load_snapshot_at(&table, 2).await?;

        println!("🎉 loaded snapshot: {snapshot:?}");

        Ok(())
    }

    // ignored test which you can run manually to play around with writing to a UC table. run with:
    // `ENDPOINT=".." TABLENAME=".." TOKEN=".." cargo t write_uc_table --nocapture -- --ignored`
    #[ignore]
    #[tokio::test]
    async fn write_uc_table() -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = env::var("ENDPOINT").expect("ENDPOINT environment variable not set");
        let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        let table_name = env::var("TABLENAME").expect("TABLENAME environment variable not set");

        // build UC client, get table info and credentials
        let client = Arc::new(UCClient::builder(endpoint, &token).build()?);
        let (table_id, table_uri) = get_table(&client, &table_name).await?;
        let creds = client
            .get_credentials(&table_id, Operation::ReadWrite)
            .await
            .map_err(|e| format!("Failed to get credentials: {}", e))?;

        // build catalog
        let catalog = UCCatalog::new(&client);

        // TODO: support non-AWS
        let creds = creds
            .aws_temp_credentials
            .ok_or("No AWS temporary credentials found")?;

        let options = [
            ("region", "us-west-2"),
            ("access_key_id", &creds.access_key_id),
            ("secret_access_key", &creds.secret_access_key),
            ("session_token", &creds.session_token),
        ];

        let table_url = Url::parse(&table_uri)?;
        let (store, path) = object_store::parse_url_opts(&table_url, options)?;
        let store: Arc<dyn ObjectStore> = store.into();

        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));
        let committer = Arc::new(UCCommitter::new(client.clone()));
        let snapshot = catalog
            .load_snapshot(&table_id, &table_uri, &engine)
            .await?;
        println!("latest snapshot version: {:?}", snapshot.version());
        let txn = snapshot
            .clone()
            .transaction()?
            .with_committer(committer as Arc<dyn Committer>);
        let _write_context = txn.get_write_context();
        // do a write.

        let last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64;
        let size: i64 = 1500;
        let uuid = uuid::Uuid::new_v4();
        let version = snapshot.version() + 1;
        let filename = format!("{version:020}.{uuid}.json");
        let mut commit_path = table_url.clone();
        commit_path.path_segments_mut().unwrap().extend(&[
            "_delta_log",
            "_staged_commits",
            &filename,
        ]);

        // commit info only
        let actions = txn.hack_actions(&engine);
        engine
            .json_handler()
            .write_json_file(&commit_path, actions, false)?;

        // print the log
        use futures::stream::StreamExt;
        let mut stream = store.list(Some(&path));
        while let Some(path) = stream.next().await {
            println!("object: {:?}", path.unwrap().location);
        }

        let commit_req = CommitRequest::new(
            table_id,
            table_uri,
            Commit::new(
                version.try_into().unwrap(),
                last_modified,
                filename,
                size,
                last_modified,
            ),
        );
        client.commit(commit_req).await?;

        // match txn.commit(&engine)? {
        //     CommitResult::CommittedTransaction(t) => {
        //         println!("🎉 committed version {}", t.version());
        //     }
        //     CommitResult::ConflictedTransaction(t) => {
        //         println!("💥 commit conflicted at version {}", t.conflict_version);
        //     }
        //     CommitResult::RetryableTransaction(_) => {
        //         println!("we should retry...");
        //     }
        // }
        // let commit = store
        //     .get(&object_store::path::Path::from("19a85dee-54bc-43a2-87ab-023d0ec16013/tables/cdac4b37-fc11-4148-ae02-512e6cc4e1bd/_delta_log/_staged_commits/00000000000000000001.5400fd23-8e08-4f87-8049-7b37431fb826.json"))
        //     .await
        //     .unwrap();
        // let bytes = commit.bytes().await?;
        // println!("commit file contents:\n{}", String::from_utf8_lossy(&bytes));

        // let ctx = UCCommitterContext {
        //     table_id: table_id.clone(),
        //     table_uri: table_uri.clone(),
        // };
        // let t = match txn.commit(&engine, ctx)? {
        //     CommitResult::CommittedTransaction(t) => {
        //         println!("🎉 committed version {}", t.version());
        //         Some(t)
        //     }
        //     CommitResult::ConflictedTransaction(t) => {
        //         println!("💥 commit conflicted at version {}", t.conflict_version);
        //         None
        //     }
        //     CommitResult::RetryableTransaction(_) => {
        //         println!("we should retry...");
        //         None
        //     }
        // };

        // // FIXME! need to plumb log_tail
        // // let snapshot = t.unwrap().post_commit_snapshot(&engine)?;
        // let new_version = snapshot.version() + 1;
        // let snapshot = catalog
        //     .load_snapshot_at(&table_id, &table_uri, new_version, &engine)
        //     .await?;
        // let _published = Arc::into_inner(snapshot).unwrap().publish(&engine)?;

        // // notify UC of publish. since Commmitter is tied to Transaction it's a bit hard to plumb
        // // this through to Snapshot... TODO
        // let request = CommitRequest::ack_publish(&table_id, &table_uri, new_version as i64);
        // client.commit(request).await?;

        Ok(())
    }
}
