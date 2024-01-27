use std::path::PathBuf;
use std::{fs::DirEntry, time::SystemTime};

use bytes::Bytes;
use itertools::Itertools;
use url::Url;

use crate::{DeltaResult, Error, FileMeta, FileSlice, FileSystemClient};

pub(crate) struct SimpleFilesystemClient;

impl FileSystemClient for SimpleFilesystemClient {
    /// List the paths in the same directory that are lexicographically greater or equal to
    /// (UTF-8 sorting) the given `path`. The result is sorted by the file name.
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        if path.scheme() == "file" {
            let path = path.path();
            let last_slash = path.rfind('/').ok_or(Error::Generic(format!(
                "Invalid path for list_from: {}",
                path
            )))?;
            let all_ents: std::io::Result<Vec<DirEntry>> = std::fs::read_dir(&path[0..last_slash])?
                .sorted_by_key(|ent_res| {
                    ent_res
                        .as_ref()
                        .map(|ent| ent.path())
                        .unwrap_or_else(|_| PathBuf::new())
                })
                .collect();
            let all_ents = all_ents?; // any errors in reading dir entries will force a return here
                                      // now all_ents is a sorted list of DirEntries, we can just map over it
            let it = all_ents.into_iter().map(|ent| {
                ent.metadata()
                    .map_err(|e| Error::IOError(e))
                    .and_then(|metadata| {
                        let last_modified: u64 = metadata
                            .modified()
                            .map(
                                |modified| match modified.duration_since(SystemTime::UNIX_EPOCH) {
                                    Ok(d) => d.as_secs(),
                                    Err(_) => 0,
                                },
                            )
                            .unwrap_or(0);
                        println!("Adding {:#?}", ent);
                        Url::from_file_path(ent.path())
                            .map(|location| FileMeta {
                                location,
                                last_modified: last_modified as i64,
                                size: metadata.len() as usize,
                            })
                            .map_err(|_| Error::Generic(format!("Invalid path: {:?}", ent.path())))
                    })
            });
            Ok(Box::new(it))
        } else {
            Err(Error::Generic("Can only read local filesystem".to_string()))
        }
    }

    /// Read data specified by the start and end offset from the file.
    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        let iter = files.into_iter().map(|(url, _range_opt)| {
            if url.scheme() == "file" {
                let bytes_vec_res = std::fs::read(url.path());
                let bytes: std::io::Result<Bytes> = bytes_vec_res.map(|bytes_vec| bytes_vec.into());
                bytes.map_err(|_| Error::FileNotFound(url.path().to_string()))
            } else {
                Err(Error::Generic("Can only read local filesystem".to_string()))
            }
        });
        Ok(Box::new(iter))
    }
}
