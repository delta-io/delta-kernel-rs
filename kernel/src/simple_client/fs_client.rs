use std::path::{Path, PathBuf};
use std::{fs, time::SystemTime};

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
        url_path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
        if url_path.scheme() == "file" {
            let path = Path::new(url_path.path());
            let (path_to_read, min_file_name) = if path.is_dir() {
                // passed path is an existing dir, don't strip anything and don't filter the results
                (path, None)
            } else {
                // path doesn't exist, or is not a dir, assume the final part is a filename. strip
                // that and use it as the min_file_name to return
                let parent = path.parent().ok_or_else(|| {
                    Error::Generic(format!("Invalid path for list_from: {:?}", path))
                })?;
                let file_name = path.file_name().ok_or_else(|| {
                    Error::Generic(format!("Invalid path for list_from: {:?}", path))
                })?;
                (parent, Some(file_name))
            };

            let all_ents: std::io::Result<Vec<fs::DirEntry>> = std::fs::read_dir(path_to_read)?
                .sorted_by_key(|ent_res| {
                    ent_res
                        .as_ref()
                        .map(|ent| ent.path())
                        .unwrap_or_else(|_| PathBuf::new())
                })
                .filter(|ent_res| {
                    match ent_res {
                        Ok(ent) => {
                            if let Some(min_file_name) = min_file_name {
                                ent.file_name() >= *min_file_name
                            } else {
                                true
                            }
                        }
                        Err(_) => true, // keep errors so line below will return them
                    }
                })
                .collect();
            let all_ents = all_ents?; // any errors in reading dir entries will force a return here
                                      // now all_ents is a sorted list of DirEntries, we can just map over it

            let it = all_ents.into_iter().map(|ent| {
                ent.metadata().map_err(Error::IOError).and_then(|metadata| {
                    let last_modified: u64 = metadata
                        .modified()
                        .map(
                            |modified| match modified.duration_since(SystemTime::UNIX_EPOCH) {
                                Ok(d) => d.as_secs(),
                                Err(_) => 0,
                            },
                        )
                        .unwrap_or(0);
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

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use bytes::{BufMut, BytesMut};
    use url::Url;

    use super::SimpleFilesystemClient;
    use crate::FileSystemClient;

    #[test]
    fn test_list_from() -> Result<(), Box<dyn std::error::Error>> {
        let client = SimpleFilesystemClient;
        let tmp_dir = tempfile::tempdir().unwrap();
        for i in 0..3 {
            let path = tmp_dir.path().join(format!("000{i}.json"));
            let mut f = File::create(path)?;
            writeln!(f, "null")?;
        }
        let url_path = tmp_dir.path().join("0001.json");
        let url = Url::from_file_path(url_path).unwrap();
        let list = client.list_from(&url)?;
        let mut file_count = 0;
        for _ in list {
            file_count += 1;
        }
        assert_eq!(file_count, 2);

        let url_path = tmp_dir.path().join("");
        let url = Url::from_file_path(url_path).unwrap();
        let list = client.list_from(&url)?;
        file_count = 0;
        for _ in list {
            file_count += 1;
        }
        assert_eq!(file_count, 3);

        let url_path = tmp_dir.path().join("0001");
        let url = Url::from_file_path(url_path).unwrap();
        let list = client.list_from(&url)?;
        file_count = 0;
        for _ in list {
            file_count += 1;
        }
        assert_eq!(file_count, 2);
        Ok(())
    }

    #[test]
    fn test_read_files() -> Result<(), Box<dyn std::error::Error>> {
        let client = SimpleFilesystemClient;
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().join("0001.json");
        let mut f = File::create(path.clone())?;
        writeln!(f, "null")?;
        let url = Url::from_file_path(path).unwrap();
        let file_slice = (url.clone(), None);
        let read = client.read_files(vec![file_slice])?;
        let mut file_count = 0;
        let mut buf = BytesMut::with_capacity(16);
        buf.put(&b"null\n"[..]);
        let a = buf.split();
        for result in read {
            let result = result?;
            assert_eq!(result, a);
            file_count += 1;
        }
        assert_eq!(file_count, 1);
        Ok(())
    }
}
