use std::time::SystemTime;

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
            let path = url_path.to_file_path().map_err(|_| {
                Error::Generic(format!("Invalid path for list_from: {:?}", url_path))
            })?;

            let (path_to_read, min_file_name) = if path.is_dir() {
                // passed path is an existing dir, don't strip anything and don't filter the results
                (path, None)
            } else {
                // path doesn't exist, or is not a dir, assume the final part is a filename. strip
                // that and use it as the min_file_name to return
                let parent = path
                    .parent()
                    .ok_or_else(|| {
                        Error::Generic(format!("Invalid path for list_from: {:?}", path))
                    })?
                    .to_path_buf();
                let file_name = path.file_name().ok_or_else(|| {
                    Error::Generic(format!("Invalid path for list_from: {:?}", path))
                })?;
                (parent, Some(file_name))
            };

            let all_ents: Vec<_> = std::fs::read_dir(path_to_read)?
                .filter(|ent_res| {
                    match (ent_res, min_file_name) {
                        (Ok(ent), Some(min_file_name)) => ent.file_name() >= *min_file_name,
                        _ => true, // Keep unfiltered and/or error entries
                    }
                })
                .try_collect()?;
            let it = all_ents
                .into_iter()
                .sorted_by_key(|ent| ent.path())
                .map(|ent| {
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
            Err(Error::generic("Can only read local filesystem"))
        }
    }

    /// Read data specified by the start and end offset from the file.
    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        let iter = files.into_iter().map(|(url, _range_opt)| {
            if url.scheme() == "file" {
                if let Ok(file_path) = url.to_file_path() {
                    let bytes_vec_res = std::fs::read(file_path);
                    let bytes: std::io::Result<Bytes> =
                        bytes_vec_res.map(|bytes_vec| bytes_vec.into());
                    return bytes.map_err(|_| Error::file_not_found(url.path()));
                }
            }
            Err(Error::generic("Can only read local filesystem"))
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

    /// generate json filenames that follow the spec (numbered padded to 20 chars)
    fn get_json_filename(index: usize) -> String {
        format!("{index:020}.json")
    }

    #[test]
    fn test_list_from() -> Result<(), Box<dyn std::error::Error>> {
        let client = SimpleFilesystemClient;
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut expected = vec![];
        for i in 0..3 {
            let path = tmp_dir.path().join(get_json_filename(i));
            expected.push(path.clone());
            let mut f = File::create(path)?;
            writeln!(f, "null")?;
        }
        let url_path = tmp_dir.path().join(get_json_filename(1));
        let url = Url::from_file_path(url_path).unwrap();
        let list = client.list_from(&url)?;
        let mut file_count = 0;
        for (i, file) in list.enumerate() {
            // i+1 in index because we started at 0001 in the listing
            assert_eq!(
                file?.location.to_file_path().unwrap().to_str().unwrap(),
                expected[i + 1].to_str().unwrap()
            );
            file_count += 1;
        }
        assert_eq!(file_count, 2);

        let url_path = tmp_dir.path().join("");
        let url = Url::from_file_path(url_path).unwrap();
        let list = client.list_from(&url)?;
        file_count = list.count();
        assert_eq!(file_count, 3);

        let url_path = tmp_dir.path().join(format!("{:020}", 1));
        let url = Url::from_file_path(url_path).unwrap();
        let list = client.list_from(&url)?;
        file_count = list.count();
        assert_eq!(file_count, 2);
        Ok(())
    }

    #[test]
    fn test_read_files() -> Result<(), Box<dyn std::error::Error>> {
        let client = SimpleFilesystemClient;
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().join(get_json_filename(1));
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
