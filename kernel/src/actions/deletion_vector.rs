//! Code relating to parsing and using deletion vectors

use std::{
    io::{Cursor, Read},
    sync::Arc,
};

use roaring::RoaringTreemap;
use url::Url;

use crate::{DeltaResult, Error, FileSystemClient};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionVectorDescriptor {
    /// A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'].
    pub storage_type: String,

    /// Three format options are currently proposed:
    /// - If `storageType = 'u'` then `<random prefix - optional><base85 encoded uuid>`:
    ///   The deletion vector is stored in a file with a path relative to the data
    ///   directory of this Delta table, and the file name can be reconstructed from
    ///   the UUID. See Derived Fields for how to reconstruct the file name. The random
    ///   prefix is recovered as the extra characters before the (20 characters fixed length) uuid.
    /// - If `storageType = 'i'` then `<base85 encoded bytes>`: The deletion vector
    ///   is stored inline in the log. The format used is the `RoaringBitmapArray`
    ///   format also used when the DV is stored on disk and described in [Deletion Vector Format].
    /// - If `storageType = 'p'` then `<absolute path>`: The DV is stored in a file with an
    ///   absolute path given by this path, which has the same format as the `path` field
    ///   in the `add`/`remove` actions.
    ///
    /// [Deletion Vector Format]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vector-Format
    pub path_or_inline_dv: String,

    /// Start of the data for this DV in number of bytes from the beginning of the file it is stored in.
    /// Always None (absent in JSON) when `storageType = 'i'`.
    pub offset: Option<i32>,

    /// Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
    pub size_in_bytes: i32,

    /// Number of rows the given DV logically removes from the file.
    pub cardinality: i64,
}

impl DeletionVectorDescriptor {
    pub fn unique_id(&self) -> String {
        if let Some(offset) = self.offset {
            format!("{}{}@{offset}", self.storage_type, self.path_or_inline_dv)
        } else {
            format!("{}{}", self.storage_type, self.path_or_inline_dv)
        }
    }

    pub fn absolute_path(&self, parent: &Url) -> DeltaResult<Option<Url>> {
        match self.storage_type.as_str() {
            "u" => {
                let prefix_len = self.path_or_inline_dv.len() as i32 - 20;
                if prefix_len < 0 {
                    return Err(Error::DeletionVector("Invalid length".to_string()));
                }
                let decoded = z85::decode(&self.path_or_inline_dv[(prefix_len as usize)..])
                    .map_err(|_| Error::DeletionVector("Failed to decode DV uuid".to_string()))?;
                let uuid = uuid::Uuid::from_slice(&decoded)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;
                let dv_suffix = if prefix_len > 0 {
                    format!(
                        "{}/deletion_vector_{uuid}.bin",
                        &self.path_or_inline_dv[..(prefix_len as usize)]
                    )
                } else {
                    format!("deletion_vector_{uuid}.bin")
                };
                let dv_path = parent
                    .join(&dv_suffix)
                    .map_err(|_| Error::DeletionVector(format!("invalid path: {dv_suffix}")))?;
                Ok(Some(dv_path))
            }
            "p" => Ok(Some(Url::parse(&self.path_or_inline_dv).map_err(|_| {
                Error::DeletionVector(format!("invalid path: {}", self.path_or_inline_dv))
            })?)),
            "i" => Ok(None),
            other => Err(Error::DeletionVector(format!(
                "Unknown storage format: '{other}'."
            ))),
        }
    }

    pub fn read(
        &self,
        fs_client: Arc<dyn FileSystemClient>,
        parent: Url,
    ) -> DeltaResult<RoaringTreemap> {
        match self.absolute_path(&parent)? {
            None => {
                let bytes = z85::decode(&self.path_or_inline_dv)
                    .map_err(|_| Error::DeletionVector("Failed to decode DV".to_string()))?;
                RoaringTreemap::deserialize_from(&bytes[12..])
                    .map_err(|err| Error::DeletionVector(err.to_string()))
            }
            Some(path) => {
                let offset = self.offset;
                let size_in_bytes = self.size_in_bytes;

                let dv_data = fs_client
                    .read_files(vec![(path, None)])?
                    .next()
                    .ok_or(Error::MissingData("No deletion Vector data".to_string()))??;

                let mut cursor = Cursor::new(dv_data);
                if let Some(offset) = offset {
                    // TODO should we read the datasize from the DV file?
                    // offset plus datasize bytes
                    cursor.set_position((offset + 4) as u64);
                }

                let mut buf = vec![0; 4];
                cursor
                    .read(&mut buf)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;
                let magic =
                    i32::from_le_bytes(buf.try_into().map_err(|_| {
                        Error::DeletionVector("filed to read magic bytes".to_string())
                    })?);
                if magic != 1681511377 {
                    return Err(Error::DeletionVector(format!("Invalid magic {magic}")));
                }

                let mut buf = vec![0; size_in_bytes as usize];
                cursor
                    .read(&mut buf)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;

                RoaringTreemap::deserialize_from(Cursor::new(buf))
                    .map_err(|err| Error::DeletionVector(err.to_string()))
            }
        }
    }
}

pub(crate) fn treemap_to_bools(treemap: RoaringTreemap) -> Vec<bool> {
    fn combine(high_bits: u32, low_bits: u32) -> usize {
        ((u64::from(high_bits) << 32) | u64::from(low_bits)) as usize
    }

    match treemap.max() {
        Some(max) => {
            // there are values in the map
            //TODO(nick) panic if max is > MAX_USIZE
            let mut result = vec![true; max as usize + 1];
            let bitmaps = treemap.bitmaps();
            for (index, bitmap) in bitmaps {
                for bit in bitmap.iter() {
                    let vec_index = combine(index, bit);
                    result[vec_index] = false;
                }
            }
            result
        }
        None => {
            // empty set, return empty vec
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use roaring::RoaringTreemap;
    use std::path::PathBuf;

    use super::*;
    use crate::{simple_client::SimpleClient, EngineClient};

    use super::DeletionVectorDescriptor;

    fn dv_relateive() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_absolute() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "p".to_string(),
            path_or_inline_dv:
                "s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_inline() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "i".to_string(),
            path_or_inline_dv: "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L".to_string(),
            offset: None,
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_example() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        }
    }

    #[test]
    fn test_deletion_vector_absolute_path() {
        let parent = Url::parse("s3://mytable/").unwrap();

        let relative = dv_relateive();
        let expected =
            Url::parse("s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin")
                .unwrap();
        assert_eq!(expected, relative.absolute_path(&parent).unwrap().unwrap());

        let absolute = dv_absolute();
        let expected =
            Url::parse("s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin")
                .unwrap();
        assert_eq!(expected, absolute.absolute_path(&parent).unwrap().unwrap());

        let inline = dv_inline();
        assert_eq!(None, inline.absolute_path(&parent).unwrap());

        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let parent = url::Url::from_directory_path(path).unwrap();
        let dv_url = parent
            .join("deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin")
            .unwrap();
        let example = dv_example();
        assert_eq!(dv_url, example.absolute_path(&parent).unwrap().unwrap());
    }

    #[test]
    fn test_deletion_vector_read() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let parent = url::Url::from_directory_path(path).unwrap();
        let simple_client = SimpleClient::new();
        let fs_client = simple_client.get_file_system_client();

        let example = dv_example();
        let tree_map = example.read(fs_client, parent).unwrap();

        let expected: Vec<u64> = vec![0, 9];
        let found = tree_map.iter().collect::<Vec<_>>();
        assert_eq!(found, expected)
    }

    // this test is ignored by default as it's expensive to allocate such big vecs full of `true`. you can run it via:
    // cargo test actions::action_definitions::tests::test_dv_to_bools
    #[test]
    #[ignore]
    fn test_dv_to_bools() {
        let mut rb = RoaringTreemap::new();
        rb.insert(0);
        rb.insert(2);
        rb.insert(7);
        rb.insert(30854);
        rb.insert(4294967297);
        rb.insert(4294967300);
        let bools = super::treemap_to_bools(rb);
        let mut expected = vec![true; 4294967301];
        expected[0] = false;
        expected[2] = false;
        expected[7] = false;
        expected[30854] = false;
        expected[4294967297] = false;
        expected[4294967300] = false;
        assert_eq!(bools, expected);
    }
}
